"""Common utilities and core abstractions used throughout Moonraker.

This module provides foundational types and helpers used by the Moonraker
server and its components, including JSON-RPC handling, API definition
helpers, event loop wrappers, and job history trackers.

The contents are intentionally lightweight and focused on runtime
integration: enums for request and transport types, `APIDefinition` for
registering HTTP/RPC endpoints, `JsonRPC` for dispatching and handling
JSON-RPC messages, `EventLoop` and `FlexTimer` wrappers for asyncio loop
integration, and a set of field tracker implementations used by the
job history subsystem.

This module is imported by many other parts of the project; keep
backwards-compatible signatures and avoid heavy dependencies here.
"""

# Common classes used throughout Moonraker
#
# Copyright (C) 2023 Eric Callahan <arksine.code@gmail.com>
#
# This file may be distributed under the terms of the GNU GPLv3 license

from __future__ import annotations

import asyncio
import contextlib
import copy
import dataclasses
import functools
import inspect
import logging
import os
import re
import socket
import time
from abc import ABCMeta, abstractmethod
from collections.abc import Awaitable, Callable
from enum import Enum, Flag, auto

# Annotation imports
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Self,
    TypeVar,
)

from .utils import Sentinel
from .utils import json_wrapper as jsonw
from .utils.exceptions import AgentError, ServerError

# Type aliases for JSON-RPC and common data structures
JSONValue = str | int | float | bool | None | dict[str, Any] | list[Any]
JSONDict = dict[str, JSONValue]
RPCParams = dict[str, JSONValue] | list[JSONValue]
RPCMessage = dict[str, JSONValue]

logger = logging.getLogger(__name__)
if TYPE_CHECKING:
    from asyncio import AbstractEventLoop, Future
    from collections.abc import Callable, Coroutine

    from .components.authorization import Authorization
    from .components.database import DBProviderWrapper
    from .components.history import History
    from .components.websockets import WebsocketManager
    from .server import Server
    from .utils import IPAddress

    _C = TypeVar("_C", str, bool, float, int)
    ConvType = str | bool | float | int
    ArgVal = None | int | float | bool | str
    RPCCallback = Callable[..., Coroutine]
    AuthComp = Authorization | None
    FlexCallback = Callable[..., Awaitable | None]
    TimerCallback = Callable[[float], float | Awaitable[float]]

_uvl_var = os.getenv("MOONRAKER_ENABLE_UVLOOP", "y").lower()
_uvl_enabled = False
if _uvl_var in ["y", "yes", "true"]:
    with contextlib.suppress(ImportError):
        import uvloop

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        _uvl_enabled = True

_T = TypeVar("_T")
ENDPOINT_PREFIXES = ["printer", "server", "machine", "access", "api", "debug"]


class ExtendedFlag(Flag):
    """Extend ``Flag`` with convenience constructors.

    This class adds helpers to build flag values from string names or lists of
    names and to return a value with all flags set.

    Methods:
        from_string: Create a flag from a string representation.
        from_string_list: Create a flag from a list of string representations.
        all: Retrieve all possible flags.

    """

    @classmethod
    def from_string(cls, flag_name: str) -> Self:
        """Create and return a flag member matching the provided name.

        The comparison is case-insensitive. If no matching member exists
        a ValueError is raised.

        Args:
            flag_name: Name of the flag to lookup.

        Returns:
            The corresponding flag member.

        Raises:
            ValueError: If no flag member with the provided name exists.

        """
        str_name = flag_name.upper()
        for name, member in cls.__members__.items():
            if name == str_name:
                return cls(member.value)
        msg = f"No flag member named {flag_name}"
        raise ValueError(msg)

    @classmethod
    def from_string_list(cls, flag_list: list[str]) -> Self:
        """Build a combined Flag value from a list of flag names.

        Args:
            flag_list: List of flag names (strings).

        Returns:
            A Flag instance representing the bitwise OR of all named flags.

        """
        ret = cls(0)
        for flag in flag_list:
            flag_name = flag.upper()
            ret |= cls.from_string(flag_name)
        return ret

    @classmethod
    def all(cls) -> Self:
        """Return a Flag value representing all members set.

        This is implemented by inverting the zero-value of the enum.
        """
        return ~cls(0)


class RequestType(ExtendedFlag):
    """Represents the type of HTTP/REST API requests.

    Attributes:
        GET: Represents an HTTP GET request.
        POST: Represents an HTTP POST request.
        DELETE: Represents an HTTP DELETE request.

    """

    GET = auto()
    POST = auto()
    DELETE = auto()


class TransportType(ExtendedFlag):
    """Represents the type of transport mechanisms used in the application.

    Attributes:
        HTTP: Represents HTTP transport.
        WEBSOCKET: Represents WebSocket transport.
        MQTT: Represents MQTT transport.
        INTERNAL: Represents internal transport.

    """

    HTTP = auto()
    WEBSOCKET = auto()
    MQTT = auto()
    INTERNAL = auto()


class ExtendedEnum(Enum):
    """Extend ``Enum`` with convenience constructors and string output.

    Adds a case-insensitive ``from_string`` constructor and makes ``str()``
    produce a lowercase name for nicer logging and display.

    Methods:
        from_string: Create an enum member from a string representation.

    """

    @classmethod
    def from_string(cls, enum_name: str) -> Self:
        """Create an enum member from a case-insensitive name.

        Args:
            enum_name: Name of the enum member to lookup.

        Returns:
            The matching enum member.

        Raises:
            ValueError: If the name does not match any member.

        """
        str_name = enum_name.upper()
        for name, member in cls.__members__.items():
            if name == str_name:
                return cls(member.value)
        msg = f"No enum member named {enum_name}"
        raise ValueError(msg)

    def __str__(self) -> str:
        """Return the lowercase name of the enum member.

        This makes printing and logging more readable by returning a
        consistent lowercase representation.
        """
        return self._name_.lower()


class JobEvent(ExtendedEnum):
    """Represents the various states of a job.

    Attributes:
        STANDBY: The job is in standby mode.
        STARTED: The job has started.
        PAUSED: The job is paused.
        RESUMED: The job has resumed.
        COMPLETE: The job is complete.
        ERROR: The job encountered an error.
        CANCELLED: The job was cancelled.

    Properties:
        finished: Indicates whether the job is finished.
        aborted: Indicates whether the job was aborted.
        is_printing: Indicates whether the job is currently printing.

    """

    STANDBY = 1
    STARTED = 2
    PAUSED = 3
    RESUMED = 4
    COMPLETE = 5
    ERROR = 6
    CANCELLED = 7

    @property
    def finished(self) -> bool:
        """Return True if the job is in a finished state.

        Finished states are COMPLETE and any state with a numeric value greater
        than or equal to COMPLETE.
        """
        return self.value >= JobEvent.COMPLETE

    @property
    def aborted(self) -> bool:
        """Return True if the job aborted due to an error or cancellation."""
        return self.value >= JobEvent.ERROR

    @property
    def is_printing(self) -> bool:
        """Return True if the job is actively printing (started or resumed)."""
        return self.value in [JobEvent.STARTED, JobEvent.RESUMED]


class KlippyState(ExtendedEnum):
    """Represents the various states of the Klippy firmware.

    Attributes:
        DISCONNECTED: The firmware is disconnected.
        STARTUP: The firmware is starting up.
        READY: The firmware is ready.
        ERROR: The firmware encountered an error.
        SHUTDOWN: The firmware is shut down.

    Methods:
        from_string: Create a KlippyState member from a string representation.
        set_message: Set a custom message for the state.

    Properties:
        message: Retrieve the custom message for the state.
        startup_complete: Indicates whether the startup process is complete.

    """

    DISCONNECTED = 1
    STARTUP = 2
    READY = 3
    ERROR = 4
    SHUTDOWN = 5

    @classmethod
    def from_string(cls, enum_name: str, msg: str = "") -> Self:
        """Create a KlippyState from a case-insensitive name and optional message.

        If `msg` is provided it will be stored on the returned instance and
        can be retrieved via the `message` property.
        """
        str_name = enum_name.upper()
        for name, member in cls.__members__.items():
            if name == str_name:
                instance = cls(member.value)
                if msg:
                    instance.set_message(msg)
                return instance
        msg = f"No enum member named {enum_name}"
        raise ValueError(msg)

    def set_message(self, msg: str) -> None:
        """Attach a human-readable message to this state instance.

        The message is stored on the instance and retrievable via the
        `message` property.
        """
        self._state_message: str = msg

    @property
    def message(self) -> str:
        """Return the human-readable message attached to this state, if any."""
        if hasattr(self, "_state_message"):
            return self._state_message
        return ""

    def startup_complete(self) -> bool:
        """Return True when the startup process is considered complete."""
        return self.value > 2


class RenderableTemplate(metaclass=ABCMeta):
    """Abstract base class for renderable templates.

    Methods:
        __str__: Convert the template to a string.
        render: Render the template with a given context.
        render_async: Asynchronously render the template with a given context.

    """

    @abstractmethod
    def __str__(self) -> str:
        """Return a human-readable string representation of the template.

        Implementations should return the fully rendered template as a
        string (equivalent to calling ``render()`` without a context when
        appropriate). This method is used when the template is converted to
        a string, for example by ``str(template)`` or logging.
        """
        raise NotImplementedError

    @abstractmethod
    def render(self, context: dict[str, object] | None = None) -> str:
        """Render the template synchronously with the given context.

        Args:
            context: Optional mapping of values to make available to the
                template during rendering. If ``None``, implementations may
                use an empty context or default values.

        Returns:
            The rendered template as a string.

        """
        raise NotImplementedError

    @abstractmethod
    async def render_async(self, context: dict[str, object] | None = None) -> str:
        """Asynchronously render the template with the given context.

        This coroutine variant allows implementations to perform I/O-bound
        work (for example, loading partials from disk) while rendering.

        Args:
            context: Optional mapping of values to make available to the
                template during rendering.

        Returns:
            The rendered template as a string.

        """
        raise NotImplementedError


@dataclasses.dataclass
class UserInfo:
    """Represents user information.

    Attributes:
        username: The username of the user.
        password: The password of the user.
        created_on: The timestamp when the user was created.
        salt: The salt used for password hashing.
        source: The source of the user information.
        jwt_secret: The JWT secret for the user.
        jwk_id: The JWK ID for the user.
        groups: The groups the user belongs to.

    Methods:
        as_tuple: Convert the user information to a tuple.
        as_dict: Convert the user information to a dictionary.

    """

    username: str
    password: str
    created_on: float = dataclasses.field(default_factory=time.time)
    salt: str = ""
    source: str = "moonraker"
    jwt_secret: str | None = None
    jwk_id: str | None = None
    groups: list[str] = dataclasses.field(default_factory=lambda: ["admin"])

    def as_tuple(self) -> tuple[str | float | list[str] | None, ...]:
        """Return the UserInfo as a tuple matching the dataclass order.

        This is useful for persisting or comparing user records in legacy
        code paths that operate on tuples.
        """
        return dataclasses.astuple(self)

    def as_dict(self) -> dict[str, str | float | list[str] | None]:
        """Return the UserInfo as a plain dictionary.

        The result matches the dataclass field names and is safe to
        serialize to JSON or store in a database.
        """
        return dataclasses.asdict(self)


@dataclasses.dataclass(frozen=True)
class APIDefinition:
    """Represents the definition of an API endpoint.

    Attributes:
        endpoint: The endpoint of the API.
        http_path: The HTTP path of the API.
        rpc_methods: The RPC methods associated with the API.
        request_types: The request types supported by the API.
        transports: The transport mechanisms supported by the API.
        callback: The callback function for the API.
        auth_required: Indicates whether authentication is required for the API.

    Methods:
        request: Handle an API request.
        need_object_parser: Check if the endpoint requires an object parser.
        rpc_items: Retrieve the RPC items for the API.
        create: Create a new API definition.
        pop_cached_def: Remove a cached API definition.
        get_cache: Retrieve the cache of API definitions.
        reset_cache: Clear the cache of API definitions.

    """

    endpoint: str
    http_path: str
    rpc_methods: list[str]
    request_types: RequestType
    transports: TransportType
    callback: Callable[[WebRequest], Coroutine]
    auth_required: bool
    _cache: ClassVar[dict[str, APIDefinition]] = {}

    def __str__(self) -> str:
        """Return a compact, human-readable description of the API definition.

        The returned string includes allowed transports, http request types and
        RPC methods when applicable, and whether auth is required.
        """
        tprt_str = "|".join([tprt.name for tprt in self.transports if tprt.name])
        val: str = f"(Transports: {tprt_str})"
        if TransportType.HTTP in self.transports:
            req_types = "|".join([rt.name for rt in self.request_types if rt.name])
            val += f" (HTTP Request: {req_types} {self.http_path})"
        if self.rpc_methods:
            methods = " ".join(self.rpc_methods)
            val += f" (RPC Methods: {methods})"
        val += f" (Auth Required: {self.auth_required})"
        return val

    def request(
        self,
        args: JSONDict,
        request_type: RequestType,
        transport: APITransport | None = None,
        ip_addr: IPAddress | None = None,
        user: UserInfo | None = None,
    ) -> Coroutine:
        """Invoke the API callback with a constructed `WebRequest`.

        Args:
            args: The request arguments as a JSON-compatible dict.
            request_type: The type of HTTP request.
            transport: The transport instance for this request.
            ip_addr: Optional client IP address.
            user: Optional authenticated user.

        Returns:
            The coroutine returned by the registered callback.

        """
        return self.callback(
            WebRequest(self.endpoint, args, request_type, transport, ip_addr, user),
        )

    @property
    def need_object_parser(self) -> bool:
        """Return True if the endpoint looks like it requires the object parser.

        Endpoints that operate on objects typically start with the
        "objects/" prefix.
        """
        return self.endpoint.startswith("objects/")

    def rpc_items(self) -> zip[tuple[RequestType, str]]:
        """Return an iterator that pairs request types with RPC method names.

        The iterator yields tuples of (RequestType, rpc_method_name)
        and is useful when registering methods with the JSON-RPC layer.
        """
        return zip(self.request_types, self.rpc_methods, strict=True)

    @classmethod
    def create(
        cls,
        endpoint: str,
        request_types: list[str] | RequestType,
        callback: Callable[[WebRequest], Coroutine],
        transports: list[str] | TransportType | None = None,
        *,
        auth_required: bool = True,
        is_remote: bool = False,
    ) -> APIDefinition:
        """Create and cache an APIDefinition for the given endpoint.

        This convenience constructs a properly-normalized HTTP path, derives
        RPC method names when necessary, validates the endpoint prefixes and
        returns a cached instance when available.

        Args:
            endpoint: The logical endpoint name.
            request_types: Supported HTTP request types or list of names.
            callback: Coroutine function that implements the endpoint.
            transports: Optional list or TransportType flag for supported
                transports. Defaults to all transports.
            auth_required: Whether authentication is required (default True).
            is_remote: If True, treats the endpoint as a remote-style API
                and adjusts the HTTP path and RPC method naming.

        Returns:
            An `APIDefinition` instance (cached by endpoint).

        Raises:
            ServerError: If the endpoint name or definition is invalid.

        """
        if isinstance(request_types, list):
            request_types = RequestType.from_string_list(request_types)
        if transports is None:
            transports = TransportType.all()
        if isinstance(transports, list):
            transports = TransportType.from_string_list(transports)
        if endpoint in cls._cache:
            return cls._cache[endpoint]
        http_path = f"/printer/{endpoint.strip('/')}" if is_remote else endpoint
        prf_match = re.match(r"/([^/]+)", http_path)
        if TransportType.HTTP in transports and (
            prf_match is None or prf_match.group(1) not in ENDPOINT_PREFIXES
        ):
            prefixes = [f"/{prefix} " for prefix in ENDPOINT_PREFIXES]
            msg = (
                f"Invalid endpoint name '{endpoint}', must start with one of "
                f"the following: {prefixes}"
            )
            raise ServerError(
                msg,
            )
        rpc_methods: list[str] = []
        if is_remote:
            # Request Types have no meaning for remote requests.  Therefore
            # both GET and POST http requests are accepted.  JRPC requests do
            # not need an associated RequestType, so the unknown value is used.
            request_types = RequestType.GET | RequestType.POST
            rpc_methods.append(http_path[1:].replace("/", "."))
        elif transports != TransportType.HTTP:
            name_parts = http_path[1:].split("/")
            if len(request_types) > 1:
                for rtype in request_types:
                    if rtype.name is None:
                        continue
                    func_name = rtype.name.lower() + "_" + name_parts[-1]
                    rpc_methods.append(".".join([*name_parts[:-1], func_name]))
            else:
                rpc_methods.append(".".join(name_parts))
            if len(request_types) != len(rpc_methods):
                msg = (
                    "Invalid API definition.  Number of websocket methods must "
                    "match the number of request methods"
                )
                raise ServerError(
                    msg,
                )

        api_def = cls(
            endpoint,
            http_path,
            rpc_methods,
            request_types,
            transports,
            callback,
            auth_required,
        )
        cls._cache[endpoint] = api_def
        return api_def

    @classmethod
    def pop_cached_def(cls, endpoint: str) -> APIDefinition | None:
        """Remove and return a cached APIDefinition for ``endpoint``.

        Returns None if no cached definition exists for the given endpoint.
        """
        return cls._cache.pop(endpoint, None)

    @classmethod
    def get_cache(cls) -> dict[str, APIDefinition]:
        """Return the current APIDefinition cache mapping endpoint -> definition."""
        return cls._cache

    @classmethod
    def reset_cache(cls) -> None:
        """Clear the internal APIDefinition cache."""
        cls._cache.clear()


class APITransport:
    """Represents the transport mechanism for API communication.

    Properties:
        transport_type: The type of transport.
        user_info: The user information associated with the transport.
        ip_addr: The IP address associated with the transport.

    Methods:
        screen_rpc_request: Screen an RPC request.
        send_status: Send a status update.

    """

    @property
    def transport_type(self) -> TransportType:
        """Return the TransportType for this transport instance.

        Subclasses should override to provide the accurate transport type.
        """
        return TransportType.INTERNAL

    @property
    def user_info(self) -> UserInfo | None:
        """Return the authenticated UserInfo for this transport, if any."""
        return None

    @property
    def ip_addr(self) -> IPAddress | None:
        """Return the client IP address associated with this transport, if known."""
        return None

    def screen_rpc_request(
        self,
        _api_def: APIDefinition,
        _req_type: RequestType,
        _args: JSONDict,
    ) -> None:
        """Optionally inspect and validate an incoming RPC request.

        The base implementation is a no-op; subclasses may raise exceptions to
        reject requests.
        """
        return

    def send_status(
        self,
        status: JSONDict,
        eventtime: float,
    ) -> None:
        """Send a status update to the transport's peer.

        Subclasses must implement this to deliver status notifications.
        """
        raise NotImplementedError


class BaseRemoteConnection(APITransport):
    """Represents a base class for remote connections.

    Properties:
        user_info: The user information associated with the connection.
        need_auth: Indicates whether authentication is required.
        uid: The unique identifier for the connection.
        hostname: The hostname of the connection.
        start_time: The start time of the connection.
        identified: Indicates whether the connection is identified.
        client_data: The client data associated with the connection.
        transport_type: The type of transport used by the connection.

    Methods:
        screen_rpc_request: Screen an RPC request.
        authenticate: Authenticate the connection.
        check_authenticated: Check if the connection is authenticated.
        on_user_logout: Handle user logout.
        queue_message: Queue a message for sending.
        send_status: Send a status update.
        call_method_with_response: Call a method and wait for a response.
        call_method: Call a method without waiting for a response.
        send_notification: Send a notification to the client.
        resolve_pending_response: Resolve a pending response.
        close_socket: Close the socket connection.

    """

    def on_create(self, server: Server) -> None:
        """Initialize connection state when the connection is attached to a server.

        This method is called once the connection object is created and given a
        reference to the owning `Server`. It sets up event-loop helpers and
        initializes internal buffers/state used for message queuing and
        response matching.

        Args:
            server: The server instance this connection belongs to.

        """
        self.server = server
        self.eventloop = server.get_event_loop()
        self.wsm: WebsocketManager = self.server.lookup_component("websockets")
        self.rpc: JsonRPC = self.server.lookup_component("jsonrpc")
        self._uid = id(self)
        self.is_closed: bool = False
        self.queue_busy: bool = False
        self.pending_responses: dict[int, Future] = {}
        self.message_buf: list[bytes | str] = []
        self._connected_time: float = 0.0
        self._identified: bool = False
        self._client_data: dict[str, str] = {
            "name": "unknown",
            "version": "",
            "type": "",
            "url": "",
        }
        self._need_auth: bool = False
        self._user_info: UserInfo | None = None

    @property
    def user_info(self) -> UserInfo | None:
        """Return the UserInfo associated with this connection, if any."""
        return self._user_info

    @user_info.setter
    def user_info(self, uinfo: UserInfo) -> None:
        """Set the connection's authenticated user info.

        Setting user_info clears the connection's need-for-auth flag.
        """
        self._user_info = uinfo
        self._need_auth = False

    @property
    def need_auth(self) -> bool:
        """Return True if this connection currently requires authentication."""
        return self._need_auth

    @property
    def uid(self) -> int:
        """Return a unique identifier for this connection instance."""
        return self._uid

    @property
    def hostname(self) -> str:
        """Return the hostname reported by the client (empty by default)."""
        return ""

    @property
    def start_time(self) -> float:
        """Return the timestamp when the connection was established."""
        return self._connected_time

    @property
    def identified(self) -> bool:
        """Return True if the connection has completed client identification."""
        return self._identified

    @property
    def client_data(self) -> dict[str, str]:
        """Return client-supplied metadata such as name, version and type."""
        return self._client_data

    @client_data.setter
    def client_data(self, data: dict[str, str]) -> None:
        """Set client metadata and mark the connection as identified."""
        self._client_data = data
        self._identified = True

    @property
    def transport_type(self) -> TransportType:
        """Return the TransportType used by this connection.

        Default is WEBSOCKET for remote connections.
        """
        return TransportType.WEBSOCKET

    def screen_rpc_request(
        self,
        api_def: APIDefinition,
        _req_type: RequestType,
        _args: JSONDict,
    ) -> None:
        """Sanity-check an incoming RPC request for this connection.

        The default implementation enforces authentication requirements by
        delegating to :meth:`check_authenticated`. Subclasses may override to
        implement additional per-connection screening logic.

        Args:
            api_def: The API definition associated with the RPC method.
            _req_type: The request type of the RPC call (unused by default).
            _args: The parsed RPC params (unused by default).

        """
        self.check_authenticated(api_def)

    async def _process_message(self, message: str) -> None:
        """Process a raw JSON-RPC message string received from the client.

        The message will be dispatched via the shared :class:`JsonRPC` instance
        and, if a response is produced, queued for transmission. Exceptions are
        caught and logged to avoid crashing the connection task.

        Args:
            message: The raw JSON/bytes (decoded to str) representing the
                incoming RPC request or batch.

        """
        try:
            response = await self.rpc.dispatch(message, self)
            if response is not None:
                self.queue_message(response)
        except Exception:
            logger.exception("Websocket Command Error")

    def queue_message(self, message: bytes | str | RPCMessage) -> None:
        """Enqueue a message for delivery to the remote client.

        Messages may be Python dicts (RPCMessage), strings, or bytes. Dicts are
        serialized to JSON before being buffered. Messages are written from the
        buffer by the background writer scheduled on the event loop.

        Args:
            message: The message to queue (dict, str, or bytes).

        """
        self.message_buf.append(
            jsonw.dumps(message) if isinstance(message, dict) else message,
        )
        if self.queue_busy:
            return
        self.queue_busy = True
        self.eventloop.register_callback(self._write_messages)

    def authenticate(
        self,
        token: str | None = None,
        api_key: str | None = None,
    ) -> None:
        """Attempt to authenticate this connection using token or API key.

        If an authorization component is available on the server, this will
        validate the provided credentials and set ``user_info`` on success. If
        authentication is required but no valid credentials are presented a
        401 ServerError is raised.

        Args:
            token: Optional JWT access token to validate.
            api_key: Optional API key to validate.

        """
        auth: AuthComp = self.server.lookup_component("authorization", None)
        if auth is None:
            return
        if token is not None:
            self.user_info = auth.validate_jwt(token)
        elif api_key is not None and self.user_info is None:
            self.user_info = auth.validate_api_key(api_key)
        elif self._need_auth:
            msg = "Unauthorized"
            raise self.server.error(msg, 401)

    def check_authenticated(self, api_def: APIDefinition) -> None:
        """Ensure the connection is authorized to invoke the given API.

        By default, if the connection requires authentication and the server
        provides an authorization component, requests to endpoints that require
        auth will result in a 401 ServerError when no valid user is associated
        with the connection.

        Args:
            api_def: The API definition being invoked.

        """
        if not self._need_auth:
            return
        auth: AuthComp = self.server.lookup_component("authorization", None)
        if auth is None:
            return
        if api_def.auth_required:
            msg = "Unauthorized"
            raise self.server.error(msg, 401)

    def on_user_logout(self, user: str) -> bool:
        """Handle a user logout event by clearing stored credentials.

        If the given username matches the connection's authenticated user the
        user_info is cleared and True is returned.

        Args:
            user: Username that was logged out.

        Returns:
            True if the connection's user info was cleared, False otherwise.

        """
        if self._user_info is None:
            return False
        if user == self._user_info.username:
            self._user_info = None
            return True
        return False

    async def _write_messages(self) -> None:
        """Background writer that flushes the outgoing message buffer.

        This coroutine writes buffered messages to the underlying socket/transport
        by repeatedly calling :meth:`write_to_socket`. If the connection is
        closed the buffer is cleared and the writer exits early.
        """
        if self.is_closed:
            self.message_buf = []
            self.queue_busy = False
            return
        while self.message_buf:
            msg = self.message_buf.pop(0)
            await self.write_to_socket(msg)
        self.queue_busy = False

    async def write_to_socket(self, message: bytes | str) -> None:
        """Write a single message to the underlying socket.

        Subclasses must implement this coroutine to perform the actual
        transmission (e.g. websocket send). The base implementation raises
        NotImplementedError.

        Args:
            message: The encoded message to send (bytes or str).

        """
        msg = "Children must implement write_to_socket"
        raise NotImplementedError(msg)

    def send_status(
        self,
        status: JSONDict,
        eventtime: float,
    ) -> None:
        """Notify the remote client of a status update.

        The status is delivered as a JSON-RPC notification with method
        ``notify_status_update``. Empty status objects are ignored.

        Args:
            status: The status payload to send.
            eventtime: Timestamp associated with the status.

        """
        if not status:
            return
        self.queue_message(
            {
                "jsonrpc": "2.0",
                "method": "notify_status_update",
                "params": [status, eventtime],
            },
        )

    def call_method_with_response(
        self,
        method: str,
        params: RPCParams | None = None,
    ) -> Awaitable:
        """Call an RPC method on the remote endpoint and return a Future.

        A unique id tied to a local Future is attached to the outgoing message so
        that when the remote side replies the associated Future can be resolved
        by :meth:`resolve_pending_response`.

        Args:
            method: RPC method name to invoke.
            params: Optional params to include.

        Returns:
            A Future that will be resolved with the method's response.

        """
        fut = self.eventloop.create_future()
        msg: RPCMessage = {
            "jsonrpc": "2.0",
            "method": method,
            "id": id(fut),
        }
        if params:
            msg["params"] = params
        self.pending_responses[id(fut)] = fut
        self.queue_message(msg)
        return fut

    def call_method(
        self,
        method: str,
        params: RPCParams | None = None,
    ) -> None:
        """Send a JSON-RPC notification (no response expected) to the client.

        Args:
            method: Method name to invoke remotely.
            params: Optional parameters to include in the notification.

        """
        msg: RPCMessage = {
            "jsonrpc": "2.0",
            "method": method,
        }
        if params:
            msg["params"] = params
        self.queue_message(msg)

    def send_notification(self, name: str, data: list) -> None:
        """Forward a named notification to the websocket manager for this client.

        This is a thin helper that restricts the notification to this
        connection's UID.

        Args:
            name: Notification event name.
            data: Payload to attach to the notification.

        """
        self.wsm.notify_clients(name, data, [self._uid])

    def resolve_pending_response(
        self,
        response_id: int,
        result: JSONValue | ServerError,
    ) -> bool:
        """Resolve a previously-registered Future for an outgoing RPC call.

        When a response arrives from the remote side this method is called to
        locate the matching Future and set its result or exception.

        Args:
            response_id: The numeric id previously attached to the request.
            result: The response payload or a ServerError instance.

        Returns:
            True if a matching future was found and resolved, False otherwise.

        """
        fut = self.pending_responses.pop(response_id, None)
        if fut is None:
            return False
        if isinstance(result, ServerError):
            fut.set_exception(result)
        else:
            fut.set_result(result)
        return True

    def close_socket(self, code: int, reason: str) -> None:
        """Close the underlying socket/transport with a code and reason.

        Subclasses must implement this to perform transport-specific teardown
        (for example, sending a close frame on a websocket).

        Args:
            code: Numeric close code.
            reason: Human-readable reason for the close.

        """
        msg = "Children must implement close_socket()"
        raise NotImplementedError(msg)


class WebRequest:
    """Represents a web request.

    Attributes:
        endpoint: The endpoint of the request.
        args: The arguments of the request.
        request_type: The type of the request.
        transport: The transport mechanism for the request.
        ip_addr: The IP address associated with the request.
        current_user: The current user associated with the request.

    Methods:
        get_endpoint: Retrieve the endpoint of the request.
        get_request_type: Retrieve the type of the request.
        get_action: Retrieve the action of the request.
        get_args: Retrieve the arguments of the request.
        get_subscribable: Retrieve the subscribable transport.
        get_client_connection: Retrieve the client connection.
        get_ip_address: Retrieve the IP address.
        get_current_user: Retrieve the current user.
        get: Retrieve an argument by key.
        get_str: Retrieve a string argument by key.
        get_int: Retrieve an integer argument by key.
        get_float: Retrieve a float argument by key.
        get_boolean: Retrieve a boolean argument by key.
        get_list: Retrieve a list argument by key.

    """

    def __init__(
        self,
        endpoint: str,
        args: JSONDict,
        request_type: RequestType | None = None,
        transport: APITransport | None = None,
        ip_addr: IPAddress | None = None,
        user: UserInfo | None = None,
    ) -> None:
        """Create a WebRequest wrapper for an incoming API call.

        Args:
            endpoint: Logical endpoint name.
            args: Parsed JSON-compatible argument dict for the request.
            request_type: Optional RequestType enum value.
            transport: Optional transport instance handling the request.
            ip_addr: Optional client IP address.
            user: Optional authenticated UserInfo.

        """
        self.endpoint = endpoint
        self.args = args
        self.transport = transport
        if request_type is None:
            request_type = RequestType(0)
        self.request_type = request_type
        self.ip_addr: IPAddress | None = ip_addr
        self.current_user = user

    def get_endpoint(self) -> str:
        """Return the logical endpoint string for this request."""
        return self.endpoint

    def get_request_type(self) -> RequestType:
        """Return the RequestType for this request."""
        return self.request_type

    def get_action(self) -> str:
        """Return the action name corresponding to the RequestType."""
        return self.request_type.name or ""

    def get_args(self) -> JSONDict:
        """Return the raw JSON-compatible argument dictionary."""
        return self.args

    def get_subscribable(self) -> APITransport | None:
        """Return the transport instance if it supports subscriptions."""
        return self.transport

    def get_client_connection(self) -> BaseRemoteConnection | None:
        """Return the BaseRemoteConnection for websocket clients, if present."""
        if isinstance(self.transport, BaseRemoteConnection):
            return self.transport
        return None

    def get_ip_address(self) -> IPAddress | None:
        """Return the client IP address associated with this request, if any."""
        return self.ip_addr

    def get_current_user(self) -> UserInfo | None:
        """Return the authenticated user for this request, if any."""
        return self.current_user

    def _get_converted_arg(
        self,
        key: str,
        default: Sentinel | _T,
        dtype: type[_C],
    ) -> _C | _T:
        def _raise_type_error() -> os.NoReturn:
            raise TypeError

        if key not in self.args:
            if default is Sentinel.MISSING:
                msg = f"No data for argument: {key}"
                raise ServerError(msg)
            return default
        val = self.args[key]
        try:
            if dtype is not bool:
                return dtype(val)
            if isinstance(val, str):
                val = val.lower()
                if val in ["true", "false"]:
                    return val == "true"
            elif isinstance(val, bool):
                return val
            _raise_type_error()
        except Exception as err:
            msg = (
                f"Unable to convert argument [{key}] to {dtype}: value received: {val}"
            )
            raise ServerError(
                msg,
            ) from err

    def get(
        self,
        key: str,
        default: Sentinel | _T = Sentinel.MISSING,
    ) -> _T | JSONValue:
        """Return a raw argument value or raise ServerError if missing.

        Args:
            key: Name of the argument to retrieve.
            default: Optional default to return instead of raising.

        """
        val = self.args.get(key, default)
        if val is Sentinel.MISSING:
            msg = f"No data for argument: {key}"
            raise ServerError(msg)
        return val

    def get_str(
        self,
        key: str,
        default: Sentinel | _T = Sentinel.MISSING,
    ) -> str | _T:
        return self._get_converted_arg(key, default, str)

    def get_int(
        self,
        key: str,
        default: Sentinel | _T = Sentinel.MISSING,
    ) -> int | _T:
        return self._get_converted_arg(key, default, int)

    def get_float(
        self,
        key: str,
        default: Sentinel | _T = Sentinel.MISSING,
    ) -> float | _T:
        return self._get_converted_arg(key, default, float)

    def get_boolean(
        self,
        key: str,
        default: Sentinel | _T = Sentinel.MISSING,
    ) -> bool | _T:
        return self._get_converted_arg(key, default, bool)

    def _parse_list(
        self,
        key: str,
        sep: str,
        ltype: type[_C],
        count: int | None,
        default: Sentinel | _T,
    ) -> list[_C] | _T:
        if key not in self.args:
            if default is Sentinel.MISSING:
                msg = f"No data for argument: {key}"
                raise ServerError(msg)
            return default
        value = self.args[key]
        if isinstance(value, str):
            try:
                ret = [ltype(val.strip()) for val in value.split(sep) if val.strip()]
            except Exception as e:
                msg = (
                    f"Invalid list format received for argument '{key}', "
                    "parsing failed."
                )
                raise ServerError(
                    msg,
                ) from e
        elif isinstance(value, list):
            for val in value:
                if not isinstance(val, ltype):
                    msg = (
                        f"Invalid list format for argument '{key}', expected all "
                        f"values to be of type {ltype.__name__}."
                    )
                    raise ServerError(
                        msg,
                    )
            # List already parsed
            ret = value
        else:
            msg = (
                f"Invalid value received for argument '{key}'.  Expected List type, "
                f"received {type(value).__name__}"
            )
            raise ServerError(
                msg,
            )
        if count is not None and len(ret) != count:
            msg = (
                f"Invalid list received for argument '{key}', count mismatch. "
                f"Expected {count} items, got {len(ret)}."
            )
            raise ServerError(
                msg,
            )
        return ret

    def get_list(
        self,
        key: str,
        default: Sentinel | _T = Sentinel.MISSING,
        sep: str = ",",
        count: int | None = None,
    ) -> _T | list[str]:
        return self._parse_list(key, sep, str, count, default)


class JsonRPC:
    """Represents a JSON-RPC handler.

    Attributes:
        methods: The registered methods for the JSON-RPC handler.
        sanitize_response: Indicates whether to sanitize responses.
        verbose: Indicates whether verbose logging is enabled.

    Methods:
        register_method: Register a method with the JSON-RPC handler.
        get_method: Retrieve a registered method.
        remove_method: Remove a registered method.
        dispatch: Dispatch a JSON-RPC request.
        process_object: Process a JSON-RPC object.
        process_response: Process a JSON-RPC response.
        execute_method: Execute a JSON-RPC method.
        build_result: Build a JSON-RPC result.
        build_error: Build a JSON-RPC error.

    """

    def __init__(self, server: Server) -> None:
        self.methods: dict[str, tuple[RequestType, APIDefinition]] = {}
        self.sanitize_response = False
        self.verbose = server.is_verbose_enabled()

    def _log_request(self, rpc_obj: RPCMessage, trtype: TransportType) -> None:
        if not self.verbose:
            return
        self.sanitize_response = False
        output = rpc_obj
        method: JSONValue = rpc_obj.get("method")
        params: JSONValue = rpc_obj.get("params", {})
        if isinstance(method, str):
            if method.startswith("access.") or method == "machine.sudo.password":
                self.sanitize_response = True
                if params and isinstance(params, dict):
                    output = copy.deepcopy(rpc_obj)
                    output["params"] = dict.fromkeys(params, "<sanitized>")
            elif method == "server.connection.identify":
                output = copy.deepcopy(rpc_obj)
                if isinstance(params, dict):
                    for field in ["access_token", "api_key"]:
                        if field in params:
                            output["params"][field] = "<sanitized>"
        logger.debug("%s Received::%s", trtype, jsonw.dumps(output).decode())

    def _log_response(
        self,
        resp_obj: RPCMessage | None,
        trtype: TransportType,
    ) -> None:
        if not self.verbose:
            return
        if resp_obj is None:
            return
        output = resp_obj
        if self.sanitize_response and "result" in resp_obj:
            output = copy.deepcopy(resp_obj)
            output["result"] = "<sanitized>"
        self.sanitize_response = False
        logger.debug("%s Response::%s", trtype, jsonw.dumps(output).decode())

    def register_method(
        self,
        name: str,
        request_type: RequestType,
        api_definition: APIDefinition,
    ) -> None:
        self.methods[name] = (request_type, api_definition)

    def get_method(self, name: str) -> tuple[RequestType, APIDefinition] | None:
        return self.methods.get(name, None)

    def remove_method(self, name: str) -> None:
        self.methods.pop(name, None)

    async def dispatch(
        self,
        data: str | bytes,
        transport: APITransport,
    ) -> bytes | None:
        transport_type = transport.transport_type
        try:
            obj: RPCMessage | list[RPCMessage] = jsonw.loads(data)
        except Exception:
            if isinstance(data, bytes):
                data = data.decode()
            msg = f"{transport_type} data not valid json: {data}"
            logger.exception(msg)
            err = self.build_error(-32700, "Parse error")
            return jsonw.dumps(err)
        if isinstance(obj, list):
            responses: list[RPCMessage] = []
            for item in obj:
                self._log_request(item, transport_type)
                resp = await self.process_object(item, transport)
                if resp is not None:
                    self._log_response(resp, transport_type)
                    responses.append(resp)
            if responses:
                return jsonw.dumps(responses)
        else:
            self._log_request(obj, transport_type)
            response = await self.process_object(obj, transport)
            if response is not None:
                self._log_response(response, transport_type)
                return jsonw.dumps(response)
        return None

    async def process_object(
        self,
        obj: RPCMessage,
        transport: APITransport,
    ) -> RPCMessage | None:
        req_id: int | None = obj.get("id")
        rpc_version: str = obj.get("jsonrpc", "")
        if rpc_version != "2.0":
            return self.build_error(-32600, "Invalid Request", req_id)
        method_name = obj.get("method", Sentinel.MISSING)
        if method_name is Sentinel.MISSING:
            self.process_response(obj, transport)
            return None
        if not isinstance(method_name, str):
            return self.build_error(
                -32600,
                "Invalid Request",
                req_id,
                method_name=str(method_name),
            )
        method_info = self.methods.get(method_name, None)
        if method_info is None:
            return self.build_error(
                -32601,
                "Method not found",
                req_id,
                method_name=method_name,
            )
        request_type, api_definition = method_info
        transport_type = transport.transport_type
        if transport_type not in api_definition.transports:
            return self.build_error(
                -32601,
                f"Method not found for transport {transport_type.name}",
                req_id,
                method_name=method_name,
            )
        params: JSONDict = {}
        if "params" in obj:
            params_value: JSONValue = obj["params"]
            if not isinstance(params_value, dict):
                return self.build_error(
                    -32602,
                    "Invalid params:",
                    req_id,
                    method_name=method_name,
                )
            params = params_value
        return await self.execute_method(
            method_name,
            request_type,
            api_definition,
            req_id,
            transport,
            params,
        )

    def process_response(
        self,
        obj: RPCMessage,
        conn: APITransport,
    ) -> None:
        if not isinstance(conn, BaseRemoteConnection):
            logger.debug("RPC Response to non-socket request: %s", obj)
            return
        response_id = obj.get("id")
        if response_id is None:
            logger.debug("RPC Response with null ID: %s", obj)
            return
        result = obj.get("result")
        if result is None:
            name = conn.client_data["name"]
            msg = f"Agent {name} RPC error"
            ret = AgentError(msg, obj.get("error"))
        else:
            ret = result
        conn.resolve_pending_response(response_id, ret)

    async def execute_method(
        self,
        method_name: str,
        request_type: RequestType,
        api_definition: APIDefinition,
        req_id: int | None,
        transport: APITransport,
        params: JSONDict,
    ) -> RPCMessage | None:
        try:
            transport.screen_rpc_request(api_definition, request_type, params)
            result = await api_definition.request(
                params,
                request_type,
                transport,
                transport.ip_addr,
                transport.user_info,
            )
        except TypeError as e:
            return self.build_error(
                -32602,
                f"Invalid params:\n{e}",
                req_id,
                e,
                method_name,
            )
        except ServerError as e:
            code = e.status_code
            if code == 404:
                code = -32601
            elif code == 401:
                code = -32602
            return self.build_error(code, str(e), req_id, e, method_name)
        except Exception as e:
            return self.build_error(500, str(e), req_id, e, method_name)

        if req_id is None:
            return None
        return self.build_result(result, req_id)

    def build_result(self, result: JSONValue, req_id: int) -> RPCMessage:
        return {
            "jsonrpc": "2.0",
            "result": result,
            "id": req_id,
        }

    def build_error(
        self,
        code: int,
        msg: str,
        req_id: int | None = None,
        exc: Exception | None = None,
        method_name: str = "",
    ) -> RPCMessage:
        if method_name:
            method_name = f"Requested Method: {method_name}, "
        log_msg = f"JSON-RPC Request Error - {method_name}Code: {code}, Message: {msg}"
        err = {"code": code, "message": msg}
        if isinstance(exc, AgentError):
            err["data"] = exc.error_data
            if self.verbose:
                log_msg += f"\nExtra data: {exc.error_data}"
        logger.info(log_msg, exc_info=(exc is not None and self.verbose))
        return {
            "jsonrpc": "2.0",
            "error": err,
            "id": req_id,
        }


# *** Job History Common Classes ***


class FieldTracker[T]:
    """Base class for history field trackers.

    Trackers maintain aggregate state for a particular history field and are
    driven by periodic updates from providers. Concrete subclasses implement
    specific accumulation strategies such as delta, cumulative, averaging,
    or collection tracking.
    """

    history: History = None

    def __init__(
        self,
        value: _T = None,
        reset_callback: Callable[[], _T] | None = None,
        *,
        exclude_paused: bool = False,
    ) -> None:
        """Initialize a FieldTracker.

        Args:
            value: Initial tracked value.
            reset_callback: Optional callable returning an initial/reset value.
            exclude_paused: When True, paused job time is excluded from
                tracking calculations.

        """
        self.tracked_value = value
        self.exclude_paused = exclude_paused
        self.reset_callback: Callable[[], _T] | None = reset_callback

    def set_reset_callback(self, cb: Callable[[], _T] | None) -> None:
        """Set a callable to obtain an initial/reset value for the tracker.

        The provided callback will be invoked when :meth:`reset` is called and
        allows external providers to supply initial tracker state.

        Args:
            cb: Callable that returns the reset value or None to clear.

        """
        self.reset_callback = cb

    def set_exclude_paused(self, *, exclude: bool) -> None:
        """Control whether paused job time should be excluded from tracking.

        Args:
            exclude: True to exclude paused periods when computing tracked
                values, False to include them.

        """
        self.exclude_paused = exclude

    def reset(self) -> None:
        """Reset the tracker's internal state.

        Concrete tracker implementations must override this to restore the
        tracked value to an initial or zero state.
        """
        raise NotImplementedError

    def update(self, value: _T) -> None:
        """Update the tracker's state with a new observed value.

        Concrete implementations decide how the incoming value affects
        the tracked state (e.g. delta, cumulative, maximum, etc.).
        """
        raise NotImplementedError

    def get_tracked_value(self) -> _T:
        """Return the current tracked value.

        Returns:
            The tracker's current aggregated/observed value.

        """
        return self.tracked_value

    def has_totals(self) -> bool:
        """Return True if this tracker supports total/maximum reporting.

        Override in trackers that produce numeric totals.
        """
        return False

    @classmethod
    def class_init(cls, history: History) -> None:
        """Class-level initializer used to wire in the runtime History instance.

        This is called once by the history subsystem so trackers can consult
        runtime state (for example, whether tracking is currently enabled).

        Args:
            history: The active History instance.

        """
        cls.history = history


class BasicTracker(FieldTracker[object]):
    """A simple tracker that stores the most recent value.

    This tracker can optionally be initialized via a reset callback and will
    replace its tracked value whenever an update is received while tracking is
    enabled.
    """

    def __init__(
        self,
        value: object = None,
        reset_callback: Callable[[], object] | None = None,
        *,
        exclude_paused: bool = False,
    ) -> None:
        super().__init__(value, reset_callback, exclude_paused=exclude_paused)

    def reset(self) -> None:
        """Reset basic tracker using the optional reset callback.

        If a reset callback is provided it is used to obtain the new tracked
        value; otherwise the tracked value is left unchanged.
        """
        if self.reset_callback is not None:
            self.tracked_value = self.reset_callback()

    def update(self, value: object) -> None:
        """Set the tracked value when tracking is enabled.

        Args:
            value: New value to store.

        """
        if self.history.tracking_enabled(self.exclude_paused):
            self.tracked_value = value

    def has_totals(self) -> bool:
        """Return True if the tracked value is numeric (and therefore totals)."""
        return isinstance(self.tracked_value, (int, float))


class DeltaTracker(FieldTracker[int | float]):
    """Tracker that accumulates the differences between successive values.

    The tracker stores the last observed value and adds the delta between the
    current and previous observations to its cumulative total. Useful for
    tracking metrics that represent monotonic counters.
    """

    def __init__(
        self,
        value: float = 0,
        reset_callback: Callable[[], float | int] | None = None,
        *,
        exclude_paused: bool = False,
    ) -> None:
        super().__init__(value, reset_callback, exclude_paused=exclude_paused)
        self.last_value: float | int | None = None

    def reset(self) -> None:
        """Reset delta tracking state.

        The delta tracker keeps the last observed value and accumulates the
        difference between successive observations into ``tracked_value``.
        """
        self.tracked_value = 0
        self.last_value = None
        if self.reset_callback is not None:
            self.last_value = self.reset_callback()
            if not isinstance(self.last_value, (float, int)):
                logger.info("DeltaTracker reset to invalid type")
            self.last_value = None

    def update(self, value: float) -> None:
        """Update the delta tracker with a new numeric observation.

        When a previous value exists the difference is added to the cumulative
        tracked total.
        """
        if not isinstance(value, (int, float)):
            return
        if (
            self.history.tracking_enabled(self.exclude_paused)
            and self.last_value is not None
        ):
            self.tracked_value += value - self.last_value
        self.last_value = value

    def has_totals(self) -> bool:
        """Delta tracker always supports totals reporting."""
        return True


class CumulativeTracker(FieldTracker[int | float]):
    """Tracker that accumulates numeric values into a running total.

    Each call to :meth:`update` adds the provided numeric value to the
    internal tracked total when tracking is enabled.
    """

    def __init__(
        self,
        value: float = 0,
        reset_callback: Callable[[], float | int] | None = None,
        *,
        exclude_paused: bool = False,
    ) -> None:
        super().__init__(value, reset_callback, exclude_paused=exclude_paused)

    def reset(self) -> None:
        """Reset cumulative total to initial or zero value.

        If a reset callback is provided it is used to obtain an initial value.
        """
        if self.reset_callback is not None:
            self.tracked_value = self.reset_callback()
            if not isinstance(self.tracked_value, (float, int)):
                logger.info("%s reset to invalid type", self.__class__.__name__)
                self.tracked_value = 0
        else:
            self.tracked_value = 0

    def update(self, value: float) -> None:
        """Accumulate numeric values into the tracked total.

        Args:
            value: Numeric amount to add to the cumulative total.

        """
        if not isinstance(value, (int, float)):
            return
        if self.history.tracking_enabled(self.exclude_paused):
            self.tracked_value += value

    def has_totals(self) -> bool:
        """Cumulative tracker supports totals reporting."""
        return True


class AveragingTracker(CumulativeTracker):
    """Tracker that computes a running average of observed numeric values.

    Maintains a count and uses an incremental averaging formula to avoid
    retaining all samples.
    """

    def __init__(
        self,
        value: float = 0,
        reset_callback: Callable[[], float | int] | None = None,
        *,
        exclude_paused: bool = False,
    ) -> None:
        super().__init__(value, reset_callback, exclude_paused=exclude_paused)
        self.count = 0

    def reset(self) -> None:
        """Reset the averaging tracker and observation count."""
        super().reset()
        self.count = 0

    def update(self, value: float) -> None:
        """Update the running average with a new numeric sample.

        Uses an incremental averaging formula to avoid storing individual
        samples.
        """
        if not isinstance(value, (int, float)):
            return
        if self.history.tracking_enabled(self.exclude_paused):
            lv = self.tracked_value
            self.count += 1
            self.tracked_value = (lv * (self.count - 1) + value) / self.count


class MaximumTracker(CumulativeTracker):
    """Tracker that records the maximum observed numeric value.

    The tracker can be initialized from a callback or will adopt the first
    observed value as the initial maximum.
    """

    def __init__(
        self,
        value: float = 0,
        reset_callback: Callable[[], float | int] | None = None,
        *,
        exclude_paused: bool = False,
    ) -> None:
        super().__init__(value, reset_callback, exclude_paused=exclude_paused)
        self.initialized = False

    def reset(self) -> None:
        """Reset maximum tracker state.

        Optionally initialize from a reset callback; otherwise the tracker
        starts uninitialized and will adopt the first observed value.
        """
        self.initialized = False
        if self.reset_callback is not None:
            self.tracked_value = self.reset_callback()
            if not isinstance(self.tracked_value, (int, float)):
                self.tracked_value = 0
                logger.info("MaximumTracker reset to invalid type")
            else:
                self.initialized = True
        else:
            self.tracked_value = 0

    def update(self, value: float) -> None:
        """Update stored maximum with a new numeric observation.

        The first observed value initializes the maximum unless a reset
        callback already supplied one.
        """
        if not isinstance(value, (int, float)):
            return
        if self.history.tracking_enabled(self.exclude_paused):
            if not self.initialized:
                self.tracked_value = value
                self.initialized = True
            else:
                self.tracked_value = max(self.tracked_value, value)


class MinimumTracker(CumulativeTracker):
    """Tracker that records the minimum observed numeric value.

    Mirrors :class:`MaximumTracker` but tracks minima instead of maxima.
    """

    def __init__(
        self,
        value: float = 0,
        reset_callback: Callable[[], float | int] | None = None,
        *,
        exclude_paused: bool = False,
    ) -> None:
        super().__init__(value, reset_callback, exclude_paused=exclude_paused)
        self.initialized = False

    def reset(self) -> None:
        """Reset minimum tracker state.

        Behaves similarly to :class:`MaximumTracker` but tracks minima.
        """
        self.initialized = False
        if self.reset_callback is not None:
            self.tracked_value = self.reset_callback()
            if not isinstance(self.tracked_value, (int, float)):
                self.tracked_value = 0
                logger.info("MinimumTracker reset to invalid type")
            else:
                self.initialized = True
        else:
            self.tracked_value = 0

    def update(self, value: float) -> None:
        """Update stored minimum with a new numeric observation."""
        if not isinstance(value, (int, float)):
            return
        if self.history.tracking_enabled(self.exclude_paused):
            self.tracked_value = (
                value if not self.initialized else min(self.tracked_value, value)
            )
            self.initialized = True


class CollectionTracker(FieldTracker[list[object]]):
    """Tracker that collects a bounded list of unique objects.

    The collection will retain at most ``MAX_SIZE`` items; oldest items are
    dropped when the capacity is exceeded.
    """

    MAX_SIZE = 100

    def __init__(
        self,
        value: list[object] | None = None,
        reset_callback: Callable[[], list[object]] | None = None,
        *,
        exclude_paused: bool = False,
    ) -> None:
        super().__init__(
            list(value) if value is not None else [],
            reset_callback,
            exclude_paused=exclude_paused,
        )

    def reset(self) -> None:
        """Reset the collection to the callback-provided list or clear it."""
        if self.reset_callback is not None:
            self.tracked_value = self.reset_callback()
            if not isinstance(self.tracked_value, list):
                logger.info("CollectionTracker reset to invalid type")
                self.tracked_value = []
        else:
            self.tracked_value.clear()

    def update(self, value: object) -> None:
        """Add a new unique value to the collection, trimming to MAX_SIZE.

        Values already present are ignored.
        """
        if value in self.tracked_value:
            return
        if self.history.tracking_enabled(self.exclude_paused):
            self.tracked_value.append(value)
            if len(self.tracked_value) > self.MAX_SIZE:
                self.tracked_value.pop(0)

    def has_totals(self) -> bool:
        """Return False to indicate collections do not report numeric totals."""
        return False


class TrackingStrategy(ExtendedEnum):
    """Enumeration of available tracking strategies.

    Use :meth:`get_tracker` to obtain a concrete :class:`FieldTracker` for the
    given strategy.

    """

    BASIC = 1
    DELTA = 2
    ACCUMULATE = 3
    AVERAGE = 4
    MAXIMUM = 5
    MINIMUM = 6
    COLLECT = 7

    def get_tracker(self, **kwargs: object) -> FieldTracker:
        trackers: dict[TrackingStrategy, type[FieldTracker]] = {
            TrackingStrategy.BASIC: BasicTracker,
            TrackingStrategy.DELTA: DeltaTracker,
            TrackingStrategy.ACCUMULATE: CumulativeTracker,
            TrackingStrategy.AVERAGE: AveragingTracker,
            TrackingStrategy.MAXIMUM: MaximumTracker,
            TrackingStrategy.MINIMUM: MinimumTracker,
            TrackingStrategy.COLLECT: CollectionTracker,
        }
        return trackers[self](**kwargs)


class HistoryFieldData:
    """Container describing a single history field and its reporting rules.

    Instances encapsulate provider identity, description, reporting strategy
    and formatting/precision options.

    """

    def __init__(
        self,
        field_name: str,
        provider: str,
        desc: str,
        strategy: str,
        units: str | None = None,
        reset_callback: Callable[[], _T] | None = None,
        *,
        exclude_paused: bool = False,
        report_total: bool = False,
        report_maximum: bool = False,
        precision: int | None = None,
    ) -> None:
        self._name = field_name
        self._provider = provider
        self._desc = desc
        self._strategy = TrackingStrategy.from_string(strategy)
        self._units = units
        self._tracker = self._strategy.get_tracker(
            reset_callback=reset_callback,
            exclude_paused=exclude_paused,
        )
        self._report_total = report_total
        self._report_maximum = report_maximum
        """Initialize a HistoryFieldData descriptor.

        Args:
            field_name: Name of the field.
            provider: Provider identifier string.
            desc: Short description of the field.
            strategy: Tracking strategy name.
            units: Optional units string.
            reset_callback: Optional callable to initialize tracker state.
            exclude_paused: When True, exclude paused periods from tracking.
            report_total: When True, include a running total in reports.
            report_maximum: When True, include a running maximum in reports.
            precision: Optional rounding precision for reported values.

        """
        self._precision = precision

    @property
    def name(self) -> str:
        return self._name

    @property
    def provider(self) -> str:
        return self._provider

    @property
    def tracker(self) -> FieldTracker:
        return self._tracker

    def __eq__(self, value: object) -> bool:
        """Compare two HistoryFieldData objects for provider and name equality.

        Raises:
            ValueError: If compared with a non-HistoryFieldData object.

        """
        if isinstance(value, HistoryFieldData):
            return value._provider == self._provider and value._name == self._name
        msg = "Invalid type for comparison"
        raise ValueError(msg)

    def get_configuration(self) -> dict[str, str | bool | int | None]:
        return {
            "field": self._name,
            "provider": self._provider,
            "description": self._desc,
            "strategy": self._strategy.name.lower(),
            "units": self._units,
            "init_tracker": self._tracker.reset_callback is not None,
            "exclude_paused": self._tracker.exclude_paused,
            "report_total": self._report_total,
            "report_maximum": self._report_maximum,
            "precision": self._precision,
        }

    def as_dict(self) -> dict[str, str | object | None]:
        val = self._tracker.get_tracked_value()
        if self._precision is not None and isinstance(val, float):
            val = round(val, self._precision)
        return {
            "provider": self._provider,
            "name": self.name,
            "value": val,
            "description": self._desc,
            "units": self._units,
        }

    def has_totals(self) -> bool:
        return self._tracker.has_totals() and (
            self._report_total or self._report_maximum
        )

    def get_totals(
        self,
        last_totals: list[dict[str, str | float | None]],
        *,
        reset: bool = False,
    ) -> dict[str, str | float | None]:
        if not self.has_totals():
            return {}
        if reset:
            maximum: float | None = 0 if self._report_maximum else None
            total: float | None = 0 if self._report_total else None
        else:
            cur_val: float | int = self._tracker.get_tracked_value()
            maximum = cur_val if self._report_maximum else None
            total = cur_val if self._report_total else None
            for obj in last_totals:
                if obj["provider"] == self._provider and obj["field"] == self._name:
                    if maximum is not None:
                        maximum = max(cur_val, obj["maximum"] or 0)
                    if total is not None:
                        total = cur_val + (obj["total"] or 0)
                    break
            if self._precision is not None:
                if maximum is not None:
                    maximum = round(maximum, self._precision)
                if total is not None:
                    total = round(total, self._precision)
        return {
            "provider": self._provider,
            "field": self._name,
            "maximum": maximum,
            "total": total,
        }


class SqlTableDefType(type):
    """Metaclass used to validate SQL table definition classes.

    Ensures subclasses provide required class attributes and a valid SQL
    prototype string at class creation time.

    """

    def __new__(
        metacls,
        clsname: str,
        bases: tuple[type, ...],
        cls_attrs: dict[str, object],
    ) -> Self:
        if clsname != "SqlTableDefinition":
            for item in ("name", "prototype"):
                if not cls_attrs[item]:
                    msg = f"Class attribute `{item}` must be set for class {clsname}"
                    raise ValueError(
                        msg,
                    )
            if cls_attrs["version"] < 1:
                msg = f"The 'version' attribute of {clsname} must be greater than 0"
                raise ValueError(
                    msg,
                )
            cls_attrs["prototype"] = inspect.cleandoc(cls_attrs["prototype"].strip())
            prototype = cls_attrs["prototype"]
            proto_match = re.match(
                r"([a-zA-Z][0-9a-zA-Z_-]+)\s*\((.+)\)\s*;?$",
                prototype,
                re.DOTALL,
            )
            if proto_match is None:
                msg = f"Invalid SQL Table prototype:\n{prototype}"
                raise ValueError(msg)
            table_name = cls_attrs["name"]
            parsed_name = proto_match.group(1)
            if table_name != parsed_name:
                msg = (
                    f"Table name '{table_name}' does not match parsed name from "
                    f"table prototype '{parsed_name}'"
                )
                raise ValueError(
                    msg,
                )
        return super().__new__(metacls, clsname, bases, cls_attrs)


class SqlTableDefinition(metaclass=SqlTableDefType):
    """Base class for SQL table definitions.

    Concrete subclasses must set ``name``, ``version`` and ``prototype`` class
    attributes. Instances are not intended to be created directly.

    """

    name: str = ""
    version: int = 0
    prototype: str = ""

    def __init__(self) -> None:
        """Prevent direct instantiation of the base SqlTableDefinition.

        Subclasses should implement concrete definitions and may be instantiated
        normally.
        """
        if self.__class__ == SqlTableDefinition:
            msg = "Cannot directly instantiate SqlTableDefinition"
            raise ServerError(msg)

    def migrate(
        self,
        last_version: int,
        db_provider: DBProviderWrapper,
    ) -> None:
        msg = "Children must implement migrate"
        raise NotImplementedError(msg)


class EventLoop:
    """A thin wrapper around asyncio's event loop providing helpers used by Moonraker.

    This class centralizes event-loop operations so the rest of the codebase can
    interact with either the system event loop or a uvloop-based loop through a
    stable interface.
    """

    UVLOOP_ENABLED = _uvl_enabled
    TimeoutError = asyncio.TimeoutError

    def __init__(self) -> None:
        self.reset()

    @property
    def asyncio_loop(self) -> AbstractEventLoop:
        return self.aioloop

    def reset(self) -> None:
        """Bind the wrapper to the currently running asyncio loop.

        This grabs commonly used loop helpers (create_task, call_later, etc.) and
        stores them as attributes on this wrapper to simplify access elsewhere.
        """
        self.aioloop = asyncio.get_running_loop()
        self.bg_tasks: set[asyncio.Task] = set()
        self.add_signal_handler = self.aioloop.add_signal_handler
        self.remove_signal_handler = self.aioloop.remove_signal_handler
        self.add_reader = self.aioloop.add_reader
        self.add_writer = self.aioloop.add_writer
        self.remove_reader = self.aioloop.remove_reader
        self.remove_writer = self.aioloop.remove_writer
        self.get_loop_time = self.aioloop.time
        self.create_future = self.aioloop.create_future
        self.call_at = self.aioloop.call_at
        self.set_debug = self.aioloop.set_debug
        self.is_running = self.aioloop.is_running

    def create_task(self, coro: asyncio._CoroutineLike[_T]) -> asyncio.Task[_T]:
        """Schedule a coroutine as a background task and track it.

        The created Task is kept in :pyattr:`bg_tasks` until completion to allow
        for introspection and cleanup.
        """
        tsk = self.aioloop.create_task(coro)
        self.bg_tasks.add(tsk)
        tsk.add_done_callback(self.bg_tasks.discard)
        return tsk

    def _create_new_loop(self) -> asyncio.AbstractEventLoop:
        """Create and set a fresh asyncio event loop.

        The implementation retries a few times to work around transient
        platform issues that occasionally produce closed loops.
        """
        for _ in range(5):
            # Sometimes the new loop does not properly instantiate.
            # Give 5 attempts before raising an exception
            new_loop = asyncio.new_event_loop()
            if not new_loop.is_closed():
                break
            logger.info("Failed to create open eventloop, retrying in .5 seconds...")
            time.sleep(0.5)
        else:
            msg = "Unable to create new open eventloop"
            raise RuntimeError(msg)
        asyncio.set_event_loop(new_loop)
        return new_loop

    def register_callback(
        self,
        callback: FlexCallback,
        *args: object,
        **kwargs: object,
    ) -> None:
        """Schedule a synchronous or asynchronous callback to run on the loop.

        The provided callable may return an awaitable; if so it will be awaited.
        Any exceptions raised by the callback are caught and logged.
        """

        async def _wrapper() -> None:
            try:
                ret = callback(*args, **kwargs)
                if inspect.isawaitable(ret):
                    await ret
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Error Running Callback")

        self.create_task(_wrapper())

    def delay_callback(
        self,
        delay: float,
        callback: FlexCallback,
        *args: object,
        **kwargs: object,
    ) -> asyncio.TimerHandle:
        """Schedule a callback to run after ``delay`` seconds.

        The callback will be executed via :meth:`register_callback` so it may
        be synchronous or async.
        """
        return self.aioloop.call_later(
            delay,
            self.register_callback,
            functools.partial(callback, *args, **kwargs),
        )

    def register_timer(self, callback: TimerCallback) -> FlexTimer:
        """Create a recurring FlexTimer bound to this EventLoop.

        The returned FlexTimer can be started and stopped independently.
        """
        return FlexTimer(self, callback)

    def run_in_thread(
        self,
        callback: Callable[..., _T],
        *args: object,
    ) -> Awaitable[_T]:
        """Run a blocking function in a thread pool and return an awaitable.

        This is a convenience wrapper around ``loop.run_in_executor``.
        """
        return self.aioloop.run_in_executor(None, callback, *args)

    async def create_socket_connection(
        self,
        address: tuple[str, int],
        timeout: float | None = None,
    ) -> socket.socket:
        host, port = address
        """
        async port of socket.create_connection()
        """
        loop = self.aioloop
        err = None
        ainfo = await loop.getaddrinfo(
            host,
            port,
            family=0,
            type=socket.SOCK_STREAM,
        )
        for res in ainfo:
            af, socktype, proto, _cannon_name, _sa = res
            sock = None
            try:
                sock = socket.socket(af, socktype, proto)
                sock.settimeout(0)
                sock.setblocking(blocking=False)
                await asyncio.wait_for(
                    loop.sock_connect(sock, (host, port)),
                    timeout,
                )
                # Break explicitly a reference cycle
                err = None
            except (TimeoutError, OSError) as _:
                err = _
                if sock is not None:
                    loop.remove_writer(sock.fileno())
                    sock.close()
            else:
                return sock
        if err is not None:
            try:
                raise err
            finally:
                # Break explicitly a reference cycle
                err = None
        else:
            msg = "getaddrinfo returns an empty list"
            raise OSError(msg)

    def close(self) -> None:
        """Close the underlying asyncio event loop.

        After calling this the EventLoop wrapper should not be used without a
        subsequent call to :meth:`reset` (or creating a new wrapper instance).
        """
        self.aioloop.close()


class FlexTimer:
    """A flexible recurring timer that schedules callbacks on an EventLoop.

    The timer repeatedly calls a user-supplied callback which returns the next
    scheduled loop time. The callback may be synchronous or return an
    awaitable.
    """

    def __init__(
        self,
        eventloop: EventLoop,
        callback: TimerCallback,
    ) -> None:
        self.eventloop = eventloop
        self.callback = callback
        self.timer_handle: asyncio.TimerHandle | None = None
        self.timer_task: asyncio.Task | None = None
        self.running: bool = False

    def in_callback(self) -> bool:
        """Return True if the timer's callback is actively executing.

        This is useful to avoid re-entrancy when starting or stopping the
        timer from within the callback itself.
        """
        return self.timer_task is not None and not self.timer_task.done()

    def start(self, delay: float = 0.0) -> None:
        """Start the timer, scheduling the first callback after ``delay`` seconds.

        If the timer is already running this is a no-op.
        """
        if self.running:
            return
        self.running = True
        if self.in_callback():
            return
        call_time = self.eventloop.get_loop_time() + delay
        self.timer_handle = self.eventloop.call_at(call_time, self._schedule_task)

    def stop(self) -> None:
        """Stop the timer and cancel any scheduled callback.

        The currently executing callback (if any) will be allowed to finish.
        """
        if not self.running:
            return
        self.running = False
        if self.timer_handle is not None:
            self.timer_handle.cancel()
            self.timer_handle = None

    async def wait_timer_done(self) -> None:
        """Wait until the currently executing timer callback completes.

        If no callback is running this returns immediately.
        """
        if self.timer_task is None:
            return
        await self.timer_task

    def _schedule_task(self) -> None:
        """Schedule the timer callback to run as a background task."""
        self.timer_handle = None
        self.timer_task = self.eventloop.create_task(self._call_wrapper())

    def is_running(self) -> bool:
        """Return True when the timer is active (started but not stopped)."""
        return self.running

    async def _call_wrapper(self) -> None:
        """Invoke the user callback and schedule the next run.

        The user's callback should return the next absolute loop time (as a
        float) at which the timer should run again. If the callback raises an
        exception the timer will be stopped and the exception propagated.
        """
        if not self.running:
            return
        try:
            ret = self.callback(self.eventloop.get_loop_time())
            if isinstance(ret, Awaitable):
                ret = await ret
        except Exception:
            self.running = False
            raise
        finally:
            self.timer_task = None
        if self.running:
            self.timer_handle = self.eventloop.call_at(ret, self._schedule_task)
