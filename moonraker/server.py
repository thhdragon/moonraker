#!/usr/bin/env python3
# Moonraker - HTTP/Websocket API Server for Klipper
#
# Copyright (C) 2020 Eric Callahan <arksine.code@gmail.com>
#
# This file may be distributed under the terms of the GNU GPLv3 license

from __future__ import annotations

import argparse
import asyncio
import importlib
import io
import logging
import os
import pathlib
import signal
import socket
import sys
import time
import traceback
import uuid

# Annotation imports
from typing import (
    TYPE_CHECKING,
    Protocol,
    TypedDict,
    TypeVar,
)

from . import helper as confighelper
from .common import RequestType
from .common import EventLoop
from .helper import LogManager
from .utils import (
    Sentinel,
    ServerError,
    get_software_info,
    json_wrapper,
    pip_utils,
    source_info,
)


class Component(Protocol):
    """Protocol for server components.

    Components may optionally implement component_init, on_exit, or close methods.
    """

    def component_init(self) -> Coroutine[Any, Any, None] | None:
        """Initialize component asynchronously. Optional method."""
        ...

    def on_exit(self) -> Coroutine[Any, Any, None] | None:
        """Handle component exit. Optional method."""
        ...

    def close(self) -> Coroutine[Any, Any, None] | None:
        """Close component resources. Optional method."""
        ...


class AppArgs(TypedDict, total=False):
    """Application arguments passed to the server."""

    data_path: str
    is_default_data_path: bool
    config_file: str
    backup_config: str | None
    startup_warnings: list[str]
    verbose: bool
    debug: bool
    asyncio_debug: bool
    is_backup_config: bool
    is_python_package: bool
    instance_uuid: str
    unix_socket_path: str
    structured_logging: bool
    log_file: str
    python_version: str
    launch_args: str
    msgspec_enabled: bool
    uvloop_enabled: bool
    software_version: str
    software_branch: str
    git_version: str
    git_branch: str
    official_release: bool
    unofficial_components: list[str]


class HostInfo(TypedDict):
    """Information about the server host."""

    hostname: str
    address: str
    port: int
    ssl_port: int


class KlippyInfo(TypedDict, total=False):
    """Information about Klippy (dynamically populated with printer objects)."""

    state: str | None


class ServerInfo(TypedDict):
    """Server information response."""

    klippy_connected: bool
    klippy_state: str
    components: list[str]
    failed_components: list[str]
    registered_directories: list[str]
    warnings: list[str]
    websocket_count: int
    moonraker_version: str
    missing_klippy_requirements: list[str]
    api_version: tuple[int, int, int]
    api_version_string: str


class ConfigFile(TypedDict):
    """Configuration file information."""

    filename: str
    sections: list[str]


class ConfigResponse(TypedDict):
    """Configuration response."""

    config: dict[str, Any]
    orig: dict[str, Any]
    files: list[ConfigFile]


if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine
    from typing import Any

    from .common import WebRequest
    from .components.application import MoonrakerApp
    from .components.extensions import ExtensionManager
    from .components.file_manager.file_manager import FileManager
    from .components.klippy_connection import KlippyConnection
    from .components.machine import Machine
    from .components.websockets import WebsocketManager

    FlexCallback = Callable[..., Coroutine | None]
    _T = TypeVar("_T", Sentinel, Component)


# Module-level logger
logger = logging.getLogger(__name__)

API_VERSION = (1, 5, 0)
SERVER_COMPONENTS = ["application", "websockets", "klippy_connection"]
CORE_COMPONENTS = [
    "dbus_manager",
    "database",
    "file_manager",
    "authorization",
    "klippy_apis",
    "machine",
    "data_store",
    "shell_command",
    "proc_stats",
    "job_state",
    "job_queue",
    "history",
    "http_client",
    "announcements",
    "webcam",
    "extensions",
]


class Server:
    """The main Moonraker server class. Manages components, events, & server lifecycle.

    This class handles the initialization, configuration, and operation of the server,
    including loading components, managing websockets, and interfacing with Klipper.
    """

    error = ServerError
    config_error = confighelper.ConfigError

    def __init__(
        self,
        args: AppArgs,
        log_manager: LogManager,
        event_loop: EventLoop,
    ) -> None:
        """Initialize the Server instance.

        Parameters
        ----------
        args : AppArgs
            Application arguments passed to the server.
        log_manager : LogManager
            The log manager for handling logging.
        event_loop : EventLoop
            The event loop for asynchronous operations.

        """
        self.event_loop = event_loop
        self.log_manager = log_manager
        self.app_args = args
        self.events: dict[str, list[FlexCallback]] = {}
        self.components: dict[str, Component] = {}
        self.failed_components: list[str] = []
        self.warnings: dict[str, str] = {}
        self._is_configured: bool = False

        self.config = config = self._parse_config()
        self.host: str = config.get("host", "127.0.0.1")
        self.port: int = config.getint("port", 7125)
        self.ssl_port: int = config.getint("ssl_port", 7130)
        self.exit_reason: str = ""
        self.server_running: bool = False
        self.app_running_evt = asyncio.Event()
        self.pip_recovery_attempted: bool = False

        # Configure Debug Logging
        config.getboolean("enable_debug_logging", False, deprecate=True)
        self.debug = args["debug"]
        log_level = logging.DEBUG if args["verbose"] else logging.INFO
        logger.setLevel(log_level)
        self.event_loop.set_debug(args["asyncio_debug"])
        self.klippy_connection: KlippyConnection
        self.klippy_connection = self.load_component(config, "klippy_connection")

        # Tornado Application/Server
        self.moonraker_app: MoonrakerApp = self.load_component(config, "application")
        app = self.moonraker_app
        self.register_endpoint = app.register_endpoint
        self.register_debug_endpoint = app.register_debug_endpoint
        self.register_static_file_handler = app.register_static_file_handler
        self.register_upload_handler = app.register_upload_handler
        self.log_manager.set_server(self)
        self.websocket_manager: WebsocketManager
        self.websocket_manager = self.load_component(config, "websockets")

        for warning in args.get("startup_warnings", []):
            self.add_warning(warning)

        self.register_endpoint(
            "/server/info",
            RequestType.GET,
            self._handle_info_request,
        )
        self.register_endpoint(
            "/server/config",
            RequestType.GET,
            self._handle_config_request,
        )
        self.register_endpoint(
            "/server/restart",
            RequestType.POST,
            self._handle_server_restart,
        )
        self.register_notification("server:klippy_ready")
        self.register_notification("server:klippy_shutdown")
        self.register_notification("server:klippy_disconnect", "klippy_disconnected")
        self.register_notification("server:gcode_response")

    def get_app_args(self) -> AppArgs:
        """Get a copy of the application arguments.

        Returns
        -------
        AppArgs
            The application arguments.

        """
        return dict(self.app_args)

    def get_app_arg(self, key: str, default=Sentinel.MISSING) -> object:
        """Get a specific application argument.

        Parameters
        ----------
        key : str
            The key of the argument to retrieve.
        default : object, optional
            The default value if the key is not found.

        Returns
        -------
        object
            The value of the argument.

        Raises
        ------
        KeyError
            If the key is not found and no default is provided.

        """
        val = self.app_args.get(key, default)
        if val is Sentinel.MISSING:
            msg = f"No key '{key}' in Application Arguments"
            raise KeyError(msg)
        return val

    def get_event_loop(self) -> EventLoop:
        """Get the event loop instance.

        Returns
        -------
        EventLoop
            The event loop used by the server.

        """
        return self.event_loop

    def get_api_version(self) -> tuple[int, int, int]:
        """Get the API version of the server.

        Returns
        -------
        tuple[int, int, int]
            The API version as a tuple (major, minor, patch).

        """
        return API_VERSION

    def get_warnings(self) -> list[str]:
        """Get the list of current warnings.

        Returns
        -------
        list[str]
            A list of warning messages.

        """
        return list(self.warnings.values())

    def is_running(self) -> bool:
        """Check if the server is currently running.

        Returns
        -------
        bool
            True if the server is running, False otherwise.

        """
        return self.server_running

    def is_configured(self) -> bool:
        """Check if the server has been configured.

        Returns
        -------
        bool
            True if the server is configured, False otherwise.

        """
        return self._is_configured

    def is_debug_enabled(self) -> bool:
        """Check if debug mode is enabled.

        Returns
        -------
        bool
            True if debug is enabled, False otherwise.

        """
        return self.debug

    def is_verbose_enabled(self) -> bool:
        """Check if verbose logging is enabled.

        Returns
        -------
        bool
            True if verbose logging is enabled, False otherwise.

        """
        return self.app_args["verbose"]

    def _parse_config(self) -> confighelper.ConfigHelper:
        """Parse the server configuration.

        Returns
        -------
        confighelper.ConfigHelper
            The parsed configuration helper.

        """
        config = confighelper.get_configuration(self, self.app_args)
        # log config file
        cfg_files = "\n".join(config.get_config_files())
        strio = io.StringIO()
        config.write_config(strio)
        cfg_item = f"\n{'#' * 20} Moonraker Configuration {'#' * 20}\n\n"
        cfg_item += strio.getvalue()
        cfg_item += "#" * 65
        cfg_item += f"\nAll Configuration Files:\n{cfg_files}\n"
        cfg_item += "#" * 65
        strio.close()
        self.add_log_rollover_item("config", cfg_item)
        return config

    async def server_init(self, *, start_server: bool = True) -> None:
        """Initialize the server asynchronously.

        Parameters
        ----------
        start_server : bool, optional
            Whether to start the server after initialization. Default is True.

        """
        self.event_loop.add_signal_handler(signal.SIGTERM, self._handle_term_signal)

        # Perform asynchronous init after the event loop starts
        optional_comps: list[Coroutine] = []
        for name, component in self.components.items():
            if not hasattr(component, "component_init"):
                continue
            if name in CORE_COMPONENTS:
                # Process core components in order synchronously
                await self._initialize_component(name, component)
            else:
                optional_comps.append(self._initialize_component(name, component))

        # Asynchronous Optional Component Initialization
        if optional_comps:
            await asyncio.gather(*optional_comps)

        # Wait until all components are initialized to start the file
        # observer.  This allows other components to register gcode file
        # processors before metadata is processed for gcode files that
        # do not have a metadata entry.
        file_manager: FileManager = self.lookup_component("file_manager")
        file_manager.start_file_observer()

        if not self.warnings:
            await self.event_loop.run_in_thread(self.config.create_backup)

        machine: Machine = self.lookup_component("machine")
        restarting = await machine.validate_installation()
        if not restarting and start_server:
            await self.start_server()

    async def start_server(self, *, connect_to_klippy: bool = True) -> None:
        """Start the server.

        Parameters
        ----------
        connect_to_klippy : bool, optional
            Whether to connect to Klippy. Default is True.

        """
        # Open Unix Socket Server
        extm: ExtensionManager = self.lookup_component("extensions")
        await extm.start_unix_server()

        # Start HTTP Server
        logger.info(
            "Starting Moonraker on (%s, %s), Hostname: %s",
            self.host,
            self.port,
            socket.gethostname(),
        )
        self.moonraker_app.listen(self.host, self.port, self.ssl_port)
        self.server_running = True
        if connect_to_klippy:
            self.klippy_connection.connect()

    async def run_until_exit(self) -> None:
        """Run the server until exit is requested."""
        await self.app_running_evt.wait()

    def add_log_rollover_item(
        self,
        name: str,
        item: str,
        *,
        log: bool = True,
    ) -> None:
        """Add an item to the log rollover.

        Parameters
        ----------
        name : str
            The name of the item.
        item : str
            The item content.
        log : bool, optional
            Whether to log the item. Default is True.

        """
        self.log_manager.set_rollover_info(name, item)
        if log and item is not None:
            logger.info(item)

    def add_warning(
        self,
        warning: str,
        warn_id: str | None = None,
        *,
        log: bool = True,
        exc_info: BaseException | None = None,
    ) -> str:
        """Add a warning.

        Parameters
        ----------
        warning : str
            The warning message.
        warn_id : str or None, optional
            The warning ID. If None, an ID is generated.
        log : bool, optional
            Whether to log the warning. Default is True.
        exc_info : BaseException or None, optional
            Exception info to include in the log.

        Returns
        -------
        str
            The warning ID.

        """
        if warn_id is None:
            warn_id = str(id(warning))
        self.warnings[warn_id] = warning
        if log:
            logger.warning(warning, exc_info=exc_info)
        return warn_id

    def remove_warning(self, warn_id: str) -> None:
        """Remove a warning by ID.

        Parameters
        ----------
        warn_id : str
            The warning ID to remove.

        """
        self.warnings.pop(warn_id, None)

    # ***** Component Management *****
    async def _initialize_component(self, name: str, component: Component) -> None:
        """Initialize a component asynchronously.

        Parameters
        ----------
        name : str
            The component name.
        component : Component
            The component instance.

        """
        logger.info("Performing Component Post Init: [%s]", name)
        try:
            ret = component.component_init()
            if ret is not None:
                await ret
        except Exception as e:
            self.add_warning(
                f"Component '{name}' failed to load with error: {e}",
                exc_info=e,
            )
            self.set_failed_component(name)

    def load_components(self) -> None:
        """Load all server components."""
        config = self.config
        cfg_sections = {s.split()[0] for s in config.sections()}
        cfg_sections.remove("server")

        # load database to initialize saved state
        self.load_component(config, "database")
        self.klippy_connection.load_saved_state()

        # load core components
        for component in CORE_COMPONENTS:
            self.load_component(config, component)
            cfg_sections.discard(component)

        # load remaining optional components
        for section in cfg_sections:
            self.load_component(config, section, None)

        config.validate_config()
        self._is_configured = True

    def load_component(
        self,
        config: confighelper.ConfigHelper,
        component_name: str,
        default: _T = Sentinel.MISSING,
    ) -> _T | Component:
        """Load a specific component.

        Parameters
        ----------
        config : confighelper.ConfigHelper
            The configuration helper.
        component_name : str
            The name of the component to load.
        default : _T, optional
            The default value if loading fails.

        Returns
        -------
        _T | Component
            The loaded component.

        Raises
        ------
        ServerError
            If the component fails to load and no default is provided.

        """
        if component_name in self.components:
            return self.components[component_name]
        if self.is_configured():
            msg = "Cannot load components after configuration"
            raise self.error(
                msg,
                500,
            )
        if component_name in self.failed_components:
            msg = f"Component {component_name} previously failed to load"
            raise self.error(
                msg,
                500,
            )
        full_name = f"moonraker.components.{component_name}"
        try:
            module = importlib.import_module(full_name)
            # Server components use the [server] section for configuration
            if component_name not in SERVER_COMPONENTS:
                is_core = component_name in CORE_COMPONENTS
                fallback: str | None = "server" if is_core else None
                config = config.getsection(component_name, fallback)
            load_func = module.load_component
            component = load_func(config)
        except Exception as e:
            ucomps: list[str] = self.app_args.get("unofficial_components", [])
            if (
                isinstance(e, ModuleNotFoundError)
                and full_name != e.name
                and component_name not in ucomps
            ) and self.try_pip_recovery(e.name or "unknown"):
                return self.load_component(config, component_name, default)
            logger.exception("Unable to load component: (%s)", component_name)
            if component_name not in self.failed_components:
                self.failed_components.append(component_name)
            if default is Sentinel.MISSING:
                raise
            return default
        self.components[component_name] = component
        logger.info("Component (%s) loaded", component_name)
        return component

    def try_pip_recovery(self, missing_module: str) -> bool:
        """Try recovery from missing module by updating pip and installing dependencies.

        Parameters
        ----------
        missing_module : str
            The name of the missing module.

        Returns
        -------
        bool
            True if recovery was successful, False otherwise.

        """
        if self.pip_recovery_attempted:
            return False
        self.pip_recovery_attempted = True
        src_dir = source_info.source_path()
        req_file = src_dir.joinpath("scripts/moonraker-requirements.txt")
        if not req_file.is_file():
            return False
        pip_cmd = f"{sys.executable} -m pip"
        pip_exec = pip_utils.PipExecutor(pip_cmd, logger.info)
        logger.info("Module '%s' not found. Attempting Pip Update...", missing_module)
        logger.info("Checking Pip Version...")
        try:
            pipver = pip_exec.get_pip_version()
            if pipver.needs_pip_update:
                cur_ver = pipver.pip_version_string
                new_ver = pipver.max_pip_version_string
                logger.info("Updating Pip from %s to %s...", cur_ver, new_ver)
                pip_exec.update_pip()
        except Exception:
            logger.exception("Pip version check failed")
            return False
        logger.info("Installing Moonraker python dependencies...")
        try:
            pip_exec.install_packages(req_file, {"SKIP_CYTHON": "Y"})
        except Exception:
            logger.exception("Failed to install python packages")
            return False
        return True

    def lookup_component(
        self,
        component_name: str,
        default: _T = Sentinel.MISSING,
    ) -> _T | Component:
        """Look up a loaded component.

        Parameters
        ----------
        component_name : str
            The name of the component.
        default : _T, optional
            The default value if not found.

        Returns
        -------
        _T | Component
            The component instance.

        Raises
        ------
        ServerError
            If the component is not found and no default is provided.

        """
        component = self.components.get(component_name, default)
        if component is Sentinel.MISSING:
            msg = f"Component ({component_name}) not found"
            raise ServerError(msg)
        return component

    def set_failed_component(self, component_name: str) -> None:
        """Mark a component as failed.

        Parameters
        ----------
        component_name : str
            The name of the failed component.

        """
        if component_name not in self.failed_components:
            self.failed_components.append(component_name)

    def register_component(self, component_name: str, component: Component) -> None:
        """Register a component.

        Parameters
        ----------
        component_name : str
            The name of the component.
        component : Component
            The component instance.

        Raises
        ------
        ServerError
            If the component is already registered.

        """
        if component_name in self.components:
            msg = f"Component '{component_name}' already registered"
            raise self.error(msg)
        self.components[component_name] = component

    def register_notification(
        self,
        event_name: str,
        notify_name: str | None = None,
    ) -> None:
        """Register a notification for an event.

        Parameters
        ----------
        event_name : str
            The event name.
        notify_name : str or None, optional
            The notification name. If None, uses the event name.

        """
        self.websocket_manager.register_notification(event_name, notify_name)

    def register_event_handler(
        self,
        event: str,
        callback: FlexCallback,
    ) -> None:
        """Register an event handler.

        Parameters
        ----------
        event : str
            The event name.
        callback : FlexCallback
            The callback function.

        """
        self.events.setdefault(event, []).append(callback)

    def send_event(self, event: str, *args) -> asyncio.Future:
        """Send an event to registered handlers.

        Parameters
        ----------
        event : str
            The event name.
        *args
            Arguments to pass to the handlers.

        Returns
        -------
        asyncio.Future
            A future representing the event processing.

        """
        fut = self.event_loop.create_future()
        self.event_loop.register_callback(self._process_event, fut, event, *args)
        return fut

    async def _process_event(
        self,
        fut: asyncio.Future,
        event: str,
        *args,
    ) -> None:
        """Process an event by calling registered handlers.

        Parameters
        ----------
        fut : asyncio.Future
            The future to set when done.
        event : str
            The event name.
        *args
            Arguments to pass to the handlers.

        """
        events = self.events.get(event, [])
        coroutines: list[Coroutine] = []
        for func in events:
            try:
                ret = func(*args)
            except Exception:
                logger.exception("Error processing callback in event %s", event)
            else:
                if ret is not None:
                    coroutines.append(ret)
        if coroutines:
            results = await asyncio.gather(*coroutines, return_exceptions=True)
            for val in results:
                if isinstance(val, Exception):
                    exc_info = "".join(traceback.format_exception(val))
                    logger.error(
                        "Error processing callback in event %s\n%s",
                        event,
                        exc_info,
                    )
        if not fut.done():
            fut.set_result(None)

    def register_remote_method(
        self,
        method_name: str,
        cb: FlexCallback,
    ) -> None:
        """Register a remote method.

        Parameters
        ----------
        method_name : str
            The method name.
        cb : FlexCallback
            The callback function.

        """
        self.klippy_connection.register_remote_method(method_name, cb)

    def get_host_info(self) -> HostInfo:
        """Get host information.

        Returns
        -------
        HostInfo
            A dictionary with host details.

        """
        return {
            "hostname": socket.gethostname(),
            "address": self.host,
            "port": self.port,
            "ssl_port": self.ssl_port,
        }

    def get_klippy_info(self) -> dict[str, object]:
        """Get Klippy information.

        Returns
        -------
        dict[str, object]
            A dictionary with Klippy details.

        """
        return self.klippy_connection.klippy_info

    def _handle_term_signal(self) -> None:
        """Handle the SIGTERM signal."""
        logger.info("Exiting with signal SIGTERM")
        self.event_loop.register_callback(self._stop_server, "terminate")

    def restart(self, delay: float | None = None) -> None:
        """Restart the server.

        Parameters
        ----------
        delay : float or None, optional
            Delay in seconds before restarting. If None, restart immediately.

        """
        if delay is None:
            self.event_loop.register_callback(self._stop_server)
        else:
            self.event_loop.delay_callback(delay, self._stop_server)

    async def _stop_server(self, exit_reason: str = "restart") -> None:
        """Stop the server.

        Parameters
        ----------
        exit_reason : str, optional
            The reason for stopping. Default is "restart".

        """
        self.server_running = False
        # Call each component's "on_exit" method
        for name, component in self.components.items():
            if hasattr(component, "on_exit"):
                func: FlexCallback = component.on_exit
                try:
                    ret = func()
                    if ret is not None:
                        await ret
                except Exception:
                    logger.exception(
                        "Error executing 'on_exit()' for component: %s",
                        name,
                    )

        # Sleep for 100ms to allow connected websockets to write out
        # remaining data
        await asyncio.sleep(0.1)
        try:
            await self.moonraker_app.close()
            await self.websocket_manager.close()
        except Exception:
            logger.exception("Error closing application")

        # Disconnect from Klippy
        try:
            await asyncio.wait_for(
                asyncio.shield(self.klippy_connection.close(wait_closed=True)),
                2.0,
            )
        except Exception:
            logger.exception("Error disconnecting from Klippy")

        # Close all components
        for name, component in self.components.items():
            if name in ["application", "websockets", "klippy_connection"]:
                # These components have already been closed
                continue
            if hasattr(component, "close"):
                func = component.close
                try:
                    ret = func()
                    if ret is not None:
                        await ret
                except Exception:
                    logger.exception(
                        "Error executing 'close()' for component: %s",
                        name,
                    )
        # Allow cancelled tasks a chance to run in the eventloop
        await asyncio.sleep(0.001)

        self.exit_reason = exit_reason
        self.event_loop.remove_signal_handler(signal.SIGTERM)
        self.app_running_evt.set()

    async def _handle_server_restart(self, _web_request: WebRequest) -> str:
        """Handle a server restart request.

        Parameters
        ----------
        _web_request : WebRequest
            The web request.

        Returns
        -------
        str
            Response string.

        """
        self.event_loop.register_callback(self._stop_server)
        return "ok"

    async def _handle_info_request(self, web_request: WebRequest) -> ServerInfo:
        """Handle an info request.

        Parameters
        ----------
        web_request : WebRequest
            The web request.

        Returns
        -------
        ServerInfo
            The info response.

        """
        raw = web_request.get_boolean("raw", default=False)
        file_manager: FileManager | None = self.lookup_component("file_manager", None)
        reg_dirs = []
        if file_manager is not None:
            reg_dirs = file_manager.get_registered_dirs()
        mreqs = self.klippy_connection.missing_requirements
        if raw:
            warnings = list(self.warnings.values())
        else:
            warnings = [w.replace("\n", "<br/>") for w in self.warnings.values()]
        return {
            "klippy_connected": self.klippy_connection.is_connected(),
            "klippy_state": str(self.klippy_connection.state),
            "components": list(self.components.keys()),
            "failed_components": self.failed_components,
            "registered_directories": reg_dirs,
            "warnings": warnings,
            "websocket_count": self.websocket_manager.get_count(),
            "moonraker_version": self.app_args["software_version"],
            "missing_klippy_requirements": mreqs,
            "api_version": API_VERSION,
            "api_version_string": ".".join([str(v) for v in API_VERSION]),
        }

    async def _handle_config_request(self, _web_request: WebRequest) -> ConfigResponse:
        """Handle a config request.

        Parameters
        ----------
        _web_request : WebRequest
            The web request.

        Returns
        -------
        ConfigResponse
            The config response.

        """
        cfg_file_list: list[ConfigFile] = []
        cfg_parent = (
            pathlib.Path(
                self.app_args["config_file"],
            )
            .expanduser()
            .resolve()
            .parent
        )
        for fname, sections in self.config.get_file_sections().items():
            path = pathlib.Path(fname)
            try:
                rel_path = str(path.relative_to(str(cfg_parent)))
            except ValueError:
                rel_path = fname
            cfg_file_list.append({"filename": rel_path, "sections": sections})
        return {
            "config": self.config.get_parsed_config(),
            "orig": self.config.get_orig_config(),
            "files": cfg_file_list,
        }


async def launch_server(
    log_manager: LogManager,
    app_args: AppArgs,
) -> int | None:
    """Launch the server.

    Parameters
    ----------
    log_manager : LogManager
        The log manager.
    app_args : AppArgs
        Application arguments.

    Returns
    -------
    int or None
        Exit code or None for restart.

    """
    eventloop = EventLoop()
    startup_warnings: list[str] = app_args["startup_warnings"]
    try:
        server = Server(app_args, log_manager, eventloop)
        server.load_components()
    except confighelper.ConfigError as e:
        logger.exception("Server configuration error")
        backup_cfg: str | None = app_args["backup_config"]
        if app_args["is_backup_config"] or backup_cfg is None:
            return 1
        app_args["is_backup_config"] = True
        startup_warnings.append(
            f"Server configuration error: {e}\n"
            f"Loading most recent working configuration: '{backup_cfg}'\n"
            f"Please fix the issue in moonraker.conf and restart the server.",
        )
        return True
    except Exception:
        logger.exception("Fatal Moonraker error during initialization")
        return 1
    try:
        await server.server_init()
        await server.run_until_exit()
    except Exception:
        logger.exception("Fatal error during server execution")
        return 1
    if server.exit_reason == "terminate":
        return 0
    # Restore the original config and clear the warning
    # before the server restarts
    if app_args["is_backup_config"]:
        startup_warnings.pop()
        app_args["is_backup_config"] = False
    del server
    return None


def main(*, from_package: bool = True) -> None:
    """Enter the main entry point for the Moonraker server.

    Parameters
    ----------
    from_package : bool, optional
        Whether running from a package. Default is True.

    """

    def get_env_bool(key: str) -> bool:
        return os.getenv(key, "").lower() in ["y", "yes", "true"]

    # Parse start arguments
    parser = argparse.ArgumentParser(description="Moonraker - Klipper API Server")
    parser.add_argument(
        "-d",
        "--datapath",
        default=os.getenv("MOONRAKER_DATA_PATH"),
        metavar="<data path>",
        help="Location of Moonraker Data File Path",
    )
    parser.add_argument(
        "-c",
        "--configfile",
        default=os.getenv("MOONRAKER_CONFIG_PATH"),
        metavar="<configfile>",
        help="Path to Moonraker's configuration file",
    )
    parser.add_argument(
        "-l",
        "--logfile",
        default=os.getenv("MOONRAKER_LOG_PATH"),
        metavar="<logfile>",
        help="Path to Moonraker's log file",
    )
    parser.add_argument(
        "-u",
        "--unixsocket",
        default=os.getenv("MOONRAKER_UDS_PATH"),
        metavar="<unixsocket>",
        help="Path to Moonraker's unix domain socket",
    )
    parser.add_argument(
        "-n",
        "--nologfile",
        action="store_const",
        const=True,
        default=get_env_bool("MOONRAKER_DISABLE_FILE_LOG"),
        help="disable logging to a file",
    )
    parser.add_argument(
        "-s",
        "--structured-logging",
        action="store_const",
        const=True,
        default=get_env_bool("MOONRAKER_STRUCTURED_LOGGING"),
        help="Enable structured file logging",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_const",
        const=True,
        default=get_env_bool("MOONRAKER_VERBOSE_LOGGING"),
        help="Enable verbose logging",
    )
    parser.add_argument(
        "-g",
        "--debug",
        action="store_const",
        const=True,
        default=get_env_bool("MOONRAKER_ENABLE_DEBUG"),
        help="Enable Moonraker debug features",
    )
    parser.add_argument(
        "-o",
        "--asyncio-debug",
        action="store_const",
        const=True,
        default=get_env_bool("MOONRAKER_ASYNCIO_DEBUG"),
        help="Enable asyncio debug flag",
    )
    cmd_line_args = parser.parse_args()

    startup_warnings: list[str] = []
    dp: str = cmd_line_args.datapath or "~/printer_data"
    data_path = pathlib.Path(dp).expanduser().resolve()
    if not data_path.exists():
        try:
            data_path.mkdir()
        except Exception:
            startup_warnings.append(
                f"Unable to create data path folder at {data_path}",
            )
    uuid_path = data_path.joinpath(".moonraker.uuid")
    if not uuid_path.is_file():
        instance_uuid = uuid.uuid4().hex
        uuid_path.write_text(instance_uuid)
    else:
        instance_uuid = uuid_path.read_text().strip()
    if cmd_line_args.configfile is not None:
        cfg_file: str = cmd_line_args.configfile
    else:
        cfg_file = str(data_path.joinpath("config/moonraker.conf"))
    if cmd_line_args.unixsocket is not None:
        unix_sock: str = cmd_line_args.unixsocket
    else:
        comms_dir = data_path.joinpath("comms")
        if not comms_dir.exists():
            comms_dir.mkdir()
        unix_sock = str(comms_dir.joinpath("moonraker.sock"))
    misc_dir = data_path.joinpath("misc")
    if not misc_dir.exists():
        misc_dir.mkdir()
    app_args = {
        "data_path": str(data_path),
        "is_default_data_path": cmd_line_args.datapath is None,
        "config_file": cfg_file,
        "backup_config": confighelper.find_config_backup(cfg_file),
        "startup_warnings": startup_warnings,
        "verbose": cmd_line_args.verbose,
        "debug": cmd_line_args.debug,
        "asyncio_debug": cmd_line_args.asyncio_debug,
        "is_backup_config": False,
        "is_python_package": from_package,
        "instance_uuid": instance_uuid,
        "unix_socket_path": unix_sock,
        "structured_logging": cmd_line_args.structured_logging,
    }

    # Setup Logging
    app_args.update(get_software_info())
    if cmd_line_args.nologfile:
        app_args["log_file"] = ""
    elif cmd_line_args.logfile:
        app_args["log_file"] = os.path.normpath(
            os.path.expanduser(cmd_line_args.logfile),
        )
    else:
        app_args["log_file"] = str(data_path.joinpath("logs/moonraker.log"))
    app_args["python_version"] = sys.version.replace("\n", " ")
    app_args["launch_args"] = " ".join([sys.executable, *sys.argv]).strip()
    app_args["msgspec_enabled"] = json_wrapper.MSGSPEC_ENABLED
    app_args["uvloop_enabled"] = EventLoop.UVLOOP_ENABLED
    log_manager = LogManager(app_args, startup_warnings)

    # Start asyncio event loop and server
    while True:
        estatus = asyncio.run(launch_server(log_manager, app_args))
        if estatus is not None:
            break
        # Since we are running outside of the server
        # it is ok to use a blocking sleep here
        time.sleep(0.5)
        logger.info("Attempting server restart...")
    logger.info("Server shutdown")
    log_manager.stop_logging()
    sys.exit(estatus)


if __name__ == "__main__":
    import pathlib
    import sys

    pkg_parent = pathlib.Path(__file__).parent.parent
    sys.path.pop(0)
    sys.path.insert(0, str(pkg_parent))
    main(False)
