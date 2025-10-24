"""Type definitions for Moonraker server.

This module provides type-safe protocols and TypedDicts for component
management and server configuration.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, TypedDict

if TYPE_CHECKING:
    from typing import Any, Coroutine


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
