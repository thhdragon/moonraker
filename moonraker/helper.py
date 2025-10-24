# Log Management
#
# Copyright (C) 2023 Eric Callahan <arksine.code@gmail.com>
#
# This file may be distributed under the terms of the GNU GPLv3 license

from __future__ import annotations

import asyncio
import configparser
import copy
import hashlib
import logging
import logging.handlers
import os
import pathlib
import platform
import re
import sys
import threading
import time
from io import StringIO
from queue import SimpleQueue as Queue

# Annotation imports
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    TextIO,
    TypeVar,
)

from .common import RenderableTemplate, RequestType
from .utils import Sentinel
from .utils import json_wrapper as jsonw

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from .common import WebRequest
    from .components.gpio import (
        GpioEvent,
        GpioEventCallback,
        GpioFactory,
        GpioOutputPin,
    )
    from .components.klippy_connection import KlippyConnection
    from .components.template import TemplateFactory
    from .server import Server

    _T = TypeVar("_T")
    ConfigVal = None | int | float | bool | str | dict | list


class StructuredFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        record.message = record.getMessage()
        record.asctime = self.formatTime(record, self.datefmt)
        msg = record.message
        if record.exc_info:
            # Cache the traceback text to avoid converting it multiple times
            # (it's constant anyway)
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            if msg[-1:] != "\n":
                msg += "\n"
            msg += record.exc_text
        if record.stack_info:
            if msg[-1:] != "\n":
                msg += "\n"
            msg += self.formatStack(record.stack_info)
        data = {
            "time": record.asctime,
            "level": record.levelname,
            "file": record.filename,
            "function": record.funcName,
            "line": record.lineno,
            "message": msg.strip(),
        }
        return jsonw.dumps(data).decode()


# Coroutine friendly QueueHandler courtesy of Martjin Pieters:
# https://www.zopatista.com/python/2019/05/11/asyncio-logging/
class LocalQueueHandler(logging.handlers.QueueHandler):
    def emit(self, record: logging.LogRecord) -> None:
        # Removed the call to self.prepare(), handle task cancellation
        try:
            self.enqueue(record)
        except asyncio.CancelledError:
            raise
        except Exception:
            self.handleError(record)


# Timed Rotating File Handler, based on Klipper's implementation
class MoonrakerLoggingHandler(logging.handlers.TimedRotatingFileHandler):
    def __init__(self, app_args: dict[str, Any], **kwargs) -> None:
        super().__init__(app_args["log_file"], **kwargs)
        self.app_args = app_args
        self.rollover_info: dict[str, str] = {}

    def set_rollover_info(self, name: str, item: str) -> None:
        self.rollover_info[name] = item

    def doRollover(self) -> None:
        super().doRollover()
        self.write_header()

    def write_header(self) -> None:
        if self.stream is None:
            return
        if self.app_args["structured_logging"]:
            self._write_structured_header()
            return
        strtime = time.asctime(time.gmtime())
        header = f"{'-' * 20} Log Start | {strtime} {'-' * 20}\n"
        self.stream.write(header)
        self.stream.write(f"platform: {platform.platform(terse=True)}\n")
        app_section = "\n".join([f"{k}: {v}" for k, v in self.app_args.items()])
        self.stream.write(app_section + "\n")
        if self.rollover_info:
            lines = [line for line in self.rollover_info.values() if line]
            self.stream.write("\n".join(lines) + "\n")

    def _write_structured_header(self) -> None:
        msg_parts = [f"platform: {platform.platform(terse=True)}"]
        msg_parts.extend([f"{k}: {v}" for k, v in self.app_args.items()])
        if self.rollover_info:
            msg_parts.extend([line for line in self.rollover_info.values() if line])
        msg = "\n".join(msg_parts)
        record = logging.LogRecord(
            "root",
            logging.INFO,
            __file__,
            93,
            msg,
            None,
            None,
            func="write_header",
        )
        self.emit(record)


class LogManager:
    def __init__(
        self,
        app_args: dict[str, Any],
        startup_warnings: list[str],
    ) -> None:
        root_logger = logging.getLogger()
        while root_logger.hasHandlers():
            root_logger.removeHandler(root_logger.handlers[0])
        queue: Queue = Queue()
        queue_handler = LocalQueueHandler(queue)
        root_logger.addHandler(queue_handler)
        root_logger.setLevel(logging.INFO)
        stdout_hdlr = logging.StreamHandler(sys.stdout)
        stdout_fmt = logging.Formatter("[%(filename)s:%(funcName)s()] - %(message)s")
        stdout_hdlr.setFormatter(stdout_fmt)
        app_args_str = f"platform: {platform.platform(terse=True)}\n"
        app_args_str += "\n".join([f"{k}: {v}" for k, v in app_args.items()])
        sys.stdout.write(f"\nApplication Info:\n{app_args_str}\n")
        self.file_hdlr: MoonrakerLoggingHandler | None = None
        self.listener: logging.handlers.QueueListener | None = None
        log_file: str = app_args.get("log_file", "")
        if log_file:
            try:
                self.file_hdlr = MoonrakerLoggingHandler(
                    app_args,
                    when="midnight",
                    backupCount=2,
                )
                if app_args["structured_logging"]:
                    formatter: logging.Formatter = StructuredFormatter()
                else:
                    formatter = logging.Formatter(
                        "%(asctime)s [%(filename)s:%(funcName)s()] - %(message)s",
                    )
                self.file_hdlr.setFormatter(formatter)
                self.listener = logging.handlers.QueueListener(
                    queue,
                    self.file_hdlr,
                    stdout_hdlr,
                )
                self.file_hdlr.write_header()
            except Exception:
                log_file = os.path.normpath(log_file)
                dir_name = os.path.dirname(log_file)
                startup_warnings.append(
                    f"Unable to create log file at '{log_file}'. "
                    f"Make sure that the folder '{dir_name}' exists "
                    "and Moonraker has Read/Write access to the folder. ",
                )
        if self.listener is None:
            self.listener = logging.handlers.QueueListener(queue, stdout_hdlr)
        self.listener.start()

    def set_server(self, server: Server) -> None:
        self.server = server
        self.server.register_endpoint(
            "/server/logs/rollover",
            RequestType.POST,
            self._handle_log_rollover,
        )

    def set_rollover_info(self, name: str, item: str) -> None:
        if self.file_hdlr is not None:
            self.file_hdlr.set_rollover_info(name, item)

    def rollover_log(self) -> Awaitable[None]:
        if self.file_hdlr is None:
            msg = "File Logging Disabled"
            raise self.server.error(msg)
        eventloop = self.server.get_event_loop()
        return eventloop.run_in_thread(self.file_hdlr.doRollover)

    def stop_logging(self):
        if self.listener is not None:
            self.listener.stop()

    async def _handle_log_rollover(
        self,
        web_request: WebRequest,
    ) -> dict[str, Any]:
        log_apps = ["moonraker", "klipper"]
        app = web_request.get_str("application", None)
        result: dict[str, Any] = {"rolled_over": [], "failed": {}}
        if app is not None:
            if app not in log_apps:
                msg = f"Unknown application {app}"
                raise self.server.error(msg)
            log_apps = [app]
        if "moonraker" in log_apps:
            try:
                ret = self.rollover_log()
                if ret is not None:
                    await ret
            except asyncio.CancelledError:
                raise
            except Exception as e:
                result["failed"]["moonraker"] = str(e)
            else:
                result["rolled_over"].append("moonraker")
        if "klipper" in log_apps:
            kconn: KlippyConnection
            kconn = self.server.lookup_component("klippy_connection")
            try:
                await kconn.rollover_log()
            except self.server.error as e:
                result["failed"]["klipper"] = str(e)
            else:
                result["rolled_over"].append("klipper")
        return result


# Configuration Helper
#
# Copyright (C) 2020 Eric Callahan <arksine.code@gmail.com>
#
# This file may be distributed under the terms of the GNU GPLv3 license

DOCS_URL = "https://moonraker.readthedocs.io/en/latest"
CFG_ERROR_KEY = "__CONFIG_ERROR__"


class ConfigError(Exception):
    pass


class ConfigHelper:
    error = ConfigError

    def __init__(
        self,
        server: Server,
        config_source: ConfigSourceWrapper,
        section: str,
        parsed: dict[str, dict[str, ConfigVal]],
        fallback_section: str | None = None,
    ) -> None:
        self.server = server
        self.source = config_source
        self.config = config_source.get_parser()
        self.section = section
        self.fallback_section: str | None = fallback_section
        self.parsed = parsed
        if self.section not in self.parsed:
            self.parsed[self.section] = {}
        self.sections = self.config.sections
        self.has_section = self.config.has_section

    def get_server(self) -> Server:
        return self.server

    def get_source(self) -> ConfigSourceWrapper:
        return self.source

    def __getitem__(self, key: str) -> ConfigHelper:
        return self.getsection(key)

    def __contains__(self, key: str) -> bool:
        return key in self.config

    def has_option(self, option: str) -> bool:
        return self.config.has_option(self.section, option)

    def set_option(self, option: str, value: str) -> None:
        self.source.set_option(self.section, option, value)

    def get_name(self) -> str:
        return self.section

    def get_file(self) -> pathlib.Path | None:
        return self.source.find_config_file(self.section)

    def get_options(self) -> dict[str, str]:
        if self.section not in self.config:
            return {}
        return dict(self.config[self.section])

    def get_hash(self) -> hashlib._Hash:
        hash_ = hashlib.sha256()
        section = self.section
        if self.section not in self.config:
            return hash_
        for option, val in self.config[section].items():
            hash_.update(option.encode())
            hash_.update(val.encode())
        return hash_

    def get_prefix_sections(self, prefix: str) -> list[str]:
        return [s for s in self.sections() if s.startswith(prefix)]

    def getsection(
        self,
        section: str,
        fallback: str | None = None,
    ) -> ConfigHelper:
        return ConfigHelper(
            self.server,
            self.source,
            section,
            self.parsed,
            fallback,
        )

    def _get_option(
        self,
        func: Callable[..., Any],
        option: str,
        default: Sentinel | _T,
        above: float | None = None,
        below: float | None = None,
        minval: float | None = None,
        maxval: float | None = None,
        deprecate: bool = False,
    ) -> Any:
        section = self.section
        warn_fallback = False
        if self.section not in self.config and self.fallback_section is not None:
            section = self.fallback_section
            warn_fallback = True
        try:
            val = func(section, option)
        except (configparser.NoOptionError, configparser.NoSectionError) as e:
            if default is Sentinel.MISSING:
                self.parsed[self.section][CFG_ERROR_KEY] = True
                raise ConfigError(str(e)) from None
            val = default
            section = self.section
        except Exception as e:
            self.parsed[self.section][CFG_ERROR_KEY] = True
            msg = (
                f"[{self.section}]: Option '{option}' encountered the following "
                f"error while parsing: {e}"
            )
            raise ConfigError(
                msg,
            ) from e
        else:
            if deprecate:
                self.server.add_warning(
                    f"[{self.section}]: Option '{option}' is "
                    "deprecated, see the configuration documentation "
                    f"at {DOCS_URL}/configuration/",
                )
            if warn_fallback:
                help = f"{DOCS_URL}/configuration/#option-moved-deprecations"
                self.server.add_warning(
                    f"[{section}]: Option '{option}' has been moved "
                    f"to section [{self.section}].  Please correct your "
                    f"configuration, see {help} for detailed documentation.",
                )
            if isinstance(val, (int, float)):
                self._check_option(option, val, above, below, minval, maxval)
        if option not in self.parsed[section]:
            if val is None or isinstance(val, (int, float, bool, str, dict, list)):
                self.parsed[section][option] = copy.deepcopy(val)
            else:
                # If the item cannot be encoded to json serialize to a string
                self.parsed[section][option] = str(val)
        return val

    def _check_option(
        self,
        option: str,
        value: float,
        above: float | None,
        below: float | None,
        minval: float | None,
        maxval: float | None,
    ) -> None:
        if above is not None and value <= above:
            msg = (
                f"Config Error: Section [{self.section}], Option "
                f"'{option}: {value}': value is not above {above}"
            )
            raise self.error(
                msg,
            )
        if below is not None and value >= below:
            msg = (
                f"Config Error: Section [{self.section}], Option "
                f"'{option}: {value}': value is not below {below}"
            )
            raise self.error(
                msg,
            )
        if minval is not None and value < minval:
            msg = (
                f"Config Error: Section [{self.section}], Option "
                f"'{option}: {value}': value is below minimum value {minval}"
            )
            raise self.error(
                msg,
            )
        if maxval is not None and value > maxval:
            msg = (
                f"Config Error: Section [{self.section}], Option "
                f"'{option}: {value}': value is above maximum value {minval}"
            )
            raise self.error(
                msg,
            )

    def get(
        self,
        option: str,
        default: Sentinel | _T = Sentinel.MISSING,
        deprecate: bool = False,
    ) -> str | _T:
        return self._get_option(self.config.get, option, default, deprecate=deprecate)

    def getint(
        self,
        option: str,
        default: Sentinel | _T = Sentinel.MISSING,
        above: int | None = None,
        below: int | None = None,
        minval: int | None = None,
        maxval: int | None = None,
        deprecate: bool = False,
    ) -> int | _T:
        return self._get_option(
            self.config.getint,
            option,
            default,
            above,
            below,
            minval,
            maxval,
            deprecate,
        )

    def getboolean(
        self,
        option: str,
        default: Sentinel | _T = Sentinel.MISSING,
        deprecate: bool = False,
    ) -> bool | _T:
        return self._get_option(
            self.config.getboolean,
            option,
            default,
            deprecate=deprecate,
        )

    def getfloat(
        self,
        option: str,
        default: Sentinel | _T = Sentinel.MISSING,
        above: float | None = None,
        below: float | None = None,
        minval: float | None = None,
        maxval: float | None = None,
        deprecate: bool = False,
    ) -> float | _T:
        return self._get_option(
            self.config.getfloat,
            option,
            default,
            above,
            below,
            minval,
            maxval,
            deprecate,
        )

    def getchoice(
        self,
        option: str,
        choices: dict[str, _T] | list[_T],
        default_key: Sentinel | str = Sentinel.MISSING,
        force_lowercase: bool = False,
        deprecate: bool = False,
    ) -> _T:
        result: str = self._get_option(
            self.config.get,
            option,
            default_key,
            deprecate=deprecate,
        )
        if force_lowercase:
            result = result.lower()
        if result not in choices:
            items = list(choices.keys()) if isinstance(choices, dict) else choices
            msg = (
                f"Section [{self.section}], Option '{option}: Value "
                f"{result} is not a vailid choice.  Must be one of the "
                f"following {items}"
            )
            raise ConfigError(
                msg,
            )
        if isinstance(choices, dict):
            return choices[result]
        return result  # type: ignore

    def getlists(
        self,
        option: str,
        default: Sentinel | _T = Sentinel.MISSING,
        list_type: type = str,
        separators: tuple[str | None, ...] = ("\n",),
        count: tuple[int | None, ...] | None = None,
        deprecate: bool = False,
    ) -> list[Any] | _T:
        if count is not None and len(count) != len(separators):
            msg = (
                f"Option '{option}' in section "
                f"[{self.section}]: length of 'count' argument must "
            )
            raise ConfigError(
                msg,
                "match length of 'separators' argument",
            )
        if count is None:
            count = tuple(None for _ in range(len(separators)))

        def list_parser(
            value: str,
            ltype: type,
            seps: tuple[str | None, ...],
            expected_cnt: tuple[int | None, ...],
        ) -> list[Any]:
            sep = seps[0]
            seps = seps[1:]
            cnt = expected_cnt[0]
            expected_cnt = expected_cnt[1:]
            ret: list[Any] = []
            if seps:
                sub_lists = [val.strip() for val in value.split(sep) if val.strip()]
                for sub_list in sub_lists:
                    ret.append(list_parser(sub_list, ltype, seps, expected_cnt))
            else:
                ret = [ltype(val.strip()) for val in value.split(sep) if val.strip()]
            if cnt is not None and len(ret) != cnt:
                msg = f"List length mismatch, expected {cnt}, parsed {len(ret)}"
                raise ConfigError(
                    msg,
                )
            return ret

        def getlist_wrapper(sec: str, opt: str) -> list[Any]:
            val = self.config.get(sec, opt)
            assert count is not None
            return list_parser(val, list_type, separators, count)

        return self._get_option(getlist_wrapper, option, default, deprecate=deprecate)

    def getlist(
        self,
        option: str,
        default: Sentinel | _T = Sentinel.MISSING,
        separator: str | None = "\n",
        count: int | None = None,
        deprecate: bool = False,
    ) -> list[str] | _T:
        return self.getlists(
            option,
            default,
            str,
            (separator,),
            (count,),
            deprecate=deprecate,
        )

    def getintlist(
        self,
        option: str,
        default: Sentinel | _T = Sentinel.MISSING,
        separator: str | None = "\n",
        count: int | None = None,
        deprecate: bool = False,
    ) -> list[int] | _T:
        return self.getlists(
            option,
            default,
            int,
            (separator,),
            (count,),
            deprecate=deprecate,
        )

    def getfloatlist(
        self,
        option: str,
        default: Sentinel | _T = Sentinel.MISSING,
        separator: str | None = "\n",
        count: int | None = None,
        deprecate: bool = False,
    ) -> list[float] | _T:
        return self.getlists(
            option,
            default,
            float,
            (separator,),
            (count,),
            deprecate=deprecate,
        )

    def getdict(
        self,
        option: str,
        default: Sentinel | _T = Sentinel.MISSING,
        separators: tuple[str | None, str | None] = ("\n", "="),
        dict_type: type = str,
        allow_empty_fields: bool = False,
        deprecate: bool = False,
    ) -> dict[str, Any] | _T:
        if len(separators) != 2:
            msg = "The `separators` argument of getdict() must be a Tupleof length of 2"
            raise ConfigError(
                msg,
            )

        def getdict_wrapper(sec: str, opt: str) -> dict[str, Any]:
            val = self.config.get(sec, opt)
            ret: dict[str, Any] = {}
            for raw_line in val.split(separators[0]):
                line = raw_line.strip()
                if not line:
                    continue
                parts = line.split(separators[1], 1)
                if len(parts) == 1:
                    if allow_empty_fields:
                        ret[parts[0].strip()] = None
                    else:
                        msg = f"Failed to parse dictionary field, {line}"
                        raise ConfigError(msg)
                else:
                    ret[parts[0].strip()] = dict_type(parts[1].strip())
            return ret

        return self._get_option(getdict_wrapper, option, default, deprecate=deprecate)

    def getgpioout(
        self,
        option: str,
        default: Sentinel | _T = Sentinel.MISSING,
        initial_value: int = 0,
        deprecate: bool = False,
    ) -> GpioOutputPin | _T:
        try:
            gpio: GpioFactory = self.server.load_component(self, "gpio")
        except Exception:
            msg = (
                f"Section [{self.section}], option '{option}', "
                "GPIO Component not available"
            )
            raise ConfigError(
                msg,
            )

        def getgpio_wrapper(sec: str, opt: str) -> GpioOutputPin:
            val = self.config.get(sec, opt)
            return gpio.setup_gpio_out(val, initial_value)

        return self._get_option(getgpio_wrapper, option, default, deprecate=deprecate)

    def getgpioevent(
        self,
        option: str,
        event_callback: GpioEventCallback,
        default: Sentinel | _T = Sentinel.MISSING,
        deprecate: bool = False,
    ) -> GpioEvent | _T:
        try:
            gpio: GpioFactory = self.server.load_component(self, "gpio")
        except Exception:
            msg = (
                f"Section [{self.section}], option '{option}', "
                "GPIO Component not available"
            )
            raise ConfigError(
                msg,
            )

        def getgpioevent_wrapper(sec: str, opt: str) -> GpioEvent:
            val = self.config.get(sec, opt)
            return gpio.register_gpio_event(val, event_callback)

        return self._get_option(
            getgpioevent_wrapper,
            option,
            default,
            deprecate=deprecate,
        )

    def gettemplate(
        self,
        option: str,
        default: Sentinel | _T = Sentinel.MISSING,
        is_async: bool = False,
        deprecate: bool = False,
    ) -> RenderableTemplate | _T:
        try:
            template: TemplateFactory = self.server.load_component(self, "template")
        except Exception:
            msg = (
                f"Section [{self.section}], option '{option}': "
                "Failed to load 'template' component."
            )
            raise ConfigError(
                msg,
            )

        def gettemplate_wrapper(sec: str, opt: str) -> RenderableTemplate:
            val = self.config.get(sec, opt)
            return template.create_template(val.strip(), is_async)

        return self._get_option(
            gettemplate_wrapper,
            option,
            default,
            deprecate=deprecate,
        )

    def load_template(
        self,
        option: str,
        default: Sentinel | str = Sentinel.MISSING,
        is_async: bool = False,
        deprecate: bool = False,
    ) -> RenderableTemplate:
        val = self.gettemplate(option, default, is_async, deprecate)
        if isinstance(val, str):
            template: TemplateFactory
            template = self.server.lookup_component("template")
            return template.create_template(val.strip(), is_async)
        return val

    def getpath(
        self,
        option: str,
        default: Sentinel | _T = Sentinel.MISSING,
        deprecate: bool = False,
    ) -> pathlib.Path | _T:
        val = self.gettemplate(option, default, deprecate=deprecate)
        if isinstance(val, RenderableTemplate):
            ctx = {"data_path": self.server.get_app_args()["data_path"]}
            strpath = val.render(ctx)
            return pathlib.Path(strpath).expanduser().resolve()
        return val

    def read_supplemental_dict(self, obj: dict[str, Any]) -> ConfigHelper:
        if not obj:
            msg = "Cannot ready Empty Dict"
            raise ConfigError(msg)
        source = DictSourceWrapper()
        source.read_dict(obj)
        sections = source.config.sections()
        return ConfigHelper(self.server, source, sections[0], {})

    def read_supplemental_config(self, file_name: str) -> ConfigHelper:
        fpath = pathlib.Path(file_name).expanduser().resolve()
        source = FileSourceWrapper(self.server)
        source.read_file(fpath)
        sections = source.config.sections()
        return ConfigHelper(self.server, source, sections[0], {})

    def write_config(self, file_obj: IO[str]) -> None:
        self.config.write(file_obj)

    def get_parsed_config(self) -> dict[str, dict[str, ConfigVal]]:
        return dict(self.parsed)

    def get_orig_config(self) -> dict[str, dict[str, str]]:
        return self.source.as_dict()

    def get_file_sections(self) -> dict[str, list[str]]:
        return self.source.get_file_sections()

    def get_config_files(self) -> list[str]:
        return [str(f) for f in self.source.get_files()]

    def validate_config(self) -> None:
        for sect in self.config.sections():
            if sect not in self.parsed:
                self.server.add_warning(
                    f"Unparsed config section [{sect}] detected.  This "
                    "may be the result of a component that failed to "
                    "load.  In the future this will result in a startup "
                    "error.",
                )
                continue
            parsed_opts = self.parsed[sect]
            if CFG_ERROR_KEY in parsed_opts:
                # Skip validation for sections that have encountered an error,
                # as this will always result in unparsed options.
                continue
            for opt, val in self.config.items(sect):
                if opt not in parsed_opts:
                    self.server.add_warning(
                        f"Unparsed config option '{opt}: {val}' detected in "
                        f"section [{sect}].  This may be an option no longer "
                        "available or could be the result of a module that "
                        "failed to load.  In the future this will result "
                        "in a startup error.",
                    )

    def create_backup(self) -> None:
        cfg_path = self.server.get_app_args()["config_file"]
        cfg = pathlib.Path(cfg_path).expanduser()
        backup = cfg.parent.joinpath(f".{cfg.name}.bkp")
        backup_fp: TextIO | None = None
        try:
            if backup.exists():
                cfg_mtime: int = 0
                for cfg in self.source.get_files():
                    cfg_mtime = max(cfg_mtime, cfg.stat().st_mtime_ns)
                backup_mtime = backup.stat().st_mtime_ns
                if backup_mtime >= cfg_mtime:
                    # Backup already exists and is current
                    return
            backup_fp = backup.open("w")
            self.config.write(backup_fp)
            logging.info(f"Backing up last working configuration to '{backup}'")
        except Exception:
            logging.exception("Failed to create a backup")
        finally:
            if backup_fp is not None:
                backup_fp.close()


class ConfigSourceWrapper:
    def __init__(self):
        self.config = configparser.ConfigParser(interpolation=None)

    def get_parser(self):
        return self.config

    def as_dict(self) -> dict[str, dict[str, str]]:
        return {key: dict(val) for key, val in self.config.items()}

    def write_to_string(self) -> str:
        sio = StringIO()
        self.config.write(sio)
        val = sio.getvalue()
        sio.close()
        return val

    def get_files(self) -> list[pathlib.Path]:
        return []

    def set_option(self, section: str, option: str, value: str) -> None:
        self.config.set(section, option, value)

    def remove_option(self, section: str, option: str) -> None:
        self.config.remove_option(section, option)

    def add_section(self, section: str) -> None:
        self.config.add_section(section)

    def remove_section(self, section: str) -> None:
        self.config.remove_section(section)

    def get_file_sections(self) -> dict[str, list[str]]:
        return {}

    def find_config_file(
        self,
        section: str,
        option: str | None = None,
    ) -> pathlib.Path | None:
        return None


class DictSourceWrapper(ConfigSourceWrapper):
    def __init__(self):
        super().__init__()

    def read_dict(self, cfg: dict[str, Any]) -> None:
        try:
            self.config.read_dict(cfg)
        except Exception as e:
            msg = "Error Reading config as dict"
            raise ConfigError(msg) from e


class FileSourceWrapper(ConfigSourceWrapper):
    section_r = re.compile(r"\s*\[([^]]+)\]")

    def __init__(self, server: Server) -> None:
        super().__init__()
        self.server = server
        self.files: list[pathlib.Path] = []
        self.raw_config_data: list[str] = []
        self.updates_pending: set[int] = set()
        self.file_section_map: dict[str, list[int]] = {}
        self.file_option_map: dict[tuple[str, str], list[int]] = {}
        self.save_lock = threading.Lock()
        self.backup: dict[str, Any] = {}

    def get_files(self) -> list[pathlib.Path]:
        return self.files

    def is_in_transaction(self) -> bool:
        return len(self.updates_pending) > 0 or self.save_lock.locked()

    def backup_source(self) -> None:
        self.backup = {
            "raw_data": list(self.raw_config_data),
            "section_map": copy.deepcopy(self.file_section_map),
            "option_map": copy.deepcopy(self.file_option_map),
            "config": self.write_to_string(),
        }

    def _acquire_save_lock(self) -> None:
        if not self.files:
            msg = "Can only modify file backed configurations"
            raise ConfigError(
                msg,
            )
        if not self.save_lock.acquire(blocking=False):
            msg = "Configuration locked, cannot modify"
            raise ConfigError(msg)

    def set_option(self, section: str, option: str, value: str) -> None:
        self._acquire_save_lock()
        try:
            value = value.strip()
            try:
                if self.config.get(section, option).strip() == value:
                    return
            except (configparser.NoSectionError, configparser.NoOptionError):
                pass
            file_idx: int = 0
            has_sec = has_opt = False
            if (section, option) in self.file_option_map:
                file_idx = self.file_option_map[(section, option)][0]
                has_sec = has_opt = True
            elif section in self.file_section_map:
                file_idx = self.file_section_map[section][0]
                has_sec = True
            buf = self.raw_config_data[file_idx].splitlines()
            new_opt_list = [f"{option}: {value}"]
            if "\n" in value:
                vals = [f"  {v}" for v in value.split("\n")]
                new_opt_list = [f"{option}:"] + vals
            sec_info = self._find_section_info(section, buf, raise_error=False)
            if sec_info:
                options: dict[str, Any] = sec_info["options"]
                indent: int = sec_info["indent"]
                opt_start: int = sec_info["end"]
                opt_end: int = sec_info["end"]
                opt_info: dict[str, Any] | None = options.get(option)
                if opt_info is not None:
                    indent = opt_info["indent"]
                    opt_start = opt_info["start"]
                    opt_end = opt_info["end"]
                elif options:
                    # match indentation of last option in section
                    last_opt = list(options.values())[-1]
                    indent = last_opt["indent"]
                if indent:
                    padding = " " * indent
                    new_opt_list = [f"{padding}{v}" for v in new_opt_list]
                buf[opt_start:] = new_opt_list + buf[opt_end:]
            else:
                # Append new section to the end of the file
                new_opt_list.insert(0, f"[{section}]")
                if buf and buf[-1].strip() != "":
                    new_opt_list.insert(0, "")
                buf.extend(new_opt_list)
            buf.append("")
            updated_cfg = "\n".join(buf)
            # test changes to the configuration
            test_parser = configparser.ConfigParser(interpolation=None)
            try:
                test_parser.read_string(updated_cfg)
                if not test_parser.has_option(section, option):
                    msg = "Option not added"
                    raise ConfigError(msg)
            except Exception as e:
                msg = (
                    f"Failed to set option '{option}' in section "
                    f"[{section}], file: {self.files[file_idx]}"
                )
                raise ConfigError(
                    msg,
                ) from e
            # Update local configuration/tracking
            self.raw_config_data[file_idx] = updated_cfg
            self.updates_pending.add(file_idx)
            if not has_sec:
                self.file_section_map[section] = [file_idx]
            if not has_opt:
                self.file_option_map[(section, option)] = [file_idx]
            if not self.config.has_section(section):
                self.config.add_section(section)
            self.config.set(section, option, value)
        finally:
            self.save_lock.release()

    def remove_option(self, section: str, option: str) -> None:
        self._acquire_save_lock()
        try:
            key = (section, option)
            if key not in self.file_option_map:
                return
            pending: list[tuple[int, str]] = []
            file_indices = self.file_option_map[key]
            for idx in file_indices:
                buf = self.raw_config_data[idx].splitlines()
                try:
                    sec_info = self._find_section_info(section, buf)
                    opt_info = sec_info["options"][option]
                    start = opt_info["start"]
                    end = opt_info["end"]
                    if (
                        end < len(buf)
                        and not buf[start - 1].strip()
                        and not buf[end].strip()
                    ):
                        end += 1
                    buf[start:] = buf[end:]
                    buf.append("")
                    updated_cfg = "\n".join(buf)
                    test_parser = configparser.ConfigParser(interpolation=None)
                    test_parser.read_string(updated_cfg)
                    if test_parser.has_option(section, option):
                        msg = "Option still exists"
                        raise ConfigError(msg)
                    pending.append((idx, updated_cfg))
                except Exception as e:
                    msg = (
                        f"Failed to remove option '{option}' from section "
                        f"[{section}], file: {self.files[idx]}"
                    )
                    raise ConfigError(
                        msg,
                    ) from e
            # Update configuration/tracking
            for idx, data in pending:
                self.updates_pending.add(idx)
                self.raw_config_data[idx] = data
            del self.file_option_map[key]
            self.config.remove_option(section, option)
        finally:
            self.save_lock.release()

    def add_section(self, section: str) -> None:
        self._acquire_save_lock()
        try:
            if section in self.file_section_map:
                return
            # add section to end of primary file
            buf = self.raw_config_data[0].splitlines()
            if buf and buf[-1].strip() != "":
                buf.append("")
            buf.extend([f"[{section}]", ""])
            updated_cfg = "\n".join(buf)
            try:
                test_parser = configparser.ConfigParser(interpolation=None)
                test_parser.read_string(updated_cfg)
                if not test_parser.has_section(section):
                    msg = "Section not added"
                    raise ConfigError(msg)
            except Exception as e:
                msg = f"Failed to add section [{section}], file: {self.files[0]}"
                raise ConfigError(
                    msg,
                ) from e
            self.updates_pending.add(0)
            self.file_section_map[section] = [0]
            self.raw_config_data[0] = updated_cfg
            self.config.add_section(section)
        finally:
            self.save_lock.release()

    def remove_section(self, section: str) -> None:
        self._acquire_save_lock()
        try:
            if section not in self.file_section_map:
                return
            pending: list[tuple[int, str]] = []
            file_indices = self.file_section_map[section]
            for idx in file_indices:
                buf = self.raw_config_data[idx].splitlines()
                try:
                    sec_info = self._find_section_info(section, buf)
                    start = sec_info["start"]
                    end = sec_info["end"]
                    if (
                        end < len(buf)
                        and not buf[start - 1].strip()
                        and not buf[end].strip()
                    ):
                        end += 1
                    buf[start:] = buf[end:]
                    buf.append("")
                    updated_cfg = "\n".join(buf)
                    test_parser = configparser.ConfigParser(interpolation=None)
                    test_parser.read_string(updated_cfg)
                    if test_parser.has_section(section):
                        msg = "Section still exists"
                        raise ConfigError(msg)
                    pending.append((idx, updated_cfg))
                except Exception as e:
                    msg = f"Failed to remove section [{section}], file: {self.files[0]}"
                    raise ConfigError(
                        msg,
                    ) from e
            for idx, data in pending:
                self.updates_pending.add(idx)
                self.raw_config_data[idx] = data
            del self.file_section_map[section]
            self.config.remove_section(section)
        finally:
            self.save_lock.release()

    def save(self) -> Awaitable[bool]:
        eventloop = self.server.get_event_loop()
        if self.server.is_running():
            fut = eventloop.run_in_thread(self._do_save)
        else:
            fut = eventloop.create_future()
            fut.set_result(self._do_save())
        return fut

    def _do_save(self) -> bool:
        with self.save_lock:
            self.backup.clear()
            if not self.updates_pending:
                return False
            for idx in self.updates_pending:
                fpath = self.files[idx]
                fpath.write_text(
                    self.raw_config_data[idx],
                    encoding="utf-8",
                )
            self.updates_pending.clear()
            return True

    def cancel(self):
        self._acquire_save_lock()
        try:
            if not self.backup or not self.updates_pending:
                self.backup.clear()
                return
            self.raw_config_data = self.backup["raw_data"]
            self.file_option_map = self.backup["option_map"]
            self.file_section_map = self.backup["section_map"]
            self.config.clear()
            self.config.read_string(self.backup["config"])
            self.updates_pending.clear()
            self.backup.clear()
        finally:
            self.save_lock.release()

    def revert(self) -> Awaitable[bool]:
        eventloop = self.server.get_event_loop()
        if self.server.is_running():
            fut = eventloop.run_in_thread(self._do_revert)
        else:
            fut = eventloop.create_future()
            fut.set_result(self._do_revert())
        return fut

    def _do_revert(self) -> bool:
        with self.save_lock:
            if not self.updates_pending:
                return False
            self.backup.clear()
            entry = self.files[0]
            self.read_file(entry)
            return True

    def write_config(
        self,
        dest_folder: str | pathlib.Path,
    ) -> Awaitable[None]:
        eventloop = self.server.get_event_loop()
        if self.server.is_running():
            fut = eventloop.run_in_thread(self._do_write, dest_folder)
        else:
            self._do_write(dest_folder)
            fut = eventloop.create_future()
            fut.set_result(None)
        return fut

    def _do_write(self, dest_folder: str | pathlib.Path) -> None:
        with self.save_lock:
            if isinstance(dest_folder, str):
                dest_folder = pathlib.Path(dest_folder)
            dest_folder = dest_folder.expanduser().resolve()
            cfg_parent = self.files[0].parent
            for i, path in enumerate(self.files):
                try:
                    rel_path = path.relative_to(cfg_parent)
                    dest_file = dest_folder.joinpath(rel_path)
                except ValueError:
                    dest_file = dest_folder.joinpath(
                        f"{path.parent.name}-{path.name}",
                    )
                os.makedirs(str(dest_file.parent), exist_ok=True)
                dest_file.write_text(self.raw_config_data[i])

    def _find_section_info(
        self,
        section: str,
        file_data: list[str],
        raise_error: bool = True,
    ) -> dict[str, Any]:
        options: dict[str, dict[str, Any]] = {}
        result: dict[str, Any] = {
            "indent": -1,
            "start": -1,
            "end": -1,
            "options": options,
        }
        last_option: str = ""
        opt_indent = -1
        for idx, raw_line in enumerate(file_data):
            if not raw_line.strip() or raw_line.lstrip()[0] in "#;":
                # skip empty lines, whitespace, and comments
                continue
            line = raw_line.expandtabs()
            line_indent = len(line) - len(line.lstrip())
            if opt_indent != -1 and line_indent > opt_indent:
                if last_option:
                    options[last_option]["end"] = idx + 1
                # Continuation of an option
                if result["start"] != -1:
                    result["end"] = idx + 1
                continue
            sec_match = self.section_r.match(line)
            if sec_match is not None:
                opt_indent = -1
                if result["start"] != -1:
                    break
                cursec = sec_match.group(1)
                if section == cursec:
                    result["indent"] = line_indent
                    result["start"] = idx
                    result["end"] = idx + 1
            else:
                # This is an option
                opt_indent = line_indent
                if result["start"] != -1:
                    result["end"] = idx + 1
                    last_option = re.split(r"[:=]", line, 1)[0].strip()
                    options[last_option] = {
                        "indent": line_indent,
                        "start": idx,
                        "end": idx + 1,
                    }
        if result["start"] != -1:
            return result
        if raise_error:
            msg = f"Unable to find section [{section}]"
            raise ConfigError(msg)
        return {}

    def get_file_sections(self) -> dict[str, list[str]]:
        sections_by_file: dict[str, list[str]] = {
            str(fname): [] for fname in self.files
        }
        for section, idx_list in self.file_section_map.items():
            for idx in idx_list:
                fname = str(self.files[idx])
                sections_by_file[fname].append(section)
        return sections_by_file

    def find_config_file(
        self,
        section: str,
        option: str | None = None,
    ) -> pathlib.Path | None:
        idx: int = -1
        if option is not None:
            key = (section, option)
            if key in self.file_option_map:
                idx = self.file_option_map[key][0]
        elif section in self.file_section_map:
            idx = self.file_section_map[section][0]
        if idx == -1:
            return None
        return self.files[idx]

    def _write_buffer(self, buffer: list[str], fpath: pathlib.Path) -> None:
        if not buffer:
            return
        self.config.read_string("\n".join(buffer), fpath.name)

    def _parse_file(
        self,
        file_path: pathlib.Path,
        visited: list[tuple[int, int]],
    ) -> None:
        buffer: list[str] = []
        try:
            stat = file_path.stat()
            cur_stat = (stat.st_dev, stat.st_ino)
            if cur_stat in visited:
                msg = f"Recursive include directive detected, {file_path}"
                raise ConfigError(
                    msg,
                )
            visited.append(cur_stat)
            self.files.append(file_path)
            file_index = len(self.files) - 1
            cfg_data = file_path.read_text(encoding="utf-8")
            self.raw_config_data.append(cfg_data)
            lines = cfg_data.splitlines()
            last_section = ""
            opt_indent = -1
            for raw_line in lines:
                if not raw_line.strip() or raw_line.lstrip()[0] in "#;":
                    # ignore lines that contain only whitespace/comments
                    continue
                line = raw_line.expandtabs(tabsize=4)
                # Search for and remove inline comments
                cmt_match = re.search(r" +[#;]", line)
                if cmt_match is not None:
                    line = line[: cmt_match.start()]
                # Unescape prefix chars that are preceded by whitespace
                line = re.sub(r" \\(#|;)", r" \1", line)
                line_indent = len(line) - len(line.lstrip())
                if opt_indent != -1 and line_indent > opt_indent:
                    # Multi-line value, append to buffer and resume parsing
                    buffer.append(line)
                    continue
                sect_match = self.section_r.match(line)
                if sect_match is not None:
                    # Section detected
                    opt_indent = -1
                    section = sect_match.group(1)
                    if section.startswith("include "):
                        inc_path = section[8:].strip()
                        if not inc_path:
                            msg = f"Invalid include directive: [{section}]"
                            raise ConfigError(
                                msg,
                            )
                        if inc_path[0] == "/":
                            new_path = pathlib.Path(inc_path).resolve()
                            paths = sorted(new_path.parent.glob(new_path.name))
                        else:
                            paths = sorted(file_path.parent.glob(inc_path))
                        if not paths:
                            msg = f"No files matching include directive [{section}]"
                            raise ConfigError(
                                msg,
                            )
                        # Write out buffered data to the config before parsing
                        # included files
                        self._write_buffer(buffer, file_path)
                        buffer.clear()
                        for p in paths:
                            self._parse_file(p, visited)
                        # Don't add included sections to the configparser
                        continue
                    last_section = section
                    if section not in self.file_section_map:
                        self.file_section_map[section] = []
                    elif file_index in self.file_section_map[section]:
                        msg = f"Duplicate section [{section}] in file {file_path}"
                        raise ConfigError(
                            msg,
                        )
                    self.file_section_map[section].insert(0, file_index)
                else:
                    # This line must specify an option
                    opt_indent = line_indent
                    option = re.split(r"[:=]", line, 1)[0].strip()
                    key = (last_section, option)
                    if key not in self.file_option_map:
                        self.file_option_map[key] = []
                    elif file_index in self.file_option_map[key]:
                        msg = (
                            f"Duplicate option '{option}' in section "
                            f"[{last_section}], file {file_path} "
                        )
                        raise ConfigError(
                            msg,
                        )
                    self.file_option_map[key].insert(0, file_index)
                buffer.append(line)
            self._write_buffer(buffer, file_path)
        except ConfigError:
            raise
        except Exception as e:
            if not file_path.is_file():
                msg = f"Configuration File Not Found: '{file_path}''"
                raise ConfigError(
                    msg,
                ) from e
            if not os.access(file_path, os.R_OK):
                msg = (
                    "Moonraker does not have Read/Write permission for "
                    f"config file at path '{file_path}'"
                )
                raise ConfigError(
                    msg,
                ) from e
            msg = f"Error Reading Config: '{file_path}'"
            raise ConfigError(msg) from e

    def read_file(self, main_conf: pathlib.Path) -> None:
        self.config.clear()
        self.files.clear()
        self.raw_config_data.clear()
        self.updates_pending.clear()
        self.file_section_map.clear()
        self.file_option_map.clear()
        self._parse_file(main_conf, visited=[])
        size = sum([len(rawcfg) for rawcfg in self.raw_config_data])
        logging.info(
            f"Configuration File '{main_conf}' parsed, total size: {size} B",
        )


def get_configuration(
    server: Server,
    app_args: dict[str, Any],
) -> ConfigHelper:
    cfg_file = app_args["config_file"]
    if app_args["is_backup_config"]:
        cfg_file = app_args["backup_config"]
    start_path = pathlib.Path(cfg_file).expanduser().absolute()
    source = FileSourceWrapper(server)
    source.read_file(start_path)
    if not source.config.has_section("server"):
        msg = "No section [server] in config"
        raise ConfigError(msg)
    return ConfigHelper(server, source, "server", {})


def find_config_backup(cfg_path: str) -> str | None:
    cfg = pathlib.Path(cfg_path).expanduser()
    backup = cfg.parent.joinpath(f".{cfg.name}.bkp")
    if backup.is_file():
        return str(backup)
    return None
