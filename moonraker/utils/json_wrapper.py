# Wrapper for msgspec with stdlib fallback
#
# Copyright (C) 2023 Eric Callahan <arksine.code@gmail.com>
#
# This file may be distributed under the terms of the GNU GPLv3 license

from __future__ import annotations

import contextlib
import os
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:

    def dumps(obj: Any) -> bytes: ...  # type: ignore
    def loads(data: str | bytes | bytearray) -> Any: ...


MSGSPEC_ENABLED = False
_msgspc_var = os.getenv("MOONRAKER_ENABLE_MSGSPEC", "y").lower()
if _msgspc_var in ["y", "yes", "true"]:
    with contextlib.suppress(ImportError):
        import msgspec
        from msgspec import DecodeError as JSONDecodeError

        encoder = msgspec.json.Encoder()
        decoder = msgspec.json.Decoder()
        dumps = encoder.encode
        loads = decoder.decode
        MSGSPEC_ENABLED = True
if not MSGSPEC_ENABLED:
    import json
    from json import JSONDecodeError  # type: ignore # noqa: F401

    loads = json.loads  # type: ignore

    def dumps(obj) -> bytes:  # type: ignore
        return json.dumps(obj).encode("utf-8")
