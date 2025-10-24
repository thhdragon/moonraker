# Package definition for the file_manager
#
# Copyright (C) 2021 Eric Callahan <arksine.code@gmail.com>
#
# This file may be distributed under the terms of the GNU GPLv3 license.

from __future__ import annotations

from typing import TYPE_CHECKING

from . import file_manager as fm

if TYPE_CHECKING:
    from ...confighelper import ConfigHelper


def load_component(config: ConfigHelper) -> fm.FileManager:
    return fm.load_component(config)
