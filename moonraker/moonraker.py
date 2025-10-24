#!/usr/bin/env python3
# Legacy entry point for Moonraker
#
# Copyright (C) 2022 Eric Callahan <arksine.code@gmail.com>
#
# This file may be distributed under the terms of the GNU GPLv3 license

"""Legacy entry point for Moonraker.

This module serves as the main entry point for starting the Moonraker server
when executed directly.
"""

if __name__ == "__main__":
    import importlib
    import pathlib
    import sys

    pkg_parent = pathlib.Path(__file__).parent.parent
    sys.path.pop(0)
    sys.path.insert(0, str(pkg_parent))
    svr = importlib.import_module(".server", "moonraker")
    svr.main(False)  # noqa: FBT003
