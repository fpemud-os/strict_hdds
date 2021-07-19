#!/usr/bin/env python3

# strict_hdds.py - strict harddisks
#
# Copyright (c) 2020-2021 Fpemud <fpemud@sina.com>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

"""
strict_hdds

@author: Fpemud
@license: GPLv3 License
@contact: fpemud@sina.com
"""

import os
import pwd
import grp
import stat


__author__ = "fpemud@sina.com (Fpemud)"
__version__ = "0.0.1"


class StorageLayout:

    BOOT_MODE_BIOS = 1
    BOOT_MODE_EFI = 2

    @property
    def name(self):
        raise NotImplementedError()

    @property
    def boot_mode(self):
        raise NotImplementedError()

    def is_ready(self):
        raise NotImplementedError()


from .layouts import StorageLayoutBiosSimple
from .layouts import StorageLayoutBiosLvm
from .layouts import StorageLayoutEfiSimple
from .layouts import StorageLayoutEfiLvm
from .layouts import StorageLayoutEfiBcacheLvm

from .manager import get_supported_storage_layouts
from .manager import create_storage_layout
from .manager import parse_storage_layout
from .manager import ParseStorageLayoutError
