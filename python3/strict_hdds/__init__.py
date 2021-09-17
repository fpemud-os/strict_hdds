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

    @property
    def dev_rootfs(self):
        raise NotImplementedError()

    @property
    def dev_swap(self):
        raise NotImplementedError()

    def get_boot_disk(self):
        raise NotImplementedError()

    def check_swap_size(self):
        raise NotImplementedError()


class StorageLayoutError(Exception):
    pass


class StorageLayoutCreateError(StorageLayoutError):
    pass


class StorageLayoutAddDiskError(StorageLayoutError):

    def __init__(self, disk_devpath, message):
        self.disk_devpath = disk_devpath
        self.message = message


class StorageLayoutReleaseDiskError(StorageLayoutError):

    def __init__(self, disk_devpath, message):
        self.disk_devpath = disk_devpath
        self.message = message


class StorageLayoutParseError(StorageLayoutError):

    def __init__(self, layout_name, message):
        self.layout_name = layout_name
        self.message = message


from .core import get_supported_storage_layouts
from .core import create_storage_layout
from .core import parse_storage_layout
