#!/usr/bin/env python3

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


import re
from . import util
from . import layout_bios_simple
from . import layout_efi_simple
from . import layout_efi_lvm
from . import layout_efi_bcache_lvm


def get_supported_storage_layouts():
    return [
        layout_bios_simple.StorageLayoutBiosSimple.name,
        layout_efi_simple.StorageLayoutEfiSimple.name,
        layout_efi_lvm.StorageLayoutEfiLvm.name,
        layout_efi_bcache_lvm.StorageLayoutEfiBcacheLvm.name,
    ]


def create_storage_layout(layout_name, dry_run=False):
    if layout_name == layout_bios_simple.StorageLayoutBiosSimple.name:
        return layout_bios_simple.create_layout(dry_run=dry_run)
    elif layout_name == layout_efi_simple.StorageLayoutEfiSimple.name:
        return layout_efi_simple.create_layout(dry_run=dry_run)
    elif layout_name == layout_efi_lvm.StorageLayoutEfiLvm.name:
        return layout_efi_lvm.create_layout(dry_run=dry_run)
    elif layout_name == layout_efi_bcache_lvm.StorageLayoutEfiBcacheLvm.name:
        return layout_efi_bcache_lvm.create_layout(dry_run=dry_run)
    else:
        assert False


def parse_storage_layout():
    rootDev = util.getMountDeviceForPath("/")
    bootDev = util.getMountDeviceForPath("/boot")

    assert rootDev is not None
    if bootDev is not None:
        lvmInfo = util.getBlkDevLvmInfo(rootDev)
        if lvmInfo is not None:
            tlist = util.lvmGetSlaveDevPathList(lvmInfo[0])
            if any(re.fullmatch("/dev/bcache[0-9]+", x) is not None for x in tlist):
                return layout_efi_bcache_lvm.parse_layout(bootDev, rootDev)
            else:
                return layout_efi_lvm.parse_layout(bootDev, rootDev)
        else:
            return layout_efi_simple.parse_layout(bootDev, rootDev)
    else:
        return layout_bios_simple.parse_layout(bootDev, rootDev)
