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
import pkgutil
from . import util
from . import StorageLayoutCreateError
from . import StorageLayoutParseError


def get_supported_storage_layouts():
    ret = []
    for mod in pkgutil.iter_modules(["."]):
        if mod.name.startswith("layout_"):
            ret.append(util.modName2layoutName(mod.name))
    return ret


def create_storage_layout(layout_name, dry_run=False):
    for mod in pkgutil.iter_modules(["."]):
        if mod.name.startswith("layout_"):
            if layout_name == util.modName2layoutName(mod.name):
                return mod.create_layout(dry_run=dry_run)
    raise StorageLayoutCreateError("layout \"%s\" not supported")


def parse_storage_layout():
    rootDev = util.getMountDeviceForPath("/")
    bootDev = util.getMountDeviceForPath("/boot")

    assert rootDev is not None
    if bootDev is not None:
        lvmInfo = util.getBlkDevLvmInfo(rootDev)
        if lvmInfo is not None:
            tlist = util.lvmGetSlaveDevPathList(lvmInfo[0])
            if any(re.fullmatch("/dev/bcache[0-9]+", x) is not None for x in tlist):
                return _parseOneStorageLayout("efi-bcache-lvm", bootDev, rootDev)
            else:
                return _parseOneStorageLayout("efi-lvm", bootDev, rootDev)
        else:
            return _parseOneStorageLayout("efi-simple", bootDev, rootDev)
    else:
        return _parseOneStorageLayout("bios-simple", bootDev, rootDev)


def _parseOneStorageLayout(layoutName, bootDev, rootDev):
    modname = util.layoutName2modName(layoutName)
    try:
        exec("import strict_hdds.%s" % (modname))
        f = eval("strict_hdds.%s.parse_layout" % (modname))
        return f(bootDev, rootDev)
    except ModuleNotFoundError:
        raise StorageLayoutParseError("", "unknown storage layout")
