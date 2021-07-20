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


import os
from . import util
from . import StorageLayout
from . import StorageLayoutCreateError
from . import StorageLayoutParseError


class StorageLayoutBiosSimple(StorageLayout):
    """Layout:
           /dev/sda          MBR, BIOS-GRUB
               /dev/sda1     root device, EXT4
       Description:
           1. partition number of /dev/sda1 and /dev/sda2 is irrelevant
           2. use optional swap file /var/swap.dat
           3. extra partition is allowed to exist
    """

    name = "bios-simple"

    def __init__(self):
        self._hdd = None              # boot harddisk name
        self._hddRootParti = False    # root partition name
        self._bSwapFile = None        # whether swap file exists

    @property
    def boot_mode(self):
        return StorageLayout.BOOT_MODE_BIOS

    @property
    def device_rootfs(self):
        return self._hddRootParti

    @property
    def device_swap(self):
        return util.swapFilename if self._bSwapFile else None

    def check_swap_size(self):
        assert self._bSwapFile
        return os.path.getsize(util.swapFilename) >= util.getSwapSizeInGb() * 1024 * 1024 * 1024

    def get_boot_disk(self):
        return self._hdd

    def create_swap_file(self):
        assert not self._bSwapFile
        util.createSwapFile(util.swapFilename)
        self._bSwapFile = True

    def remove_swap_file(self):
        assert self._bSwapFile
        os.remove(util.swapFilename)
        self._bSwapFile = False


def create_layout(hdd=None):
    if hdd is None:
        hddList = util.getDevPathListForFixedHdd()
        if len(hddList) == 0:
            raise StorageLayoutCreateError("no harddisk")
        if len(hddList) > 1:
            raise StorageLayoutCreateError("multiple harddisks")
        hdd = hddList[0]

    # create partitions
    util.initializeDisk(hdd, "mbr", [
        ("*", "ext4"),
    ])

    ret = StorageLayoutBiosSimple()
    ret._hdd = hdd
    ret._hddRootParti = util.devPathDiskToPartition(hdd, 1)
    ret._bSwapFile = False
    return ret


def parse_layout(rootDev):
    ret = StorageLayoutBiosSimple()

    ret._hdd = util.devPathPartitionToDisk(rootDev)
    if util.getBlkDevPartitionTableType(ret._hdd) != "dos":
        raise StorageLayoutParseError(StorageLayoutBiosSimple.name, "partition type of %s is not \"dos\"" % (ret._hdd))

    ret._hddRootParti = rootDev
    fs = util.getBlkDevFsType(ret._hddRootParti)
    if fs != "ext4":
        raise StorageLayoutParseError(StorageLayoutBiosSimple.name, "root partition file system is \"%s\", not \"ext4\"" % (fs))

    return ret
