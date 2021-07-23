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


class StorageLayoutEfiSimple(StorageLayout):
    """Layout:
           /dev/sda          GPT, EFI-GRUB
               /dev/sda1     ESP partition
               /dev/sda2     root device, EXT4
       Description:
           1. the 3 partition in /dev/sda is order-insensitive
           2. use optional swap file /var/cache/swap.dat
           3. extra partition is allowed to exist
    """

    name = "efi-simple"

    def __init__(self):
        self._hdd = None              # boot harddisk name
        self._hddEspParti = None      # ESP partition name
        self._hddRootParti = False    # root partition name
        self._bSwapFile = None        # whether swap file exists

    @property
    def boot_mode(self):
        return StorageLayout.BOOT_MODE_EFI

    @property
    def dev_rootfs(self):
        return self._hddRootParti

    @property
    def dev_swap(self):
        return util.swapFilename if self._bSwapFile else None

    def get_boot_disk(self):
        return self._hdd

    def check_swap_size(self):
        assert self._bSwapFile
        return os.path.getsize(util.swapFilename) >= util.getSwapSizeInGb() * 1024 * 1024 * 1024

    def get_esp(self):
        return self._hddEspParti

    def create_swap_file(self):
        assert not self._bSwapFile
        util.createSwapFile(util.swapFilename)
        self._bSwapFile = True

    def remove_swap_file(self):
        assert self._bSwapFile
        os.remove(self._bSwapFile)
        self._bSwapFile = False


def create_layout(hdd=None, dry_run=False):
    if hdd is None:
        hddList = util.getDevPathListForFixedHdd()
        if len(hddList) == 0:
            raise StorageLayoutCreateError("no harddisk")
        if len(hddList) > 1:
            raise StorageLayoutCreateError("multiple harddisks")
        hdd = hddList[0]

    if not dry_run:
        # create partitions
        util.initializeDisk(hdd, "gpt", [
            ("%dMiB" % (util.getEspSizeInMb()), util.fsTypeFat),
            ("*", util.fsTypeExt4),
        ])

    ret = StorageLayoutEfiSimple()
    ret._hdd = hdd
    ret._hddEspParti = util.devPathDiskToPartition(hdd, 1)
    ret._hddRootParti = util.devPathDiskToPartition(hdd, 2)
    return ret


def parse_layout(bootDev, rootDev):
    ret = StorageLayoutEfiSimple()

    if not util.gptIsEspPartition(bootDev):
        raise StorageLayoutParseError(StorageLayoutEfiSimple.name, "boot device is not an ESP partitiion")

    ret._hdd = util.devPathPartitionToDisk(bootDev)
    if ret._hdd != util.devPathPartitionToDisk(rootDev):
        raise StorageLayoutParseError(StorageLayoutEfiSimple.name, "boot device and root device is not the same")

    ret._hddEspParti = bootDev

    ret._hddRootParti = rootDev
    if True:
        fs = util.getBlkDevFsType(ret._hddRootParti)
        if fs != util.fsTypeExt4:
            raise StorageLayoutParseError(StorageLayoutEfiSimple.name, "root partition file system is \"%s\", not \"ext4\"" % (fs))

    if os.path.exists(util.swapFilename) and util.cmdCallTestSuccess("/sbin/swaplabel", util.swapFilename):
        ret._bSwapFile = True
    else:
        ret._bSwapFile = False

    return ret
