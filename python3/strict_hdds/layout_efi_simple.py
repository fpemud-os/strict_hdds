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


class StorageLayoutEfiSimple(StorageLayout):
    """Layout:
           /dev/sda          GPT, EFI-GRUB
               /dev/sda1     ESP partition
               /dev/sda2     root device, EXT4
       Description:
           1. the 3 partition in /dev/sda is order-insensitive
           2. use optional swap file /var/swap.dat
           3. extra partition is allowed to exist
    """

    name = "efi-simple"

    def __init__(self):
        self._hdd = None
        self._bEspParti = None
        self._bRootParti = None
        self._bSwapFile = None

    @property
    def boot_mode(self):
        return StorageLayout.BOOT_MODE_EFI

    def is_ready(self):
        assert self._hdd is not None
        assert self._bEspParti
        assert self._bRootParti
        assert self._bSwapFile is not None
        return True

    def get_rootdev(self):
        assert self.is_ready()
        return util.devPathDiskToPartition(self._hdd, 2)

    def get_swap(self):
        assert self.is_ready()
        return util.swapFilename if self._bSwapFile else None

    def check_swap_size(self):
        assert self.is_ready() and self._bSwapFile
        return os.path.getsize(util.swapFilename) >= util.getSwapSizeInGb() * 1024 * 1024 * 1024

    def get_esp(self):
        assert self.is_ready()
        return util.devPathDiskToPartition(self._hdd, 1)

    def create_swap_file(self):
        assert self.is_ready() and not self._bSwapFile
        util.createSwapFile(util.swapFilename)
        self._bSwapFile = True

    def remove_swap_file(self):
        assert self.is_ready() and self._bSwapFile
        os.remove(self._bSwapFile)
        self._bSwapFile = False


def create_layout(hdd=None):
    if hdd is None:
        hddList = util.getDevPathListForFixedHdd()
        if len(hddList) == 0:
            raise Exception("no harddisks")
        if len(hddList) > 1:
            raise Exception("multiple harddisks")
        hdd = hddList[0]

    # create partitions
    util.initializeDisk(hdd, "gpt", [
        ("%dMiB" % (util.getEspSizeInMb()), "vfat"),
        ("*", "ext4"),
    ])


def try_parse_layout(bootDev, rootDev):
    if not util.gptIsEspPartition(bootDev):
        raise StorageLayoutParseError(StorageLayoutEfiSimple, "boot device is not ESP partitiion")

    ret = StorageLayoutEfiSimple()

    # ret.hdd
    ret.hdd = util.devPathPartitionToDisk(bootDev)
    if ret.hdd != util.devPathPartitionToDisk(rootDev):
        raise StorageLayoutParseError(StorageLayoutEfiSimple, "boot device and root device is not the same")

    # ret.hddEspParti
    ret.hddEspParti = bootDev

    # ret.hddRootParti
    ret.hddRootParti = rootDev
    if True:
        fs = util.getBlkDevFsType(ret.hddRootParti)
        if fs != "ext4":
            raise StorageLayoutParseError(StorageLayoutEfiSimple, "root partition file system is \"%s\", not \"ext4\"" % (fs))

    # ret._bSwapFile
    if os.path.exists(util.swapFilename) and util.cmdCallTestSuccess("/sbin/swaplabel", util.swapFilename):
        ret._bSwapFile = True
    else:
        ret._bSwapFile = False

    return ret
