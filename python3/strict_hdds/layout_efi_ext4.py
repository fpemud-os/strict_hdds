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


from .util import Util, SwapFile

from . import errors
from . import StorageLayout


class StorageLayoutImpl(StorageLayout):
    """Layout:
           /dev/sda          GPT
               /dev/sda1     ESP partition
               /dev/sda2     root device, EXT4
       Description:
           1. the 3 partition in /dev/sda is order-insensitive
           2. use optional swap file /var/cache/swap.dat
           3. extra partition is allowed to exist
    """

    def __init__(self):
        super().__init__()

        self._hdd = None              # boot harddisk name
        self._hddEspParti = None      # ESP partition name
        self._hddRootParti = False    # root partition name
        self._sf = None               # SwapFile

    @property
    def boot_mode(self):
        return StorageLayout.BOOT_MODE_EFI

    @property
    def dev_rootfs(self):
        return self._hddRootParti

    @property
    def dev_swap(self):
        return self._sf.get_swap_devname()

    def get_boot_disk(self):
        return self._hdd

    @SwapFile.proxy
    def check_swap_size(self):
        pass

    def get_esp(self):
        return self._hddEspParti

    @SwapFile.proxy
    def create_swap_file(self):
        pass

    @SwapFile.proxy
    def remove_swap_file(self):
        pass


def create_layout(hdd=None, dry_run=False):
    if hdd is None:
        hddList = Util.getDevPathListForFixedHdd()
        if len(hddList) == 0:
            raise errors.StorageLayoutCreateError(errors.NO_DISK)
        if len(hddList) > 1:
            raise errors.StorageLayoutCreateError(errors.MULTIPLE_DISKS)
        hdd = hddList[0]

    if not dry_run:
        # create partitions
        Util.initializeDisk(hdd, "gpt", [
            ("%dMiB" % (Util.getEspSizeInMb()), Util.fsTypeFat),
            ("*", Util.fsTypeExt4),
        ])

    ret = StorageLayoutImpl()
    ret._hdd = hdd
    ret._hddEspParti = Util.devPathDiskToPartition(hdd, 1)
    ret._hddRootParti = Util.devPathDiskToPartition(hdd, 2)
    ret._sf = SwapFile(False)
    return ret


def parse_layout(bootDev, rootDev):
    ret = StorageLayoutImpl()

    if not Util.gptIsEspPartition(bootDev):
        raise errors.StorageLayoutParseError(ret.name, errors.BOOT_DEV_IS_NOT_ESP)

    ret._hdd = Util.devPathPartitionToDisk(bootDev)
    if ret._hdd != Util.devPathPartitionToDisk(rootDev):
        raise errors.StorageLayoutParseError(ret.name, "boot device and root device is not the same")

    ret._hddEspParti = bootDev

    ret._hddRootParti = rootDev
    if True:
        fs = Util.getBlkDevFsType(ret._hddRootParti)
        if fs != Util.fsTypeExt4:
            raise errors.StorageLayoutParseError(ret.name, "root partition file system is \"%s\", not \"ext4\"" % (fs))

    ret._sf = SwapFile.detect_and_new_swap_file_object()

    return ret
