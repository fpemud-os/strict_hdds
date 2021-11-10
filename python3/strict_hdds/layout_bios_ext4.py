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
from .util import Util, SwapFile
from . import errors
from . import StorageLayout


class StorageLayoutImpl(StorageLayout):
    """Layout:
           /dev/sda          MBR, BIOS-GRUB
               /dev/sda1     root device, EXT4
       Description:
           1. partition number of /dev/sda1 and /dev/sda2 is irrelevant
           2. use optional swap file /var/cache/swap.dat
           3. extra partition is allowed to exist
    """

    def __init__(self):
        super().__init__()

        self._hdd = None              # boot harddisk name
        self._hddRootParti = False    # root partition name
        self._sf = None               # SwapFile

    @property
    def boot_mode(self):
        return StorageLayout.BOOT_MODE_BIOS

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

    @SwapFile.proxy
    def create_swap_file(self):
        pass

    @SwapFile.proxy
    def remove_swap_file(self):
        pass


def create(hdd=None, dry_run=False):
    if hdd is None:
        hddList = Util.getDevPathListForFixedDisk()
        if len(hddList) == 0:
            raise errors.StorageLayoutCreateError(errors.NO_DISK_WHEN_CREATE)
        if len(hddList) > 1:
            raise errors.StorageLayoutCreateError(errors.MULTIPLE_DISKS)
        hdd = hddList[0]

    if not dry_run:
        # create partitions
        Util.initializeDisk(hdd, "mbr", [
            ("*", Util.fsTypeExt4),
        ])

    ret = StorageLayoutImpl()
    ret._hdd = hdd
    ret._hddRootParti = Util.devPathDiskToPartition(hdd, 1)
    ret._sf = SwapFile(False)
    return ret


def parse(boot_dev, root_dev):
    ret = StorageLayoutImpl()

    if boot_dev is not None:
        raise errors.StorageLayoutParseError(ret.name, errors.BOOT_DEV_SHOULD_NOT_EXIST)

    ret._hdd = Util.devPathPartitionToDisk(root_dev)
    if Util.getBlkDevPartitionTableType(ret._hdd) != "dos":
        raise errors.StorageLayoutParseError(ret.name, errors.PARTITION_TYPE_SHOULD_BE(ret._hdd, "dos"))

    ret._hddRootParti = root_dev
    fs = Util.getBlkDevFsType(ret._hddRootParti)
    if fs != Util.fsTypeExt4:
        raise errors.StorageLayoutParseError(ret.name, errors.ROOT_PARTITION_FS_SHOULD_BE(fs, "ext4"))

    ret._sf = SwapFile.detectAndNewSwapFileObject()

    return ret


def detect_and_mount(disk_list, mount_dir, mount_options):
    ret = StorageLayoutImpl()

    rootPartitionList = []
    for disk in disk_list:
        with open(disk, "rb") as f:
            if Util.isBufferAllZero(f.read(440)):
                continue                        # no boot code, ignore unbootable disk

        if Util.getBlkDevPartitionTableType(disk) != "dos":
            continue                            # only accept disk with MBR partition table

        i = 1
        while True:
            parti = Util.devPathDiskToPartition(disk, i)
            if not os.path.exists(parti):
                break
            if Util.getBlkDevFsType(parti) == Util.fsTypeExt4:
                rootPartitionList.append(parti)
            i += 1

    if len(rootPartitionList) == 0:
        raise errors.StorageLayoutParseError(ret.name, errors.ROOT_PARTITION_NOT_FOUND)
    elif len(rootPartitionList) > 1:
        raise errors.StorageLayoutParseError(ret.name, errors.ROOT_PARTITIONS_TOO_MANY)
    else:



        ret._hdd = Util.devPathPartitionToDisk(rootPartitionList[0])
        ret._hddRootParti = rootPartitionList[0]
        ret._sf = SwapFile.detectAndNewSwapFileObject()
        return ret
