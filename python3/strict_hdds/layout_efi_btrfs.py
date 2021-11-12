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


from .util import Util, GptUtil, MultiDisk

from . import errors
from . import StorageLayout


class StorageLayoutImpl(StorageLayout):
    """Layout:
           /dev/sda                 GPT
               /dev/sda1            ESP partition
               /dev/sda2            btrfs device
           /dev/sdb                 GPT
               /dev/sdb1            reserved ESP partition
               /dev/sdb2            btrfs device
           /dev/sda1:/dev/sda2      root device, btrfs
       Description:
           1. /dev/sda1 and /dev/sdb1 must has the same size
           2. /dev/sda1 and /dev/sda2 is order-sensitive, no extra partition is allowed
           3. /dev/sdb1 and /dev/sdb2 is order-sensitive, no extra partition is allowed
           4. use optional swap file /var/swap/swap.dat, at this time /var/swap is a standalone sub-volume
           5. extra harddisk is allowed to exist
    """

    def __init__(self):
        self._md = None         # MultiDisk

    @property
    def boot_mode(self):
        return StorageLayout.BOOT_MODE_EFI

    @property
    def dev_rootfs(self):
        return self._md.get_disk_list()[0]

    @property
    def dev_swap(self):
        return None

    @MultiDisk.proxy
    def get_boot_disk(self):
        pass

    def check_swap_size(self):
        assert False

    @MultiDisk.proxy
    def get_esp(self):
        pass

    @MultiDisk.proxy
    def get_esp_sync_info(self):
        pass

    @MultiDisk.proxy
    def sync_esp(self, src, dst):
        pass

    @MultiDisk.proxy
    def get_disk_list(self):
        pass

    def add_disk(self, disk):
        assert disk is not None

        if disk not in Util.getDevPathListForFixedDisk():
            raise errors.StorageLayoutAddDiskError(disk, errors.NOT_DISK)

        lastBootDisk = self._md.get_boot_disk()

        # add
        self._md.add_disk(disk)

        # hdd partition 2: make it as backing device and add it to btrfs filesystem
        Util.cmdCall("/sbin/btrfs", "device", "add", self._md.get_disk_data_partition(disk), "/")

        return lastBootDisk != self._md.get_boot_disk()     # boot disk may change

    def remove_disk(self, disk):
        assert disk is not None
        assert disk in self._md.get_disk_list()

        if self._md.get_disk_count() <= 1:
            raise errors.StorageLayoutRemoveDiskError(errors.CAN_NOT_REMOVE_LAST_HDD)

        lastBootHdd = self._md.get_boot_disk()

        # hdd partition 2: remove from btrfs and bcache
        Util.cmdCall("/sbin/btrfs", "device", "delete", self._md.get_disk_data_partition(disk), "/")

        # remove
        self._md.remove_disk(disk)

        return lastBootHdd != self._md.get_boot_disk()     # boot disk may change


def create(disk_list=None, dry_run=False):
    if disk_list is None:
        disk_list = Util.getDevPathListForFixedDisk()
        if len(disk_list) == 0:
            raise errors.StorageLayoutCreateError(errors.NO_DISK_WHEN_CREATE)
    else:
        assert len(disk_list) > 0

    ret = StorageLayoutImpl()

    if not dry_run:
        ret._md = MultiDisk()
        for devpath in disk_list:
            ret._md.add_disk(devpath)
    else:
        ret._md = MultiDisk(disk_list)

    return ret


def parse(bootDev, rootDev):
    ret = StorageLayoutImpl()

    if not GptUtil.isEspPartition(bootDev):
        raise errors.StorageLayoutParseError(ret.name, errors.BOOT_DEV_IS_NOT_ESP)

    # boot harddisk
    ret._md = MultiDisk()
    ret._md = Util.devPathPartiToDisk(bootDev)

    return ret
