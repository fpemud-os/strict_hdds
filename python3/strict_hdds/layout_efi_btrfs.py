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


from .util import Util, EfiMultiDisk, BtrfsUtil
from .handy import MountEfi, HandyMd
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
        self._md = None              # MultiDisk
        self._mnt = None             # MountEfi

    @property
    def boot_mode(self):
        return StorageLayout.BOOT_MODE_EFI

    @property
    def dev_rootfs(self):
        return self.get_disk_data_partition(self.get_disk_list()[0])

    @property
    @EfiMultiDisk.proxy
    def dev_boot(self):
        pass

    @property
    def dev_swap(self):
        assert False

    @EfiMultiDisk.proxy
    def boot_disk(self):
        pass

    def umount_and_dispose(self):
        if True:
            self._mnt.umount()
            del self._mnt
        del self._md

    @MountEfi.proxy
    def remount_rootfs(self, mount_options):
        pass

    @MountEfi.proxy
    def get_bootdir_rw_controller(self):
        pass

    def check(self):
        pass

    @EfiMultiDisk.proxy
    def get_esp(self):
        pass

    @EfiMultiDisk.proxy
    def get_pending_esp_list(self):
        pass

    @EfiMultiDisk.proxy
    def sync_esp(self, dst):
        pass

    @EfiMultiDisk.proxy
    def get_disk_list(self):
        pass

    @EfiMultiDisk.proxy
    def get_disk_esp_partition(self, disk):
        pass

    @EfiMultiDisk.proxy
    def get_disk_data_partition(self, disk):
        pass

    def add_disk(self, disk):
        assert disk is not None

        if disk not in Util.getDevPathListForFixedDisk():
            raise errors.StorageLayoutAddDiskError(disk, errors.NOT_DISK)

        lastBootDisk = self._md.boot_disk

        # add
        self._md.add_disk(disk)

        # hdd partition 2: make it as backing device and add it to btrfs filesystem
        Util.cmdCall("/sbin/btrfs", "device", "add", self._md.get_disk_data_partition(disk), "/")

        return lastBootDisk != self._md.boot_disk     # boot disk may change

    def remove_disk(self, disk):
        assert disk is not None
        assert disk in self._md.get_disk_list()

        if len(self._md.get_disk_list()) <= 1:
            raise errors.StorageLayoutRemoveDiskError(errors.CAN_NOT_REMOVE_LAST_HDD)

        lastBootHdd = self._md.boot_disk

        # hdd partition 2: remove from btrfs and bcache
        Util.cmdCall("/sbin/btrfs", "device", "delete", self._md.get_disk_data_partition(disk), "/")

        # remove
        self._md.remove_disk(disk)

        return lastBootHdd != self._md.boot_disk     # boot disk may change


def parse(boot_dev, root_dev):
    if boot_dev is None:
        raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.BOOT_DEV_NOT_EXIST)
    if Util.getBlkDevFsType(root_dev) != Util.fsTypeBtrfs:
        raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.ROOT_PARTITION_FS_SHOULD_BE(Util.fsTypeBtrfs))

    # disk_list, boot_disk
    diskList = BtrfsUtil.getSlaveDevPathList(root_dev)
    bootHdd = HandyMd.checkAndGetBootDiskFromBootDev(boot_dev, diskList)

    # return
    ret = StorageLayoutImpl()
    ret._md = EfiMultiDisk(diskList=diskList, bootHdd=bootHdd)
    ret._mnt = MountEfi("/")
    return ret


def detect_and_mount(disk_list, mount_dir):
    # disk_list
    diskList = [x for x in disk_list if Util.getBlkDevFsType(x) == Util.fsTypeBtrfs]
    if len(diskList) == 0:
        raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.DISK_NOT_FOUND)

    # boot_disk, boot_device
    bootHdd = HandyMd.checkAndGetBootDiskAndBootDev(diskList)[0]

    # return
    ret = StorageLayoutImpl()
    ret._md = EfiMultiDisk(diskList=diskList, bootHdd=bootHdd)
    ret._mnt = MountEfi(mount_dir)

    # mount
    MountEfi.mount(ret.dev_rootfs, ret.dev_boot, mount_dir)
    return ret


def create_and_mount(disk_list, mount_dir):
    # add disks
    md = EfiMultiDisk()
    HandyMd.checkAndAddDisks(disk_list)

    # create and mount
    Util.cmdCall("/usr/sbin/mkfs.btrfs", "-d", "single", "-m", "single", *[md.get_disk_data_partition(x) for x in md.get_disk_list()])

    # return
    ret = StorageLayoutImpl()
    ret._md = md
    ret._mnt = MountEfi(mount_dir)

    # mnount
    MountEfi.mount(ret.dev_rootfs, ret.dev_boot, mount_dir)
    return ret
