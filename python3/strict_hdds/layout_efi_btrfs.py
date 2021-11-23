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


from .util import Util, BtrfsUtil
from .handy import EfiMultiDisk, Snapshot, SnapshotBtrfs, MountEfi, HandyMd
from . import errors
from . import StorageLayout, MountParam


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
        self._snapshot = None        # SnapshotBtrfs
        self._mnt = None             # MountEfi

    @property
    def boot_mode(self):
        return StorageLayout.BOOT_MODE_EFI

    @property
    def dev_rootfs(self):
        return self.get_disk_data_partition(self.get_disk_list()[0])

    @EfiMultiDisk.proxy
    @property
    def dev_boot(self):
        pass

    @EfiMultiDisk.proxy
    def boot_disk(self):
        pass

    @Snapshot.proxy
    @property
    def snapshot(self):
        pass

    @MountEfi.proxy
    @property
    def mount_point(self):
        pass

    def umount_and_dispose(self):
        if True:
            Util.mntUmount(self.mount_point, ["/boot"] + self._snapshot.getDirpathsForUmount())
            del self._mnt
        del self._md

    @MountEfi.proxy
    def get_bootdir_rw_controller(self):
        pass

    def get_params_for_mount(self, **kwargs):
        ret = []
        for dirPath, mntOpts in self._snapshot.getDirPathsAndMntOptsForMount(kwargs):
            ret.append(MountParam(self.dev_rootfs, dirPath, mntOpts))
        ret.append(MountParam(self.dev_boot, "/boot", "ro"))
        return ret

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

    @Snapshot.proxy
    def get_snapshot_list(self):
        pass

    def add_disk(self, disk):
        assert disk is not None

        if disk not in Util.getDevPathListForFixedDisk():
            raise errors.StorageLayoutAddDiskError(disk, errors.NOT_DISK)

        lastBootDisk = self._md.boot_disk

        # add
        self._md.add_disk(disk)

        # hdd partition 2: make it as backing device and add it to btrfs filesystem
        Util.cmdCall("/sbin/btrfs", "device", "add", self._md.get_disk_data_partition(disk), self._mnt.mount_point)

        return lastBootDisk != self._md.boot_disk     # boot disk may change

    def remove_disk(self, disk):
        assert disk is not None
        assert disk in self._md.get_disk_list()

        if len(self._md.get_disk_list()) <= 1:
            raise errors.StorageLayoutRemoveDiskError(errors.CAN_NOT_REMOVE_LAST_HDD)

        lastBootHdd = self._md.boot_disk

        # hdd partition 2: remove from btrfs and bcache
        Util.cmdCall("/sbin/btrfs", "device", "delete", self._md.get_disk_data_partition(disk), self._mnt.mount_point)

        # remove
        self._md.remove_disk(disk)

        return lastBootHdd != self._md.boot_disk     # boot disk may change

    @Snapshot.proxy
    def create_snapshot(self, snapshot_name):
        pass

    @Snapshot.proxy
    def remove_snapshot(self, snapshot_name):
        pass

    def _check_impl(self, check_item, *kargs, auto_fix=False, error_callback=None):
        if check_item == Util.checkItemBasic:
            self._md.check_esp(auto_fix, error_callback)
            self._snapshot.check(auto_fix, error_callback)
        else:
            assert False


def parse(boot_dev, root_dev):
    if boot_dev is None:
        raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.BOOT_DEV_NOT_EXIST)
    if Util.getBlkDevFsType(root_dev) != Util.fsTypeBtrfs:
        raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.ROOT_PARTITION_FS_SHOULD_BE(Util.fsTypeBtrfs))

    # disk_list, boot_disk
    diskList = BtrfsUtil.getSlaveDevPathList(root_dev)
    bootHdd = HandyMd.checkAndGetBootDiskFromBootDev(StorageLayoutImpl.name, boot_dev, diskList)

    # FIXME: check mount options
    pass

    # return
    ret = StorageLayoutImpl()
    ret._md = EfiMultiDisk(diskList=diskList, bootHdd=bootHdd)
    ret._snapshot = SnapshotBtrfs("/")
    ret._mnt = MountEfi("/")
    return ret


def detect_and_mount(disk_list, mount_dir, mount_options):
    # disk_list
    diskList = [x for x in disk_list if Util.getBlkDevFsType(x) == Util.fsTypeBtrfs]
    if len(diskList) == 0:
        raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.DISK_NOT_FOUND)

    # boot_disk, boot_device
    bootHdd = HandyMd.checkAndGetBootDiskAndBootDev(StorageLayoutImpl.name, diskList)[0]

    # return
    ret = StorageLayoutImpl()
    ret._md = EfiMultiDisk(diskList=diskList, bootHdd=bootHdd)
    ret._snapshot = SnapshotBtrfs(mount_dir)
    ret._mnt = MountEfi(mount_dir)

    # mount
    Util.mntMount(mount_dir, Util.optimizeMntParamList(ret.get_params_for_mount(), mount_options))
    return ret


def create_and_mount(disk_list, mount_dir, mount_options):
    # add disks
    md = EfiMultiDisk()
    HandyMd.checkAndAddDisks(disk_list)

    # create and mount
    Util.cmdCall("/usr/sbin/mkfs.btrfs", "-d", "single", "-m", "single", *[md.get_disk_data_partition(x) for x in md.get_disk_list()])
    SnapshotBtrfs.initializeFs(md.dev_rootfs)

    # return
    ret = StorageLayoutImpl()
    ret._md = md
    ret._snapshot = SnapshotBtrfs(mount_dir)
    ret._mnt = MountEfi(mount_dir)

    # mnount
    Util.mntMount(mount_dir, Util.optimizeMntParamList(ret.get_params_for_mount(), mount_options))
    return ret
