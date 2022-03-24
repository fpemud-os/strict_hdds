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


from .util import Util, PartiUtil, BtrfsUtil
from .handy import EfiMultiDisk, Snapshot, SnapshotBtrfs, MountEfi, MountParam, HandyMd, DisksChecker
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
            self._mnt.umount()
            del self._mnt
        del self._md

    @MountEfi.proxy
    @property
    def get_mount_entries(self):
        pass

    @MountEfi.proxy
    def get_bootdir_rw_controller(self):
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

    @Snapshot.proxy
    def get_snapshot_list(self):
        pass

    def add_disk(self, disk):
        assert disk is not None

        if disk not in Util.getDevPathListForFixedDisk():
            raise errors.StorageLayoutAddDiskError(disk, errors.NOT_DISK)

        # add
        self._md.add_disk(disk, Util.fsTypeBtrfs)

        # hdd partition 2: make it as backing device and add it to btrfs filesystem
        BtrfsUtil.addDiskToBtrfs(self._md.get_disk_data_partition(disk), self._mnt.mount_point)

        # boot disk change
        if disk == self._md.boot_disk:
            self._mnt.mount_esp(self._md.get_disk_esp_partition(self._md.boot_disk))
            return True
        else:
            return False

    def remove_disk(self, disk):
        assert disk is not None
        assert disk in self._md.get_disk_list()

        if len(self._md.get_disk_list()) <= 1:
            raise errors.StorageLayoutRemoveDiskError(errors.CAN_NOT_REMOVE_LAST_HDD)

        # boot disk change
        if disk == self._md.boot_disk:
            self._mnt.umount_esp(self._md.get_disk_esp_partition(self._md.boot_disk))
            bChange = True
        else:
            bChange = False

        # hdd partition 2: remove from btrfs and bcache
        Util.cmdCall("btrfs", "device", "delete", self._md.get_disk_data_partition(disk), self._mnt.mount_point)

        # remove
        self._md.remove_disk(disk)

        # boot disk change
        if bChange:
            assert self._md.boot_disk is not None
            self._mnt.mount_esp(self._md.get_disk_esp_partition(self._md.boot_disk))
            return True
        else:
            return False

    @Snapshot.proxy
    def create_snapshot(self, snapshot_name):
        pass

    @Snapshot.proxy
    def remove_snapshot(self, snapshot_name):
        pass

    def _check_impl(self, check_item, *kargs, auto_fix=False, error_callback=None):
        if check_item == Util.checkItemBasic:
            if True:
                dc = DisksChecker(self._md.get_disk_list())
                dc.check_partition_type("gpt", auto_fix, error_callback)
                dc.check_boot_sector(auto_fix, error_callback)
                dc.check_logical_sector_size(auto_fix, error_callback)
            self._md.check_esp(auto_fix, error_callback)
            self._snapshot.check(auto_fix, error_callback)
        else:
            assert False


def parse(boot_dev, root_dev, mount_dir):
    if boot_dev is None:
        raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.BOOT_DEV_NOT_EXIST)
    if Util.getBlkDevFsType(root_dev) != Util.fsTypeBtrfs:
        raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.ROOT_PARTITION_FS_SHOULD_BE(Util.fsTypeBtrfs))

    # disk_list, boot_disk
    partiList = BtrfsUtil.getSlaveDevPathList(mount_dir)
    diskList = [PartiUtil.partiToDisk(x) for x in partiList]
    bootHdd = HandyMd.checkAndGetBootDiskFromBootDev(StorageLayoutImpl.name, boot_dev, diskList)

    # get kwargsDict from mount options
    kwargsDict = dict()
    if True:
        ret = Util.mntGetSubVol(self._mntDir)
        if ret is not None:
            if not ret.startswith("@"):
                raise errors.StorageLayoutParseError("sub-volume \"%s\" is not supported" % (ret))
            kwargsDict["snapshot"] = ret[1:]

    # return
    ret = StorageLayoutImpl()
    ret._md = EfiMultiDisk(diskList=diskList, bootHdd=bootHdd)
    ret._snapshot = SnapshotBtrfs(mount_dir, snapshot=kwargsDict.get("snapshot", None))
    ret._mnt = MountEfi(True, mount_dir, _params_for_mount(ret), kwargsDict)
    return ret


def detect_and_mount(disk_list, mount_dir, kwargsDict):
    # filter
    diskList = []
    for d in disk_list:
        i = 1
        while True:
            parti = PartiUtil.diskToParti(d, i)
            if not PartiUtil.partiExists(parti):
                break
            if Util.getBlkDevFsType(parti) == Util.fsTypeBtrfs:
                diskList.append(d)
                break
            i += 1
    if len(diskList) == 0:
        raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.DISK_NOT_FOUND)

    # md
    bootHdd = HandyMd.checkAndGetBootDiskAndBootDev(StorageLayoutImpl.name, diskList)[0]
    md = EfiMultiDisk(diskList=diskList, bootHdd=bootHdd)

    # check snapshot
    snapshotName = kwargsDict.get("snapshot", None)
    SnapshotBtrfs.checkFs(StorageLayoutImpl.name, partiList[0], ",".join(["device=%s" % (md.get_disk_data_partition(x)) for x in md.get_disk_list()]), snapshotName)

    # return
    ret = StorageLayoutImpl()
    ret._md = md
    ret._snapshot = SnapshotBtrfs(mount_dir, snapshot=snapshotName)
    ret._mnt = MountEfi(False, mount_dir, _params_for_mount(ret), kwargsDict)       # do mount during MountEfi initialization
    return ret


def create_and_mount(disk_list, mount_dir, kwargsDict):
    # add disks
    md = EfiMultiDisk()
    HandyMd.checkAndAddDisks(disk_list, Util.fsTypeBtrfs)

    # create and mount
    partiList = [md.get_disk_data_partition(x) for x in md.get_disk_list()]
    Util.cmdCall("mkfs.btrfs", "-f", "-d", "single", "-m", "single", *partiList)
    SnapshotBtrfs.initializeFs(partiList[0], ",".join(["device=%s" % (x) for x in partiList]))

    # return
    ret = StorageLayoutImpl()
    ret._md = md
    ret._snapshot = SnapshotBtrfs(mount_dir, snapshot=kwargsDict.get("snapshot", None))
    ret._mnt = MountEfi(False, mount_dir, _params_for_mount(ret), kwargsDict)       # do mount during MountEfi initialization
    return ret


def _params_for_mount(obj):
    ret = []
    for dirPath, dirMode, dirUid, dirGid, mntOptList in obj._snapshot.getParamsForMount():
        tlist = mntOptList + ["device=%s" % (obj._md.get_disk_data_partition(x)) for x in obj._md.get_disk_list()]
        ret.append(MountParam(dirPath, dirMode, dirUid, dirGid, obj.dev_rootfs, Util.fsTypeBtrfs, mnt_opt_list=tlist))
    ret.append(MountParam(Util.bootDir, 0o40755, 0, 0, obj.dev_boot, Util.fsTypeFat, mnt_opt_list=Util.bootDirMntOptList))
    return ret
