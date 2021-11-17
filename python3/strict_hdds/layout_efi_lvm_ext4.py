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


from .util import Util, PartiUtil, GptUtil, LvmUtil, EfiMultiDisk, SwapLvmLv
from .handy import MountEfi, CommonChecks, HandyUtil
from . import errors
from . import StorageLayout


class StorageLayoutImpl(StorageLayout):
    """Layout:
           /dev/sda                 GPT
               /dev/sda1            ESP partition
               /dev/sda2            LVM-PV for VG hdd
           /dev/sdb                 GPT
               /dev/sdb1            reserved ESP partition
               /dev/sdb2            LVM-PV for VG hdd
           /dev/mapper/hdd.root     root device, EXT4
           /dev/mapper/hdd.swap     swap device
       Description:
           1. /dev/sda1 and /dev/sdb1 must has the same size
           2. /dev/sda1 and /dev/sda2 is order-sensitive, no extra partition is allowed
           3. /dev/sdb1 and /dev/sdb2 is order-sensitive, no extra partition is allowed
           4. swap device is optional
           5. extra LVM-LV is allowed to exist
           6. extra harddisk is allowed to exist
    """

    def __init__(self):
        self._md = None              # MultiDisk
        self._swap = None            # SwapLvmLv
        self._mnt = None             # MountEfi

    @property
    def boot_mode(self):
        return StorageLayout.BOOT_MODE_EFI

    @property
    def dev_rootfs(self):
        return LvmUtil.rootLvDevPath

    @property
    @EfiMultiDisk.proxy
    def dev_boot(self):
        pass

    @property
    @SwapLvmLv.proxy
    def dev_swap(self):
        pass

    @EfiMultiDisk.proxy
    def boot_disk(self):
        pass

    def umount_and_dispose(self):
        if True:
            self._mnt.umount()
            del self._mnt
        del self._swap
        del self._md

    @MountEfi.proxy
    def remount_rootfs(self, mount_options):
        pass

    @MountEfi.proxy
    def get_bootdir_rw_controller(self):
        pass

    def check(self):
        CommonChecks.storageLayoutCheckSwapSize(self)

    def optimize_rootdev(self):
        LvmUtil.autoExtendLv(LvmUtil.rootLvDevPath)
        Util.cmdExec("/sbin/resize2fs", LvmUtil.rootLvDevPath)

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

        lastBootHdd = self._md.get_boot_hdd()

        # add
        self._md.add_disk(disk)

        # create lvm physical volume on partition2 and add it to volume group
        LvmUtil.addPvToVg(self._md.get_disk_data_partition(disk), LvmUtil.vgName)

        return lastBootHdd != self._md.get_boot_hdd()     # boot disk may change

    def release_disk(self, disk):
        assert disk is not None
        assert disk in self._md.disk_list

        # check
        if len(self._md.get_disk_list()) <= 1:
            raise errors.StorageLayoutReleaseDiskError(disk, errors.CAN_NOT_REMOVE_LAST_HDD)

        # move data
        rc, out = Util.cmdCallWithRetCode("/sbin/lvm", "pvmove", PartiUtil.diskToParti(disk, 1))
        if rc != 5:
            raise errors.StorageLayoutRemoveDiskError(disk, "failed")

    def remove_disk(self, disk):
        assert disk is not None

        if len(self._md.get_disk_list()) <= 1:
            raise errors.StorageLayoutRemoveDiskError(errors.CAN_NOT_REMOVE_LAST_HDD)

        lastBootHdd = self._cg.boot_disk
        parti = self._md.get_disk_data_partition(disk)

        # hdd partition 2: remove from volume group
        rc, out = Util.cmdCallWithRetCode("/sbin/lvm", "pvmove", parti)
        if rc != 5:
            raise errors.StorageLayoutRemoveDiskError("failed")
        Util.cmdCall("/sbin/lvm", "vgreduce", LvmUtil.vgName, parti)

        # remove
        self._md.remove_disk(disk)

        return lastBootHdd != self._md.get_boot_hdd()     # boot disk may change

    @SwapLvmLv.proxy
    def create_swap_lv(self):
        pass

    @SwapLvmLv.proxy
    def remove_swap_lv(self):
        pass


def parse(boot_dev, root_dev):
    if not GptUtil.isEspPartition(boot_dev):
        raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.BOOT_DEV_IS_NOT_ESP)

    # get disk list and check
    diskList = HandyUtil.lvmEnsureVgLvAndGetDiskList(StorageLayoutImpl.name)
    bootHdd = PartiUtil.partiToDisk(boot_dev)
    if bootHdd not in diskList:
        raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.BOOT_DISK_MUST_IN_SLAVE_DISK_LIST)

    # check root lv
    if root_dev != LvmUtil.rootLvDevPath:
        raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.ROOT_DEV_MUST_BE(LvmUtil.rootLvDevPath))
    if Util.getBlkDevFsType(LvmUtil.rootLvDevPath) != Util.fsTypeExt4:
        raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.ROOT_PARTITION_FS_SHOULD_BE(Util.fsTypeExt4))

    # return
    ret = StorageLayoutImpl()
    ret._md = EfiMultiDisk(diskList=diskList, bootHdd=bootHdd)
    ret._swap = HandyUtil.swapLvDetectAndNew(StorageLayoutImpl.name)
    ret._mnt = MountEfi("/")
    return ret


def detect_and_mount(disk_list, mount_dir):
    LvmUtil.activateAll()

    # get disk list and check
    lvmDiskList = HandyUtil.lvmEnsureVgLvAndGetDiskList(StorageLayoutImpl.name)
    if True:
        d = list(set(lvmDiskList) - set(disk_list))
        if len(d) > 0:
            raise errors.StorageLayoutParseError(StorageLayoutImpl.name, "extra disk \"%s\" needed" % (d[0]))

    # get boot disk and boot partition
    bootDisk = lvmDiskList[0]
    bootParti = PartiUtil.diskToParti(bootDisk, 1)

    # check root lv file system
    if Util.getBlkDevFsType(LvmUtil.rootLvDevPath) != Util.fsTypeExt4:
        raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.ROOT_PARTITION_FS_SHOULD_BE(Util.fsTypeExt4))

    # mount
    MountEfi.mount(LvmUtil.rootLvDevPath, bootParti, mount_dir)

    # return
    ret = StorageLayoutImpl()
    ret._md = EfiMultiDisk(diskList=lvmDiskList, bootHdd=bootDisk)
    ret._swap = HandyUtil.swapLvDetectAndNew(StorageLayoutImpl.name)
    ret._mnt = MountEfi(mount_dir)
    return ret


def create_and_mount(disk_list, mount_dir):
    # add disks
    md = EfiMultiDisk()
    HandyUtil.mdCheckAndAddDisks(disk_list)

    # create pv, create vg, create root lv
    for disk in md.get_disk_list():
        LvmUtil.addPvToVg(md.get_disk_data_partition(disk), LvmUtil.vgName)
    LvmUtil.createLvWithDefaultSize(LvmUtil.vgName, LvmUtil.rootLvName)

    # mount
    MountEfi.mount(LvmUtil.rootLvDevPath, md.dev_boot, mount_dir)

    # return
    ret = StorageLayoutImpl()
    ret._md = md
    ret._swap = SwapLvmLv(False)
    ret._mnt = MountEfi(mount_dir)
    return ret
