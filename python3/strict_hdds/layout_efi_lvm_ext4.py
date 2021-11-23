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


from .util import Util, PartiUtil, LvmUtil
from .handy import EfiMultiDisk, SwapLvmLv, MountEfi, HandyMd, HandyUtil
from . import errors
from . import StorageLayout, MountParam


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

    @EfiMultiDisk.proxy
    @property
    def dev_boot(self):
        pass

    @SwapLvmLv.proxy
    @property
    def dev_swap(self):
        pass

    @EfiMultiDisk.proxy
    def boot_disk(self):
        pass

    @MountEfi.proxy
    @property
    def mount_point(self):
        pass

    def umount_and_dispose(self):
        if True:
            Util.mntUmount(self.mount_point, ["/boot", "/"])
            del self._mnt
        del self._swap
        del self._md

    @MountEfi.proxy
    def get_bootdir_rw_controller(self):
        pass

    def get_params_for_mount(self, **kwargs):
        return [
            MountParam(self.dev_rootfs, "/", ""),
            MountParam(self.dev_boot, "/boot", "ro"),
        ]

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

    def _check_impl(self, check_item, *kargs, auto_fix=False, error_callback=None):
        if check_item == Util.checkItemBasic:
            self._md.check_esp(auto_fix, error_callback)
        elif check_item == "swap":
            self._swap.check(auto_fix, error_callback)
        else:
            assert False


def parse(boot_dev, root_dev):
    if boot_dev is None:
        raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.BOOT_DEV_NOT_EXIST)
    if root_dev != LvmUtil.rootLvDevPath:
        raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.ROOT_DEV_MUST_BE(LvmUtil.rootLvDevPath))
    if Util.getBlkDevFsType(LvmUtil.rootLvDevPath) != Util.fsTypeExt4:
        raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.ROOT_PARTITION_FS_SHOULD_BE(Util.fsTypeExt4))

    # disk_list, boot_disk
    pvDevPathList = HandyUtil.lvmEnsureVgLvAndGetPvList(StorageLayoutImpl.name)
    diskList = [PartiUtil.partiToDisk(x) for x in pvDevPathList]
    bootHdd = HandyMd.checkAndGetBootDiskFromBootDev(StorageLayoutImpl.name, boot_dev, diskList)

    # return
    ret = StorageLayoutImpl()
    ret._md = EfiMultiDisk(diskList=diskList, bootHdd=bootHdd)
    ret._swap = HandyUtil.swapLvDetectAndNew(StorageLayoutImpl.name)
    ret._mnt = MountEfi("/")
    return ret


def detect_and_mount(disk_list, mount_dir, mount_options):
    LvmUtil.activateAll()

    # pv list
    pvDevPathList = HandyUtil.lvmEnsureVgLvAndGetPvList(StorageLayoutImpl.name)
    diskList = [PartiUtil.partiToDisk(x) for x in pvDevPathList]
    HandyMd.checkExtraDisks(StorageLayoutImpl.name, pvDevPathList, disk_list)
    bootHdd, bootDev = HandyMd.checkAndGetBootDiskAndBootDev(StorageLayoutImpl.name, diskList)

    # check root lv
    if Util.getBlkDevFsType(LvmUtil.rootLvDevPath) != Util.fsTypeExt4:
        raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.ROOT_PARTITION_FS_SHOULD_BE(Util.fsTypeExt4))

    # return
    ret = StorageLayoutImpl()
    ret._md = EfiMultiDisk(diskList=diskList, bootHdd=bootHdd)
    ret._swap = HandyUtil.swapLvDetectAndNew(StorageLayoutImpl.name)
    ret._mnt = MountEfi(mount_dir)

    # mount
    Util.mntMount(mount_dir, Util.optimizeMntParamList(ret.get_params_for_mount(), mount_options))
    return ret


def create_and_mount(disk_list, mount_dir, mount_options):
    # add disks
    md = EfiMultiDisk()
    HandyMd.checkAndAddDisks(disk_list)

    # create pv, create vg, create root lv
    for disk in md.get_disk_list():
        LvmUtil.addPvToVg(md.get_disk_data_partition(disk), LvmUtil.vgName)
    LvmUtil.createLvWithDefaultSize(LvmUtil.vgName, LvmUtil.rootLvName)

    # return
    ret = StorageLayoutImpl()
    ret._md = md
    ret._swap = SwapLvmLv(False)
    ret._mnt = MountEfi(mount_dir)

    # mount
    Util.mntMount(mount_dir, Util.optimizeMntParamList(ret.get_params_for_mount(), mount_options))
    return ret
