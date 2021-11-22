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


from .util import Util, BcachefsUtil
from .handy import EfiCacheGroup, SnapshotBcachefs, MountEfi, HandyCg, HandyUtil
from . import errors
from . import StorageLayout


class StorageLayoutImpl(StorageLayout):
    """Layout:
           /dev/sda                         SSD, GPT (cache-disk)
               /dev/sda1                    ESP partition
               /dev/sda2                    swap device
               /dev/sda3                    bcachefs cache device
           /dev/sdb                         Non-SSD, GPT
               /dev/sdb1                    reserved ESP partition
               /dev/sdb2                    bcachefs backing device
           /dev/sdc                         Non-SSD, GPT
               /dev/sdc1                    reserved ESP partition
               /dev/sdc2                    bcachefs backing device
           /dev/sda3:/dev/sdb2:/dev/sdc2    root device
       Description:
           1. /dev/sda1 and /dev/sd{b,c}1 must has the same size
           2. /dev/sda1, /dev/sda2 and /dev/sda3 is order-sensitive, no extra partition is allowed
           3. /dev/sd{b,c}1 and /dev/sd{b,c}2 is order-sensitive, no extra partition is allowed
           4. cache-disk is optional, and only one cache-disk is allowed at most
           5. cache-disk can have no swap partition, /dev/sda2 would be the cache device then
           6. extra harddisk is allowed to exist
    """

    def __init__(self, mount_dir):
        self._cg = None                     # EfiCacheGroup
        self._mnt = None                    # MountEfi

    @property
    def boot_mode(self):
        return StorageLayout.BOOT_MODE_EFI

    @property
    def dev_rootfs(self):
        tlist = []
        if self.get_ssd() is not None:
            tlist.append(self.get_ssd_cache_partition)
        for hdd in self.get_hdd_list():
            tlist.append(self.get_hdd_data_partition(hdd))
        return ":".join(tlist)

    @EfiCacheGroup.proxy
    @property
    def dev_boot(self):
        pass

    @EfiCacheGroup.proxy
    @property
    def dev_swap(self):
        pass

    @EfiCacheGroup.proxy
    @property
    def boot_disk(self):
        pass

    @SnapshotBcachefs.proxy
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
        if True:
            del self._cg

    @MountEfi.proxy
    def get_bootdir_rw_controller(self):
        pass

    def get_mntopt_list_for_mount(self, **kwargs):
        return []

    @EfiCacheGroup.proxy
    def get_esp(self):
        pass

    @EfiCacheGroup.proxy
    def get_pending_esp_list(self):
        pass

    @EfiCacheGroup.proxy
    def sync_esp(self, dst):
        pass

    @EfiCacheGroup.proxy
    def get_disk_list(self):
        pass

    @EfiCacheGroup.proxy
    def get_ssd(self):
        pass

    @EfiCacheGroup.proxy
    def get_ssd_esp_partition(self):
        pass

    @EfiCacheGroup.proxy
    def get_ssd_swap_partition(self):
        pass

    @EfiCacheGroup.proxy
    def get_ssd_cache_partition(self):
        pass

    @EfiCacheGroup.proxy
    def get_hdd_list(self):
        pass

    @EfiCacheGroup.proxy
    def get_hdd_esp_partition(self, disk):
        pass

    @EfiCacheGroup.proxy
    def get_hdd_data_partition(self, disk):
        pass

    @SnapshotBcachefs.proxy
    def get_snapshot_list(self):
        pass

    def add_disk(self, disk):
        assert disk is not None

        if disk not in Util.getDevPathListForFixedDisk():
            raise errors.StorageLayoutAddDiskError(disk, errors.NOT_DISK)

        if Util.isBlkDevSsdOrHdd(disk):
            self._cg.add_ssd(disk)

            # ssd partition 3: make it as cache device and add it to bcachefs
            parti = self._cg.get_ssd_cache_partition()
            BcachefsUtil.makeDevice(parti)

            return True     # boot disk changed
        else:
            lastBootHdd = self._cg.boot_disk

            self._cg.add_hdd(disk)

            # hdd partition 2: make it as backing device and add it to bcachefs
            parti = self._cg.get_hdd_data_partition(disk)
            BcachefsUtil.makeDevice(parti)
            Util.cmdCall("/sbin/bcachefs", "device", "add", parti, self._mnt.mount_point)

            return lastBootHdd != self._cg.boot_disk     # boot disk may change

    def remove_disk(self, disk):
        assert disk is not None

        if self._cg.get_ssd() is not None and disk == self._cg.get_ssd():
            if self._cg.get_ssd_swap_partition() is not None:
                if Util.isSwapFileOrPartitionBusy(self._cg.get_ssd_swap_partition()):
                    raise errors.StorageLayoutRemoveDiskError(errors.SWAP_IS_IN_USE)

            # ssd partition 3: remove from bcachefs
            BcachefsUtil.removeDevice(self._cg.get_ssd_cache_partition())

            # remove
            self._cg.remove_ssd()

            return True     # boot disk changed
        else:
            assert disk in self._cg.get_hdd_list()

            if len(self._cg.get_hdd_list()) <= 1:
                raise errors.StorageLayoutRemoveDiskError(errors.CAN_NOT_REMOVE_LAST_HDD)

            lastBootHdd = self._cg.boot_disk

            # hdd partition 2: remove from bcachefs
            BcachefsUtil.removeDevice(self._cg.get_hdd_data_partition(disk))

            # remove
            self._cg.remove_hdd(disk)

            return lastBootHdd != self._cg.boot_disk     # boot disk may change

    @SnapshotBcachefs.proxy
    def create_snapshot(self, snapshot_name):
        pass

    @SnapshotBcachefs.proxy
    def remove_snapshot(self, snapshot_name):
        pass

    def _check_impl(self, check_item, auto_fix=False, error_callback=None):
        if check_item == Util.checkItemBasic:
            self._cg.check_esp(auto_fix, error_callback)
        elif check_item == "ssd":
            self._cg.check_ssd(auto_fix, error_callback)
        elif check_item == "swap":
            self._cg.check_swap(auto_fix, error_callback)
        else:
            assert False


def parse(boot_dev, root_dev):
    if boot_dev is None:
        raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.BOOT_DEV_NOT_EXIST)
    if Util.getBlkDevFsType(root_dev) != Util.fsTypeBcachefs:
        raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.ROOT_PARTITION_FS_SHOULD_BE(Util.fsTypeBcachefs))

    # ssd, hdd_list, boot_disk
    ssd, hddList = HandyCg.checkAndGetSsdAndHddList(BcachefsUtil.getSlaveSsdDevPatListAndHddDevPathList(root_dev), False)
    ssdEspParti, ssdSwapParti, ssdCacheParti = HandyCg.checkAndGetSsdPartitions(StorageLayoutImpl.name, ssd)
    bootHdd = HandyCg.checkAndGetBootHddFromBootDev(StorageLayoutImpl.name, boot_dev, ssdEspParti, hddList)

    # return
    ret = StorageLayoutImpl()
    ret._cg = EfiCacheGroup(ssd=ssd, ssdEspParti=ssdEspParti, ssdSwapParti=ssdSwapParti, ssdCacheParti=ssdCacheParti, hddList=hddList, bootHdd=bootHdd)
    ret._mnt = MountEfi("/")
    return ret


def detect_and_mount(disk_list, mount_dir, mnt_opt_list):
    # filter
    diskList = [x for x in disk_list if Util.getBlkDevFsType(x) == Util.fsTypeBcachefs]
    if len(diskList) == 0:
        raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.DISK_NOT_FOUND)

    # ssd, hdd_list, boot_disk
    ssd, hddList = HandyCg.checkAndGetSsdAndHddList(Util.splitSsdAndHddFromFixedDiskDevPathList(disk_list))
    ssdEspParti, ssdSwapParti, ssdCacheParti = HandyCg.checkAndGetSsdPartitions(StorageLayoutImpl.name, ssd)
    bootHdd = HandyCg.checkAndGetBootHddAndBootDev(StorageLayoutImpl.name, ssdEspParti, hddList)[0]

    # return
    ret = StorageLayoutImpl()
    ret._cg = EfiCacheGroup(ssd=ssd, ssdEspParti=ssdEspParti, ssdSwapParti=ssdSwapParti, ssdCacheParti=ssdCacheParti, hddList=hddList, bootHdd=bootHdd)
    ret._mnt = MountEfi(mount_dir)

    # mount
    tlist = mnt_opt_list + ret.get_mntopt_list_for_mount()
    HandyUtil.checkMntOptList(tlist)
    MountEfi.mount(ret.dev_rootfs, ret.dev_boot, mount_dir, tlist)
    return ret


def create_and_mount(disk_list, mount_dir, mnt_opt_list):
    # add disks to cache group
    cg = EfiCacheGroup()
    HandyCg.checkAndAddDisks(cg, *Util.splitSsdAndHddFromFixedDiskDevPathList(disk_list))

    # create bcachefs
    if cg.get_ssd() is not None:
        ssd_list2 = [cg.get_ssd_cache_partition()]
    else:
        ssd_list2 = []
    hdd_list2 = [cg.get_hdd_data_partition(x) for x in cg.get_hdd_list()]
    BcachefsUtil.createBcachefs(ssd_list2, hdd_list2, 1, 1)

    # return
    ret = StorageLayoutImpl()
    ret._cg = cg
    ret._mnt = MountEfi(mount_dir)

    # mount
    tlist = mnt_opt_list + ret.get_mntopt_list_for_mount()
    HandyUtil.checkMntOptList(tlist)
    MountEfi.mount(ret.dev_rootfs, ret.dev_boot, mount_dir, tlist)
    return ret
