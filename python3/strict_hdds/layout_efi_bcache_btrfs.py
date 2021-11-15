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


from .util import Util, GptUtil, BcacheUtil, EfiCacheGroup, SwapParti
from .mount import MountEfi
from . import errors
from . import StorageLayout


class StorageLayoutImpl(StorageLayout):
    """Layout:
           /dev/sda                      SSD, GPT (cache-disk)
               /dev/sda1                 ESP partition
               /dev/sda2                 swap device
               /dev/sda3                 bcache cache device
           /dev/sdb                      Non-SSD, GPT
               /dev/sdb1                 reserved ESP partition
               /dev/sdb2                 bcache backing device
           /dev/sdc                      Non-SSD, GPT
               /dev/sdc1                 reserved ESP partition
               /dev/sdc2                 bcache backing device
           /dev/bcache0:/dev/bcache1     root device, btrfs
              /dev/bcache0               corresponds to /dev/sdb2, btrfs device
              /dev/bcache1               corresponds to /dev/sdc2, btrfs device
       Description:
           1. /dev/sda1 and /dev/sd{b,c}1 must has the same size
           2. /dev/sda1, /dev/sda2 and /dev/sda3 is order-sensitive, no extra partition is allowed
           3. /dev/sd{b,c}1 and /dev/sd{b,c}2 is order-sensitive, no extra partition is allowed
           4. cache-disk is optional, and only one cache-disk is allowed at most
           5. cache-disk can have no swap partition, /dev/sda2 would be the cache device then
           6. extra harddisk is allowed to exist
    """

    def __init__(self):
        self._cg = None                     # EfiCacheGroup
        self._hddDict = dict()              # dict<hddDev,bcacheDev>
        self._swap = None                   # SwapParti
        self._mnt = None                    # MountEfi

    @property
    def boot_mode(self):
        return StorageLayout.BOOT_MODE_EFI

    @property
    def dev_rootfs(self):
        return sorted(self._cg.get_hdd_list())[0]

    @property
    @EfiCacheGroup.proxy
    def dev_boot(self):
        pass

    @property
    @EfiCacheGroup.proxy
    def dev_swap(self):
        pass

    @property
    @MountEfi.proxy
    def mount_point(self):
        pass

    @property
    @EfiCacheGroup.proxy
    def boot_disk(self):
        pass

    def umount_and_dispose(self):
        if True:
            self._mnt.umount()
            del self._mnt
        del self._swap
        if True:
            # FIXME: stop and unregister bcache
            del self._hddDict
            del self._cg

    @MountEfi.proxy
    def remount_rootfs(self, mount_options):
        pass

    @MountEfi.proxy
    def get_bootdir_rw_controller(self):
        pass

    def check(self):
        self._swap.check_swap_size()

    def optimize_rootdev(self):
        # FIXME: btrfs balance
        pass

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

    def add_disk(self, disk):
        assert disk is not None

        if disk not in Util.getDevPathListForFixedDisk():
            raise errors.StorageLayoutAddDiskError(disk, errors.NOT_DISK)

        lastBootHdd = self._cg.boot_disk
        self._cg.add_ssd(disk)

        if Util.isBlkDevSsdOrHdd(disk):
            # ssd partition 3: make it as cache device
            parti = self._cg.get_ssd_cache_partition()
            BcacheUtil.makeDevice(parti, False)
            BcacheUtil.registerCacheDevice(parti)
            BcacheUtil.attachCacheDevice(self._cg.get_hdd_list(), parti)
        else:
            # hdd partition 2: make it as backing device and add it to btrfs filesystem
            parti = self._cg.get_hdd_data_partition(disk)
            BcacheUtil.makeDevice(parti, True)
            BcacheUtil.registerBackingDevice(parti)
            bcacheDev = BcacheUtil.findByBackingDevice(parti)
            if self._cg.get_ssd() is not None:
                BcacheUtil.attachCacheDevice([bcacheDev], self._cg.get_ssd_cache_partition())
            Util.cmdCall("/sbin/btrfs", "device", "add", bcacheDev, "/")
            self._hddDict[disk] = bcacheDev

        # return True means boot disk is changed
        return lastBootHdd != self._cg.boot_disk

    def remove_disk(self, disk):
        assert disk is not None

        lastBootHdd = self._cg.boot_disk

        if self._cg.get_ssd() is not None and disk == self._cg.get_ssd():
            # check if swap is in use
            if self._cg.get_ssd_swap_partition() is not None:
                if Util.systemdFindSwapService(self._cg.get_ssd_swap_partition()) is not None:
                    raise errors.StorageLayoutRemoveDiskError(errors.SWAP_IS_IN_USE)

            # ssd partition 3: remove from cache
            BcacheUtil.unregisterCacheDevice(self._cg.get_ssd_cache_partition())
        elif disk in self._cg.get_hdd_list():
            # check for last hdd
            if len(self._cg.get_hdd_list()) <= 1:
                raise errors.StorageLayoutRemoveDiskError(errors.CAN_NOT_REMOVE_LAST_HDD)

            # hdd partition 2: remove from btrfs and bcache
            bcacheDev = BcacheUtil.findByBackingDevice(self._cg.get_hdd_data_partition(disk))
            Util.cmdCall("/sbin/btrfs", "device", "delete", bcacheDev, "/")
            BcacheUtil.stopBackingDevice(bcacheDev)
            del self._hddDict[disk]
        else:
            assert False

        # remove
        self._cg.remove_hdd(disk)

        # return True means boot disk is changed
        return lastBootHdd != self._cg.boot_disk


def parse(boot_dev, root_dev):
    if not GptUtil.isEspPartition(boot_dev):
        raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.BOOT_DEV_IS_NOT_ESP)

    ret = StorageLayoutImpl()
    return ret


def detect_and_mount(disk_list, mount_dir):
    ssd_list, hdd_list = Util.splitSsdAndHddFromFixedDiskDevPathList(disk_list)
    _checkSsdAndHdd(ssd_list, hdd_list)



def create_and_mount(disk_list, mount_dir):
    ssd_list, hdd_list = Util.splitSsdAndHddFromFixedDiskDevPathList(disk_list)
    _checkSsdAndHdd(ssd_list, hdd_list)


    ret._cg = EfiCacheGroup()

    # add disks, process ssd first so that minimal boot disk change is need
    if ssd is not None:
        ret._cg.add_ssd(ssd)
    for hdd in hdd_list:
        ret._cg.add_hdd(hdd)

    # hdd partition 2: make them as backing device
    for hdd in hdd_list:
        parti = ret._cg.get_hdd_data_partition(hdd)
        BcacheUtil.makeDevice(parti, True)
        BcacheUtil.registerBackingDevice(parti)
        ret._hddDict[hdd] = BcacheUtil.findByBackingDevice(parti)

    # ssd partition 3: make it as cache device
    BcacheUtil.makeDevice(ret._cg.get_ssd_cache_partition(), False)
    BcacheUtil.registerCacheDevice(ret._cg.get_ssd_cache_partition())
    BcacheUtil.attachCacheDevice(ret._cg.get_hdd_list(), ret._cg.get_ssd_cache_partition())

    # create btrfs filesystem
    Util.cmdCall("/usr/sbin/mkfs.btrfs", "-d", "single", "-m", "single", *ret._hddDict.values())

    ret = StorageLayoutImpl()
    return ret


def _checkSsdAndHdd(ssd_list, hdd_list):
    if len(ssd_list) == 0:
        ssd = None
    elif len(ssd_list) == 1:
        ssd = ssd_list[0]
    else:
        raise errors.StorageLayoutCreateError(errors.MULTIPLE_SSD)
    if len(hdd_list) == 0:
        raise errors.StorageLayoutCreateError(errors.NO_DISK_WHEN_CREATE)
