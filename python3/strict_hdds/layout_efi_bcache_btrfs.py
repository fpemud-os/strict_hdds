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


from .util import Util, GptUtil, BcacheUtil, CacheGroup, SwapParti
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
        super().__init__()

        self._cg = None                     # CacheGroup
        self._hddDict = dict()              # dict<hddDev,bcacheDev>

    @property
    def boot_mode(self):
        return StorageLayout.BOOT_MODE_EFI

    @property
    def dev_rootfs(self):
        return sorted(self._cg.get_hdd_list())[0]

    @property
    def dev_swap(self):
        return self._cg.get_ssd_swap_partition()

    def get_boot_disk(self):
        return self._cg.get_ssd() if self._cg.get_ssd() is not None else self._cg.get_boot_hdd()

    @SwapParti.proxy
    def check_swap_size(self):
        pass

    def optimize_rootdev(self):
        # FIXME: btrfs balance
        pass

    @CacheGroup.proxy
    def get_esp(self):
        pass

    @CacheGroup.proxy
    def get_esp_sync_info(self):
        pass

    @CacheGroup.proxy
    def sync_esp(self, src, dst):
        pass

    @CacheGroup.proxy
    def get_ssd(self):
        pass

    @CacheGroup.proxy
    def get_ssd_esp_partition(self):
        pass

    @CacheGroup.proxy
    def get_ssd_swap_partition(self):
        pass

    @CacheGroup.proxy
    def get_ssd_cache_partition(self):
        pass

    @CacheGroup.proxy
    def get_disk_list(self):
        pass

    def add_disk(self, devpath):
        assert devpath is not None

        if devpath not in Util.getDevPathListForFixedDisk():
            raise errors.StorageLayoutAddDiskError(devpath, errors.NOT_DISK)

        if Util.isBlkDevSsdOrHdd(devpath):
            self._cg.add_ssd(devpath)

            # ssd partition 3: make it as cache device
            parti = self._cg.get_ssd_cache_partition()
            BcacheUtil.makeDevice(parti, False)
            BcacheUtil.registerCacheDevice(parti)
            BcacheUtil.attachCacheDevice(self._cg.get_hdd_list(), parti)

            return True     # boot disk changed
        else:
            lastBootHdd = self._cg.get_boot_hdd()

            self._cg.add_hdd(devpath)

            # hdd partition 2: make it as backing device and add it to btrfs filesystem
            parti = self._cg.get_hdd_data_partition(devpath)
            BcacheUtil.makeDevice(parti, True)
            BcacheUtil.registerBackingDevice(parti)
            bcacheDev = BcacheUtil.findByBackingDevice(parti)
            if self._cg.get_ssd() is not None:
                BcacheUtil.attachCacheDevice([bcacheDev], self._cg.get_ssd_cache_partition())
            Util.cmdCall("/sbin/btrfs", "device", "add", bcacheDev, "/")
            self._hddDict[devpath] = bcacheDev

            return lastBootHdd != self._cg.get_boot_hdd()     # boot disk may change

    def remove_disk(self, devpath):
        assert devpath is not None

        if self._cg.get_ssd() is not None and devpath == self._cg.get_ssd():
            if self._cg.get_ssd_swap_partition() is not None:
                if Util.systemdFindSwapService(self._cg.get_ssd_swap_partition()) is not None:
                    raise errors.StorageLayoutRemoveDiskError(errors.SWAP_IS_IN_USE)

            # ssd partition 3: remove from cache
            BcacheUtil.unregisterCacheDevice(self._cg.get_ssd_cache_partition())

            # remove
            self._cg.remove_ssd()

            return True     # boot disk changed
        else:
            assert devpath in self._cg.get_hdd_list()

            if self._cg.get_hdd_count() <= 1:
                raise errors.StorageLayoutRemoveDiskError(errors.CAN_NOT_REMOVE_LAST_HDD)

            lastBootHdd = self._cg.get_boot_hdd()

            # hdd partition 2: remove from btrfs and bcache
            bcacheDev = BcacheUtil.findByBackingDevice(self._cg.get_hdd_data_partition(devpath))
            Util.cmdCall("/sbin/btrfs", "device", "delete", bcacheDev, "/")
            BcacheUtil.stopBackingDevice(bcacheDev)
            del self._hddDict[devpath]

            # remove
            self._cg.remove_hdd(devpath)

            return lastBootHdd != self._cg.get_boot_hdd()     # boot disk may change


def create(ssd=None, hdd_list=None, dry_run=False):
    if ssd is None and hdd_list is None:
        # discover all fixed harddisks
        ssd_list, hdd_list = Util.getDevPathListForFixedSsdAndHdd()
        if len(ssd_list) == 0:
            pass
        elif len(ssd_list) == 1:
            ssd = ssd_list[0]
        else:
            raise errors.StorageLayoutCreateError(errors.MULTIPLE_SSD)
        if len(hdd_list) == 0:
            raise errors.StorageLayoutCreateError(errors.NO_DISK_WHEN_CREATE)
    else:
        assert hdd_list is not None and len(hdd_list) > 0

    ret = StorageLayoutImpl()

    if not dry_run:
        ret._cg = CacheGroup()

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
    else:
        ret._cg = CacheGroup(ssd=ssd,
                             ssdEspParti=Util.devPathDiskToPartition(ssd, 1),
                             ssdSwapParti=Util.devPathDiskToPartition(ssd, 2),
                             ssdCacheParti=Util.devPathDiskToPartition(ssd, 3),
                             hddList=hdd_list)
        for i in range(0, len(hdd_list)):
            ret._hddDict[hdd_list[i]] = "/dev/bcache%d" % (i)

    return ret


def parse(bootDev, rootDev):
    ret = StorageLayoutImpl()

    if not GptUtil.isEspPartition(bootDev):
        raise errors.StorageLayoutParseError(ret.name, errors.BOOT_DEV_IS_NOT_ESP)

    return ret
