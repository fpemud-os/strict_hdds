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
from .util import Util, GptUtil, BcachefsUtil, CacheGroup, SwapParti
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

    def __init__(self):
        super().__init__()

        self._cg = None         # CacheGroup

    @property
    def boot_mode(self):
        return StorageLayout.BOOT_MODE_EFI

    @property
    def dev_rootfs(self):
        return ":".join(self.get_disk_list())

    @property
    def dev_swap(self):
        return self._cg.get_ssd_swap_partition()

    @SwapParti.proxy
    def get_boot_disk(self):
        pass

    @SwapParti.proxy
    def check_swap_size(self):
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

            # ssd partition 3: make it as cache device and add it to bcachefs
            parti = self._cg.get_ssd_cache_partition()
            BcachefsUtil.makeDevice(parti)

            return True     # boot disk changed
        else:
            lastBootHdd = self._cg.get_boot_hdd()

            self._cg.add_hdd(devpath)

            # hdd partition 2: make it as backing device and add it to bcachefs
            parti = self._cg.get_hdd_data_partition(devpath)
            BcachefsUtil.makeDevice(parti)
            Util.cmdCall("/sbin/bcachefs", "device", "add", parti, "/")

            return lastBootHdd != self._cg.get_boot_hdd()     # boot disk may change

    def remove_disk(self, devpath):
        assert devpath is not None

        if self._cg.get_ssd() is not None and devpath == self._cg.get_ssd():
            if self._cg.get_ssd_swap_partition() is not None:
                if Util.systemdFindSwapService(self._cg.get_ssd_swap_partition()) is not None:
                    raise errors.StorageLayoutRemoveDiskError(errors.SWAP_IS_IN_USE)

            # ssd partition 3: remove from bcachefs
            BcachefsUtil.removeDevice(self._cg.get_ssd_cache_partition())

            # remove
            self._cg.remove_ssd()

            return True     # boot disk changed
        else:
            assert devpath in self._cg.get_hdd_list()

            if self._cg.get_hdd_count() <= 1:
                raise errors.StorageLayoutRemoveDiskError(errors.CAN_NOT_REMOVE_LAST_HDD)

            lastBootHdd = self._cg.get_boot_hdd()

            # hdd partition 2: remove from bcachefs
            BcachefsUtil.removeDevice(self._cg.get_hdd_data_partition(devpath))

            # remove
            self._cg.remove_hdd(devpath)

            return lastBootHdd != self._cg.get_boot_hdd()     # boot disk may change


def create(ssd=None, hdd_list=None, dry_run=False):
    if ssd is None and hdd_list is None:
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

        # create bcachefs
        if ret._cg.get_ssd() is not None:
            ssd_list2 = [ret._cg.get_ssd_cache_partition()]
        else:
            ssd_list2 = []
        hdd_list2 = [ret._cg.get_hdd_data_partition(x) for x in hdd_list]
        BcachefsUtil.createBcachefs(ssd_list2, hdd_list2, 1, 1)
    else:
        ret._cg = CacheGroup(ssd=ssd,
                             ssdEspParti=Util.devPathDiskToPartition(ssd, 1),
                             ssdSwapParti=Util.devPathDiskToPartition(ssd, 2),
                             ssdCacheParti=Util.devPathDiskToPartition(ssd, 3),
                             hddList=hdd_list)

    return ret


def parse(bootDev, rootDev):
    ret = StorageLayoutImpl()

    if not GptUtil.isEspPartition(bootDev):
        raise errors.StorageLayoutParseError(ret.name, errors.BOOT_DEV_IS_NOT_ESP)

    # ssd
    ret._ssd = Util.devPathPartitionToDisk(bootDev)
    if ret._ssd not in ret._hddDict:
        ret._ssdEspParti = Util.devPathDiskToPartition(ret._ssd, 1)
        if os.path.exists(Util.devPathDiskToPartition(ret._ssd, 3)):
            ret._ssdSwapParti = Util.devPathDiskToPartition(ret._ssd, 2)
            ret._ssdCacheParti = Util.devPathDiskToPartition(ret._ssd, 3)
            if os.path.exists(Util.devPathDiskToPartition(ret._ssd, 4)):
                raise errors.StorageLayoutParseError(ret.name, errors.DISK_HAS_REDUNDANT_PARTITION(ret._ssd))
        else:
            ret._ssdCacheParti = Util.devPathDiskToPartition(ret._ssd, 2)

        # ret._ssdEspParti
        if ret._ssdEspParti != bootDev:
            raise errors.StorageLayoutParseError(ret.name, "SSD is not boot device")
        if Util.getBlkDevSize(ret._ssdEspParti) != Util.getEspSize():
            raise errors.StorageLayoutParseError(ret.name, errors.DISK_SIZE_INVALID(ret._ssdEspParti))

        # ret._ssdSwapParti
        if ret._ssdSwapParti is not None:
            if not os.path.exists(ret._ssdSwapParti):
                raise errors.StorageLayoutParseError(ret.name, "SSD has no swap partition")
            if Util.getBlkDevFsType(ret._ssdSwapParti) != Util.fsTypeSwap:
                raise errors.StorageLayoutParseError(ret.name, errors.SWAP_DEV_HAS_INVALID_FS_FLAG(ret._ssdSwapParti))

        # ret._ssdCacheParti
        if not os.path.exists(ret._ssdCacheParti):
            raise errors.StorageLayoutParseError(ret.name, "SSD has no cache partition")

        for pvHdd, bcacheDev in ret._hddDict.items():
            tlist = BcachefsUtil.getSlaveDevPathList(bcacheDev)
            if len(tlist) < 2:
                raise errors.StorageLayoutParseError(ret.name, "%s(%s) has no cache device" % (pvHdd, bcacheDev))
            if len(tlist) > 2:
                raise errors.StorageLayoutParseError(ret.name, "%s(%s) has multiple cache devices" % (pvHdd, bcacheDev))
            if tlist[0] != ret._ssdCacheParti:
                raise errors.StorageLayoutParseError(ret.name, "%s(%s) has invalid cache device" % (pvHdd, bcacheDev))
    else:
        ret._ssd = None

    # boot harddisk
    if ret._ssd is None:
        ret._bootHdd = Util.devPathPartitionToDisk(bootDev)

    return ret