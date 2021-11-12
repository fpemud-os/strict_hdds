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
import re
from .util import Util, GptUtil, BcacheUtil, LvmUtil, CacheGroup, SwapParti
from . import errors
from . import StorageLayout


class StorageLayoutImpl(StorageLayout):
    """Layout:
           /dev/sda                 SSD, GPT (cache-disk)
               /dev/sda1            ESP partition
               /dev/sda2            swap device
               /dev/sda3            bcache cache device
           /dev/sdb                 Non-SSD, GPT
               /dev/sdb1            reserved ESP partition
               /dev/sdb2            bcache backing device
           /dev/sdc                 Non-SSD, GPT
               /dev/sdc1            reserved ESP partition
               /dev/sdc2            bcache backing device
           /dev/bcache0             corresponds to /dev/sdb2, LVM-PV for VG hdd
           /dev/bcache1             corresponds to /dev/sdc2, LVM-PV for VG hdd
           /dev/mapper/hdd.root     root device, EXT4
       Description:
           1. /dev/sda1 and /dev/sd{b,c}1 must has the same size
           2. /dev/sda1, /dev/sda2 and /dev/sda3 is order-sensitive, no extra partition is allowed
           3. /dev/sd{b,c}1 and /dev/sd{b,c}2 is order-sensitive, no extra partition is allowed
           4. cache-disk is optional, and only one cache-disk is allowed at most
           5. cache-disk can have no swap partition, /dev/sda2 would be the cache device then
           6. extra LVM-LV is allowed to exist
           7. extra harddisk is allowed to exist
    """

    def __init__(self):
        super().__init__()

        self._cg = None                  # CacheGroup
        self._hddDict = dict()           # dict<hddDev,bcacheDev>

    @property
    def boot_mode(self):
        return StorageLayout.BOOT_MODE_EFI

    @property
    def dev_rootfs(self):
        return LvmUtil.rootLvDevPath

    @property
    def dev_swap(self):
        return self._cg.get_ssd_swap_partition()

    def get_boot_disk(self):
        return self._cg.get_ssd() if self._cg.get_ssd() is not None else self._cg.get_boot_hdd()

    @SwapParti.proxy
    def check_swap_size(self):
        pass

    def optimize_rootdev(self):
        LvmUtil.autoExtendLv(LvmUtil.rootLvDevPath)
        Util.cmdExec("/sbin/resize2fs", LvmUtil.rootLvDevPath)

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
            BcacheUtil.attachCacheDevice(self._hddDict.values(), parti)

            return True     # boot disk changed
        else:
            lastBootHdd = self._cg.get_boot_hdd()

            self._cg.add_hdd(devpath)

            # hdd partition 2: make it as backing device, create lvm physical volume on bcache device and add it to volume group
            parti = self._cg.get_ssd_cache_partition()
            BcacheUtil.makeDevice(parti, True)
            BcacheUtil.registerBackingDevice(parti)
            bcacheDev = BcacheUtil.findByBackingDevice(parti)
            if self._ssd is not None:
                BcacheUtil.attachCacheDevice([bcacheDev], self._cg.get_ssd_cache_partition())
            LvmUtil.addPvToVg(bcacheDev, LvmUtil.vgName)
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

            # hdd partition 2: remove from volume group
            bcacheDev = BcacheUtil.findByBackingDevice(self._cg.get_hdd_data_partition(devpath))
            rc, out = Util.cmdCallWithRetCode("/sbin/lvm", "pvmove", bcacheDev)
            if rc != 5:
                raise errors.StorageLayoutRemoveDiskError("failed")
            Util.cmdCall("/sbin/lvm", "vgreduce", LvmUtil.vgName, bcacheDev)
            BcacheUtil.stopBackingDevice(bcacheDev)
            del self._hddDict[devpath]

            # remove
            self._cg.remove_hdd(devpath)

            return lastBootHdd != self._cg.get_boot_hdd()     # boot disk may change


def create(ssd=None, hdd_list=None, dry_run=False):
    if ssd is None and hdd_list is None:
        # discover all fixed harddisks
        ssdList, hdd_list = Util.getDevPathListForFixedSsdAndHdd()
        if len(ssdList) == 0:
            pass
        elif len(ssdList) == 1:
            ssd = ssdList[0]
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

        # create lvm physical volume on bcache device and add it to volume group
        for bcacheDev in ret._hddDict.values():
            LvmUtil.addPvToVg(bcacheDev, LvmUtil.vgName, mayCreate=True)

        # create root lv
        LvmUtil.createLvWithDefaultSize(LvmUtil.vgName, LvmUtil.rootLvName)
    else:
        ret._cg = CacheGroup(ssd=ssd,
                             ssdEspParti=Util.devPathDiskToParti(ssd, 1),
                             ssdSwapParti=Util.devPathDiskToParti(ssd, 2),
                             ssdCacheParti=Util.devPathDiskToParti(ssd, 3),
                             hddList=hdd_list)
        for i in range(0, len(hdd_list)):
            ret._hddDict[hdd_list[i]] = "/dev/bcache%d" % (i)

    return ret


def parse(bootDev, rootDev):
    ret = StorageLayoutImpl()

    if not GptUtil.isEspPartition(bootDev):
        raise errors.StorageLayoutParseError(ret.name, errors.BOOT_DEV_IS_NOT_ESP)

    # vg
    if not Util.cmdCallTestSuccess("/sbin/lvm", "vgdisplay", LvmUtil.vgName):
        raise errors.StorageLayoutParseError(ret.name, errors.LVM_VG_NOT_FOUND(LvmUtil.vgName))

    # pv list
    out = Util.cmdCall("/sbin/lvm", "pvdisplay", "-c")
    for m in re.finditer("(/dev/\\S+):%s:.*" % (LvmUtil.vgName), out, re.M):
        if re.fullmatch("/dev/bcache[0-9]+", m.group(1)) is None:
            raise errors.StorageLayoutParseError(ret.name, "volume group \"%s\" has non-bcache physical volume" % (LvmUtil.vgName))
        bcacheDev = m.group(1)
        tlist = BcacheUtil.getSlaveDevPathList(bcacheDev)
        hddDev, partId = Util.devPathPartiToDiskAndPartiId(tlist[-1])
        if partId != 2:
            raise errors.StorageLayoutParseError(ret.name, "physical volume partition of %s is not %s" % (hddDev, Util.devPathDiskToParti(hddDev, 2)))
        if os.path.exists(Util.devPathDiskToParti(hddDev, 3)):
            raise errors.StorageLayoutParseError(ret.name, errors.DISK_HAS_REDUNDANT_PARTITION(hddDev))
        ret._hddDict[hddDev] = bcacheDev

    # root lv
    out = Util.cmdCall("/sbin/lvm", "lvdisplay", "-c")
    if re.search("/dev/hdd/root:%s:.*" % (LvmUtil.vgName), out, re.M) is not None:
        fs = Util.getBlkDevFsType(LvmUtil.rootLvDevPath)
        if fs != Util.fsTypeExt4:
            raise errors.StorageLayoutParseError(ret.name, "root partition file system is \"%s\", not \"ext4\"" % (fs))
    else:
        raise errors.StorageLayoutParseError(ret.name, errors.LVM_LV_NOT_FOUND(LvmUtil.rootLvDevPath))

    # ssd
    ssd = Util.devPathPartiToDisk(bootDev)
    if ssd not in ret._hddDict:
        ssdEspParti = Util.devPathDiskToParti(ssd, 1)
        if os.path.exists(Util.devPathDiskToParti(ssd, 3)):
            ssdSwapParti = Util.devPathDiskToParti(ssd, 2)
            ssdCacheParti = Util.devPathDiskToParti(ssd, 3)
            if os.path.exists(Util.devPathDiskToParti(ssd, 4)):
                raise errors.StorageLayoutParseError(ret.name, errors.DISK_HAS_REDUNDANT_PARTITION(ssd))
        else:
            ssdCacheParti = Util.devPathDiskToParti(ssd, 2)

        # ssdEspParti
        if ssdEspParti != bootDev:
            raise errors.StorageLayoutParseError(ret.name, "SSD is not boot device")
        if Util.getBlkDevSize(ssdEspParti) != Util.getEspSize():
            raise errors.StorageLayoutParseError(ret.name, errors.PARTITION_SIZE_INVALID(ssdEspParti))

        # ssdSwapParti
        if ssdSwapParti is not None:
            if not os.path.exists(ssdSwapParti):
                raise errors.StorageLayoutParseError(ret.name, "SSD has no swap partition")
            if Util.getBlkDevFsType(ssdSwapParti) != Util.fsTypeSwap:
                raise errors.StorageLayoutParseError(ret.name, errors.SWAP_DEV_HAS_INVALID_FS_FLAG(ssdSwapParti))

        # ssdCacheParti
        if not os.path.exists(ssdCacheParti):
            raise errors.StorageLayoutParseError(ret.name, "SSD has no cache partition")

        for pvHdd, bcacheDev in ret._hddDict.items():
            tlist = BcacheUtil.getSlaveDevPathList(bcacheDev)
            if len(tlist) < 2:
                raise errors.StorageLayoutParseError(ret.name, "%s(%s) has no cache device" % (pvHdd, bcacheDev))
            if len(tlist) > 2:
                raise errors.StorageLayoutParseError(ret.name, "%s(%s) has multiple cache devices" % (pvHdd, bcacheDev))
            if tlist[0] != ssdCacheParti:
                raise errors.StorageLayoutParseError(ret.name, "%s(%s) has invalid cache device" % (pvHdd, bcacheDev))
        if True:
            partName, partId = Util.devPathPartiToDiskAndPartiId(ssdCacheParti)
            nextPartName = Util.devPathDiskToParti(partName, partId + 1)
            if os.path.exists(nextPartName):
                raise errors.StorageLayoutParseError(ret.name, errors.DISK_HAS_REDUNDANT_PARTITION(ssd))
    else:
        ssd = None
        ssdEspParti = None
        ssdSwapParti = None
        ssdCacheParti = None

    # boot harddisk
    if ssd is None:
        bootHdd = Util.devPathPartiToDisk(bootDev)

    # CacheGroup object
    ret._cg = CacheGroup(ssd=ssd, ssdEspParti=ssdEspParti, ssdSwapParti=ssdSwapParti, ssdCacheParti=ssdCacheParti,
                         hddList=ret._hddDict.keys(), bootHdd=bootHdd)

    return ret
