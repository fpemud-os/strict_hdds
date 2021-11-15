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
from .util import Util, GptUtil, BcacheUtil, LvmUtil, EfiCacheGroup
from .mount import MountEfi
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
        self._cg = None                     # EfiCacheGroup
        self._hddDict = dict()              # dict<hddDev,bcacheDev>
        self._mnt = None                    # MountEfi

    @property
    def boot_mode(self):
        return StorageLayout.BOOT_MODE_EFI

    @property
    def dev_rootfs(self):
        return LvmUtil.rootLvDevPath

    @property
    @EfiCacheGroup.proxy
    def dev_boot(self):
        pass

    @property
    @EfiCacheGroup.proxy
    def dev_swap(self):
        pass

    @property
    @EfiCacheGroup.proxy
    def boot_disk(self):
        pass

    def umount_and_dispose(self):
        if True:
            self._mnt.umount()
            del self._mnt
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
        if self.dev_swap is not None:
            if Util.getBlkDevSize(self.dev_swap) < Util.getSwapSize():
                raise errors.StorageLayoutCheckError(self.name, errors.SWAP_SIZE_TOO_SMALL)

    def optimize_rootdev(self):
        LvmUtil.autoExtendLv(LvmUtil.rootLvDevPath)
        Util.cmdExec("/sbin/resize2fs", LvmUtil.rootLvDevPath)

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

        if Util.isBlkDevSsdOrHdd(disk):
            self._cg.add_ssd(disk)

            # ssd partition 3: make it as cache device
            parti = self._cg.get_ssd_cache_partition()
            BcacheUtil.makeDevice(parti, False)
            BcacheUtil.registerCacheDevice(parti)
            BcacheUtil.attachCacheDevice(self._hddDict.values(), parti)
        else:
            self._cg.add_hdd(disk)

            # hdd partition 2: make it as backing device, create lvm physical volume on bcache device and add it to volume group
            parti = self._cg.get_ssd_cache_partition()
            BcacheUtil.makeDevice(parti, True)
            BcacheUtil.registerBackingDevice(parti)
            bcacheDev = BcacheUtil.findByBackingDevice(parti)
            if self.cg.get_ssd() is not None:
                BcacheUtil.attachCacheDevice([bcacheDev], self._cg.get_ssd_cache_partition())
            LvmUtil.addPvToVg(bcacheDev, LvmUtil.vgName)
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

            # remove
            self._cg.remove_ssd()
        elif disk in self._cg.get_hdd_list():
            # check for last hdd
            if len(self._cg.get_hdd_list()) <= 1:
                raise errors.StorageLayoutRemoveDiskError(errors.CAN_NOT_REMOVE_LAST_HDD)

            # hdd partition 2: remove from volume group
            bcacheDev = BcacheUtil.findByBackingDevice(self._cg.get_hdd_data_partition(disk))
            rc, out = Util.cmdCallWithRetCode("/sbin/lvm", "pvmove", bcacheDev)
            if rc != 5:
                raise errors.StorageLayoutRemoveDiskError("failed")
            Util.cmdCall("/sbin/lvm", "vgreduce", LvmUtil.vgName, bcacheDev)
            BcacheUtil.stopBackingDevice(bcacheDev)
            del self._hddDict[disk]

            # remove
            self._cg.remove_hdd(disk)
        else:
            assert False

        return lastBootHdd != self._cg.boot_disk     # boot disk may change


def parse(bootDev, rootDev):
    if not GptUtil.isEspPartition(bootDev):
        raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.BOOT_DEV_IS_NOT_ESP)

    hddDict = dict()

    # vg
    if not Util.cmdCallTestSuccess("/sbin/lvm", "vgdisplay", LvmUtil.vgName):
        raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.LVM_VG_NOT_FOUND(LvmUtil.vgName))

    # pv list
    out = Util.cmdCall("/sbin/lvm", "pvdisplay", "-c")
    for m in re.finditer("(/dev/\\S+):%s:.*" % (LvmUtil.vgName), out, re.M):
        if re.fullmatch("/dev/bcache[0-9]+", m.group(1)) is None:
            raise errors.StorageLayoutParseError(StorageLayoutImpl.name, "volume group \"%s\" has non-bcache physical volume" % (LvmUtil.vgName))
        bcacheDev = m.group(1)
        tlist = BcacheUtil.getSlaveDevPathList(bcacheDev)
        hddDev, partId = Util.devPathPartiToDiskAndPartiId(tlist[-1])
        if partId != 2:
            raise errors.StorageLayoutParseError(StorageLayoutImpl.name, "physical volume partition of %s is not %s" % (hddDev, Util.devPathDiskToParti(hddDev, 2)))
        if os.path.exists(Util.devPathDiskToParti(hddDev, 3)):
            raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.DISK_HAS_REDUNDANT_PARTITION(hddDev))
        hddDict[hddDev] = bcacheDev

    # root lv
    out = Util.cmdCall("/sbin/lvm", "lvdisplay", "-c")
    if re.search("/dev/hdd/root:%s:.*" % (LvmUtil.vgName), out, re.M) is not None:
        fs = Util.getBlkDevFsType(LvmUtil.rootLvDevPath)
        if fs != Util.fsTypeExt4:
            raise errors.StorageLayoutParseError(StorageLayoutImpl.name, "root partition file system is \"%s\", not \"ext4\"" % (fs))
    else:
        raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.LVM_LV_NOT_FOUND(LvmUtil.rootLvDevPath))

    # ssd
    ssd = Util.devPathPartiToDisk(bootDev)
    if ssd not in hddDict:
        ssdEspParti = Util.devPathDiskToParti(ssd, 1)
        if os.path.exists(Util.devPathDiskToParti(ssd, 3)):
            ssdSwapParti = Util.devPathDiskToParti(ssd, 2)
            ssdCacheParti = Util.devPathDiskToParti(ssd, 3)
            if os.path.exists(Util.devPathDiskToParti(ssd, 4)):
                raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.DISK_HAS_REDUNDANT_PARTITION(ssd))
        else:
            ssdCacheParti = Util.devPathDiskToParti(ssd, 2)

        # ssdEspParti
        if ssdEspParti != bootDev:
            raise errors.StorageLayoutParseError(StorageLayoutImpl.name, "SSD is not boot device")
        if Util.getBlkDevSize(ssdEspParti) != Util.getEspSize():
            raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.PARTITION_SIZE_INVALID(ssdEspParti))

        # ssdSwapParti
        if ssdSwapParti is not None:
            if not os.path.exists(ssdSwapParti):
                raise errors.StorageLayoutParseError(StorageLayoutImpl.name, "SSD has no swap partition")
            if Util.getBlkDevFsType(ssdSwapParti) != Util.fsTypeSwap:
                raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.SWAP_DEV_HAS_INVALID_FS_FLAG(ssdSwapParti))

        # ssdCacheParti
        if not os.path.exists(ssdCacheParti):
            raise errors.StorageLayoutParseError(StorageLayoutImpl.name, "SSD has no cache partition")

        for pvHdd, bcacheDev in hddDict.items():
            tlist = BcacheUtil.getSlaveDevPathList(bcacheDev)
            if len(tlist) < 2:
                raise errors.StorageLayoutParseError(StorageLayoutImpl.name, "%s(%s) has no cache device" % (pvHdd, bcacheDev))
            if len(tlist) > 2:
                raise errors.StorageLayoutParseError(StorageLayoutImpl.name, "%s(%s) has multiple cache devices" % (pvHdd, bcacheDev))
            if tlist[0] != ssdCacheParti:
                raise errors.StorageLayoutParseError(StorageLayoutImpl.name, "%s(%s) has invalid cache device" % (pvHdd, bcacheDev))
        if True:
            partName, partId = Util.devPathPartiToDiskAndPartiId(ssdCacheParti)
            nextPartName = Util.devPathDiskToParti(partName, partId + 1)
            if os.path.exists(nextPartName):
                raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.DISK_HAS_REDUNDANT_PARTITION(ssd))
    else:
        ssd = None
        ssdEspParti = None
        ssdSwapParti = None
        ssdCacheParti = None

    # boot harddisk
    if ssd is not None:
        bootHdd = ssdEspParti
    else:
        bootHdd = Util.devPathPartiToDisk(bootDev)

    # EfiCacheGroup object
    ret = StorageLayoutImpl()
    ret._cg = EfiCacheGroup(ssd=ssd, ssdEspParti=ssdEspParti, ssdSwapParti=ssdSwapParti, ssdCacheParti=ssdCacheParti, hddList=hddDict.keys(), bootHdd=bootHdd)
    ret._hddDict = hddDict
    ret._mnt = MountEfi("/")
    return ret


def detect_and_mount(disk_list, mount_dir):
    ssd, hdd_list = _getSsdAndHddList(disk_list)
    cg = EfiCacheGroup()


def create_and_mount(disk_list, mount_dir):
    ssd, hdd_list = _getSsdAndHddList(disk_list)
    cg = EfiCacheGroup()
    hddDict = dict()

    # add disks, process ssd first so that minimal boot disk change is need
    if ssd is not None:
        cg.add_ssd(ssd)
    for hdd in hdd_list:
        cg.add_hdd(hdd)

    # hdd partition 2: make them as backing device
    for hdd in hdd_list:
        parti = cg.get_hdd_data_partition(hdd)
        BcacheUtil.makeDevice(parti, True)
        BcacheUtil.registerBackingDevice(parti)
        hddDict[hdd] = BcacheUtil.findByBackingDevice(parti)

    # ssd partition 3: make it as cache device
    BcacheUtil.makeDevice(cg.get_ssd_cache_partition(), False)
    BcacheUtil.registerCacheDevice(cg.get_ssd_cache_partition())
    BcacheUtil.attachCacheDevice(cg.get_hdd_list(), cg.get_ssd_cache_partition())

    # create lvm physical volume on bcache device and add it to volume group
    for bcacheDev in hddDict.values():
        LvmUtil.addPvToVg(bcacheDev, LvmUtil.vgName, mayCreate=True)

    # create root lv and mount
    LvmUtil.createLvWithDefaultSize(LvmUtil.vgName, LvmUtil.rootLvName)
    Util.cmdCall("/bin/mount", LvmUtil.rootLvDevPath, mount_dir)

    # return
    ret = StorageLayoutImpl()
    ret._cg = cg
    ret._hddDict = hddDict
    ret._mnt = MountEfi(mount_dir)
    return ret


def _getSsdAndHddList(disk_list):
    ssd_list, hdd_list = Util.splitSsdAndHddFromFixedDiskDevPathList(disk_list)
    if len(ssd_list) == 0:
        ssd = None
    elif len(ssd_list) == 1:
        ssd = ssd_list[0]
    else:
        raise errors.StorageLayoutCreateError(errors.MULTIPLE_SSD)
    if len(hdd_list) == 0:
        raise errors.StorageLayoutCreateError(errors.NO_DISK_WHEN_CREATE)
    return (ssd, hdd_list)
