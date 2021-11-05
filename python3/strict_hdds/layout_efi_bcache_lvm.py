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

from .util import Util
from .util import BcacheUtil
from .util import LvmUtil

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

        self._ssd = None
        self._ssdEspParti = None
        self._ssdSwapParti = None
        self._ssdCacheParti = None
        self._hddDict = dict()           # dict<hddDev,bcacheDev>
        self._bootHdd = None             # boot harddisk name, must be None if ssd exists

    @property
    def boot_mode(self):
        return StorageLayout.BOOT_MODE_EFI

    @property
    def dev_rootfs(self):
        return LvmUtil.rootLvDevPath

    @property
    def dev_swap(self):
        return self._ssdSwapParti

    def get_boot_disk(self):
        return self._ssd if self._ssd is not None else self._bootHdd

    def check_swap_size(self):
        assert self._ssdSwapParti is not None
        return Util.getBlkDevSize(self._ssdSwapParti) >= Util.getSwapSize()

    def optimize_rootdev(self):
        LvmUtil.autoExtendLv(LvmUtil.rootLvDevPath)
        Util.cmdExec("/sbin/resize2fs", LvmUtil.rootLvDevPath)

    def get_esp(self):
        return self._getCurEsp()

    def get_esp_sync_info(self):
        return (self._getCurEsp(), self._getOtherEspList())

    def sync_esp(self, src, dst):
        assert src is not None and dst is not None
        assert src == self._getCurEsp() and dst in self._getOtherEspList()
        Util.syncBlkDev(src, dst, mountPoint1=Util.bootDir)

    def get_ssd(self):
        return self._ssd

    def get_ssd_esp_partition(self):
        assert self._ssd is not None
        return self._ssdEspParti

    def get_ssd_swap_partition(self):
        assert self._ssd is not None
        return self._ssdSwapParti

    def get_ssd_cache_partition(self):
        assert self._ssd is not None
        return self._ssdCacheParti

    def get_disk_list(self):
        if self._ssd is not None:
            return [self._ssd] + list(self._hddDict.keys())
        else:
            return list(self._hddDict.keys())

    def add_disk(self, devpath):
        assert devpath is not None
        assert devpath not in self.get_disk_list()

        if devpath not in Util.getDevPathListForFixedHdd():
            raise errors.StorageLayoutAddDiskError(devpath, errors.NOT_DISK)

        if Util.isBlkDevSsdOrHdd(devpath):
            assert self._ssd is None
            return self._addSsd(devpath)
        else:
            return self._addHdd(devpath)

    def release_disk(self, devpath):
        assert devpath is not None
        assert devpath in self.get_disk_list()

        if devpath == self._ssd:
            self._releaseSsd()
        else:
            self._releaseHdd()

    def remove_disk(self, devpath):
        assert devpath is not None
        assert devpath in self.get_disk_list()

        if devpath == self._ssd:
            return self._removeSsd()
        else:
            return self._removeHdd(devpath)

    def _addSsd(self, devpath):
        # create partitions
        Util.initializeDisk(devpath, "gpt", [
            ("%dMiB" % (Util.getEspSizeInMb()), "esp"),
            ("%dGiB" % (Util.getSwapSizeInGb()), Util.fsTypeSwap),
            ("*", "bcache"),
        ])
        self._ssd = devpath

        # sync partition1 as boot partition
        parti = Util.devPathDiskToPartition(devpath, 1)
        Util.cmdCall("/usr/sbin/mkfs.vfat", parti)
        Util.syncBlkDev(Util.devPathDiskToPartition(self._bootHdd, 1), parti, mountPoint1=Util.bootDir)
        self._ssdEspParti = parti

        # make partition2 as swap partition
        parti = Util.devPathDiskToPartition(devpath, 2)
        Util.cmdCall("/sbin/mkswap", parti)
        self._ssdSwapParti = parti

        # make partition3 as cache partition
        parti = Util.devPathDiskToPartition(devpath, 3)
        BcacheUtil.makeDevice(parti, False)
        self._ssdCacheParti = parti

        # enable cache partition
        BcacheUtil.registerCacheDevice(parti)
        BcacheUtil.attachCacheDevice(self._hddDict.values(), parti)

        # change boot device
        Util.cmdCall("/bin/umount", Util.bootDir)
        Util.gptToggleEspPartition(Util.devPathDiskToPartition(self._bootHdd, 1), False)
        Util.cmdCall("/bin/mount", self._ssdEspParti, Util.bootDir, "-o", "ro")
        self._bootHdd = None

        return True

    def _addHdd(self, devpath):
        if devpath == self._ssd or devpath in self._hddDict:
            raise Exception("the specified device is already managed")
        if devpath not in Util.getDevPathListForFixedHdd():
            raise Exception("the specified device is not a fixed harddisk")

        if Util.isBlkDevSsdOrHdd(devpath):
            print("WARNING: \"%s\" is an SSD harddisk, perhaps you want to add it as mainboot device?" % (devpath))

        # create partitions
        Util.initializeDisk(devpath, "gpt", [
            ("%dMiB" % (Util.getEspSizeInMb()), Util.fsTypeFat),
            ("*", "bcache"),
        ])

        # fill partition1
        parti = Util.devPathDiskToPartition(devpath, 1)
        Util.cmdCall("/usr/sbin/mkfs.vfat", parti)
        if self._ssd is not None:
            Util.syncBlkDev(self._ssdEspParti, parti, mountPoint1=Util.bootDir)
        else:
            Util.syncBlkDev(Util.devPathDiskToPartition(self._bootHdd, 1), parti, mountPoint1=Util.bootDir)

        # add partition2 to bcache
        parti = Util.devPathDiskToPartition(devpath, 2)
        BcacheUtil.makeDevice(parti, True)
        BcacheUtil.registerBackingDevice(parti)
        bcacheDev = BcacheUtil.findByBackingDevice(parti)
        if self._ssd is not None:
            BcacheUtil.attachCacheDevice([parti], self._ssdCacheParti)

        # create lvm physical volume on bcache device and add it to volume group
        Util.cmdCall("/sbin/lvm", "pvcreate", bcacheDev)
        Util.cmdCall("/sbin/lvm", "vgextend", LvmUtil.vgName, bcacheDev)
        self._hddDict[devpath] = bcacheDev

        return False

    def _releaseSsd(self):
        pass

    def _releaseHdd(self, devpath):
        if len(self._hddDict) <= 1:
            raise errors.StorageLayoutReleaseDiskError(errors.CAN_NOT_RELEASE_LAST_HDD)

        parti = Util.devPathDiskToPartition(devpath, 2)
        bcacheDev = BcacheUtil.findByBackingDevice(parti)
        rc, out = Util.cmdCallWithRetCode("/sbin/lvm", "pvmove", bcacheDev)
        if rc != 5:
            raise errors.StorageLayoutReleaseDiskError("failed")

    def _removeSsd(self):
        assert self._ssd is not None
        assert len(self._hddDict) > 0

        # check
        if Util.systemdFindSwapService(self._ssdSwapParti) is not None:
            raise Exception("swap partition is in use")

        # remove cache partition
        setUuid = BcacheUtil.getSetUuid(self._ssdCacheParti)
        with open("/sys/fs/bcache/%s/unregister" % (setUuid), "w") as f:
            f.write(self._ssdCacheParti)
        self._ssdCacheParti = None

        # remove swap partition
        self._ssdSwapParti = None

        # change boot device
        Util.cmdCall("/bin/umount", Util.bootDir)
        self._bootHdd = list(self._hddDict.keys())[0]
        Util.gptToggleEspPartition(Util.devPathDiskToPartition(self._bootHdd, 1), True)
        Util.cmdCall("/bin/mount", Util.devPathDiskToPartition(self._bootHdd, 1), Util.bootDir, "-o", "ro")
        self._ssdEspParti = None

        # wipe disk
        Util.wipeHarddisk(self._ssd)
        self._ssd = None

        return True

    def _removeHdd(self, devpath):
        assert devpath in self._hddDict

        if len(self._hddDict) <= 1:
            raise Exception("can not remove the last physical volume")

        # change boot device if needed
        ret = False
        if self._bootHdd is not None and self._bootHdd == devpath:
            Util.cmdCall("/bin/umount", Util.bootDir)
            del self._hddDict[devpath]
            self._bootHdd = list(self._hddDict.keys())[0]
            Util.gptToggleEspPartition(Util.devPathDiskToPartition(self._bootHdd, 1), True)
            Util.cmdCall("/bin/mount", Util.devPathDiskToPartition(self._bootHdd, 1), Util.bootDir, "-o", "ro")
            ret = True

        # remove harddisk
        bcacheDev = BcacheUtil.findByBackingDevice(Util.devPathDiskToPartition(devpath, 2))
        Util.cmdCall("/sbin/lvm", "vgreduce", LvmUtil.vgName, bcacheDev)
        with open("/sys/block/%s/bcache/stop" % (os.path.basename(bcacheDev)), "w") as f:
            f.write("1")
        Util.wipeHarddisk(devpath)

        return ret

    def _getCurEsp(self):
        if self._ssd is not None:
            return self._ssdEspParti
        else:
            return Util.devPathDiskToPartition(self._bootHdd, 1)

    def _getOtherEspList(self):
        ret = []
        for hdd in self._hddDict:
            if self._bootHdd is None or hdd != self._bootHdd:
                ret.append(Util.devPathDiskToPartition(hdd, 1))
        return ret


def create_layout(ssd=None, hdd_list=None, dry_run=False):
    if ssd is None and hdd_list is None:
        ssdList = []
        hdd_list = []
        for devpath in Util.getDevPathListForFixedHdd():
            if Util.isBlkDevSsdOrHdd(devpath):
                ssdList.append(devpath)
            else:
                hdd_list.append(devpath)
        if len(ssdList) == 0:
            pass
        elif len(ssdList) == 1:
            ssd = ssdList[0]
        else:
            raise errors.StorageLayoutCreateError(errors.MULTIPLE_SSD)
        if len(hdd_list) == 0:
            raise errors.StorageLayoutCreateError(errors.NO_DISK)
    else:
        assert hdd_list is not None and len(hdd_list) > 0

    ret = StorageLayoutImpl()
    if ssd is not None:
        ret._ssd = ssd
        ret._ssdEspParti = Util.devPathDiskToPartition(ssd, 1)
        ret._ssdSwapParti = Util.devPathDiskToPartition(ssd, 2)
        ret._ssdCacheParti = Util.devPathDiskToPartition(ssd, 3)
    else:
        ret._bootHdd = hdd_list[0]
    for i in range(0, len(hdd_list)):
        ret._hddDict[hdd_list[i]] = "/dev/bcache%d" % (i)       # would be overwrited if not dry-run

    if not dry_run:
        for devpath in ret._hddDict:
            # create partitions
            Util.initializeDisk(devpath, "gpt", [
                ("%dMiB" % (Util.getEspSizeInMb()), Util.fsTypeFat),
                ("*", "bcache"),
            ])

            # partition1: ESP partition
            parti = Util.devPathDiskToPartition(devpath, 1)
            Util.cmdCall("/usr/sbin/mkfs.vfat", parti)

            # partition2: bcache partition
            parti = Util.devPathDiskToPartition(devpath, 2)
            BcacheUtil.makeDevice(parti, True)
            BcacheUtil.registerBackingDevice(parti)

            # record
            ret._hddDict[devpath] = BcacheUtil.findByBackingDevice(parti)

        if ssd is not None:
            # create partitions
            Util.initializeDisk(ssd, "gpt", [
                ("%dMiB" % (Util.getEspSizeInMb()), "esp"),
                ("%dGiB" % (Util.getSwapSizeInGb()), Util.fsTypeSwap),
                ("*", "bcache"),
            ])

            # esp partition
            Util.cmdCall("/usr/sbin/mkfs.vfat", ret._ssdEspParti)

            # swap partition
            Util.cmdCall("/sbin/mkswap", ret._ssdSwapParti)

            # cache partition
            BcacheUtil.makeDevice(ret._ssdCacheParti, False)
            BcacheUtil.registerCacheDevice(ret._ssdCacheParti)
            BcacheUtil.attachCacheDevice(ret._hddDict.values(), ret._ssdCacheParti)

        # create lvm physical volume on bcache device and add it to volume group
        for bcacheDev in ret._hddDict.values():
            Util.cmdCall("/sbin/lvm", "pvcreate", bcacheDev)
            if not Util.cmdCallTestSuccess("/sbin/lvm", "vgdisplay", LvmUtil.vgName):
                Util.cmdCall("/sbin/lvm", "vgcreate", LvmUtil.vgName, bcacheDev)
            else:
                Util.cmdCall("/sbin/lvm", "vgextend", LvmUtil.vgName, bcacheDev)

        # create root lv
        out = Util.cmdCall("/sbin/lvm", "vgdisplay", "-c", LvmUtil.vgName)
        freePe = int(out.split(":")[15])
        Util.cmdCall("/sbin/lvm", "lvcreate", "-l", "%d" % (freePe // 2), "-n", LvmUtil.rootLvName, LvmUtil.vgName)

    return ret


def parse_layout(bootDev, rootDev):
    ret = StorageLayoutImpl()

    if not Util.gptIsEspPartition(bootDev):
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
        hddDev, partId = Util.devPathPartitionToDiskAndPartitionId(tlist[-1])
        if partId != 2:
            raise errors.StorageLayoutParseError(ret.name, "physical volume partition of %s is not %s" % (hddDev, Util.devPathDiskToPartition(hddDev, 2)))
        if os.path.exists(Util.devPathDiskToPartition(hddDev, 3)):
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
            raise errors.StorageLayoutParseError(ret.name, errors.PARTITION_HAS_INVALID_SIZE(ret._ssdEspParti))

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
            tlist = BcacheUtil.getSlaveDevPathList(bcacheDev)
            if len(tlist) < 2:
                raise errors.StorageLayoutParseError(ret.name, "%s(%s) has no cache device" % (pvHdd, bcacheDev))
            if len(tlist) > 2:
                raise errors.StorageLayoutParseError(ret.name, "%s(%s) has multiple cache devices" % (pvHdd, bcacheDev))
            if tlist[0] != ret._ssdCacheParti:
                raise errors.StorageLayoutParseError(ret.name, "%s(%s) has invalid cache device" % (pvHdd, bcacheDev))
        if True:
            partName, partId = Util.devPathPartitionToDiskAndPartitionId(ret._ssdCacheParti)
            nextPartName = Util.devPathDiskToPartition(partName, partId + 1)
            if os.path.exists(nextPartName):
                raise errors.StorageLayoutParseError(ret.name, errors.DISK_HAS_REDUNDANT_PARTITION(ret._ssd))
    else:
        ret._ssd = None

    # boot harddisk
    if ret._ssd is None:
        ret._bootHdd = Util.devPathPartitionToDisk(bootDev)

    return ret
