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
import psutil
from .util import Util, PartiUtil, GptUtil, BcacheUtil, LvmUtil, SwapFile, SwapLvmLv
from . import errors
from . import BootDirRwController


class MountBios:

    class BootDirRwController(BootDirRwController):

        @property
        def is_writable(self):
            return True

        def to_read_write(self):
            pass

        def to_read_only(self):
            pass

    @staticmethod
    def mount(rootParti, mountDir):
        Util.cmdCall("/bin/mount", rootParti, mountDir)

    @staticmethod
    def proxy(func):
        def f(self, *args):
            return getattr(self._mnt, func.__name__)(*args)
        return f

    def __init__(self, mountDir):
        self._mountDir = mountDir
        self._rwCtrl = self.BootDirRwController()

    @property
    def mount_point(self):
        return self._mountDir

    def umount(self):
        Util.cmdCall("/bin/umount", self._mountDir)

    def remount_rootfs(self, mount_options):
        Util.cmdCall("/bin/mount", "--remount", ",".join(mount_options))

    def get_bootdir_rw_controller(self):
        return self._rwCtrl


class MountEfi:

    class BootDirRwController(BootDirRwController):

        def __init__(self, mountDir):
            self._mountDir = mountDir

        @property
        def is_writable(self):
            for pobj in psutil.disk_partitions():
                if pobj.mountpoint == self._mountDir:
                    return ("rw" in pobj.opts.split(","))
            assert False

        def to_read_write(self):
            assert not self.is_writable
            Util.cmdCall("/bin/mount", self._mountDir, "-o", "rw,remount")

        def to_read_only(self):
            assert self.is_writable
            Util.cmdCall("/bin/mount", self._mountDir, "-o", "ro,remount")

    @staticmethod
    def mount(rootParti, espParti, mountDir):
        Util.cmdCall("/bin/mount", rootParti, mountDir)
        bootDir = os.path.join(mountDir, "boot")
        os.makedirs(bootDir, exist_ok=True)
        Util.cmdCall("/bin/mount", espParti, bootDir, "-o", "ro")

    @staticmethod
    def proxy(func):
        def f(self, *args):
            return getattr(self._mnt, func.__name__)(*args)
        return f

    def __init__(self, mountDir):
        self._mountDir = mountDir
        self._rwCtrl = self.BootDirRwController()

    @property
    def mount_point(self):
        return self._mountDir

    def umount(self):
        Util.cmdCall("/bin/umount", os.path.join(self._mountDir, "boot"))
        Util.cmdCall("/bin/umount", self._mountDir)

    def remount_rootfs(self, mount_options):
        Util.cmdCall("/bin/mount", "--remount", ",".join(mount_options))
        # FIXME: consider boot device

    def get_bootdir_rw_controller(self):
        return self._rwCtrl


class CommonChecks:

    @staticmethod
    def storageLayoutCheckSwapSize(storageLayout):
        if storageLayout.dev_swap is not None:
            if Util.getBlkDevSize(storageLayout.dev_swap) < Util.getSwapSize():
                raise errors.StorageLayoutCheckError(storageLayout.name, errors.SWAP_SIZE_TOO_SMALL)


class HandyUtil:

    @staticmethod
    def isSwapEnabled(storageLayout):
        return storageLayout.dev_swap is not None and Util.systemdFindSwapService(storageLayout.dev_swap) is not None

    @staticmethod
    def checkAndGetHdd(diskList):
        if len(diskList) == 0:
            raise errors.StorageLayoutCreateError(errors.NO_DISK_WHEN_CREATE)
        if len(diskList) > 1:
            raise errors.StorageLayoutCreateError(errors.MULTIPLE_DISKS_WHEN_CREATE)
        if not Util.isHarddiskClean(diskList[0]):
            raise errors.StorageLayoutCreateError(errors.DISK_NOT_CLEAN(diskList[0]))
        return diskList[0]

    @staticmethod
    def mdCheckAndAddDisks(md, diskList):
        if len(diskList) == 0:
            raise errors.StorageLayoutCreateError(errors.NO_DISK_WHEN_CREATE)
        for disk in diskList:
            if not Util.isHarddiskClean(disk):
                raise errors.StorageLayoutCreateError(errors.DISK_NOT_CLEAN(disk))
        for disk in diskList:
            md.add_disk(disk)

    @staticmethod
    def cgCheckAndAddDisks(cg, ssdList, hddList):
        if len(ssdList) == 0:
            ssd = None
        elif len(ssdList) == 1:
            ssd = ssdList[0]
        else:
            raise errors.StorageLayoutCreateError(errors.MULTIPLE_SSD)

        if len(hddList) == 0:
            raise errors.StorageLayoutCreateError(errors.NO_DISK_WHEN_CREATE)

        if not Util.isHarddiskClean(ssd):
            raise errors.StorageLayoutCreateError(errors.DISK_NOT_CLEAN(ssd))
        for hdd in hddList:
            if not Util.isHarddiskClean(hdd):
                raise errors.StorageLayoutCreateError(errors.DISK_NOT_CLEAN(hdd))

        # add ssd first so that minimal boot disk change is need
        if ssd is not None:
            cg.add_ssd(ssd)
        for hdd in hddList:
            cg.add_hdd(hdd)

    @staticmethod
    def cgCreateAndGetBcacheDevPathList(cg):
        # hdd partition 2: make them as backing device
        bcacheDevPathList = []
        for hdd in cg.get_hdd_list():
            parti = cg.get_hdd_data_partition(hdd)
            BcacheUtil.makeAndRegisterBackingDevice(parti)
            bcacheDevPathList.append(BcacheUtil.findByBackingDevice(parti))

        # ssd partition 3: make it as cache device
        if cg.get_ssd() is not None:
            parti = cg.get_ssd_cache_partition()
            BcacheUtil.makeAndRegisterCacheDevice(parti)
            BcacheUtil.attachCacheDevice(bcacheDevPathList, parti)

        return bcacheDevPathList

    @staticmethod
    def cgCheckAndGetSsdAndHddList(ssdList, hddList, bForCreate):
        if len(ssdList) == 0:
            ssd = None
        elif len(ssdList) == 1:
            ssd = ssdList[0]
        else:
            raise errors.StorageLayoutCreateError(errors.MULTIPLE_SSD)

        if len(hddList) == 0:
            raise errors.StorageLayoutCreateError(errors.NO_DISK_WHEN_CREATE)

        if bForCreate:
            if not Util.isHarddiskClean(ssd):
                raise errors.StorageLayoutCreateError(errors.DISK_NOT_CLEAN(ssd))
            for hdd in hddList:
                if not Util.isHarddiskClean(hdd):
                    raise errors.StorageLayoutCreateError(errors.DISK_NOT_CLEAN(hdd))

        return (ssd, hddList)

    @staticmethod
    def mdCheckHdd(storageLayoutName, hdd):
        if Util.getBlkDevPartitionTableType(hdd) != Util.diskPartTableGpt:
            raise errors.StorageLayoutParseError(storageLayoutName, errors.PARTITION_TYPE_SHOULD_BE(hdd, Util.diskPartTableGpt))

        # esp partition
        espParti = PartiUtil.diskToParti(hdd, 1)
        if Util.getBlkDevFsType(espParti) != Util.fsTypeFat:
            raise errors.StorageLayoutParseError(storageLayoutName, errors.PARTITION_TYPE_SHOULD_BE(espParti, Util.fsTypeFat))
        if Util.getBlkDevSize(espParti) != Util.getEspSize():
            raise errors.StorageLayoutParseError(storageLayoutName, errors.PARTITION_SIZE_INVALID(espParti))

        # data partition
        if not PartiUtil.diskHasParti(hdd, 2):
            raise errors.StorageLayoutParseError(storageLayoutName, "HDD \"%s\" has no data partition" % (hdd))

        # redundant partitions
        if PartiUtil.diskHasMoreParti(hdd, 2):
            raise errors.StorageLayoutParseError(storageLayoutName, errors.DISK_HAS_REDUNDANT_PARTITION(hdd))

    @staticmethod
    def cgCheckAndGetSsdFromBootDev(bootDev, hddList):
        assert bootDev is not None

        ssd = PartiUtil.partiToDisk(bootDev)
        if ssd not in hddList:
            return ssd
        else:
            return None

    @staticmethod
    def cgCheckAndGetSsdPartitions(storageLayoutName, ssd):
        if ssd is not None:
            ssdEspParti = PartiUtil.diskToParti(ssd, 1)
            if PartiUtil.diskHasParti(ssd, 3):
                ssdSwapParti = PartiUtil.diskToParti(ssd, 2)
                ssdCacheParti = PartiUtil.diskToParti(ssd, 3)
                if PartiUtil.diskHasMoreParti(ssd, 3):
                    raise errors.StorageLayoutParseError(storageLayoutName, errors.DISK_HAS_REDUNDANT_PARTITION(ssd))
            else:
                ssdCacheParti = PartiUtil.diskToParti(ssd, 2)

            # ssdEspParti
            if not GptUtil.isEspPartition(ssdEspParti):
                raise errors.StorageLayoutParseError(storageLayoutName, errors.BOOT_DEV_IS_NOT_ESP)
            if Util.getBlkDevSize(ssdEspParti) != Util.getEspSize():
                raise errors.StorageLayoutParseError(storageLayoutName, errors.PARTITION_SIZE_INVALID(ssdEspParti))

            # ssdSwapParti
            if ssdSwapParti is not None:
                if not PartiUtil.partiExists(ssdSwapParti):
                    raise errors.StorageLayoutParseError(storageLayoutName, "SSD has no swap partition")
                if Util.getBlkDevFsType(ssdSwapParti) != Util.fsTypeSwap:
                    raise errors.StorageLayoutParseError(storageLayoutName, errors.SWAP_DEV_HAS_INVALID_FS_FLAG(ssdSwapParti))

            # ssdCacheParti
            if not PartiUtil.partiExists(ssdCacheParti):
                raise errors.StorageLayoutParseError(storageLayoutName, "SSD has no cache partition")

            # redundant partitions
            if True:
                disk, partId = PartiUtil.partiToDiskAndPartiId(ssdCacheParti)
                if PartiUtil.diskHasMoreParti(disk, partId):
                    raise errors.StorageLayoutParseError(storageLayoutName, errors.DISK_HAS_REDUNDANT_PARTITION(ssd))

            return ssdEspParti, ssdSwapParti, ssdCacheParti
        else:
            return None, None, None

    @staticmethod
    def cgCheckHdd(storageLayoutName, hdd):
        return HandyUtil.mdCheckHdd(storageLayoutName, hdd)

    @staticmethod
    def cgCheckAndGetBootHdd(storageLayoutName, ssdEspParti, hddList):
        if ssdEspParti is not None:
            return None

        for hdd in hddList:
            espParti = PartiUtil.diskToParti(hdd, 1)
            if GptUtil.isEspPartition(espParti):
                return hdd
        return None

    @staticmethod
    def cgCheckAndGetBootHddFromBootDev(storageLayoutName, bootDev, ssdEspParti, hddList):
        assert bootDev is not None

        if ssdEspParti is not None:
            if ssdEspParti != bootDev:
                raise errors.StorageLayoutParseError(storageLayoutName, errors.BOOT_DEV_MUST_BE(ssdEspParti))
            return None
        else:
            if not GptUtil.isEspPartition(bootDev):
                raise errors.StorageLayoutParseError(storageLayoutName, errors.BOOT_DEV_IS_NOT_ESP)
            bootHdd = PartiUtil.partiToDisk(bootDev)
            if bootHdd not in hddList:
                raise errors.StorageLayoutParseError(storageLayoutName, errors.BOOT_DEV_INVALID)
            return bootHdd

    def cgFindByBackingDeviceList(cg):
        return [BcacheUtil.findByBackingDevice(cg.get_hdd_data_partition(x)) for x in cg.get_hdd_list()]

    @staticmethod
    def bcacheGetHddDictWithOneItem(storageLayoutName, bcacheDevPath, bcacheDev):
        hddDev, partId = PartiUtil.partiToDiskAndPartiId(BcacheUtil.getSlaveDevPathList(bcacheDevPath)[-1])
        if partId != 2:
            raise errors.StorageLayoutParseError(storageLayoutName, "bcache partition of %s is not %s" % (hddDev, PartiUtil.diskToParti(hddDev, 2)))
        if PartiUtil.diskHasParti(hddDev, 3):
            raise errors.StorageLayoutParseError(storageLayoutName, errors.DISK_HAS_REDUNDANT_PARTITION(hddDev))
        return {hddDev: bcacheDev}

    @staticmethod
    def bcacheCheckHddAndItsBcacheDev(storageLayoutName, ssdCacheParti, hdd, bcacheDev):
        tlist = BcacheUtil.getSlaveDevPathList(bcacheDev)
        if len(tlist) > 2:
            raise errors.StorageLayoutParseError(storageLayoutName, "%s(%s) has multiple cache devices" % (hdd, bcacheDev))
        if ssdCacheParti is not None:
            if len(tlist) < 2:
                raise errors.StorageLayoutParseError(storageLayoutName, "%s(%s) has no cache device" % (hdd, bcacheDev))
            if tlist[0] != ssdCacheParti:
                raise errors.StorageLayoutParseError(storageLayoutName, "%s(%s) has invalid cache device" % (hdd, bcacheDev))

    @staticmethod
    def lvmEnsureVgLvAndGetDiskList(storageLayoutName):
        # check vg
        if not Util.cmdCallTestSuccess("/sbin/lvm", "vgdisplay", LvmUtil.vgName):
            raise errors.StorageLayoutParseError(storageLayoutName, errors.LVM_VG_NOT_FOUND(LvmUtil.vgName))

        # get pv disk list, check esp partition, check data partition
        diskList = []
        out = Util.cmdCall("/sbin/lvm", "pvdisplay", "-c")
        for m in re.finditer("(/dev/\\S+):%s:.*" % (LvmUtil.vgName), out, re.M):
            hdd, partId = PartiUtil.partiToDiskAndPartiId(m.group(1))
            if Util.getBlkDevPartitionTableType(hdd) != Util.diskPartTableGpt:
                raise errors.StorageLayoutParseError(storageLayoutName, errors.PARTITION_TYPE_SHOULD_BE(hdd, Util.diskPartTableGpt))
            if partId != 2:
                raise errors.StorageLayoutParseError(storageLayoutName, "physical volume partition of %s is not %s" % (hdd, PartiUtil.diskToParti(hdd, 2)))
            if Util.getBlkDevSize(PartiUtil.diskToParti(hdd, 1)) != Util.getEspSize():
                raise errors.StorageLayoutParseError(storageLayoutName, errors.PARTITION_SIZE_INVALID(PartiUtil.diskToParti(hdd, 1)))
            if PartiUtil.diskHasParti(hdd, 3):
                raise errors.StorageLayoutParseError(storageLayoutName, errors.DISK_HAS_REDUNDANT_PARTITION(hdd))
            diskList.append(hdd)

        # check root lv
        out = Util.cmdCall("/sbin/lvm", "lvdisplay", "-c")
        if re.search("/dev/hdd/root:%s:.*" % (LvmUtil.vgName), out, re.M) is None:
            raise errors.StorageLayoutParseError(storageLayoutName, errors.LVM_LV_NOT_FOUND(LvmUtil.rootLvDevPath))

        return diskList

    @staticmethod
    def swapFileDetectAndNew(storageLayoutName, rootfs_mount_dir):
        fullfn = rootfs_mount_dir.rstrip("/") + Util.swapFilepath
        if os.path.exists(fullfn):
            if not Util.cmdCallTestSuccess("/sbin/swaplabel", fullfn):
                raise errors.StorageLayoutParseError(storageLayoutName, errors.SWAP_DEV_HAS_INVALID_FS_FLAG(fullfn))
            return SwapFile(True)
        else:
            return SwapFile(False)

    @staticmethod
    def swapLvDetectAndNew(storageLayoutName):
        out = Util.cmdCall("/sbin/lvm", "lvdisplay", "-c")
        if re.search("/dev/hdd/swap:%s:.*" % (LvmUtil.vgName), out, re.M) is not None:
            if Util.getBlkDevFsType(LvmUtil.swapLvDevPath) != Util.fsTypeSwap:
                raise errors.StorageLayoutParseError(storageLayoutName, errors.SWAP_DEV_HAS_INVALID_FS_FLAG(LvmUtil.swapLvDevPath))
            return SwapLvmLv(True)
        else:
            return SwapLvmLv(False)
