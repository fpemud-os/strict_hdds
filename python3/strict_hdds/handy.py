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
from .util import Util, PartiUtil, GptUtil, BcacheUtil, LvmUtil
from . import errors
from . import BootDirRwController


class SwapLvmLv:

    @staticmethod
    def proxy(func):
        if isinstance(func, property):
            def f_get(self):
                return getattr(self._swap, func.fget.__name__)
            f_get.__name__ = func.fget.__name__
            return property(f_get)
        else:
            def f(self, *args):
                return getattr(self._swap, func.__name__)(*args)
            return f

    def __init__(self, bSwapLv):
        self._bSwapLv = bSwapLv

    @property
    def dev_swap(self):
        return LvmUtil.swapLvDevPath if self._bSwapLv else None

    def get_suggestted_swap_size(self):
        return Util.getSwapSize()

    def create_swap_lv(self):
        assert not self._bSwapLv
        Util.cmdCall("/sbin/lvm", "lvcreate", "-L", "%dGiB" % (Util.getSwapSizeInGb()), "-n", LvmUtil.swapLvName, LvmUtil.vgName)
        self._bSwapLv = True

    def remove_swap_lv(self):
        assert self._bSwapLv
        Util.cmdCall("/sbin/lvm", "lvremove", LvmUtil.swapLvDevPath)
        self._bSwapLv = False


class SwapFile:

    @staticmethod
    def proxy(func):
        if isinstance(func, property):
            def f_get(self):
                return getattr(self._swap, func.fget.__name__)
            f_get.__name__ = func.fget.__name__
            return property(f_get)
        else:
            def f(self, *args):
                return getattr(self._swap, func.__name__)(*args)
            return f

    def __init__(self, bSwapFile):
        self._bSwapFile = bSwapFile

    @property
    def dev_swap(self):
        return Util.swapFilepath if self._bSwapFile else None

    def get_suggestted_swap_size(self):
        return Util.getSwapSize()

    def create_swap_file(self):
        assert not self._bSwapFile
        Util.createSwapFile(Util.swapFilepath)
        self._bSwapFile = True

    def remove_swap_file(self):
        assert self._bSwapFile
        os.remove(Util.swapFilepath)
        self._bSwapFile = False


class SnapshotBtrfs:

    @staticmethod
    def proxy(func):
        if isinstance(func, property):
            def f_get(self):
                return getattr(self._snapshot, func.fget.__name__)
            f_get.__name__ = func.fget.__name__
            return property(f_get)
        else:
            def f(self, *args):
                return getattr(self._snapshot, func.__name__)(*args)
            return f

    def __init__(self, mntDir):
        self._mntDir = mntDir
        for sv in self._getSubVolList():
            if not sv.startswith("@"):
                raise errors.StorageLayoutParseError("sub-volume \"%s\" is not supported" % (sv))

    @property
    def snapshot(self):
        ret = Util.mountGetSubVol(self._mntDir)
        if not ret.startswith("@"):
            raise errors.StorageLayoutParseError("sub-volume \"%s\" is not supported" % (ret))
        return ret[1:]

    def get_mntopt_list_for_mount(self, kwargsDict):
        if "snapshot" not in kwargsDict:
            return ["subvol=@"]
        else:
            assert kwargsDict["snapshot"] in self.get_snapshot_list()
            return ["subvol=@%s" % (kwargsDict["snapshot"])]

    def get_snapshot_list(self):
        ret = []
        for sv in self._getSubVolList():
            if not sv.startswith("@"):
                raise errors.StorageLayoutParseError("sub-volume \"%s\" is not supported" % (sv))
            ret.append(sv[1:])
        return ret

    def create_snapshot(self, snapshot_name):
        Util.cmdCall("/sbin/btrfs", "subvolume", "snapshot", os.path.join(self._mntDir, "@"), os.path.join(self._mntDir, "@%s" % (snapshot_name)))

    def remove_snapshot(self, snapshot_name):
        Util.cmdCall("/sbin/btrfs", "subvolume", "delete", os.path.join(self._mntDir, "@%s" % (snapshot_name)))

    def _getSubVolList(self):
        out = Util.cmdCall("/sbin/btrfs", "subvolume", "list", self._mntDir)
        # FIXME: parse out
        return out


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
    def mount(rootParti, mountDir, mntOptList):
        Util.cmdCall("/bin/mount", rootParti, mountDir, "-o", ",".join(mntOptList))

    @staticmethod
    def proxy(func):
        if isinstance(func, property):
            def f_get(self):
                return getattr(self._mnt, func.fget.__name__)
            f_get.__name__ = func.fget.__name__
            return property(f_get)
        else:
            def f(self, *args):
                return getattr(self._mnt, func.__name__)(*args)
            return f

    def __init__(self, mntDir):
        self._mntDir = mntDir
        self._rwCtrl = self.BootDirRwController(self._mntDir)

    @property
    def mount_point(self):
        return self._mntDir

    def umount(self):
        Util.cmdCall("/bin/umount", self._mntDir)

    def get_bootdir_rw_controller(self):
        return self._rwCtrl


class MountEfi:

    class BootDirRwController(BootDirRwController):

        def __init__(self, bootMntDir):
            self._mntDir = bootMntDir

        @property
        def is_writable(self):
            for pobj in psutil.disk_partitions():
                if pobj.mountpoint == MountEfi._getBootMountDir(self._mntDir):
                    return ("rw" in pobj.opts.split(","))
            assert False

        def to_read_write(self):
            assert not self.is_writable
            assert self._isRootfsWritable()
            Util.cmdCall("/bin/mount", MountEfi._getBootMountDir(self._mntDir), "-o", "rw,remount")

        def to_read_only(self):
            assert self.is_writable
            Util.cmdCall("/bin/mount", MountEfi._getBootMountDir(self._mntDir), "-o", "ro,remount")

        def _isRootfsWritable(self):
            for pobj in psutil.disk_partitions():
                if pobj.mountpoint == self._mntDir:
                    return ("rw" in pobj.opts.split(","))

    @staticmethod
    def mount(rootParti, espParti, rootMntDir, rootMntOptList):
        Util.cmdCall("/bin/mount", rootParti, rootMntDir, "-o", ",".join(rootMntOptList))
        bootDir = MountEfi._getBootMountDir(rootMntDir)
        os.makedirs(bootDir, exist_ok=True)
        Util.cmdCall("/bin/mount", espParti, bootDir, "-o", "ro")

    @staticmethod
    def proxy(func):
        if isinstance(func, property):
            def f_get(self):
                return getattr(self._mnt, func.fget.__name__)
            f_get.__name__ = func.fget.__name__
            return property(f_get)
        else:
            def f(self, *args):
                return getattr(self._mnt, func.__name__)(*args)
            return f

    def __init__(self, mountDir):
        self._mntDir = mountDir
        self._rwCtrl = self.BootDirRwController(self._mntDir)

    @property
    def mount_point(self):
        return self._mntDir

    def umount(self):
        Util.cmdCall("/bin/umount", MountEfi._getBootMountDir(self._mntDir))
        Util.cmdCall("/bin/umount", self._mntDir)

    def get_bootdir_rw_controller(self):
        return self._rwCtrl

    @staticmethod
    def _getBootMountDir(rootMountDir):
        return os.path.join(rootMountDir, "boot")


class HandyMd:

    @staticmethod
    def checkAndAddDisks(md, diskList):
        if len(diskList) == 0:
            raise errors.StorageLayoutCreateError(errors.NO_DISK_WHEN_CREATE)
        for disk in diskList:
            if not Util.isHarddiskClean(disk):
                raise errors.StorageLayoutCreateError(errors.DISK_NOT_CLEAN(disk))
        for disk in diskList:
            md.add_disk(disk)

    @staticmethod
    def checkExtraDisks(storageLayoutName, diskList, origDiskList):
        d = list(set(diskList) - set(origDiskList))
        if len(d) > 0:
            raise errors.StorageLayoutParseError(storageLayoutName, "extra disk \"%s\" needed" % (d[0]))

    @staticmethod
    def checkAndGetBootDiskFromBootDev(storageLayoutName, bootDev, diskList):
        HandyUtil._mcCheckHddOrDiskList(storageLayoutName, diskList)
        espParti = HandyUtil._mcCheckAndGetEspParti(storageLayoutName, diskList, mustHave=True)
        if espParti != bootDev:
            raise errors.StorageLayoutParseError(storageLayoutName, errors.BOOT_DEV_MUST_BE(espParti))
        return PartiUtil.partiToDisk(espParti)

    @staticmethod
    def checkAndGetBootDiskAndBootDev(storageLayoutName, diskList):
        HandyUtil._mcCheckHddOrDiskList(storageLayoutName, diskList)
        espParti = HandyUtil._mcCheckAndGetEspParti(storageLayoutName, diskList)
        return (PartiUtil.partiToDisk(espParti) if espParti is not None else None, espParti)


class HandyCg:

    @staticmethod
    def checkAndAddDisks(cg, ssdList, hddList):
        ssd, hddList = HandyCg.checkAndGetSsdAndHddList(ssdList, hddList)

        # ensure disks are clean
        if ssd is not None:
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
    def checkAndGetSsdAndHddList(ssdList, hddList):
        if len(ssdList) == 0:
            ssd = None
        elif len(ssdList) == 1:
            ssd = ssdList[0]
        else:
            raise errors.StorageLayoutCreateError(errors.MULTIPLE_SSD)
        if len(hddList) == 0:
            raise errors.StorageLayoutCreateError(errors.NO_DISK_WHEN_CREATE)
        return (ssd, hddList)

    @staticmethod
    def checkAndGetSsdPartitions(storageLayoutName, ssd):
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
    def checkExtraDisks(storageLayoutName, ssd, hddList, origDiskList):
        if ssd is not None and ssd not in origDiskList:
            raise errors.StorageLayoutParseError(storageLayoutName, "extra disk \"%s\" needed" % (ssd))
        d = list(set(hddList) - set(origDiskList))
        if len(d) > 0:
            raise errors.StorageLayoutParseError(storageLayoutName, "extra disk \"%s\" needed" % (d[0]))

    @staticmethod
    def checkAndGetBootHddFromBootDev(storageLayoutName, bootDev, ssdEspParti, hddList):
        HandyUtil._mcCheckHddOrDiskList(storageLayoutName, hddList)

        if ssdEspParti is not None:
            if ssdEspParti != bootDev:
                raise errors.StorageLayoutParseError(storageLayoutName, errors.BOOT_DEV_MUST_BE(ssdEspParti))
            HandyCg._checkNoEspPartiInHddList(storageLayoutName, hddList)
            return None
        else:
            espParti = HandyUtil._mcCheckAndGetEspParti(storageLayoutName, hddList, mustHave=True)
            if espParti != bootDev:
                raise errors.StorageLayoutParseError(storageLayoutName, errors.BOOT_DEV_MUST_BE(ssdEspParti))
            return PartiUtil.partiToDisk(espParti)

    @staticmethod
    def checkAndGetBootHddAndBootDev(storageLayoutName, ssdEspParti, hddList):
        HandyUtil._mcCheckHddOrDiskList(storageLayoutName, hddList)

        if ssdEspParti is not None:
            HandyCg._checkNoEspPartiInHddList(storageLayoutName, hddList)
            return (None, ssdEspParti)
        else:
            espParti = HandyUtil._mcCheckAndGetEspParti(storageLayoutName, hddList)
            return (PartiUtil.partiToDisk(espParti) if espParti is not None else None, espParti)

    @staticmethod
    def _checkNoEspPartiInHddList(storageLayoutName, hddList):
        for hdd in hddList:
            if GptUtil.isEspPartition(PartiUtil.diskToParti(hdd, 1)):
                raise errors.StorageLayoutParseError(storageLayoutName, "HDD \"%s\" should not have ESP partition" % (hdd))


class HandyBcache:

    @staticmethod
    def getSsdAndHddListFromBcacheDevPathList(storageLayoutName, bcacheDevPathList):
        cacheParti = None
        backingPartiList = []
        newBcacheDevPathList = []
        newBcacheDevList = []
        for bcacheDevPath in bcacheDevPathList:
            bcacheDev = BcacheUtil.getBcacheDevFromDevPath(bcacheDevPath)
            tlist = BcacheUtil.getSlaveDevPathList(bcacheDevPath)
            if len(tlist) == 0:
                assert False
            elif len(tlist) == 1:
                if len(backingPartiList) > 0:
                    if cacheParti is not None:
                        raise errors.StorageLayoutParseError(storageLayoutName, "%s(%s) has no cache device" % (tlist[0], bcacheDev))
                cacheParti = None
                backingPartiList.append(tlist[0])
                newBcacheDevPathList.append(bcacheDevPath)
                newBcacheDevList.append(bcacheDev)
            elif len(tlist) == 2:
                if len(backingPartiList) > 0:
                    if cacheParti is None:
                        raise errors.StorageLayoutParseError(storageLayoutName, "%s(%s) has no cache device" % (backingPartiList[-1], newBcacheDevList[-1]))
                    if cacheParti != tlist[0]:
                        raise errors.StorageLayoutParseError(storageLayoutName, "%s(%s) has a different cache device" % (tlist[1], bcacheDev))
                cacheParti = tlist[0]
                backingPartiList.append(tlist[1])
                newBcacheDevPathList.append(bcacheDevPath)
                newBcacheDevList.append(bcacheDev)
            else:
                raise errors.StorageLayoutParseError(storageLayoutName, "%s(%s) has multiple cache devices" % (tlist[-1], bcacheDev))

        if cacheParti is None:
            ssd = None
        else:
            ssd = PartiUtil.partiToDisk(cacheParti)
        hddList = [PartiUtil.partiToDisk(x) for x in backingPartiList]

        return (ssd, hddList)


class HandyUtil:

    @staticmethod
    def isSwapEnabled(storageLayout):
        return storageLayout.dev_swap is not None and Util.systemdFindSwapService(storageLayout.dev_swap) is not None

    @staticmethod
    def checkMntOptList(mntOptList):
        tset = set()
        for mo in mntOptList:
            idx = mo.find("=")
            if idx >= 0:
                mo2 = mo[0:idx]
            else:
                mo2 = mo
            if mo2 in tset:
                raise errors.StorageLayoutMountError("duplicate mount option \"%s\"" % (mo))
            tset.add(mo)

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
    def lvmEnsureVgLvAndGetPvList(storageLayoutName):
        # check vg
        if not Util.cmdCallTestSuccess("/sbin/lvm", "vgdisplay", LvmUtil.vgName):
            raise errors.StorageLayoutParseError(storageLayoutName, errors.LVM_VG_NOT_FOUND(LvmUtil.vgName))

        # get pv list
        pvList = []
        out = Util.cmdCall("/sbin/lvm", "pvdisplay", "-c")
        for m in re.finditer("(/dev/\\S+):%s:.*" % (LvmUtil.vgName), out, re.M):
            pvList.append(m.group(1))

        # find root lv
        out = Util.cmdCall("/sbin/lvm", "lvdisplay", "-c")
        if re.search("/dev/hdd/root:%s:.*" % (LvmUtil.vgName), out, re.M) is None:
            raise errors.StorageLayoutParseError(storageLayoutName, errors.LVM_LV_NOT_FOUND(LvmUtil.rootLvDevPath))

        return pvList

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

    @staticmethod
    def _mcCheckHddOrDiskList(storageLayoutName, diskOrHddList):
        for disk in diskOrHddList:
            if Util.getBlkDevPartitionTableType(disk) != Util.diskPartTableGpt:
                raise errors.StorageLayoutParseError(storageLayoutName, errors.PARTITION_TYPE_SHOULD_BE(disk, Util.diskPartTableGpt))

            # esp partition
            espParti = PartiUtil.diskToParti(disk, 1)
            if Util.getBlkDevFsType(espParti) != Util.fsTypeFat:
                raise errors.StorageLayoutParseError(storageLayoutName, errors.PARTITION_TYPE_SHOULD_BE(espParti, Util.fsTypeFat))
            if Util.getBlkDevSize(espParti) != Util.getEspSize():
                raise errors.StorageLayoutParseError(storageLayoutName, errors.PARTITION_SIZE_INVALID(espParti))

            # data partition
            if not PartiUtil.diskHasParti(disk, 2):
                raise errors.StorageLayoutParseError(storageLayoutName, "HDD \"%s\" has no data partition" % (disk))

            # redundant partitions
            if PartiUtil.diskHasMoreParti(disk, 2):
                raise errors.StorageLayoutParseError(storageLayoutName, errors.DISK_HAS_REDUNDANT_PARTITION(disk))

    @staticmethod
    def _mcCheckAndGetEspParti(storageLayoutName, diskOrHddList, mustHave=False):
        espPartiList = []
        for disk in diskOrHddList:
            parti = PartiUtil.diskToParti(disk, 1)
            if GptUtil.isEspPartition(parti):
                parti.append(parti)
        if len(espPartiList) == 0:
            if mustHave:
                raise errors.StorageLayoutParseError(storageLayoutName, "no ESP partitions found")
            else:
                return None
        elif len(espPartiList) == 1:
            return espPartiList[0]
        else:
            raise errors.StorageLayoutParseError(storageLayoutName, "multiple ESP partitions found")
