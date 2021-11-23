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
import abc
import psutil
from .util import Util, PartiUtil, GptUtil, BcacheUtil, LvmUtil, TmpMount
from . import errors
from . import BootDirRwController


class EfiMultiDisk:

    @staticmethod
    def proxy(func):
        if isinstance(func, property):
            def f_get(self):
                return getattr(self._md, func.fget.__name__)
            f_get.__name__ = func.fget.__name__
            return property(f_get)
        else:
            def f(self, *args):
                return getattr(self._md, func.__name__)(*args)
            return f

    def __init__(self, diskList=[], bootHdd=None):
        # assign self._hddList
        assert diskList is not None
        self._hddList = sorted(diskList)

        # assign self._bootHdd
        if len(self._hddList) > 0:
            if bootHdd is None:
                bootHdd = self._hddList[0]
            else:
                assert bootHdd in self._hddList
        else:
            assert bootHdd is None
        self._bootHdd = bootHdd

    @property
    def dev_boot(self):
        return self.get_esp()

    @property
    def boot_disk(self):
        return self._bootHdd

    def get_esp(self):
        if self._bootHdd is not None:
            return PartiUtil.diskToParti(self._bootHdd, 1)
        else:
            return None

    def get_pending_esp_list(self):
        ret = []
        for hdd in self._hddList:
            if self._bootHdd is None or hdd != self._bootHdd:
                ret.append(PartiUtil.diskToParti(hdd, 1))
        return ret

    def sync_esp(self, dst):
        assert self.get_esp() is not None
        assert dst is not None and dst in self.get_pending_esp_list()
        Util.syncBlkDev(self.get_esp(), dst, mountPoint1=Util.bootDir)

    def get_disk_list(self):
        return self._hddList

    def get_disk_esp_partition(self, disk):
        assert disk in self._hddList
        return PartiUtil.diskToParti(disk, 1)

    def get_disk_data_partition(self, disk):
        assert disk in self._hddList
        return PartiUtil.diskToParti(disk, 2)

    def add_disk(self, disk):
        assert disk is not None and disk not in self._hddList

        # create partitions
        Util.initializeDisk(disk, "gpt", [
            ("%dMiB" % (Util.getEspSizeInMb()), Util.fsTypeFat),
            ("*", "bcache"),
        ])

        # partition1: pending ESP partition
        parti = PartiUtil.diskToParti(disk, 1)
        Util.cmdCall("/usr/sbin/mkfs.vfat", parti)
        if self._bootHdd is not None:
            Util.syncBlkDev(PartiUtil.diskToParti(self._bootHdd, 1), parti, mountPoint1=Util.bootDir)
        else:
            pass

        # partition2: data partition, leave it to user
        pass

        # record result
        self._hddList.append(disk)
        self._hddList.sort()

        # change boot disk if needed
        if self._bootHdd is None:
            self._mountFirstHddAsBootHdd()

    def remove_disk(self, hdd):
        assert hdd is not None and hdd in self._hddList

        # change boot device if needed
        if self._bootHdd is not None and self._bootHdd == hdd:
            self._unmountCurrentBootHdd()
            self._hddList.remove(hdd)
            self._mountFirstHddAsBootHdd()
        else:
            self._hddList.remove(hdd)

        # wipe disk
        Util.wipeHarddisk(hdd)

    def check_esp(self, auto_fix, error_callback):
        for hdd in self._hddList:
            parti = self.get_disk_esp_partition(hdd)
            if Util.getBlkDevSize(parti) != Util.getEspSize():
                # no way to auto fix
                error_callback(errors.CheckCode.ESP_SIZE_INVALID, parti)

    def _mountFirstHddAsBootHdd(self):
        self._bootHdd = self._hddList[0]
        Util.toggleEspPartition(PartiUtil.diskToParti(self._bootHdd, 1), True)
        Util.cmdCall("/bin/mount", PartiUtil.diskToParti(self._bootHdd, 1), Util.bootDir, "-o", "ro")

    def _unmountCurrentBootHdd(self):
        Util.cmdCall("/bin/umount", Util.bootDir)
        Util.toggleEspPartition(PartiUtil.diskToParti(self._bootHdd, 1), False)
        self._bootHdd = None


class EfiCacheGroup:

    @staticmethod
    def proxy(func):
        if isinstance(func, property):
            def f_get(self):
                return getattr(self._cg, func.fget.__name__)
            f_get.__name__ = func.fget.__name__
            return property(f_get)
        else:
            def f(self, *args):
                return getattr(self._cg, func.__name__)(*args)
            return f

    def __init__(self, ssd=None, ssdEspParti=None, ssdSwapParti=None, ssdCacheParti=None, hddList=[], bootHdd=None):
        # assign self._ssd and friends
        self._ssd = ssd
        if self._ssd is not None:
            self._ssdEspParti = PartiUtil.diskToParti(ssd, 1)
            if ssdSwapParti is not None:
                self._ssdSwapParti = PartiUtil.diskToParti(ssd, 2)
                self._ssdCacheParti = PartiUtil.diskToParti(ssd, 3)
            else:
                self._ssdSwapParti = None
                self._ssdCacheParti = PartiUtil.diskToParti(ssd, 2)
        else:
            self._ssdEspParti is None
            self._ssdSwapParti is None
            self._ssdCacheParti is None
        assert self._ssdEspParti == ssdEspParti
        assert self._ssdSwapParti == ssdSwapParti
        assert self._ssdCacheParti == ssdCacheParti

        # assign self._hddList
        assert hddList is not None
        self._hddList = sorted(hddList)

        # assign self._bootHdd
        if self._ssd is not None:
            assert bootHdd is None
        else:
            if len(self._hddList) > 0:
                if bootHdd is None:
                    bootHdd = self._hddList[0]
                else:
                    assert bootHdd in self._hddList
            else:
                assert bootHdd is None
        self._bootHdd = bootHdd

    @property
    def dev_boot(self):
        return self.get_esp()

    @property
    def dev_swap(self):
        return self.get_ssd_swap_partition()

    @property
    def boot_disk(self):
        return self._ssd if self._ssd is not None else self._bootHdd

    def get_esp(self):
        if self._ssd is not None:
            return self._ssdEspParti
        elif self._bootHdd is not None:
            return PartiUtil.diskToParti(self._bootHdd, 1)
        else:
            return None

    def get_pending_esp_list(self):
        ret = []
        for hdd in self._hddList:
            if self._bootHdd is None or hdd != self._bootHdd:
                ret.append(PartiUtil.diskToParti(hdd, 1))
        return ret

    def sync_esp(self, dst):
        assert self.get_esp() is not None
        assert dst is not None and dst in self.get_pending_esp_list()
        Util.syncBlkDev(self.get_esp(), dst, mountPoint1=Util.bootDir)

    def get_disk_list(self):
        if self._ssd is not None:
            return [self._ssd] + self._hddList
        else:
            return self._hddList

    def get_ssd(self):
        return self._ssd

    def get_ssd_esp_partition(self):
        assert self._ssd is not None
        assert self._ssdEspParti is not None
        assert self._bootHdd is None
        return self._ssdEspParti

    def get_ssd_swap_partition(self):
        assert self._ssd is not None
        assert self._bootHdd is None
        return self._ssdSwapParti

    def get_ssd_cache_partition(self):
        assert self._ssd is not None
        assert self._ssdCacheParti is not None
        assert self._bootHdd is None
        return self._ssdCacheParti

    def get_hdd_list(self):
        return self._hddList

    def get_hdd_esp_partition(self, disk):
        assert disk in self._hddList
        return PartiUtil.diskToParti(disk, 1)

    def get_hdd_data_partition(self, disk):
        assert disk in self._hddList
        return PartiUtil.diskToParti(disk, 2)

    def add_ssd(self, ssd):
        assert self._ssd is None
        assert ssd is not None and ssd not in self._hddList

        self._ssd = ssd
        self._ssdEspParti = PartiUtil.diskToParti(ssd, 1)
        self._ssdSwapParti = PartiUtil.diskToParti(ssd, 2)
        self._ssdCacheParti = PartiUtil.diskToParti(ssd, 3)

        # create partitions
        Util.initializeDisk(self._ssd, "gpt", [
            ("%dMiB" % (Util.getEspSizeInMb()), "esp"),
            ("%dGiB" % (Util.getSwapSizeInGb()), Util.fsTypeSwap),
            ("*", "bcache"),
        ])

        # partition1: ESP partition
        Util.cmdCall("/usr/sbin/mkfs.vfat", self._ssdEspParti)
        if self._bootHdd is not None:
            Util.syncBlkDev(PartiUtil.diskToParti(self._bootHdd, 1), self._ssdEspParti, mountPoint1=Util.bootDir)
        else:
            pass

        # partition2: swap partition
        Util.cmdCall("/sbin/mkswap", self._ssdSwapParti)

        # partition3: cache partition, leave it to caller
        pass

        # change boot device
        if self._bootHdd is not None:
            self._unmountCurrentBootHdd()
        Util.cmdCall("/bin/mount", self._ssdEspParti, Util.bootDir, "-o", "ro")

    def remove_ssd(self):
        assert self._ssd is not None

        # partition3: cache partition, the caller should have processed it
        self._ssdCacheParti = None

        # partition2: swap partition
        if self._ssdSwapParti is not None:
            assert not Util.swapDeviceIsBusy(self._ssdSwapParti)
            self._ssdSwapParti = None

        # partition1: ESP partition
        Util.cmdCall("/bin/umount", Util.bootDir)
        self._ssdEspParti = None

        # change boot device
        if len(self._hddList) > 0:
            self._mountFirstHddAsBootHdd()

        # wipe disk
        Util.wipeHarddisk(self._ssd)
        self._ssd = None

    def add_hdd(self, hdd):
        assert hdd is not None and hdd not in self._hddList

        # create partitions
        Util.initializeDisk(hdd, "gpt", [
            ("%dMiB" % (Util.getEspSizeInMb()), Util.fsTypeFat),
            ("*", "bcache"),
        ])

        # partition1: pending ESP partition
        parti = PartiUtil.diskToParti(hdd, 1)
        Util.cmdCall("/usr/sbin/mkfs.vfat", parti)
        if self._ssd is not None:
            Util.syncBlkDev(self._ssdEspParti, parti, mountPoint1=Util.bootDir)
        elif self._bootHdd is not None:
            Util.syncBlkDev(PartiUtil.diskToParti(self._bootHdd, 1), parti, mountPoint1=Util.bootDir)
        else:
            pass

        # partition2: data partition, leave it to user
        pass

        # record result
        self._hddList.append(hdd)
        self._hddList.sort()

        # change boot disk if needed
        if self._bootHdd is None:
            self._mountFirstHddAsBootHdd()

    def remove_hdd(self, hdd):
        assert hdd is not None and hdd in self._hddList

        # change boot device if needed
        if self._bootHdd is not None and self._bootHdd == hdd:
            self._unmountCurrentBootHdd()
            self._hddList.remove(hdd)
            self._mountFirstHddAsBootHdd()
        else:
            self._hddList.remove(hdd)

        # wipe disk
        Util.wipeHarddisk(hdd)

    def check_ssd(self, auto_fix, error_callback):
        if self._ssd is None:
            # no way to auto fix
            error_callback(errors.CheckCode.TRIVIAL, "It would be better to add a cache device.")

    def check_esp(self, auto_fix, error_callback):
        if self._ssd is not None:
            tlist = [self._ssdEspParti]
        else:
            tlist = []
        tlist += [self.get_disk_esp_partition(x) for x in self._hddList]

        for hdd in tlist:
            parti = self.get_disk_esp_partition(hdd)
            if Util.getBlkDevSize(parti) != Util.getEspSize():
                # no way to auto fix
                error_callback(errors.CheckCode.ESP_SIZE_INVALID)

    def check_swap(self, auto_fix, error_callback):
        if self._ssdSwapParti is None:
            error_callback(errors.CheckCode.SWAP_NOT_ENABLED)
        else:
            if Util.getBlkDevSize(self._ssdSwapParti) >= Util.getSwapSize():
                # no way to auto fix
                error_callback(errors.CheckCode.SWAP_SIZE_TOO_SMALL, "partition")

    def _mountFirstHddAsBootHdd(self):
        self._bootHdd = self._hddList[0]
        Util.toggleEspPartition(PartiUtil.diskToParti(self._bootHdd, 1), True)
        Util.cmdCall("/bin/mount", PartiUtil.diskToParti(self._bootHdd, 1), Util.bootDir, "-o", "ro")

    def _unmountCurrentBootHdd(self):
        Util.cmdCall("/bin/umount", Util.bootDir)
        Util.toggleEspPartition(PartiUtil.diskToParti(self._bootHdd, 1), False)
        self._bootHdd = None


class BcacheRaid:

    def __init__(self, keyList=[], bcacheDevPathList=[]):
        self._backingDict = Util.keyValueListToDict(keyList, bcacheDevPathList)

    def get_bcache_dev(self, key):
        return self._backingDict[key]

    def get_all_bcache_dev_list(self):
        return self._backingDict.values()

    def add_cache(self, cacheDevPath):
        BcacheUtil.makeAndRegisterCacheDevice(cacheDevPath)
        BcacheUtil.attachCacheDevice(self._backingDict.values(), cacheDevPath)

    def add_backing(self, cacheDevPath, key, devPath):
        BcacheUtil.makeAndRegisterBackingDevice(devPath)
        bcacheDevPath = BcacheUtil.findByBackingDevice(devPath)
        if cacheDevPath is not None:
            BcacheUtil.attachCacheDevice([bcacheDevPath], cacheDevPath)
        self._backingDict[key] = bcacheDevPath
        return bcacheDevPath

    def remove_cache(self, cacheDevPath):
        BcacheUtil.unregisterCacheDevice(cacheDevPath)

    def remove_backing(self, key):
        BcacheUtil.stopBackingDevice(self._backingDict[key])
        del self._backingDict[key]

    def stop_all(self):
        for bcacheDevPath in self._backingDict.values():
            BcacheUtil.stopBackingDevice(bcacheDevPath)

    def check(self, auto_fix=False, error_callback=None):
        # check mode is consistent
        lastDevPath = None
        lastMode = None
        for bcacheDevPath in self._backingDict.values():
            mode = BcacheUtil.getMode(bcacheDevPath)
            if lastMode is not None:
                if mode != lastMode:
                    error_callback(errors.CheckCode.TRIVIAL, "BCACHE device %s and %s have inconsistent write mode." % (lastDevPath, bcacheDevPath))
            else:
                lastDevPath = bcacheDevPath
                lastMode = mode

    def check_write_mode(self, mode, auto_fix=False, error_callback=None):
        assert mode in ["writethrough", "writeback"]
        for bcacheDevPath in self._backingDict.values():
            if BcacheUtil.getMode(bcacheDevPath) != mode:
                if auto_fix:
                    BcacheRaid.setMode(mode)
                else:
                    error_callback(errors.CheckCode.TRIVIAL, "BCACHE device %s should be configured as writeback mode." % (bcacheDevPath))


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

    def create_swap_lv(self):
        assert not self._bSwapLv
        Util.cmdCall("/sbin/lvm", "lvcreate", "-L", "%dGiB" % (Util.getSwapSizeInGb()), "-n", LvmUtil.swapLvName, LvmUtil.vgName)
        self._bSwapLv = True

    def remove_swap_lv(self):
        assert self._bSwapLv
        Util.cmdCall("/sbin/lvm", "lvremove", LvmUtil.swapLvDevPath)
        self._bSwapLv = False

    def check(self, auto_fix, error_callback):
        if not self._bSwapLv:
            error_callback(errors.CheckCode.SWAP_NOT_ENABLED)
        else:
            if Util.getBlkDevSize(LvmUtil.swapLvDevPath) < Util.getSwapSize():
                if auto_fix:
                    if not Util.isSwapFileOrPartitionBusy(LvmUtil.swapLvDevPath):
                        self.remove_swap_lv()
                        self.create_swap_lv()
                        return
                error_callback(errors.CheckCode.SWAP_SIZE_TOO_SMALL, "LV")


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

    def create_swap_file(self):
        assert not self._bSwapFile
        Util.createSwapFile(Util.swapFilepath)
        self._bSwapFile = True

    def remove_swap_file(self):
        assert self._bSwapFile
        os.remove(Util.swapFilepath)
        self._bSwapFile = False

    def check(self, auto_fix, error_callback):
        if not self._bSwapFile:
            error_callback(errors.CheckCode.SWAP_NOT_ENABLED)
        else:
            if os.path.getsize(Util.swapFilepath) < Util.getSwapSize():
                if auto_fix:
                    if not Util.isSwapFileOrPartitionBusy(Util.swapFilepath):
                        self.remove_swap_file()
                        self.create_swap_file()
                        return
                error_callback(errors.CheckCode.SWAP_SIZE_TOO_SMALL, "file")


class Snapshot(abc.ABC):

    @classmethod
    def initializeFs(cls, devPath):
        with TmpMount(devPath) as mp:
            cls._createSubVol(mp.mountpoint, "@")
            cls._createSubVol(mp.mountpoint, "@root")
            cls._createSubVol(mp.mountpoint, "@home")
            cls._createSubVol(mp.mountpoint, "@var")
            cls._createSubVol(mp.mountpoint, "@snapshots")

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

    @property
    def snapshot(self):
        ret = Util.mntGetSubVol(self._mntDir)
        if not ret.startswith("@"):
            raise errors.StorageLayoutParseError("sub-volume \"%s\" is not supported" % (ret))
        return ret[1:]

    def get_snapshot_list(self):
        ret = []
        for sv in self._getSubVolList():
            m = re.fullmatch("@snapshots/([^/]+)", sv)
            if m is not None:
                ret.append(m.group(1))
        return ret

    def create_snapshot(self, snapshot_name):
        self._createSnapshotSubVol(self._mntDir, "@", os.path.join("@snapshots", snapshot_name))

    def remove_snapshot(self, snapshot_name):
        self._deleteSubVol(os.path.join("@snapshots", snapshot_name))

    def getDirPathsAndMntOptsForMount(self, kwargsDict):
        ret = []
        if "snapshot" not in kwargsDict:
            ret.append(("/", "subvol=/@"))
        else:
            assert kwargsDict["snapshot"] in self.get_snapshot_list()
            ret.append(("/", "subvol=/@snapshots/%s" % (kwargsDict["snapshot"])))
        ret += [
            ("/root", "subvol=/@root"),
            ("/home", "subvol=/@home"),
            ("/var", "subvol=/@var"),
        ]
        return ret

    def getDirpathsForUmount(self):
        return ["/var", "/home", "/root", "/"]

    def check(self, auto_fix, error_callback):
        svList = self._getSubVolList(self._mntDir)
        for sv in ["@", "@root", "@home", "@var", "@snapshots"]:
            try:
                svList.remove(sv)
            except ValueError:
                # no way to auto fix
                error_callback(errors.CheckCode.TRIVIAL, "Sub-volume \"%s\" does not exist." % (sv))
        for sv in svList:
            if not re.fullmatch("@snapshots/([^/]+)", sv) is not None:
                # no way to auto fix
                error_callback(errors.CheckCode.TRIVIAL, "Redundant sub-volume \"%s\"." % (sv))

    @staticmethod
    @abc.abstractmethod
    def _createSubVol(mntDir, subVolPath):
        pass

    @staticmethod
    @abc.abstractmethod
    def _createSnapshotSubVol(mntDir, srcSubVolPath, subVolPath):
        pass

    @staticmethod
    @abc.abstractmethod
    def _deleteSubVol(mntDir, subVolPath):
        pass

    @staticmethod
    @abc.abstractmethod
    def _getSubVolList(mntDir):
        pass


class SnapshotBtrfs(Snapshot):

    @staticmethod
    def _createSubVol(mntDir, subVolPath):
        Util.cmdCall("/sbin/btrfs", "subvolume", "create", os.path.join(mntDir, subVolPath))

    @staticmethod
    def _createSnapshotSubVol(mntDir, srcSubVolPath, subVolPath):
        Util.cmdCall("/sbin/btrfs", "subvolume", "snapshot", os.path.join(mntDir, srcSubVolPath), os.path.join(mntDir, subVolPath))

    @staticmethod
    def _deleteSubVol(mntDir, subVolPath):
        Util.cmdCall("/sbin/btrfs", "subvolume", "delete", os.path.join(mntDir, subVolPath))

    @staticmethod
    def _getSubVolList(mntDir):
        out = Util.cmdCall("/sbin/btrfs", "subvolume", "list", mntDir)
        # FIXME: parse out
        return out


class SnapshotBcachefs(Snapshot):

    @staticmethod
    def _createSubVol(mntDir, subVolPath):
        Util.cmdCall("/sbin/bcachefs", "subvolume", "create", os.path.join(mntDir, subVolPath))

    @staticmethod
    def _createSnapshotSubVol(mntDir, srcSubVolPath, subVolPath):
        Util.cmdCall("/sbin/bcachefs", "subvolume", "snapshot", os.path.join(mntDir, srcSubVolPath), os.path.join(mntDir, subVolPath))

    @staticmethod
    def _deleteSubVol(mntDir, subVolPath):
        Util.cmdCall("/sbin/bcachefs", "subvolume", "delete", os.path.join(mntDir, subVolPath))

    @staticmethod
    def _getSubVolList(mntDir):
        out = Util.cmdCall("/sbin/bcachefs", "subvolume", "list", mntDir)
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
        self._rwCtrl = self.BootDirRwController()

    @property
    def mount_point(self):
        return self._mntDir

    def get_bootdir_rw_controller(self):
        return self._rwCtrl


class MountEfi:

    class BootDirRwController(BootDirRwController):

        def __init__(self, bootMntDir):
            self._mntDir = bootMntDir

        @property
        def is_writable(self):
            for pobj in psutil.disk_partitions():
                if pobj.mountpoint == os.path.join(self._mntDir, "boot"):
                    return ("rw" in Util.mntOptsStrToList(pobj.opts))
            assert False

        def to_read_write(self):
            assert not self.is_writable
            assert self._isRootfsWritable()
            Util.cmdCall("/bin/mount", os.path.join(self._mntDir, "boot"), "-o", "rw,remount")

        def to_read_only(self):
            assert self.is_writable
            Util.cmdCall("/bin/mount", os.path.join(self._mntDir, "boot"), "-o", "ro,remount")

        def _isRootfsWritable(self):
            for pobj in psutil.disk_partitions():
                if pobj.mountpoint == self._mntDir:
                    return ("rw" in Util.mntOptsStrToList(pobj.opts))

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

    def get_bootdir_rw_controller(self):
        return self._rwCtrl


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
