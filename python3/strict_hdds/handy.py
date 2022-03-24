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
import copy
import glob
import time
import struct
import parted
from .util import Util, PartiUtil, GptUtil, BcacheUtil, LvmUtil, PhysicalDiskMounts, TmpMount
from . import errors
from . import MountEntry
from . import RwController


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

    def add_disk(self, disk, fsType):
        assert disk is not None and disk not in self._hddList

        # create partitions
        Util.initializeDisk(disk, "gpt", [
            ("%dMiB" % (Util.getEspSizeInMb()), Util.fsTypeFat),
            ("*", fsType),
        ])

        # partition1: pending ESP partition
        parti = PartiUtil.diskToParti(disk, 1)
        Util.cmdCall("mkfs.vfat", parti)
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
            self._setFirstHddAsBootHdd()

    def remove_disk(self, hdd):
        assert hdd is not None and hdd in self._hddList

        # change boot device if needed
        if self._bootHdd == hdd:
            self._unsetCurrentBootHdd()
            self._hddList.remove(hdd)
            self._setFirstHddAsBootHdd()
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

    def _setFirstHddAsBootHdd(self):
        self._bootHdd = self._hddList[0]
        Util.toggleEspPartition(PartiUtil.diskToParti(self._bootHdd, 1), True)

    def _unsetCurrentBootHdd(self):
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
            self._ssdEspParti = None
            self._ssdSwapParti = None
            self._ssdCacheParti = None
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
        return self._ssdSwapParti if self._ssdSwapParti is not None else None

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

    def get_swap_size(self):
        assert self._ssdSwapParti is not None
        return Util.getBlkDevSize(self._ssdSwapParti)

    def add_ssd(self, ssd, fsType):
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
            ("*", fsType),
        ])

        # partition1: ESP partition
        Util.cmdCall("mkfs.vfat", self._ssdEspParti)
        if self._bootHdd is not None:
            Util.syncBlkDev(PartiUtil.diskToParti(self._bootHdd, 1), self._ssdEspParti, mountPoint1=Util.bootDir)
        else:
            pass

        # partition2: swap partition
        Util.cmdCall("mkswap", self._ssdSwapParti)

        # partition3: cache partition, leave it to caller
        pass

        # change boot device
        if self._bootHdd is not None:
            self._unsetCurrentBootHdd()

    def remove_ssd(self):
        assert self._ssd is not None

        # partition3: cache partition, the caller should have processed it
        self._ssdCacheParti = None

        # partition2: swap partition
        if self._ssdSwapParti is not None:
            assert not Util.swapDeviceIsBusy(self._ssdSwapParti)
            self._ssdSwapParti = None

        # partition1: ESP partition
        self._ssdEspParti = None

        # wipe disk
        Util.wipeHarddisk(self._ssd)
        self._ssd = None

        # change boot device
        if len(self._hddList) > 0:
            self._setFirstHddAsBootHdd()

    def add_hdd(self, hdd, fsType):
        assert hdd is not None and hdd not in self._hddList

        # create partitions
        Util.initializeDisk(hdd, "gpt", [
            ("%dMiB" % (Util.getEspSizeInMb()), Util.fsTypeFat),
            ("*", fsType),
        ])

        # partition1: pending ESP partition
        parti = PartiUtil.diskToParti(hdd, 1)
        Util.cmdCall("mkfs.vfat", parti)
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
        if self._ssd is None and self._bootHdd is None:
            assert len(self._hddList) == 1
            self._setFirstHddAsBootHdd()

    def remove_hdd(self, hdd):
        assert hdd is not None and hdd in self._hddList

        # boot device change
        bChange = False
        if self._ssd is None:
            assert self._bootHdd is not None
            if self._bootHdd == hdd:
                self._unsetCurrentBootHdd()
                bChange = True

        self._hddList.remove(hdd)
        Util.wipeHarddisk(hdd)

        # boot device change
        if bChange:
            self._setFirstHddAsBootHdd()

    def check_ssd(self, auto_fix, error_callback):
        if self._ssd is None:
            # no way to auto fix
            error_callback(errors.CheckCode.TRIVIAL, "It would be better to add a cache device.")

    def check_esp(self, auto_fix, error_callback):
        if self._ssd is not None:
            tlist = [self._ssdEspParti]
        else:
            tlist = []
        tlist += [self.get_hdd_esp_partition(x) for x in self._hddList]

        for parti in tlist:
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

    def _setFirstHddAsBootHdd(self):
        self._bootHdd = self._hddList[0]
        Util.toggleEspPartition(PartiUtil.diskToParti(self._bootHdd, 1), True)

    def _unsetCurrentBootHdd(self):
        Util.toggleEspPartition(PartiUtil.diskToParti(self._bootHdd, 1), False)
        self._bootHdd = None


class Bcache:

    def __init__(self, keyList=[], bcacheDevPathList=[]):
        self._backingDict = Util.keyValueListToDict(keyList, bcacheDevPathList)

        self._cacheDevSet = set()
        for bcacheDevPath in bcacheDevPathList:
            self._cacheDevSet.update(set(BcacheUtil.getSlaveDevPathList(bcacheDevPath)[:-1]))

    def get_bcache_dev(self, key):
        return self._backingDict[key]

    def get_all_bcache_dev_list(self):
        return list(self._backingDict.values())

    def add_cache(self, cacheDevPath):
        BcacheUtil.makeAndRegisterCacheDevice(cacheDevPath)
        BcacheUtil.attachCacheDevice(self._backingDict.values(), cacheDevPath)
        self._cacheDevSet.add(cacheDevPath)

    def add_backing(self, cacheDevPath, key, devPath):
        BcacheUtil.makeAndRegisterBackingDevice(devPath)

        bcacheDevPath = None
        if True:
            devName = os.path.basename(devPath)
            bcacheSet = set()
            for i in range(0, 10):
                for fullfn in glob.glob("/dev/bcache*"):
                    if fullfn not in bcacheSet:
                        if re.fullmatch("/dev/bcache[0-9]+", fullfn):
                            bcachePath = os.path.realpath("/sys/block/" + devName + "/bcache")
                            if os.path.basename(os.path.dirname(bcachePath)) == devName:
                                bcacheDevPath = fullfn
                                break
                    bcacheSet.add(fullfn)
                if bcacheDevPath is not None:
                    break
                time.sleep(1)
            if bcacheDevPath is None:
                raise Exception("corresponding bcache device is not found")

        if cacheDevPath is not None:
            BcacheUtil.attachCacheDevice([bcacheDevPath], cacheDevPath)

        self._backingDict[key] = bcacheDevPath
        return bcacheDevPath

    def remove_cache(self, cacheDevPath):
        BcacheUtil.unregisterCacheDevice(cacheDevPath)
        self._cacheDevSet.remove(cacheDevPath)

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
                    Bcache.setMode(mode)
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
        Util.cmdCall("lvm", "lvcreate", "-L", "%dGiB" % (Util.getSwapSizeInGb()), "-n", LvmUtil.swapLvName, LvmUtil.vgName)
        self._bSwapLv = True

    def remove_swap_lv(self):
        assert self._bSwapLv
        Util.cmdCall("lvm", "lvremove", LvmUtil.swapLvDevPath)
        self._bSwapLv = False

    def get_swap_size(self):
        assert self._bSwapLv
        return Util.getBlkDevSize(LvmUtil.swapLvDevPath)

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

    def get_swap_size(self):
        assert self._bSwapFile
        return os.path.getsize(Util.swapFilepath)

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
    def initializeFs(cls, devPath, mntOpts):
        with TmpMount(devPath, options=mntOpts) as mp:
            def __mkSubVol(name, mode, uid, gid):
                cls._createSubVol(mp.mountpoint, name)
                dirpath = os.path.join(mp.mountpoint, name)
                os.chown(dirpath, uid, gid)
                os.chmod(dirpath, mode)

            def __mkDir(name, mode, uid, gid):
                dirpath = os.path.join(mp.mountpoint, name)
                os.mkdir(dirpath)
                os.chown(dirpath, uid, gid)
                os.chmod(dirpath, mode)

            __mkSubVol("@", 0o40755, 0, 0)
            for path, name, mode, uid, gid in (cls._homeSubVols() + cls._varSubVols()):
                __mkSubVol(name, mode, uid, gid)
            __mkDir("@snapshots", 0o40700, 0, 0)

    @classmethod
    def checkFs(cls, storageLayoutName, devPath, mntOpts, snapshot):
        nameList = [x[1] for x in ([cls._rootSubVol()] + cls._homeSubVols() + cls._varSubVols())]
        if snapshot is not None:
            nameList.append("@snapshots/%s/snapshot" % (snapshot))
        with TmpMount(devPath, options=mntOpts) as mp:
            svList = cls._getSubVolList(mp.mountpoint)
            for sv in nameList:
                try:
                    svList.remove(sv)
                except ValueError:
                    raise errors.StorageLayoutParseError(storageLayoutName, "sub-volume \"%s\" not found" % (sv))

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

    def __init__(self, mntDir, snapshot=None):
        self._mntDir = mntDir
        self._snapshotName = snapshot

    @property
    def snapshot(self):
        return self._snapshotName

    def get_snapshot_list(self):
        # ret = []
        # for sv in self._getSubVolList():
        #     m = re.fullmatch("@snapshots/([^/]+)/snapshot", sv)
        #     if m is not None:
        #         ret.append(m.group(1))
        # return ret
        assert False

    def create_snapshot(self, snapshot_name):
        # self._createSnapshotSubVol(self._mntDir, "@", os.path.join("@snapshots", snapshot_name))
        assert False

    def remove_snapshot(self, snapshot_name):
        # self._deleteSubVol(os.path.join("@snapshots", snapshot_name))
        assert False

    def getParamsForMount(self):
        ret = []
        if True:
            path, name, mode, uid, gid = self._rootSubVol()
            if self._snapshotName is not None:
                name = "@snapshots/%s/snapshot" % (self._snapshotName)
            ret.append((path, mode, uid, gid, ["subvol=/%s" % (name)]))
        for path, name, mode, uid, gid in (self._homeSubVols() + self._varSubVols()):
            ret.append((path, mode, uid, gid, ["subvol=/%s" % (name)]))
        return ret

    def check(self, auto_fix, error_callback):
        nameList = [x[1] for x in ([self._rootSubVol()] + self._homeSubVols() + self._varSubVols())]
        svList = self._getSubVolList(self._mntDir)

        # check existence
        for sv in nameList:
            try:
                svList.remove(sv)
            except ValueError:
                error_callback(errors.CheckCode.TRIVIAL, "Sub-volume \"%s\" does not exist." % (e))     # no way to auto fix

        # check redundancy
        prefixList = [x[1] + "/" for x in (self._homeSubVols() + self._varSubVols())]
        for sv in svList:
            if any([sv.startswith(x) for x in prefixList]):
                # sub-volumes created by other programs
                pass
            elif sv.startswith("@snapshots/"):
                if not re.fullmatch("@snapshots/[^/]+/snapshot", sv):
                    error_callback(errors.CheckCode.TRIVIAL, "Redundant sub-volume \"%s\"." % (sv))     # too dangerous to auto fix
            else:
                error_callback(errors.CheckCode.TRIVIAL, "Redundant sub-volume \"%s\"." % (sv))         # too dangerous to auto fix

    @staticmethod
    def _rootSubVol():
        return ("/", "@", 0o40700, 0, 0)

    @staticmethod
    def _homeSubVols():
        return [
            ("/root", "@root", 0o40700, 0, 0),
            ("/home", "@home", 0o40755, 0, 0),
        ]

    @staticmethod
    def _varSubVols():
        return [
            ("/var/cache", "@var_cache", 0o40755, 0, 0)
            ("/var/db",    "@var_db",    0o40755, 0, 0)
            ("/var/games", "@var_games", 0o40755, 0, 0)     # FIXME
            ("/var/lib",   "@var_lib",   0o40755, 0, 0)
            ("/var/log",   "@var_log",   0o40755, 0, 0)
            ("/var/spool", "@var_spool", 0o40755, 0, 0)
            ("/var/svc.d", "@var_svc.d", 0o40755, 0, 0)     # FIXME
            ("/var/tmp",   "@var_tmp",   0o41777, 0, 0)
            ("/var/www",   "@var_www",   0o40755, 0, 0)     # FIXME        
        ]

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
        Util.cmdCall("btrfs", "subvolume", "create", os.path.join(mntDir, subVolPath))

    @staticmethod
    def _createSnapshotSubVol(mntDir, srcSubVolPath, subVolPath):
        Util.cmdCall("btrfs", "subvolume", "snapshot", os.path.join(mntDir, srcSubVolPath), os.path.join(mntDir, subVolPath))

    @staticmethod
    def _deleteSubVol(mntDir, subVolPath):
        Util.cmdCall("btrfs", "subvolume", "delete", os.path.join(mntDir, subVolPath))

    @staticmethod
    def _getSubVolList(mntDir):
        ret = []
        out = Util.cmdCall("btrfs", "subvolume", "list", mntDir)
        for m in re.finditer("path (\\S+)", out, re.M):
            ret.append(m.group(1))
        return ret


# class SnapshotBcachefs(Snapshot):

#     @staticmethod
#     def _createSubVol(mntDir, subVolPath):
#         Util.cmdCall("bcachefs", "subvolume", "create", os.path.join(mntDir, subVolPath))

#     @staticmethod
#     def _createSnapshotSubVol(mntDir, srcSubVolPath, subVolPath):
#         Util.cmdCall("bcachefs", "subvolume", "snapshot", os.path.join(mntDir, srcSubVolPath), os.path.join(mntDir, subVolPath))

#     @staticmethod
#     def _deleteSubVol(mntDir, subVolPath):
#         Util.cmdCall("bcachefs", "subvolume", "delete", os.path.join(mntDir, subVolPath))

#     @staticmethod
#     def _getSubVolList(mntDir):
#         out = Util.cmdCall("bcachefs", "subvolume", "list", mntDir)
#         # FIXME: parse out
#         assert False


class Mount(abc.ABC):

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

    def __init__(self, bIsMounted, mntDir, mntParams, kwargsDict):
        assert len(mntParams) > 0
        assert all([isinstance(x, MountParam) for x in mntParams])
        assert mntParams[0].mountpoint == "/"

        self._mntDir = mntDir

        self._mntParams = copy.deepcopy(mntParams)
        for p in self._mntParams:
            p.setMountObj(self)

        # FIXME: we'll use kwargsDict later
        pass

        # do mount
        if not bIsMounted:
            for p in self._mntParams:
                if p.mountpoint != "/":
                    if not os.path.exists(p.real_dir):
                        os.mkdir(p.real_dir)
                        os.chmod(p.real_dir, p.mnt_dir_mode)
                        os.chown(p.real_dir, p.mnt_dir_uid, p.mnt_dir_gid)
                    elif os.path.isdir(p.real_dir) and not os.path.islink(p.real_dir):
                        st = os.stat(p.real_dir)
                        if st.st_mode != p.mnt_dir_mode:
                            raise errors.StorageLayoutMountError("mount directory \"%s\" has invalid permission" % (p.real_dir))
                        if st.st_uid != p.mnt_dir_uid:
                            raise errors.StorageLayoutMountError("mount directory \"%s\" has invalid owner" % (p.real_dir))
                        if st.st_gid != p.mnt_dir_gid:
                            raise errors.StorageLayoutMountError("mount directory \"%s\" has invalid owner group" % (p.real_dir))
                    else:
                        raise errors.StorageLayoutMountError("mount directory \"%s\" is invalid" % (p.real_dir))
                if p.device is not None:
                    Util.cmdCall("mount", "-t", p.fstype, "-o", p.opts, p.device, p.real_dir)

    @property
    def mount_point(self):
        return self._mntDir

    @property
    def mount_params(self):
        return self._mntParams

    def get_mount_entries(self):
        ret = []
        for p in self._mntParams:
            item = MountEntry(p.device, p.mountpoint, p.fstype,
                              PhysicalDiskMounts.find_entry_by_mount_point(p.real_dir).opts,
                              p.real_dir)
            ret.append(item)
        return ret

    def umount(self):
        for p in reversed(self._mntParams):
            if p.device is not None:
                Util.cmdCall("umount", p.real_dir)

    @abc.abstractmethod
    def get_bootdir_rw_controller(self):
        pass


class MountBios(Mount):

    class RwController(RwController):

        def is_writable(self):
            return True

        def to_read_write(self):
            pass

        def to_read_only(self):
            pass

    def __init__(self, bIsMounted, mntDir, mntParams, kwargsDict):
        super().__init__(bIsMounted, mntDir, mntParams, kwargsDict)
        assert len(mntParams) == 1
        self._rwCtrl = self.RwController()

    def get_bootdir_rw_controller(self):
        return self._rwCtrl


class MountEfi(Mount):

    class RwController(RwController):

        def __init__(self, parent):
            self._parent = parent

        def is_writable(self):
            return self._parent._isMountParamWritable(self._parent._pEsp)

        def to_read_write(self):
            assert self._parent._isMountParamWritable(self._parent._pRootfs)
            assert not self._parent._isMountParamWritable(self._parent._pEsp)
            Util.cmdCall("mount", self._parent._pEsp.real_dir, "-o", "rw,remount")

        def to_read_only(self):
            assert self._parent._isMountParamWritable(self._parent._pEsp)
            Util.cmdCall("mount", self._parent._pEsp.real_dir, "-o", "ro,remount")

    def __init__(self, bIsMounted, mntDir, mntParams, kwargsDict):
        super().__init__(bIsMounted, mntDir, mntParams, kwargsDict)
        self._pRootfs = self._findRootfsMountParam()
        self._pEsp = self._findEspMountParam()
        self._rwCtrl = self.RwController(self)

    def get_bootdir_rw_controller(self):
        return self._rwCtrl

    def mount_esp(self, parti):
        assert self._pEsp.device is None
        Util.cmdCall("mount", "-t", self._pEsp.fstype, "-o", self._pEsp.opts, parti, self._pEsp.real_dir)
        self._pEsp.device = parti

    def umount_esp(self, parti):
        assert parti == self._pEsp.device
        assert not self._isMountParamWritable(self._pEsp)
        Util.cmdCall("umount", self._pEsp.real_dir)
        self._pEsp.device = None

    def _findRootfsMountParam(self):
        for p in self.mount_params:
            if p.mountpoint == "/":
                return p
        assert False

    def _findEspMountParam(self):
        for p in self.mount_params:
            if p.mountpoint == Util.bootDir:
                return p
        assert False

    def _isMountParamWritable(self, p):
        return ("rw" in PhysicalDiskMounts.find_entry_by_mount_point(p.real_dir).mnt_opt_list)


class MountParam(MountEntry):

    def __init__(self, dir_path, dir_mode, dir_uid, dir_gid, device, fstype, mnt_opt_list=[]):
        assert dir_path.startswith("/")

        if dir_path == "/":
            assert dir_mode == 0o40755 and dir_uid == 0 and dir_gid == 0
        elif dir_path == "/boot":
            assert dir_mode == 0o40755 and dir_uid == 0 and dir_gid == 0 and mnt_opt_list == Util.bootDirMntOptList

        super().__init__(device, dir_path, fstype, ",".join(mnt_opt_list))
        self.mnt_dir_mode = dir_mode
        self.mnt_dir_uid = dir_uid
        self.mnt_dir_gid = dir_gid

    def setMountObj(self, mountObj):
        if self.mountpoint == "/":
            self.real_dir = mountObj.mount_point
        else:
            self.real_dir = os.path.join(mountObj.mount_point, self.mountpoint[1:])


class HandyMd:

    @staticmethod
    def checkAndAddDisks(md, diskList, fsType):
        if len(diskList) == 0:
            raise errors.StorageLayoutCreateError(errors.NO_DISK_WHEN_CREATE)
        for disk in diskList:
            if not Util.isHarddiskClean(disk):
                raise errors.StorageLayoutCreateError(errors.DISK_NOT_CLEAN(disk))
        for disk in diskList:
            md.add_disk(disk, fsType)

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
    def checkAndAddDisks(cg, ssdList, hddList, fsType):
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
            cg.add_ssd(ssd, fsType)
        for hdd in hddList:
            cg.add_hdd(hdd, fsType)

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


class DisksChecker:

    def __init__(self, disk_list):
        assert len(disk_list) > 0
        self._hddList = disk_list
        self._diskCache = dict()        # avoid create new disk object every time

    def check_partition_type(self, partition_type_list, auto_fix, error_callback):
        partTypeList = []

        bBad = False
        for hdd in self._hddList:
            dev, disk = self._partedGetDevAndDisk(hdd)
            partTypeList.append(disk.type)
            if disk.type not in [partition_type_list]:
                error_callback(errors.CheckCode.TRIVIAL, "Inappopriate partition type for %s" % (hdd))
                bBad = True

        if not bBad:
            for i in range(1, len(self._hddList)):
                if partTypeList[i - 1] != partTypeList[i]:
                    error_callback(errors.CheckCode.TRIVIAL, "%s and %s have different partition types" % (self._hddList[i - 1], self._hddList[i]))

    def check_boot_sector(self, auto_fix, error_callback):
        for hdd in self._hddList:
            dev, disk = self._partedGetDevAndDisk(hdd)
            if disk.type == "msdos":
                pass
            elif disk.type == "gpt":
                # struct mbr_partition_record {
                #     uint8_t  boot_indicator;
                #     uint8_t  start_head;
                #     uint8_t  start_sector;
                #     uint8_t  start_track;
                #     uint8_t  os_type;
                #     uint8_t  end_head;
                #     uint8_t  end_sector;
                #     uint8_t  end_track;
                #     uint32_t starting_lba;
                #     uint32_t size_in_lba;
                # };
                mbrPartitionRecordFmt = "8BII"
                assert struct.calcsize(mbrPartitionRecordFmt) == 16

                # struct mbr_header {
                #     uint8_t                     boot_code[440];
                #     uint32_t                    unique_mbr_signature;
                #     uint16_t                    unknown;
                #     struct mbr_partition_record partition_record[4];
                #     uint16_t                    signature;
                # };
                self.mbrHeaderFmt = "440sIH%dsH" % (struct.calcsize(mbrPartitionRecordFmt) * 4)
                assert struct.calcsize(self.mbrHeaderFmt) == 512

                # get Protective MBR header
                mbrHeader = None
                if True:
                    with open(hdd, "rb") as f:
                        buf = f.read(struct.calcsize(self.mbrHeaderFmt))
                        mbrHeader = struct.unpack(self.mbrHeaderFmt, buf)
                else:
                    # FIXME: we can't use self._partedReadSectors() since it returns str, not bytes, what a bug!
                    # mbrHeader = struct.unpack(self.mbrHeaderFmt, self._partedReadSectors(dev, 0, 1)[:struct.calcsize(self.mbrHeaderFmt)])
                    pass

                # check Protective MBR header
                if not Util.isBufferAllZero(mbrHeader[0]):
                    error_callback(errors.CheckCode.TRIVIAL, "Protective MBR Boot Code should be empty for %s" % (hdd))
                    continue
                if mbrHeader[1] != 0:
                    error_callback(errors.CheckCode.TRIVIAL, "Protective MBR Disk Signature should be zero for %s" % (hdd))
                    continue
                if mbrHeader[2] != 0:
                    error_callback(errors.CheckCode.TRIVIAL, "reserved area in Protective MBR should be zero for %s" % (hdd))
                    continue

                # check Protective MBR Partition Record
                pRec = struct.unpack_from(mbrPartitionRecordFmt, mbrHeader[3], 0)
                if pRec[4] != 0xEE:
                    error_callback(errors.CheckCode.TRIVIAL, "The first Partition Record should be Protective MBR Partition Record (OS Type == 0xEE) for %s" % (hdd))
                    continue
                if pRec[0] != 0:
                    error_callback(errors.CheckCode.TRIVIAL, "Boot Indicator in Protective MBR Partition Record should be zero for %s" % (hdd))
                    continue

                # other Partition Record should be filled with zero
                if not Util.isBufferAllZero(mbrHeader[struct.calcsize(mbrPartitionRecordFmt):]):
                    error_callback(errors.CheckCode.TRIVIAL, "All Partition Records should be filled with zero")
                    continue

                # ghnt and check primary and backup GPT header
                pass

    def check_logical_sector_size(self, auto_fix, error_callback):
        for hdd in self._hddList:
            dev, disk = self._partedGetDevAndDisk(hdd)
            if disk.type == "msdos":
                if dev.sectorSize != 512:
                    error_callback(errors.CheckCode.TRIVIAL, "%s uses MBR partition table, its logical sector size (%d) should be 512" % (hdd, dev.sectorSize))
            elif disk.type == "gpt":
                if dev.physicalSectorSize in [512, 4096]:
                    if dev.sectorSize != dev.physicalSectorSize:
                        error_callback(errors.CheckCode.TRIVIAL, "%s has different physical sector size (%d) and logical sector size (%d)" % (hdd, dev.physicalSectorSize, dev.sectorSize))
                else:
                    if dev.sectorSize not in [512, 4096]:
                        error_callback(errors.CheckCode.TRIVIAL, "%s has inapporiate logical sector size (%d)" % (hdd, dev.sectorSize))

    def _partedGetDevAndDisk(self, devPath):
        partedDev = parted.getDevice(devPath)
        if devPath not in self._diskCache:
            self._diskCache[devPath] = parted.newDisk(partedDev)
        return partedDev, self._diskCache[devPath]

    def _partedReadSectors(self, partedDev, startSector, sectorCount):
        partedDev.open()
        try:
            return partedDev.read(startSector, sectorCount)
        finally:
            partedDev.close()


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
        if not Util.cmdCallTestSuccess("lvm", "vgdisplay", LvmUtil.vgName):
            raise errors.StorageLayoutParseError(storageLayoutName, errors.LVM_VG_NOT_FOUND(LvmUtil.vgName))

        # get pv list
        pvList = []
        out = Util.cmdCall("lvm", "pvdisplay", "-c")
        for m in re.finditer("(/dev/\\S+):%s:.*" % (LvmUtil.vgName), out, re.M):
            pvList.append(m.group(1))

        # find root lv
        out = Util.cmdCall("lvm", "lvdisplay", "-c")
        if re.search("/dev/hdd/root:%s:.*" % (LvmUtil.vgName), out, re.M) is None:
            raise errors.StorageLayoutParseError(storageLayoutName, errors.LVM_LV_NOT_FOUND(LvmUtil.rootLvDevPath))

        return pvList

    @staticmethod
    def swapFileDetectAndNew(storageLayoutName, rootfs_mount_dir):
        fullfn = rootfs_mount_dir.rstrip("/") + Util.swapFilepath
        if os.path.exists(fullfn):
            if not Util.cmdCallTestSuccess("swaplabel", fullfn):
                raise errors.StorageLayoutParseError(storageLayoutName, errors.SWAP_DEV_HAS_INVALID_FS_FLAG(fullfn))
            return SwapFile(True)
        else:
            return SwapFile(False)

    @staticmethod
    def swapLvDetectAndNew(storageLayoutName):
        out = Util.cmdCall("lvm", "lvdisplay", "-c")
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
