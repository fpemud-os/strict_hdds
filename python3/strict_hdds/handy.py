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
from .util import Util, BcacheUtil
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
    def getSsdAndHddList(ssd_list, hdd_list):
        if len(ssd_list) == 0:
            ssd = None
        elif len(ssd_list) == 1:
            ssd = ssd_list[0]
        else:
            raise errors.StorageLayoutCreateError(errors.MULTIPLE_SSD)
        if len(hdd_list) == 0:
            raise errors.StorageLayoutCreateError(errors.NO_DISK_WHEN_CREATE)
        return (ssd, hdd_list)

    @staticmethod
    def cacheGroupGetSsdPartitions(storageLayoutName, bootDev, ssd):
        if ssd is not None:
            ssdEspParti = Util.devPathDiskToParti(ssd, 1)
            if os.path.exists(Util.devPathDiskToParti(ssd, 3)):
                ssdSwapParti = Util.devPathDiskToParti(ssd, 2)
                ssdCacheParti = Util.devPathDiskToParti(ssd, 3)
                if os.path.exists(Util.devPathDiskToParti(ssd, 4)):
                    raise errors.StorageLayoutParseError(storageLayoutName, errors.DISK_HAS_REDUNDANT_PARTITION(ssd))
            else:
                ssdCacheParti = Util.devPathDiskToParti(ssd, 2)

            # ssdEspParti
            if ssdEspParti != bootDev:
                raise errors.StorageLayoutParseError(storageLayoutName, "SSD is not boot device")
            if Util.getBlkDevSize(ssdEspParti) != Util.getEspSize():
                raise errors.StorageLayoutParseError(storageLayoutName, errors.PARTITION_SIZE_INVALID(ssdEspParti))

            # ssdSwapParti
            if ssdSwapParti is not None:
                if not os.path.exists(ssdSwapParti):
                    raise errors.StorageLayoutParseError(storageLayoutName, "SSD has no swap partition")
                if Util.getBlkDevFsType(ssdSwapParti) != Util.fsTypeSwap:
                    raise errors.StorageLayoutParseError(storageLayoutName, errors.SWAP_DEV_HAS_INVALID_FS_FLAG(ssdSwapParti))

            # ssdCacheParti
            if not os.path.exists(ssdCacheParti):
                raise errors.StorageLayoutParseError(storageLayoutName, "SSD has no cache partition")
            if True:
                disk, partId = Util.devPathPartiToDiskAndPartiId(ssdCacheParti)
                nextPartName = Util.devPathDiskToParti(disk, partId + 1)
                if os.path.exists(nextPartName):
                    raise errors.StorageLayoutParseError(storageLayoutName, errors.DISK_HAS_REDUNDANT_PARTITION(ssd))

            return ssdEspParti, ssdSwapParti, ssdCacheParti
        else:
            return None, None, None

    def cacheGroupFindByBackingDeviceList(cg):
        return [BcacheUtil.findByBackingDevice(cg.get_hdd_data_partition(x)) for x in cg.get_hdd_list()]

    @staticmethod
    def bcacheGetHddDictWithOneItem(storageLayoutName, bcacheDevPath, bcacheDev):
        hddDev, partId = Util.devPathPartiToDiskAndPartiId(BcacheUtil.getSlaveDevPathList(bcacheDevPath)[-1])
        if partId != 2:
            raise errors.StorageLayoutParseError(storageLayoutName, "bcache partition of %s is not %s" % (hddDev, Util.devPathDiskToParti(hddDev, 2)))
        if os.path.exists(Util.devPathDiskToParti(hddDev, 3)):
            raise errors.StorageLayoutParseError(storageLayoutName, errors.DISK_HAS_REDUNDANT_PARTITION(hddDev))
        return {hddDev: bcacheDev}

    @staticmethod
    def bcacheCheckHddDictItem(storageLayoutName, ssdCacheParti, hdd, bcacheDev):
        assert ssdCacheParti is not None

        tlist = BcacheUtil.getSlaveDevPathList(bcacheDev)
        if len(tlist) < 2:
            raise errors.StorageLayoutParseError(storageLayoutName, "%s(%s) has no cache device" % (hdd, bcacheDev))
        if len(tlist) > 2:
            raise errors.StorageLayoutParseError(storageLayoutName, "%s(%s) has multiple cache devices" % (hdd, bcacheDev))
        if tlist[0] != ssdCacheParti:
            raise errors.StorageLayoutParseError(storageLayoutName, "%s(%s) has invalid cache device" % (hdd, bcacheDev))
