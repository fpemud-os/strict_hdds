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

from .util import Util, LvmUtil, SwapLvmLv

from . import errors
from . import StorageLayout


class StorageLayoutImpl(StorageLayout):
    """Layout:
           /dev/sda                 MBR, BIOS-GRUB
               /dev/sda1            LVM-PV for VG hdd
           /dev/mapper/hdd.root     root device, EXT4
           /dev/mapper/hdd.swap     swap device
       Description:
           1. only one partition is allowed in LVM-PV device
           2. swap device is optional
           3. extra LVM-LV is allowed to exist
           4. extra harddisk is allowed to exist
    """

    def __init__(self):
        super().__init__()

        self._diskList = []         # harddisk list
        self._slv = None            # SwapLvmLv
        self._bootHdd = None        # boot harddisk name

    @property
    def boot_mode(self):
        return StorageLayout.BOOT_MODE_BIOS

    @property
    def dev_rootfs(self):
        return LvmUtil.rootLvDevPath

    @property
    def dev_swap(self):
        return LvmUtil.swapLvDevPath if self._bSwapLv else None

    def get_boot_disk(self):
        return self._bootHdd

    @SwapLvmLv.proxy
    def check_swap_size(self):
        pass

    def optimize_rootdev(self):
        LvmUtil.autoExtendLv(LvmUtil.rootLvDevPath)
        Util.cmdExec("/sbin/resize2fs", LvmUtil.rootLvDevPath)

    def get_disk_list(self):
        return self._diskList

    def add_disk(self, devpath):
        assert devpath is not None
        assert devpath not in self._diskList

        if devpath not in Util.getDevPathListForFixedHdd():
            raise errors.StorageLayoutAddDiskError(devpath, errors.NOT_DISK)

        # FIXME
        assert False

    def remove_disk(self, devpath):
        assert devpath is not None
        assert devpath in self._diskList
        assert len(self._diskList) > 1

        parti = Util.devPathDiskToPartition(devpath, 1)

        # move data
        rc, out = Util.cmdCallWithRetCode("/sbin/lvm", "pvmove", parti)
        if rc != 5:
            raise errors.StorageLayoutRemoveDiskError(devpath, "failed")

        # change boot device if needed
        ret = False
        if self._bootHdd == devpath:
            self._diskList.remove(devpath)
            self._bootHdd = self._diskList[0]
            # FIXME: add Boot Code for self._bootHdd?
            ret = True

        # remove harddisk
        Util.cmdCall("/sbin/lvm", "vgreduce", LvmUtil.vgName, parti)
        Util.wipeHarddisk(devpath)

        return ret

    @SwapLvmLv.proxy
    def create_swap_lv(self):
        pass

    @SwapLvmLv.proxy
    def remove_swap_lv(self):
        pass


def create_layout(disk_list=None, dry_run=False):
    if disk_list is None:
        disk_list = Util.getDevPathListForFixedHdd()
        if len(disk_list) == 0:
            raise errors.StorageLayoutCreateError(errors.NO_DISK)
    else:
        assert len(disk_list) > 0

    if not dry_run:
        for devpath in disk_list:
            # create partitions
            Util.initializeDisk(devpath, "mbr", [
                ("*", "lvm"),
            ])

            # create lvm physical volume on partition1 and add it to volume group
            LvmUtil.addPvToVg(Util.devPathDiskToPartition(devpath, 1), LvmUtil.vgName, mayCreate=True)

        # create root lv
        LvmUtil.createLvWithDefaultSize(LvmUtil.vgName, LvmUtil.rootLvName)

    # return value
    ret = StorageLayoutImpl()
    ret._diskList = disk_list
    ret._slv = SwapLvmLv()
    ret._bootHdd = ret._diskList[0]     # FIXME
    return ret


def parse_layout(booDev, rootDev):
    ret = StorageLayoutImpl()

    # vg
    if not Util.cmdCallTestSuccess("/sbin/lvm", "vgdisplay", LvmUtil.vgName):
        raise errors.StorageLayoutParseError(ret.name, errors.LVM_VG_NOT_FOUND(LvmUtil.vgName))

    # pv list
    out = Util.cmdCall("/sbin/lvm", "pvdisplay", "-c")
    for m in re.finditer("(/dev/\\S+):%s:.*" % (LvmUtil.vgName), out, re.M):
        hdd = Util.devPathPartitionToDisk(m.group(1))
        if Util.getBlkDevPartitionTableType(hdd) != "dos":
            raise errors.StorageLayoutParseError(ret.name, errors.PART_TYPE_SHOULD_BE(hdd, "dos"))
        if os.path.exists(Util.devPathDiskToPartition(hdd, 2)):
            raise errors.StorageLayoutParseError(ret.name, errors.DISK_HAS_REDUNDANT_PARTITION(hdd))
        ret._diskList.append(hdd)

    out = Util.cmdCall("/sbin/lvm", "lvdisplay", "-c")

    # root lv
    if re.search("/dev/hdd/root:%s:.*" % (LvmUtil.vgName), out, re.M) is not None:
        fs = Util.getBlkDevFsType(LvmUtil.rootLvDevPath)
        if fs != Util.fsTypeExt4:
            raise errors.StorageLayoutParseError(ret.name, "root partition file system is \"%s\", not \"ext4\"" % (fs))
    else:
        raise errors.StorageLayoutParseError(ret.name, errors.LVM_LV_NOT_FOUND(LvmUtil.rootLvDevPath))

    # swap lv
    if re.search("/dev/hdd/swap:%s:.*" % (LvmUtil.vgName), out, re.M) is not None:
        if Util.getBlkDevFsType(LvmUtil.swapLvDevPath) != Util.fsTypeSwap:
            raise errors.StorageLayoutParseError(ret.name, errors.SWAP_DEV_HAS_INVALID_FS_FLAG(LvmUtil.swapLvDevPath))
        ret._slv = SwapLvmLv(True)
    else:
        ret._slv = SwapLvmLv(False)

    # boot harddisk
    for hdd in ret._diskList:
        with open(hdd, "rb") as f:
            if not Util.isBufferAllZero(f.read(440)):
                if ret._bootHdd is not None:
                    raise errors.StorageLayoutParseError(ret.name, errors.BOOT_CODE_ON_MULTIPLE_DISKS)
                ret._bootHdd = hdd
    if ret._bootHdd is None:
        raise errors.StorageLayoutParseError(ret.name, errors.BOOT_CODE_NOT_FOUND)

    return ret
