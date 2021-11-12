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
from .util import Util, MbrUtil, LvmUtil, SwapLvmLv
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
        return self._slv.get_swap_devname()

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

    def add_disk(self, disk):
        assert disk is not None
        assert disk not in self._diskList

        # check
        if disk not in Util.getDevPathListForFixedDisk():
            raise errors.StorageLayoutAddDiskError(disk, errors.NOT_DISK)

        # create partitions
        Util.initializeDisk(disk, Util.diskPartTableMbr, [
            ("*", "lvm"),
        ])

        # add to volume group
        parti = Util.devPathDiskToParti(disk, 1)
        LvmUtil.addPvToVg(parti, LvmUtil.vgName)
        self._diskList.append(disk)

        # try to switch boot disk
        return self._selectNewBootDiskForAdd(disk)

    def release_disk(self, disk):
        assert disk is not None
        assert disk in self._diskList

        # check
        if len(self._diskList) <= 1:
            raise errors.StorageLayoutReleaseDiskError(disk, errors.CAN_NOT_REMOVE_LAST_HDD)

        # move data
        parti = Util.devPathDiskToParti(disk, 1)
        rc, out = Util.cmdCallWithRetCode("/sbin/lvm", "pvmove", parti)
        if rc != 5:
            raise errors.StorageLayoutRemoveDiskError(disk, "failed")

    def remove_disk(self, disk):
        assert disk is not None
        assert disk in self._diskList

        if len(self._diskList) <= 1:
            raise errors.StorageLayoutRemoveDiskError(disk, errors.CAN_NOT_REMOVE_LAST_HDD)

        # change boot device if needed
        ret = self._selectNewBootDiskForRemove(disk)

        # remove and wipe disk
        parti = Util.devPathDiskToParti(disk, 1)
        Util.cmdCall("/sbin/lvm", "vgreduce", LvmUtil.vgName, parti)
        Util.wipeHarddisk(disk)
        self._diskList.remove(disk)

        return ret

    def boot_code_written(self, disk):
        assert disk is not None
        assert disk in self._diskList
        return self._selectNewBootDiskForAdd(disk)

    def boot_code_cleared(self, disk):
        assert disk is not None
        assert disk in self._diskList
        return self._selectNewBootDiskForRemove(disk)

    @SwapLvmLv.proxy
    def create_swap_lv(self):
        pass

    @SwapLvmLv.proxy
    def remove_swap_lv(self):
        pass

    def _selectNewBootDiskForAdd(self, disk_added):
        if self._bootHdd is None:
            if not MbrUtil.hasBootCode(disk_added):
                self._bootHdd = disk_added
                return True
        return False

    def _selectNewBootDiskForRemove(self, disk_removed):
        if self._bootHdd == disk_removed:
            self._bootHdd = None
            for d in self._diskList:
                if d == disk_removed:
                    continue
                if not MbrUtil.hasBootCode(d):
                    self._bootHdd = d
            return True
        else:
            return False


def create(disk_list, dry_run=False):
    if len(disk_list) == 0:
        raise errors.StorageLayoutCreateError(errors.NO_DISK_WHEN_CREATE)

    if not dry_run:
        for devpath in disk_list:
            # create partitions
            Util.initializeDisk(devpath, Util.diskPartTableMbr, [
                ("*", "lvm"),
            ])

            # create lvm physical volume on partition1 and add it to volume group
            LvmUtil.addPvToVg(Util.devPathDiskToParti(devpath, 1), LvmUtil.vgName, mayCreate=True)

        # create root lv
        LvmUtil.createLvWithDefaultSize(LvmUtil.vgName, LvmUtil.rootLvName)

    # return value
    ret = StorageLayoutImpl()
    ret._diskList = disk_list
    ret._slv = SwapLvmLv()
    ret._bootHdd = None
    return ret


def parse(booDev, rootDev):
    ret = StorageLayoutImpl()

    # vg
    if not Util.cmdCallTestSuccess("/sbin/lvm", "vgdisplay", LvmUtil.vgName):
        raise errors.StorageLayoutParseError(ret.name, errors.LVM_VG_NOT_FOUND(LvmUtil.vgName))

    # pv list
    out = Util.cmdCall("/sbin/lvm", "pvdisplay", "-c")
    for m in re.finditer("(/dev/\\S+):%s:.*" % (LvmUtil.vgName), out, re.M):
        hdd = Util.devPathPartiToDisk(m.group(1))
        if Util.getBlkDevPartitionTableType(hdd) != "dos":
            raise errors.StorageLayoutParseError(ret.name, errors.PARTITION_TYPE_SHOULD_BE(hdd, "dos"))
        if os.path.exists(Util.devPathDiskToParti(hdd, 2)):
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
        if MbrUtil.hasBootCode(hdd):
            if ret._bootHdd is not None:
                raise errors.StorageLayoutParseError(ret.name, errors.BOOT_CODE_ON_MULTIPLE_DISKS)
            ret._bootHdd = hdd
    if ret._bootHdd is None:
        raise errors.StorageLayoutParseError(ret.name, errors.BOOT_CODE_NOT_FOUND)

    return ret


def detect_and_mount(disk_list, mount_dir, mount_options):
    LvmUtil.activateAll()

    # it is interesting that we can reuse all the parse code
    ret = parse(None, None)

    Util.cmdCall("/bin/mount", LvmUtil.rootLvName, mount_dir)

    return ret
