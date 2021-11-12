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
from .util import Util, GptUtil, LvmUtil, MultiDisk, SwapLvmLv
from . import errors
from . import StorageLayout


class StorageLayoutImpl(StorageLayout):
    """Layout:
           /dev/sda                 GPT
               /dev/sda1            ESP partition
               /dev/sda2            LVM-PV for VG hdd
           /dev/sdb                 GPT
               /dev/sdb1            reserved ESP partition
               /dev/sdb2            LVM-PV for VG hdd
           /dev/mapper/hdd.root     root device, EXT4
           /dev/mapper/hdd.swap     swap device
       Description:
           1. /dev/sda1 and /dev/sdb1 must has the same size
           2. /dev/sda1 and /dev/sda2 is order-sensitive, no extra partition is allowed
           3. /dev/sdb1 and /dev/sdb2 is order-sensitive, no extra partition is allowed
           4. swap device is optional
           5. extra LVM-LV is allowed to exist
           6. extra harddisk is allowed to exist
    """

    def __init__(self):
        super().__init__()

        self._md = None             # MultiDisk
        self._slv = None            # SwapLvmLv

    @property
    def boot_mode(self):
        return StorageLayout.BOOT_MODE_EFI

    @property
    def dev_rootfs(self):
        return LvmUtil.rootLvDevPath

    @property
    def dev_swap(self):
        return self._slv.get_swap_devname()

    @MultiDisk.proxy
    def get_boot_disk(self):
        pass

    @SwapLvmLv.proxy
    def check_swap_size(self):
        pass

    def optimize_rootdev(self):
        LvmUtil.autoExtendLv(LvmUtil.rootLvDevPath)
        Util.cmdExec("/sbin/resize2fs", LvmUtil.rootLvDevPath)

    @MultiDisk.proxy
    def get_esp(self):
        pass

    @MultiDisk.proxy
    def get_esp_sync_info(self):
        pass

    @MultiDisk.proxy
    def sync_esp(self, src, dst):
        pass

    @MultiDisk.proxy
    def get_disk_list(self):
        pass

    def add_disk(self, devpath):
        assert devpath is not None

        if devpath not in Util.getDevPathListForFixedDisk():
            raise errors.StorageLayoutAddDiskError(devpath, errors.NOT_DISK)

        lastBootHdd = self._md.get_boot_hdd()

        # add
        self._md.add_disk(devpath)

        # create lvm physical volume on partition2 and add it to volume group
        LvmUtil.addPvToVg(self._md.get_disk_data_partition(devpath), LvmUtil.vgName)

        return lastBootHdd != self._md.get_boot_hdd()     # boot disk may change

    def remove_disk(self, devpath):
        assert devpath is not None

        if self._md.get_hdd_count() <= 1:
            raise errors.StorageLayoutRemoveDiskError(errors.CAN_NOT_REMOVE_LAST_HDD)

        lastBootHdd = self._cg.get_boot_hdd()
        parti = self._md.get_disk_data_partition(devpath)

        # hdd partition 2: remove from volume group
        rc, out = Util.cmdCallWithRetCode("/sbin/lvm", "pvmove", parti)
        if rc != 5:
            raise errors.StorageLayoutRemoveDiskError("failed")
        Util.cmdCall("/sbin/lvm", "vgreduce", LvmUtil.vgName, parti)

        # remove
        self._md.remove_disk(devpath)

        return lastBootHdd != self._md.get_boot_hdd()     # boot disk may change

    @SwapLvmLv.proxy
    def create_swap_lv(self):
        pass

    @SwapLvmLv.proxy
    def remove_swap_lv(self):
        pass


def create(hddList=None, dry_run=False):
    if hddList is None:
        hddList = Util.getDevPathListForFixedDisk()
        if len(hddList) == 0:
            raise errors.StorageLayoutCreateError(errors.NO_DISK_WHEN_CREATE)
    else:
        assert len(hddList) > 0

    ret = StorageLayoutImpl()

    if not dry_run:
        ret._md = MultiDisk()

        # add disks
        for devpath in hddList:
            ret._md.add_disk(devpath)
            LvmUtil.addPvToVg(ret._md.get_disk_data_partition(devpath), LvmUtil.vgName)

        # create root lv
        LvmUtil.createLvWithDefaultSize(LvmUtil.vgName, LvmUtil.rootLvName)
    else:
        ret._md = MultiDisk(diskList=hddList)

    ret._slv = SwapLvmLv()

    return ret


def parse(bootDev, rootDev):
    ret = StorageLayoutImpl()

    if not GptUtil.isEspPartition(bootDev):
        raise errors.StorageLayoutParseError(ret.name, errors.BOOT_DEV_IS_NOT_ESP)

    # boot harddisk
    bootHdd = Util.devPathPartiToDisk(bootDev)

    # vg
    if not Util.cmdCallTestSuccess("/sbin/lvm", "vgdisplay", LvmUtil.vgName):
        raise errors.StorageLayoutParseError(ret.name, errors.LVM_VG_NOT_FOUND(LvmUtil.vgName))

    # pv list
    diskList = []
    out = Util.cmdCall("/sbin/lvm", "pvdisplay", "-c")
    for m in re.finditer("(/dev/\\S+):%s:.*" % (LvmUtil.vgName), out, re.M):
        hdd, partId = Util.devPathPartiToDiskAndPartiId(m.group(1))
        if Util.getBlkDevPartitionTableType(hdd) != "gpt":
            raise errors.StorageLayoutParseError(ret.name, errors.PARTITION_TYPE_SHOULD_BE(hdd, "gpt"))
        if partId != 2:
            raise errors.StorageLayoutParseError(ret.name, "physical volume partition of %s is not %s" % (hdd, Util.devPathDiskToParti(hdd, 2)))
        if Util.getBlkDevSize(Util.devPathDiskToParti(hdd, 1)) != Util.getEspSize():
            raise errors.StorageLayoutParseError(ret.name, errors.PARTITION_SIZE_INVALID(Util.devPathDiskToParti(hdd, 1)))
        if os.path.exists(Util.devPathDiskToParti(hdd, 3)):
            raise errors.StorageLayoutParseError(ret.name, errors.DISK_HAS_REDUNDANT_PARTITION(hdd))
        diskList.append(hdd)

    ret._md = MultiDisk(diskList=diskList, bootHdd=bootHdd)

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

    return ret
