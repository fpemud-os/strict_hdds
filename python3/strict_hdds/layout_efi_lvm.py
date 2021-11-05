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
from .util import LvmUtil

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

        self._diskList = []         # harddisk list
        self._bSwapLv = None        # whether swap lv exists
        self._bootHdd = None        # boot harddisk name

    @property
    def boot_mode(self):
        return StorageLayout.BOOT_MODE_EFI

    @property
    def dev_rootfs(self):
        return LvmUtil.rootLvDevPath

    @property
    def dev_swap(self):
        return LvmUtil.swapLvDevPath if self._bSwapLv else None

    def get_boot_disk(self):
        return self._bootHdd

    def check_swap_size(self):
        assert self._bSwapLv
        return Util.getBlkDevSize(LvmUtil.swapLvDevPath) >= Util.getSwapSize()

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

    def get_disk_list(self):
        return self._diskList

    def add_disk(self, devpath):
        assert devpath is not None
        assert devpath not in self._diskList

        if devpath not in Util.getDevPathListForFixedHdd():
            raise errors.StorageLayoutAddDiskError(devpath, errors.NOT_DISK)

        # create partitions
        Util.initializeDisk(devpath, "gpt", [
            ("%dMiB" % (Util.getEspSizeInMb()), Util.fsTypeFat),
            ("*", "lvm"),
        ])

        # fill partition1, mount boot device if needed
        parti = Util.devPathDiskToPartition(devpath, 1)
        Util.cmdCall("/usr/sbin/mkfs.vfat", parti)
        Util.syncBlkDev(Util.devPathDiskToPartition(self._bootHdd, 1), parti, mountPoint1=Util.bootDir)

        # create lvm physical volume on partition2 and add it to volume group
        parti = Util.devPathDiskToPartition(devpath, 2)
        Util.cmdCall("/sbin/lvm", "pvcreate", parti)
        Util.cmdCall("/sbin/lvm", "vgextend", LvmUtil.vgName, parti)
        self._diskList.append(devpath)

        return False

    def release_disk(self, devpath):
        assert devpath is not None
        assert devpath in self._diskList
        assert len(self._diskList) > 1

        parti = Util.devPathDiskToPartition(devpath, 2)
        rc, out = Util.cmdCallWithRetCode("/sbin/lvm", "pvmove", parti)
        if rc != 5:
            raise errors.StorageLayoutReleaseDiskError("failed")
        return

    def remove_disk(self, devpath):
        assert devpath is not None
        assert devpath in self._diskList
        assert len(self._diskList) > 1

        # change boot device if needed
        ret = False
        if self._bootHdd == devpath:
            Util.cmdCall("/bin/umount", Util.bootDir)
            self._diskList.remove(devpath)
            self._bootHdd = self._diskList[0]
            Util.gptToggleEspPartition(Util.devPathDiskToPartition(self._bootHdd, 1), True)
            Util.cmdCall("/bin/mount", Util.devPathDiskToPartition(self._bootHdd, 1), Util.bootDir, "-o", "ro")
            ret = True

        # remove harddisk
        parti = Util.devPathDiskToPartition(devpath, 2)
        Util.cmdCall("/sbin/lvm", "vgreduce", LvmUtil.vgName, parti)
        Util.wipeHarddisk(devpath)

        return ret

    def create_swap_lv(self):
        assert not self._bSwapLv
        Util.cmdCall("/sbin/lvm", "lvcreate", "-L", "%dGiB" % (Util.getSwapSizeInGb()), "-n", LvmUtil.swapLvName, LvmUtil.vgName)
        self._bSwapLv = True

    def remove_swap_lv(self):
        assert self._bSwapLv
        Util.cmdCall("/sbin/lvm", "lvremove", LvmUtil.swapLvDevPath)
        self._bSwapLv = False

    def _getCurEsp(self):
        return Util.devPathDiskToPartition(self._bootHdd, 1)

    def _getOtherEspList(self):
        ret = []
        for hdd in self._diskList:
            if hdd != self._bootHdd:
                ret.append(Util.devPathDiskToPartition(hdd, 1))
        return ret


def create_layout(hddList=None, dry_run=False):
    if hddList is None:
        hddList = Util.getDevPathListForFixedHdd()
        if len(hddList) == 0:
            raise errors.StorageLayoutCreateError(errors.NO_DISK)
    else:
        assert len(hddList) > 0

    if not dry_run:
        for devpath in hddList:
            # create partitions
            Util.initializeDisk(devpath, "gpt", [
                ("%dMiB" % (Util.getEspSizeInMb()), Util.fsTypeFat),
                ("*", "lvm"),
            ])

            # fill partition1
            parti = Util.devPathDiskToPartition(devpath, 1)
            Util.cmdCall("/usr/sbin/mkfs.vfat", parti)

            # create lvm physical volume on partition2 and add it to volume group
            parti = Util.devPathDiskToPartition(devpath, 2)
            Util.cmdCall("/sbin/lvm", "pvcreate", parti)
            if not Util.cmdCallTestSuccess("/sbin/lvm", "vgdisplay", LvmUtil.vgName):
                Util.cmdCall("/sbin/lvm", "vgcreate", LvmUtil.vgName, parti)
            else:
                Util.cmdCall("/sbin/lvm", "vgextend", LvmUtil.vgName, parti)

        # create root lv
        out = Util.cmdCall("/sbin/lvm", "vgdisplay", "-c", LvmUtil.vgName)
        freePe = int(out.split(":")[15])
        Util.cmdCall("/sbin/lvm", "lvcreate", "-l", "%d" % (freePe // 2), "-n", LvmUtil.rootLvName, LvmUtil.vgName)

    # return value
    ret = StorageLayoutImpl()
    ret._diskList = hddList
    ret._bSwapLv = False
    ret._bootHdd = ret._diskList[0]     # FIXME
    return ret


def parse_layout(bootDev, rootDev):
    ret = StorageLayoutImpl()

    if not Util.gptIsEspPartition(bootDev):
        raise errors.StorageLayoutParseError(ret.name, errors.BOOT_DEV_IS_NOT_ESP)

    # boot harddisk
    ret._bootHdd = Util.devPathPartitionToDisk(bootDev)

    # vg
    if not Util.cmdCallTestSuccess("/sbin/lvm", "vgdisplay", LvmUtil.vgName):
        raise errors.StorageLayoutParseError(ret.name, errors.LVM_VG_NOT_FOUND(LvmUtil.vgName))

    # pv list
    out = Util.cmdCall("/sbin/lvm", "pvdisplay", "-c")
    for m in re.finditer("(/dev/\\S+):%s:.*" % (LvmUtil.vgName), out, re.M):
        hdd, partId = Util.devPathPartitionToDiskAndPartitionId(m.group(1))
        if Util.getBlkDevPartitionTableType(hdd) != "gpt":
            raise errors.StorageLayoutParseError(ret.name, errors.PART_TYPE_SHOULD_BE(hdd, "gpt"))
        if partId != 2:
            raise errors.StorageLayoutParseError(ret.name, "physical volume partition of %s is not %s" % (hdd, Util.devPathDiskToPartition(hdd, 2)))
        if Util.getBlkDevSize(Util.devPathDiskToPartition(hdd, 1)) != Util.getEspSize():
            raise errors.StorageLayoutParseError(ret.name, errors.PARTITION_HAS_INVALID_SIZE(Util.devPathDiskToPartition(hdd, 1)))
        if os.path.exists(Util.devPathDiskToPartition(hdd, 3)):
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
        ret._bSwapLv = True

    return ret
