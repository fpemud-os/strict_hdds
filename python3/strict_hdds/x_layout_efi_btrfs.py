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

from .util import Util, MultiDisk

from . import StorageLayout


class StorageLayoutImpl(StorageLayout):
    """Layout:
           /dev/sda                 GPT
               /dev/sda1            ESP partition
               /dev/sda2            btrfs device
           /dev/sdb                 GPT
               /dev/sdb1            reserved ESP partition
               /dev/sdb2            btrfs device
           /dev/sda1:/dev/sda2      root device, btrfs
       Description:
           1. /dev/sda1 and /dev/sdb1 must has the same size
           2. /dev/sda1 and /dev/sda2 is order-sensitive, no extra partition is allowed
           3. /dev/sdb1 and /dev/sdb2 is order-sensitive, no extra partition is allowed
           4. use optional swap file /var/swap/swap.dat, at this time /var/swap is a standalone sub-volume
           5. extra harddisk is allowed to exist
    """

    def __init__(self):
        self._diskList = []         # harddisk list
        self._bootHdd = None        # boot harddisk name

    @property
    def boot_mode(self):
        return StorageLayout.BOOT_MODE_EFI

    @property
    def dev_rootfs(self):
        return sorted(self._diskList)[0]

    @property
    def dev_swap(self):
        return None

    def get_boot_disk(self):
        return self._bootHdd

    def check_swap_size(self):
        assert False

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
            raise StorageLayoutAddDiskError(devpath, "not a harddisk")

        # create partitions
        Util.initializeDisk(devpath, "gpt", [
            ("%dMiB" % (Util.getEspSizeInMb()), Util.fsTypeFat),
            ("*", "btrfs"),
        ])

        # fill partition1, synchronize boot device if needed
        parti = Util.devPathDiskToPartition(devpath, 1)
        Util.cmdCall("/usr/sbin/mkfs.vfat", parti)
        Util.syncBlkDev(Util.devPathDiskToPartition(self._bootHdd, 1), parti, mountPoint1=Util.bootDir)

        # create btrfs device
        parti = Util.devPathDiskToPartition(devpath, 2)
        Util.cmdCall("/usr/sbin/mkfs.btrfs", parti)
        self._diskList.append(devpath)

        return False

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
            raise StorageLayoutCreateError("no harddisk")
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
        raise StorageLayoutParseError(ret.name, "boot device is not an ESP partitiion")

    # boot harddisk
    ret._bootHdd = Util.devPathPartitionToDisk(bootDev)

    # vg
    if not Util.cmdCallTestSuccess("/sbin/lvm", "vgdisplay", LvmUtil.vgName):
        raise StorageLayoutParseError(ret.name, "volume group \"%s\" does not exist" % (LvmUtil.vgName))

    # pv list
    out = Util.cmdCall("/sbin/lvm", "pvdisplay", "-c")
    for m in re.finditer("(/dev/\\S+):%s:.*" % (LvmUtil.vgName), out, re.M):
        hdd, partId = Util.devPathPartitionToDiskAndPartitionId(m.group(1))
        if Util.getBlkDevPartitionTableType(hdd) != "gpt":
            raise StorageLayoutParseError(ret.name, "partition type of %s is not \"gpt\"" % (hdd))
        if partId != 2:
            raise StorageLayoutParseError(ret.name, "physical volume partition of %s is not %s" % (hdd, Util.devPathDiskToPartition(hdd, 2)))
        if Util.getBlkDevSize(Util.devPathDiskToPartition(hdd, 1)) != Util.getEspSize():
            raise StorageLayoutParseError(ret.name, "%s has an invalid size" % (Util.devPathDiskToPartition(hdd, 1)))
        if os.path.exists(Util.devPathDiskToPartition(hdd, 3)):
            raise StorageLayoutParseError(ret.name, "redundant partition exists on %s" % (hdd))
        ret._diskList.append(hdd)

    out = Util.cmdCall("/sbin/lvm", "lvdisplay", "-c")

    # root lv
    if re.search("/dev/hdd/root:%s:.*" % (LvmUtil.vgName), out, re.M) is not None:
        fs = Util.getBlkDevFsType(LvmUtil.rootLvDevPath)
        if fs != Util.fsTypeExt4:
            raise StorageLayoutParseError(ret.name, "root partition file system is \"%s\", not \"ext4\"" % (fs))
    else:
        raise StorageLayoutParseError(ret.name, "logical volume \"%s\" does not exist" % (LvmUtil.rootLvDevPath))

    # swap lv
    if re.search("/dev/hdd/swap:%s:.*" % (LvmUtil.vgName), out, re.M) is not None:
        if Util.getBlkDevFsType(LvmUtil.swapLvDevPath) != Util.fsTypeSwap:
            raise StorageLayoutParseError(ret.name, "\"%s\" has an invalid file system" % (LvmUtil.swapLvDevPath))
        ret._bSwapLv = True

    return ret
