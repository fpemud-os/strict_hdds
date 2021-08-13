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
from . import util
from . import StorageLayout
from . import StorageLayoutCreateError
from . import StorageLayoutAddDiskError
from . import StorageLayoutReleaseDiskError
from . import StorageLayoutParseError
from .disk_stack import DiskStackNodeHarddisk
from .disk_stack import DiskStackNodeLvmLv
from .disk_stack import DiskStackNodePartition
from .disk_stack import DiskStackUtil


class StorageLayoutEfiLvm(StorageLayout):
    """Layout:
           /dev/sda                 GPT, EFI-GRUB
               /dev/sda1            ESP partition
               /dev/sda2            LVM-PV for VG hdd
           /dev/sdb                 Non-SSD, GPT
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

    name = "efi-lvm"

    def __init__(self):
        self._diskList = []         # harddisk list
        self._bSwapLv = None        # whether swap lv exists
        self._bootHdd = None        # boot harddisk name

    @property
    def boot_mode(self):
        return StorageLayout.BOOT_MODE_EFI

    @property
    def dev_rootfs(self):
        return util.rootLvDevPath

    @property
    def dev_swap(self):
        return util.swapLvDevPath if self._bSwapLv else None

    def get_boot_disk(self):
        return self._bootHdd

    def check_swap_size(self):
        assert self._bSwapLv
        return util.getBlkDevSize(util.swapLvDevPath) >= util.getSwapSizeInGb() * 1024 * 1024 * 1024

    def get_disk_stack(self):
        ret = []
        ret.append(DiskStackNodeLvmLv(util.rootLvDevPath, util.vgName, util.rootLvName))
        if self._bSwapLv:
            ret.append(DiskStackNodeLvmLv(util.swapLvDevPath, util.vgName, util.swapLvName))

        for node in ret:
            for d in self._diskList:
                partNode = DiskStackNodePartition(util.devPathDiskToPartition(d, 2), DiskStackNodePartition.PART_TYPE_GPT, parent=node)
                DiskStackNodeHarddisk(d, DiskStackUtil.getBlkDevType(d), parent=partNode)

        for d in self._diskList:
            espNode = DiskStackNodePartition(util.devPathDiskToPartition(d, 1), DiskStackNodePartition.PART_TYPE_GPT)
            DiskStackNodeHarddisk(d, DiskStackUtil.getBlkDevType(d), parent=espNode)
            ret.append(espNode)

        return ret

    def optimize_rootdev(self):
        util.autoExtendLv(util.rootLvDevPath)

    def get_esp(self):
        return self._getCurEsp()

    def get_esp_sync_info(self):
        return (self._getCurEsp(), self._getOtherEspList())

    def sync_esp(self, src, dst):
        assert src is not None and dst is not None
        assert src == self._getCurEsp() and dst in self._getOtherEspList()
        util.syncBlkDev(src, dst, mountPoint1=util.bootDir)

    def get_disk_list(self):
        return self._diskList

    def add_disk(self, devpath):
        assert devpath is not None
        assert devpath not in self._diskList

        if devpath not in util.getDevPathListForFixedHdd():
            raise StorageLayoutAddDiskError(devpath, "not a harddisk")

        # create partitions
        util.initializeDisk(devpath, "gpt", [
            ("%dMiB" % (util.getEspSizeInMb()), util.fsTypeFat),
            ("*", "lvm"),
        ])

        # fill partition1, mount boot device if needed
        parti = util.devPathDiskToPartition(devpath, 1)
        util.cmdCall("/usr/sbin/mkfs.vfat", parti)
        util.syncBlkDev(util.devPathDiskToPartition(self._bootHdd, 1), parti, mountPoint1=util.bootDir)

        # create lvm physical volume on partition2 and add it to volume group
        parti = util.devPathDiskToPartition(devpath, 2)
        util.cmdCall("/sbin/lvm", "pvcreate", parti)
        util.cmdCall("/sbin/lvm", "vgextend", util.vgName, parti)
        self._diskList.append(devpath)

        return False

    def release_disk(self, devpath):
        assert devpath is not None
        assert devpath in self._diskList
        assert len(self._diskList) > 1

        parti = util.devPathDiskToPartition(devpath, 2)
        rc, out = util.cmdCallWithRetCode("/sbin/lvm", "pvmove", parti)
        if rc != 5:
            raise StorageLayoutReleaseDiskError("failed")
        return

    def remove_disk(self, devpath):
        assert devpath is not None
        assert devpath in self._diskList
        assert len(self._diskList) > 1

        # change boot device if needed
        ret = False
        if self._bootHdd == devpath:
            util.cmdCall("/bin/umount", util.bootDir)
            self._diskList.remove(devpath)
            self._bootHdd = self._diskList[0]
            util.gptToggleEspPartition(util.devPathDiskToPartition(self._bootHdd, 1), True)
            util.cmdCall("/bin/mount", util.devPathDiskToPartition(self._bootHdd, 1), util.bootDir, "-o", "ro")
            ret = True

        # remove harddisk
        parti = util.devPathDiskToPartition(devpath, 2)
        util.cmdCall("/sbin/lvm", "vgreduce", util.vgName, parti)
        util.wipeHarddisk(devpath)

        return ret

    def create_swap_lv(self):
        assert not self._bSwapLv
        util.cmdCall("/sbin/lvm", "lvcreate", "-L", "%dGiB" % (util.getSwapSizeInGb()), "-n", util.swapLvName, util.vgName)
        self._bSwapLv = True

    def remove_swap_lv(self):
        assert self._bSwapLv
        util.cmdCall("/sbin/lvm", "lvremove", util.swapLvDevPath)
        self._bSwapLv = False

    def _getCurEsp(self):
        return util.devPathDiskToPartition(self._bootHdd, 1)

    def _getOtherEspList(self):
        ret = []
        for hdd in self._diskList:
            if hdd != self._bootHdd:
                ret.append(util.devPathDiskToPartition(hdd, 1))
        return ret


def create_layout(hddList=None, dry_run=False):
    if hddList is None:
        hddList = util.getDevPathListForFixedHdd()
        if len(hddList) == 0:
            raise StorageLayoutCreateError("no harddisk")
    else:
        assert len(hddList) > 0

    if not dry_run:
        for devpath in hddList:
            # create partitions
            util.initializeDisk(devpath, "gpt", [
                ("%dMiB" % (util.getEspSizeInMb()), util.fsTypeFat),
                ("*", "lvm"),
            ])

            # fill partition1
            parti = util.devPathDiskToPartition(devpath, 1)
            util.cmdCall("/usr/sbin/mkfs.vfat", parti)

            # create lvm physical volume on partition2 and add it to volume group
            parti = util.devPathDiskToPartition(devpath, 2)
            util.cmdCall("/sbin/lvm", "pvcreate", parti)
            if not util.cmdCallTestSuccess("/sbin/lvm", "vgdisplay", util.vgName):
                util.cmdCall("/sbin/lvm", "vgcreate", util.vgName, parti)
            else:
                util.cmdCall("/sbin/lvm", "vgextend", util.vgName, parti)

        # create root lv
        out = util.cmdCall("/sbin/lvm", "vgdisplay", "-c", util.vgName)
        freePe = int(out.split(":")[15])
        util.cmdCall("/sbin/lvm", "lvcreate", "-l", "%d" % (freePe // 2), "-n", util.rootLvName, util.vgName)

    # return value
    ret = StorageLayoutEfiLvm()
    ret._diskList = hddList
    ret._bSwapLv = False
    ret._bootHdd = ret._diskList[0]     # FIXME
    return ret


def parse_layout(bootDev, rootDev):
    ret = StorageLayoutEfiLvm()

    if not util.gptIsEspPartition(bootDev):
        raise StorageLayoutParseError(StorageLayoutEfiLvm.name, "boot device is not an ESP partitiion")

    # boot harddisk
    ret._bootHdd = util.devPathPartitionToDisk(bootDev)

    # vg
    if not util.cmdCallTestSuccess("/sbin/lvm", "vgdisplay", util.vgName):
        raise StorageLayoutParseError(StorageLayoutEfiLvm.name, "volume group \"%s\" does not exist" % (util.vgName))

    # pv list
    out = util.cmdCall("/sbin/lvm", "pvdisplay", "-c")
    for m in re.finditer("(/dev/\\S+):%s:.*" % (util.vgName), out, re.M):
        hdd, partId = util.devPathPartitionToDiskAndPartitionId(m.group(1))
        if util.getBlkDevPartitionTableType(hdd) != "gpt":
            raise StorageLayoutParseError(StorageLayoutEfiLvm.name, "partition type of %s is not \"gpt\"" % (hdd))
        if partId != 2:
            raise StorageLayoutParseError(StorageLayoutEfiLvm.name, "physical volume partition of %s is not %s" % (hdd, util.devPathDiskToPartition(hdd, 2)))
        if util.getBlkDevSize(util.devPathDiskToPartition(hdd, 1)) != util.getEspSizeInMb() * 1024 * 1024:
            raise StorageLayoutParseError(StorageLayoutEfiLvm.name, "%s has an invalid size" % (util.devPathDiskToPartition(hdd, 1)))
        if os.path.exists(util.devPathDiskToPartition(hdd, 3)):
            raise StorageLayoutParseError(StorageLayoutEfiLvm.name, "redundant partition exists on %s" % (hdd))
        ret._diskList.append(hdd)

    out = util.cmdCall("/sbin/lvm", "lvdisplay", "-c")

    # root lv
    if re.search("/dev/hdd/root:%s:.*" % (util.vgName), out, re.M) is not None:
        fs = util.getBlkDevFsType(util.rootLvDevPath)
        if fs != util.fsTypeExt4:
            raise StorageLayoutParseError(StorageLayoutEfiLvm.name, "root partition file system is \"%s\", not \"ext4\"" % (fs))
    else:
        raise StorageLayoutParseError(StorageLayoutEfiLvm.name, "logical volume \"%s\" does not exist" % (util.rootLvDevPath))

    # swap lv
    if re.search("/dev/hdd/swap:%s:.*" % (util.vgName), out, re.M) is not None:
        if util.getBlkDevFsType(util.swapLvDevPath) != util.fsTypeSwap:
            raise StorageLayoutParseError(StorageLayoutEfiLvm.name, "\"%s\" has an invalid file system" % (util.swapLvDevPath))
        ret._bSwapLv = True

    return ret
