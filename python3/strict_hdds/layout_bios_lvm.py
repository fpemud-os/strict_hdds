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


class StorageLayoutBiosLvm(StorageLayout):
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

    name = "bios-lvm"

    def __init__(self):
        self.diskList = []

        self._bSwapLv = None        # whether swap lv exists
        self._bootHdd = None        # boot disk name

    @property
    def boot_mode(self):
        return StorageLayout.BOOT_MODE_BIOS

    def is_ready(self):
        assert len(self.diskList) > 0
        assert self._bSwapLv is not None
        assert self._bootHdd in self.diskList
        return True

    def get_rootdev(self):
        assert self.is_ready()
        return _rootLvDevPath

    def get_swap(self):
        assert self.is_ready()
        return _swapLvDevPath if self._bSwapLv else None

    def check_swap_size(self):
        assert self.is_ready() and self._bSwapLv
        return util.getBlkDevSize(_swapLvDevPath) >= util.getSwapSizeInGb() * 1024 * 1024 * 1024

    def get_boot_disk(self):
        assert self.is_ready()
        return self._bootHdd

    def optimize_rootdev(self):
        assert self.is_ready()
        util.autoExtendLv(self.get_rootdev())

    def add_disk(self, devpath):
        assert devpath is not None
        assert self.is_ready()
        assert devpath not in self.diskList

        if devpath not in util.getDevPathListForFixedHdd():
            raise StorageLayoutAddDiskError(devpath, "not a harddisk")

        # FIXME
        assert False

    def release_disk(self, devpath):
        assert devpath is not None
        assert self.is_ready()
        assert devpath in self.diskList and len(self.diskList) > 1 

        parti = util.devPathDiskToPartition(devpath, 1)
        rc, out = util.cmdCallWithRetCode("/sbin/lvm", "pvmove", parti)
        if rc != 5:
            raise StorageLayoutReleaseDiskError(devpath, "failed")

    def remove_disk(self, devpath):
        assert devpath is not None
        assert self.is_ready()
        assert devpath in self.diskList and len(self.diskList) > 1

        # change boot device if needed
        ret = False
        if self._bootHdd == devpath:
            self.diskList.remove(devpath)
            self._bootHdd = self.diskList[0]
            # FIXME: add Boot Code for self._bootHdd?
            ret = True

        # remove harddisk
        parti = util.devPathDiskToPartition(devpath, 1)
        util.cmdCall("/sbin/lvm", "vgreduce", _vgName, parti)
        util.wipeHarddisk(devpath)

        return ret

    def create_swap_lv(self):
        assert self.is_ready() and not self._bSwapLv
        util.cmdCall("/sbin/lvm", "lvcreate", "-L", "%dGiB" % (util.getSwapSizeInGb()), "-n", _swapLvName, _vgName)
        self._bSwapLv = True

    def remove_swap_lv(self):
        assert self.is_ready() and self._bSwapLv
        util.cmdCall("/sbin/lvm", "lvremove", _swapLvDevPath)
        self._bSwapLv = False


def create_layout(disk_list=None):
    if disk_list is None:
        disk_list = util.getDevPathListForFixedHdd()
        if len(disk_list) == 0:
            raise StorageLayoutCreateError("no harddisks")
    else:
        assert len(disk_list) > 0

    for devpath in disk_list:
        # create partitions
        util.initializeDisk(devpath, "mbr", [
            ("*", "lvm"),
        ])

        # create lvm physical volume on partition1 and add it to volume group
        parti = util.devPathDiskToPartition(devpath, 1)
        util.cmdCall("/sbin/lvm", "pvcreate", parti)
        if not util.cmdCallTestSuccess("/sbin/lvm", "vgdisplay", _vgName):
            util.cmdCall("/sbin/lvm", "vgcreate", _vgName, parti)
        else:
            util.cmdCall("/sbin/lvm", "vgextend", _vgName, parti)

    # create root lv
    out = util.cmdCall("/sbin/lvm", "vgdisplay", "-c", _vgName)
    freePe = int(out.split(":")[15])
    util.cmdCall("/sbin/lvm", "lvcreate", "-l", "%d" % (freePe // 2), "-n", _rootLvName, _vgName)


def parse_layout():
    ret = StorageLayoutBiosLvm()

    # vg
    if not util.cmdCallTestSuccess("/sbin/lvm", "vgdisplay", _vgName):
        raise StorageLayoutParseError(StorageLayoutBiosLvm.name, "volume group \"%s\" does not exist" % (_vgName))

    # pv
    out = util.cmdCall("/sbin/lvm", "pvdisplay", "-c")
    for m in re.finditer("(/dev/\\S+):%s:.*" % (_vgName), out, re.M):
        hdd = util.devPathPartitionToDisk(m.group(1))
        if util.getBlkDevPartitionTableType(hdd) != "dos":
            raise StorageLayoutParseError(StorageLayoutBiosLvm.name, "partition type of %s is not \"dos\"" % (hdd))
        if os.path.exists(util.devPathDiskToPartition(hdd, 2)):
            raise StorageLayoutParseError(StorageLayoutBiosLvm.name, "redundant partition exists on %s" % (hdd))
        ret.diskList.append(hdd)

    out = util.cmdCall("/sbin/lvm", "lvdisplay", "-c")

    # root lv
    if re.search("/dev/hdd/root:%s:.*" % (_vgName), out, re.M) is not None:
        assert os.path.exists(_rootLvDevPath)
        fs = util.getBlkDevFsType(_rootLvDevPath)
        if fs != "ext4":
            raise StorageLayoutParseError(StorageLayoutBiosLvm.name, "root partition file system is \"%s\", not \"ext4\"" % (fs))
    else:
        raise StorageLayoutParseError(StorageLayoutBiosLvm.name, "logical volume \"%s\" does not exist" % (_rootLvDevPath))

    # swap lv
    if re.search("/dev/hdd/swap:%s:.*" % (_vgName), out, re.M) is not None:
        assert os.path.exists(_swapLvDevPath)
        if util.getBlkDevFsType(_swapLvDevPath) != _swapLvName:
            raise StorageLayoutParseError(StorageLayoutBiosLvm.name, "\"%s\" has an invalid file system" % (_swapLvDevPath))

    # boot disk
    for hdd in ret.diskList:
        with open(hdd, "rb") as f:
            if not util.isBufferAllZero(f.read(440)):
                if ret._bootHdd is not None:
                    raise StorageLayoutParseError(StorageLayoutBiosLvm.name, "boot-code exists on multiple harddisks")
                ret._bootHdd = hdd
    if ret._bootHdd is None:
        raise StorageLayoutParseError(StorageLayoutBiosLvm.name, "no harddisk has boot-code")

    return ret


_vgName = "hdd"
_rootLvName = "root"
_rootLvDevPath = "/dev/mapper/hdd.root"
_swapLvName = "swap"
_swapLvDevPath = "/dev/mapper/hdd.swap"
