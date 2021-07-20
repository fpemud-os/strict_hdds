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


from . import util
from . import StorageLayout
from . import StorageLayoutAddDiskError
from . import StorageLayoutReleaseDiskError


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

        self._bVg = False
        self._bRootLv = False
        self._bSwapLv = None
        self._bootHdd = None

    @property
    def boot_mode(self):
        return StorageLayout.BOOT_MODE_BIOS

    def is_ready(self):
        assert len(self.diskList) > 0
        assert self._bVg
        assert self._bRootLv
        assert self._bSwapLv is not None
        assert self._bootHdd in self.diskList
        return True

    def get_rootdev(self):
        assert self.is_ready()
        return "/dev/mapper/hdd.root"

    def get_swap(self):
        assert self.is_ready()
        return "/dev/mapper/hdd.swap" if self._bSwapLv else None

    def check_swap_size(self):
        assert self.is_ready() and self._bSwapLv
        return util.getBlkDevSize("/dev/mapper/hdd.swap") >= util.getSwapSizeInGb() * 1024 * 1024 * 1024

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
        util.cmdCall("/sbin/lvm", "vgreduce", "hdd", parti)
        util.wipeHarddisk(devpath)

        return ret

    def create_swap_lv(self):
        assert self.is_ready() and not self._bSwapLv
        util.cmdCall("/sbin/lvm", "lvcreate", "-L", "%dGiB" % (util.getSwapSizeInGb()), "-n", "swap", "hdd")
        self._bSwapLv = True

    def remove_swap_lv(self):
        assert self.is_ready() and self._bSwapLv
        util.cmdCall("/sbin/lvm", "lvremove", "/dev/mapper/hdd.swap")
        self._bSwapLv = False


def create_layout(hddList=None):
    if hddList is None:
        hddList = util.getDevPathListForFixedHdd()
        if len(hddList) == 0:
            raise Exception("no harddisks")
    else:
        assert len(hddList) > 0

    vgCreated = False

    for devpath in hddList:
        # create partitions
        util.initializeDisk(devpath, "mbr", [
            ("*", "lvm"),
        ])

        # create lvm physical volume on partition1 and add it to volume group
        parti = util.devPathDiskToPartition(devpath, 1)
        util.cmdCall("/sbin/lvm", "pvcreate", parti)
        if not vgCreated:
            util.cmdCall("/sbin/lvm", "vgcreate", "hdd", parti)
            vgCreated = True
        else:
            util.cmdCall("/sbin/lvm", "vgextend", "hdd", parti)

    # create root lv
    out = util.cmdCall("/sbin/lvm", "vgdisplay", "-c", "hdd")
    freePe = int(out.split(":")[15])
    util.cmdCall("/sbin/lvm", "lvcreate", "-l", "%d" % (freePe // 2), "-n", "root", "hdd")


def parse_layout():
    ret = StorageLayoutBiosLvm()

    # ret.lvmVg
    if not util.cmdCallTestSuccess("/sbin/lvm", "vgdisplay", "hdd"):
        raise StorageLayoutParseError(StorageLayoutBiosLvm, "volume group \"hdd\" does not exist")
    ret.lvmVg = "hdd"

    # ret.lvmPvHddList
    out = util.cmdCall("/sbin/lvm", "pvdisplay", "-c")
    for m in re.finditer("(/dev/\\S+):hdd:.*", out, re.M):
        hdd = util.devPathPartitionToDisk(m.group(1))
        if util.getBlkDevPartitionTableType(hdd) != "dos":
            raise StorageLayoutParseError(StorageLayoutBiosLvm, "partition type of %s is not \"dos\"" % (hdd))
        if os.path.exists(util.devPathDiskToPartition(hdd, 2)):
            raise StorageLayoutParseError(StorageLayoutBiosLvm, "redundant partition exists on %s" % (hdd))
        ret.lvmPvHddList.append(hdd)

    out = util.cmdCall("/sbin/lvm", "lvdisplay", "-c")
    if True:
        # ret.lvmRootLv
        if re.search("/dev/hdd/root:hdd:.*", out, re.M) is not None:
            ret.lvmRootLv = "root"
            if os.path.exists("/dev/mapper/hdd.root"):
                fs = util.getBlkDevFsType("/dev/mapper/hdd.root")
            elif os.path.exists("/dev/mapper/hdd-root"):                # compatible with old lvm version
                fs = util.getBlkDevFsType("/dev/mapper/hdd-root")
            else:
                assert False
            if fs != "ext4":
                raise StorageLayoutParseError(StorageLayoutBiosLvm, "root partition file system is \"%s\", not \"ext4\"" % (fs))
        else:
            raise StorageLayoutParseError(StorageLayoutBiosLvm, "logical volume \"/dev/mapper/hdd.root\" does not exist")

        # ret.lvmSwapLv
        if re.search("/dev/hdd/swap:hdd:.*", out, re.M) is not None:
            ret.lvmSwapLv = "swap"
            if os.path.exists("/dev/mapper/hdd.swap"):
                if util.getBlkDevFsType("/dev/mapper/hdd.swap") != "swap":
                    raise StorageLayoutParseError(StorageLayoutBiosLvm, "/dev/mapper/hdd.swap has an invalid file system")
            elif os.path.exists("/dev/mapper/hdd-swap"):                # compatible with old lvm version
                if util.getBlkDevFsType("/dev/mapper/hdd-swap") != "swap":
                    raise StorageLayoutParseError(StorageLayoutBiosLvm, "/dev/mapper/hdd.swap has an invalid file system")
            else:
                assert False

    # ret.bootHdd
    for hdd in ret.lvmPvHddList:
        with open(hdd, "rb") as f:
            if not util.isBufferAllZero(f.read(440)):
                if ret.bootHdd is not None:
                    raise StorageLayoutParseError(StorageLayoutBiosLvm, "boot-code exists on multiple harddisks")
                ret.bootHdd = hdd
    if ret.bootHdd is None:
        raise StorageLayoutParseError(StorageLayoutBiosLvm, "no harddisk has boot-code")

    return ret
