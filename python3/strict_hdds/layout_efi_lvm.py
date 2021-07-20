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
        self.diskList = []

        self._bVg = False
        self._bRootLv = False
        self._bSwapLv = None
        self._bootHdd = None

    @property
    def boot_mode(self):
        return StorageLayout.BOOT_MODE_EFI

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

    def optimize_rootdev(self):
        assert self.is_ready()
        util.autoExtendLv(self.get_rootdev())

    def get_esp(self):
        assert self.is_ready()
        return self._getCurEsp()

    def get_esp_sync_info(self):
        assert self.is_ready()
        return (self._getCurEsp(), self._getOtherEspList())

    def sync_esp(self, src, dst):
        assert src is not None and dst is not None
        assert self.is_ready()
        assert src == self._getCurEsp() and dst in self._getOtherEspList()
        util.syncBlkDev(src, dst, mountPoint1=util.bootDir)

    def add_disk(self, devpath):
        assert devpath is not None
        assert self.is_ready()
        assert devpath not in self.diskList

        if devpath not in util.getDevPathListForFixedHdd():
            raise StorageLayoutAddDiskError(devpath, "not a harddisk")

        # create partitions
        util.initializeDisk(devpath, "gpt", [
            ("%dMiB" % (util.getEspSizeInMb()), "vfat"),
            ("*", "lvm"),
        ])

        # fill partition1, mount boot device if needed
        parti = util.devPathDiskToPartition(devpath, 1)
        util.cmdCall("/usr/sbin/mkfs.vfat", parti)
        util.syncBlkDev(util.devPathDiskToPartition(self._bootHdd, 1), parti, mountPoint1=util.bootDir)

        # create lvm physical volume on partition2 and add it to volume group
        parti = util.devPathDiskToPartition(devpath, 2)
        util.cmdCall("/sbin/lvm", "pvcreate", parti)
        util.cmdCall("/sbin/lvm", "vgextend", "hdd", parti)
        self.diskList.append(devpath)

        return False

    def release_disk(self, devpath):
        assert devpath is not None
        assert self.is_ready()
        assert devpath in self.diskList and len(self.diskList) > 1

        parti = util.devPathDiskToPartition(devpath, 2)
        rc, out = util.cmdCallWithRetCode("/sbin/lvm", "pvmove", parti)
        if rc != 5:
            raise Exception("failed")
        return

    def remove_disk(self, devpath):
        assert devpath is not None
        assert self.is_ready()
        assert devpath in self.diskList and len(self.diskList) > 1

        # change boot device if needed
        ret = False
        if self._bootHdd == devpath:
            util.cmdCall("/bin/umount", util.bootDir)
            self.diskList.remove(devpath)
            self._bootHdd = self.diskList[0]
            util.gptToggleEspPartition(util.devPathDiskToPartition(self._bootHdd, 1), True)
            util.cmdCall("/bin/mount", util.devPathDiskToPartition(self._bootHdd, 1), util.bootDir, "-o", "ro")
            ret = True

        # remove harddisk
        parti = util.devPathDiskToPartition(devpath, 2)
        util.cmdCall("/sbin/lvm", "vgreduce", "hdd", parti)
        util.wipeHarddisk(devpath)

        return ret

    def create_swap_lv(self):
        assert self.is_ready() and self._bSwapLv is None
        util.cmdCall("/sbin/lvm", "lvcreate", "-L", "%dGiB" % (util.getSwapSizeInGb()), "-n", "swap", "hdd")
        self._bSwapLv = "swap"

    def remove_swap_lv(self):
        assert self.is_ready() and self._bSwapLv == "swap"
        util.cmdCall("/sbin/lvm", "lvremove", "/dev/mapper/hdd.swap")
        self._bSwapLv = None

    def _getCurEsp(self):
        return util.devPathDiskToPartition(self._bootHdd, 1)

    def _getOtherEspList(self):
        ret = []
        for hdd in self.diskList:
            if hdd != self._bootHdd:
                ret.append(util.devPathDiskToPartition(hdd, 1))
        return ret


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
        util.initializeDisk(devpath, "gpt", [
            ("%dMiB" % (util.getEspSizeInMb()), "vfat"),
            ("*", "lvm"),
        ])

        # fill partition1
        parti = util.devPathDiskToPartition(devpath, 1)
        util.cmdCall("/usr/sbin/mkfs.vfat", parti)

        # create lvm physical volume on partition2 and add it to volume group
        parti = util.devPathDiskToPartition(devpath, 2)
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


def parse_layout(bootDev):
    if not util.gptIsEspPartition(bootDev):
        raise StorageLayoutParseError(StorageLayoutEfiLvm, "boot device is not ESP partitiion")

    ret = StorageLayoutEfiLvm()

    # ret.bootHdd
    ret.bootHdd = util.devPathPartitionToDisk(bootDev)

    # ret.lvmVg
    if not util.cmdCallTestSuccess("/sbin/lvm", "vgdisplay", "hdd"):
        raise StorageLayoutParseError(StorageLayoutEfiLvm, "volume group \"hdd\" does not exist")
    ret.lvmVg = "hdd"

    # ret.lvmPvHddList
    out = util.cmdCall("/sbin/lvm", "pvdisplay", "-c")
    for m in re.finditer("(/dev/\\S+):hdd:.*", out, re.M):
        hdd, partId = util.devPathPartitionToDiskAndPartitionId(m.group(1))
        if util.getBlkDevPartitionTableType(hdd) != "gpt":
            raise StorageLayoutParseError(StorageLayoutEfiLvm, "partition type of %s is not \"gpt\"" % (hdd))
        if partId != 2:
            raise StorageLayoutParseError(StorageLayoutEfiLvm, "physical volume partition of %s is not %s" % (hdd, util.devPathDiskToPartition(hdd, 2)))
        if util.getBlkDevSize(util.devPathDiskToPartition(hdd, 1)) != util.getEspSizeInMb() * 1024 * 1024:
            raise StorageLayoutParseError(StorageLayoutEfiLvm, "%s has an invalid size" % (util.devPathDiskToPartition(hdd, 1)))
        if os.path.exists(util.devPathDiskToPartition(hdd, 3)):
            raise StorageLayoutParseError(StorageLayoutEfiLvm, "redundant partition exists on %s" % (hdd))
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
                raise StorageLayoutParseError(StorageLayoutEfiLvm, "root partition file system is \"%s\", not \"ext4\"" % (fs))
        else:
            raise StorageLayoutParseError(StorageLayoutEfiLvm, "logical volume \"/dev/mapper/hdd.root\" does not exist")

        # ret.lvmSwapLv
        if re.search("/dev/hdd/swap:hdd:.*", out, re.M) is not None:
            ret.lvmSwapLv = "swap"
            if os.path.exists("/dev/mapper/hdd.swap"):
                if util.getBlkDevFsType("/dev/mapper/hdd.swap") != "swap":
                    raise StorageLayoutParseError(StorageLayoutEfiLvm, "/dev/mapper/hdd.swap has an invalid file system")
            elif os.path.exists("/dev/mapper/hdd-swap"):                    # compatible with old lvm version
                if util.getBlkDevFsType("/dev/mapper/hdd-swap") != "swap":
                    raise StorageLayoutParseError(StorageLayoutEfiLvm, "/dev/mapper/hdd.swap has an invalid file system")
            else:
                assert False

    # ret._bSwapFile
    if os.path.exists(util.swapFilename) and util.cmdCallTestSuccess("/sbin/swaplabel", util.swapFilename):
        ret._bSwapFile = True
    else:
        ret._bSwapFile = False

    return ret