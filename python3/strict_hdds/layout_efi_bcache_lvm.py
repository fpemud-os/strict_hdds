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
from . import util
from . import StorageLayout
from . import StorageLayoutReleaseDiskError
from . import StorageLayoutParseError


class StorageLayoutEfiBcacheLvm(StorageLayout):
    """Layout:
           /dev/sda                 SSD, GPT, EFI-GRUB (cache-disk)
               /dev/sda1            ESP partition
               /dev/sda2            swap device
               /dev/sda3            bcache cache device
           /dev/sdb                 Non-SSD, GPT
               /dev/sdb1            reserved ESP partition
               /dev/sdb2            bcache backing device
           /dev/sdc                 Non-SSD, GPT
               /dev/sdc1            reserved ESP partition
               /dev/sdc2            bcache backing device
           /dev/bcache0             corresponds to /dev/sdb2, LVM-PV for VG hdd
           /dev/bcache1             corresponds to /dev/sdc2, LVM-PV for VG hdd
           /dev/mapper/hdd.root     root device, EXT4
       Description:
           1. /dev/sda1 and /dev/sd{b,c}1 must has the same size
           2. /dev/sda1, /dev/sda2 and /dev/sda3 is order-sensitive, no extra partition is allowed
           3. /dev/sd{b,c}1 and /dev/sd{b,c}2 is order-sensitive, no extra partition is allowed
           4. cache-disk is optional, and only one cache-disk is allowed at most
           5. cache-disk must have a swap partition
           6. extra LVM-LV is allowed to exist
           7. extra harddisk is allowed to exist
    """

    name = "efi-bcache-lvm"

    def __init__(self):
        self.hddDict = dict()           # dict<hddDev,bcacheDev>
        self.ssd = None
        self.ssdEspParti = None
        self.ssdSwapParti = None
        self.ssdCacheParti = None

        self._bRootLv = None
        self._bootHdd = None

    @property
    def boot_mode(self):
        return StorageLayout.BOOT_MODE_EFI

    def get_rootdev(self):
        return util.rootLvDevPath

    def get_swap(self):
        return self.ssdSwapParti

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

    def add_disk(self, devpath):
        assert devpath is not None
        assert devpath != self.ssd
        assert devpath not in self.hddDict

        # FIXME: only one ssd is allowed, and sdd must be main-disk
        if False:
            return self._addSsdEfiBcacheLvm(devpath)
        else:
            return self._addHddEfiBcacheLvm(devpath)

    def release_disk(self, devpath):
        assert devpath is not None
        assert devpath in [self.ssd] + self.hddDict

        if devpath == self.ssd:
            return

        parti = util.devPathDiskToPartition(devpath, 2)
        bcacheDev = util.bcacheFindByBackingDevice(parti)
        rc, out = util.cmdCallWithRetCode("/sbin/lvm", "pvmove", bcacheDev)
        if rc != 5:
            raise StorageLayoutReleaseDiskError("failed")

    def remove_disk(self, devpath):
        assert devpath is not None
        assert self.is_ready()

        if devpath == self.ssd:
            return self._removeSsdEfiBcacheLvm()
        else:
            return self._removeHddEfiBcacheLvm(devpath)

    def _addSsdEfiBcacheLvm(self, devpath):
        if self.ssd is not None:
            raise Exception("mainboot device already exists")
        if devpath in self.hddDict:
            raise Exception("the specified device is already managed")
        if devpath not in util.getDevPathListForFixedHdd() or not util.isBlkDevSsdOrHdd(devpath):
            raise Exception("the specified device is not a fixed SSD harddisk")

        # create partitions
        util.initializeDisk(devpath, "gpt", [
            ("%dMiB" % (util.getEspSizeInMb()), "esp"),
            (self.swapPartiSizeStr, "swap"),
            ("*", "bcache"),
        ])
        self.ssd = devpath

        # sync partition1 as boot partition
        parti = util.devPathDiskToPartition(devpath, 1)
        util.cmdCall("/usr/sbin/mkfs.vfat", parti)
        util.syncBlkDev(util.devPathDiskToPartition(self._bootHdd, 1), parti, mountPoint1=util.bootDir)
        self.ssdEspParti = parti

        # make partition2 as swap partition
        parti = util.devPathDiskToPartition(devpath, 2)
        util.cmdCall("/sbin/mkswap", parti)
        self.ssdSwapParti = parti

        # make partition3 as cache partition
        parti = util.devPathDiskToPartition(devpath, 3)
        util.bcacheMakeDevice(parti, False)
        self.ssdCacheParti = parti

        # enable cache partition
        with open("/sys/fs/bcache/register", "w") as f:
            f.write(parti)
        setUuid = util.bcacheGetSetUuid(self.ssdCacheParti)
        for bcacheDev in self.hddDict.values():
            with open("/sys/block/%s/bcache/attach" % (os.path.basename(bcacheDev)), "w") as f:
                f.write(str(setUuid))

        # change boot device
        util.cmdCall("/bin/umount", util.bootDir)
        util.gptToggleEspPartition(util.devPathDiskToPartition(self._bootHdd, 1), False)
        util.cmdCall("/bin/mount", self.ssdEspParti, util.bootDir, "-o", "ro")
        self._bootHdd = None

        return True

    def _addHddEfiBcacheLvm(self, devpath):
        if devpath == self.ssd or devpath in self.hddDict:
            raise Exception("the specified device is already managed")
        if devpath not in util.getDevPathListForFixedHdd():
            raise Exception("the specified device is not a fixed harddisk")

        if util.isBlkDevSsdOrHdd(devpath):
            print("WARNING: \"%s\" is an SSD harddisk, perhaps you want to add it as mainboot device?" % (devpath))

        # create partitions
        util.initializeDisk(devpath, "gpt", [
            ("%dMiB" % (util.getEspSizeInMb()), "vfat"),
            ("*", "bcache"),
        ])

        # fill partition1
        parti = util.devPathDiskToPartition(devpath, 1)
        util.cmdCall("/usr/sbin/mkfs.vfat", parti)
        if self.ssd is not None:
            util.syncBlkDev(self.ssdEspParti, parti, mountPoint1=util.bootDir)
        else:
            util.syncBlkDev(util.devPathDiskToPartition(self._bootHdd, 1), parti, mountPoint1=util.bootDir)

        # add partition2 to bcache
        parti = util.devPathDiskToPartition(devpath, 2)
        util.bcacheMakeDevice(parti, True)
        with open("/sys/fs/bcache/register", "w") as f:
            f.write(parti)
        bcacheDev = util.bcacheFindByBackingDevice(parti)
        if self.ssd is not None:
            setUuid = util.bcacheGetSetUuid(self.ssdCacheParti)
            with open("/sys/block/%s/bcache/attach" % os.path.basename(bcacheDev), "w") as f:
                f.write(str(setUuid))

        # create lvm physical volume on bcache device and add it to volume group
        util.cmdCall("/sbin/lvm", "pvcreate", bcacheDev)
        util.cmdCall("/sbin/lvm", "vgextend", util.vgName, bcacheDev)
        self.hddDict[devpath] = bcacheDev

        return False

    def _removeSsdEfiBcacheLvm(self):
        assert self.ssd is not None
        assert len(self.hddDict) > 0

        # check
        if util.systemdFindSwapService(self.ssdSwapParti) is not None:
            raise Exception("swap partition is in use, please use \"sysman disable-swap\" first")

        # remove cache partition
        setUuid = util.bcacheGetSetUuid(self.ssdCacheParti)
        with open("/sys/fs/bcache/%s/unregister" % (setUuid), "w") as f:
            f.write(self.ssdCacheParti)
        self.ssdCacheParti = None

        # remove swap partition
        self.ssdSwapParti = None

        # change boot device
        util.cmdCall("/bin/umount", util.bootDir)
        self._bootHdd = list(self.hddDict.keys())[0]
        util.gptToggleEspPartition(util.devPathDiskToPartition(self._bootHdd, 1), True)
        util.cmdCall("/bin/mount", util.devPathDiskToPartition(self._bootHdd, 1), util.bootDir, "-o", "ro")
        self.ssdEspParti = None

        # wipe disk
        util.wipeHarddisk(self.ssd)
        self.ssd = None

        return True

    def _removeHddEfiBcacheLvm(self, devpath):
        assert devpath in self.hddDict

        if len(self.hddDict) <= 1:
            raise Exception("can not remove the last physical volume")

        # change boot device if needed
        ret = False
        if self._bootHdd is not None and self._bootHdd == devpath:
            util.cmdCall("/bin/umount", util.bootDir)
            del self.hddDict[devpath]
            self._bootHdd = list(self.hddDict.keys())[0]
            util.gptToggleEspPartition(util.devPathDiskToPartition(self._bootHdd, 1), True)
            util.cmdCall("/bin/mount", util.devPathDiskToPartition(self._bootHdd, 1), util.bootDir, "-o", "ro")
            ret = True

        # remove harddisk
        bcacheDev = util.bcacheFindByBackingDevice(util.devPathDiskToPartition(devpath, 2))
        util.cmdCall("/sbin/lvm", "vgreduce", util.vgName, bcacheDev)
        with open("/sys/block/%s/bcache/stop" % (os.path.basename(bcacheDev)), "w") as f:
            f.write("1")
        util.wipeHarddisk(devpath)

        return ret

    def _getCurEsp(self):
        if self.ssd is not None:
            return self.ssdEspParti
        else:
            return util.devPathDiskToPartition(self._bootHdd, 1)

    def _getOtherEspList(self):
        ret = []
        for hdd in self.hddDict:
            if self._bootHdd is None or hdd != self._bootHdd:
                ret.append(util.devPathDiskToPartition(hdd, 1))
        return ret


def create_layout(ssd=None, hddList=None):
    if ssd is None and hddList is None:
        ssdList = []
        for devpath in util.getDevPathListForFixedHdd():
            if util.isBlkDevSsdOrHdd(devpath):
                ssdList.append(devpath)
            else:
                hddList.append(devpath)
        if len(ssdList) == 0:
            pass
        elif len(ssdList) == 1:
            ssd = ssdList[0]
        else:
            raise Exception("multiple SSD harddisks")
        if len(hddList) == 0:
            raise Exception("no HDD harddisks")
    else:
        assert hddList is not None and len(hddList) > 0

    setUuid = None
    vgCreated = False

    if ssd is not None:
        # create partitions
        util.initializeDisk(ssd, "gpt", [
            ("%dMiB" % (util.getEspSizeInMb()), "esp"),
            ("%dGiB" % (util.getSwapSizeInGb()), "swap"),
            ("*", "bcache"),
        ])

        # sync partition1 as boot partition
        parti = util.devPathDiskToPartition(ssd, 1)
        util.cmdCall("/usr/sbin/mkfs.vfat", parti)

        # make partition2 as swap partition
        parti = util.devPathDiskToPartition(ssd, 2)
        util.cmdCall("/sbin/mkswap", parti)

        # make partition3 as cache partition
        parti = util.devPathDiskToPartition(ssd, 3)
        util.bcacheMakeDevice(parti, False)
        with open("/sys/fs/bcache/register", "w") as f:
            f.write(parti)
        setUuid = util.bcacheGetSetUuid(parti)

    for devpath in hddList:
        # create partitions
        util.initializeDisk(devpath, "gpt", [
            ("%dMiB" % (util.getEspSizeInMb()), "vfat"),
            ("*", "bcache"),
        ])

        # fill partition1
        parti = util.devPathDiskToPartition(devpath, 1)
        util.cmdCall("/usr/sbin/mkfs.vfat", parti)

        # add partition2 to bcache
        parti = util.devPathDiskToPartition(devpath, 2)
        util.bcacheMakeDevice(parti, True)
        with open("/sys/fs/bcache/register", "w") as f:
            f.write(parti)
        bcacheDev = util.bcacheFindByBackingDevice(parti)
        if ssd is not None:
            with open("/sys/block/%s/bcache/attach" % (os.path.basename(bcacheDev)), "w") as f:
                f.write(str(setUuid))

        # create lvm physical volume on bcache device and add it to volume group
        util.cmdCall("/sbin/lvm", "pvcreate", bcacheDev)
        if not vgCreated:
            util.cmdCall("/sbin/lvm", "vgcreate", "hdd", bcacheDev)
            vgCreated = True
        else:
            util.cmdCall("/sbin/lvm", "vgextend", "hdd", bcacheDev)

    # create root lv
    out = util.cmdCall("/sbin/lvm", "vgdisplay", "-c", "hdd")
    freePe = int(out.split(":")[15])
    util.cmdCall("/sbin/lvm", "lvcreate", "-l", "%d" % (freePe // 2), "-n", "root", "hdd")


def parse_layout(bootDev):
    ret = StorageLayoutEfiBcacheLvm()

    if not util.gptIsEspPartition(bootDev):
        raise StorageLayoutParseError(StorageLayoutEfiBcacheLvm.name, "boot device is not ESP partitiion")

    # ret.lvmVg
    if not util.cmdCallTestSuccess("/sbin/lvm", "vgdisplay", "hdd"):
        raise StorageLayoutParseError(StorageLayoutEfiBcacheLvm.name, "volume group \"hdd\" does not exist")
    ret.lvmVg = "hdd"

    # ret.lvmPvHddDict
    out = util.cmdCall("/sbin/lvm", "pvdisplay", "-c")
    for m in re.finditer("(/dev/\\S+):hdd:.*", out, re.M):
        if re.fullmatch("/dev/bcache[0-9]+", m.group(1)) is None:
            raise StorageLayoutParseError(StorageLayoutEfiBcacheLvm.name, "volume group \"hdd\" has non-bcache physical volume")
        bcacheDev = m.group(1)
        tlist = util.bcacheGetSlaveDevPathList(bcacheDev)
        hddDev, partId = util.devPathPartitionToDiskAndPartitionId(tlist[-1])
        if partId != 2:
            raise StorageLayoutParseError(StorageLayoutEfiBcacheLvm.name, "physical volume partition of %s is not %s" % (hddDev, util.devPathDiskToPartition(hddDev, 2)))
        if os.path.exists(util.devPathDiskToPartition(hddDev, 3)):
            raise StorageLayoutParseError(StorageLayoutEfiBcacheLvm.name, "redundant partition exists on %s" % (hddDev))
        ret.lvmPvHddDict[hddDev] = bcacheDev

    # ret.lvmRootLv
    out = util.cmdCall("/sbin/lvm", "lvdisplay", "-c")
    if re.search("/dev/hdd/root:hdd:.*", out, re.M) is not None:
        ret.lvmRootLv = "root"
        if os.path.exists(util.rootLvDevPath):
            fs = util.getBlkDevFsType(util.rootLvDevPath)
        elif os.path.exists("/dev/mapper/hdd-root"):                    # compatible with old lvm version
            fs = util.getBlkDevFsType("/dev/mapper/hdd-root")
        else:
            assert False
        if fs != "ext4":
            raise StorageLayoutParseError(StorageLayoutEfiBcacheLvm.name, "root partition file system is \"%s\", not \"ext4\"" % (fs))
    else:
        raise StorageLayoutParseError(StorageLayoutEfiBcacheLvm.name, "logical volume \"/dev/mapper/hdd.root\" does not exist")

    # ret.ssd
    ret.ssd = util.devPathPartitionToDisk(bootDev)
    if ret.ssd not in ret.lvmPvHddDict:
        # ret.ssdEspParti
        ret.ssdEspParti = util.devPathDiskToPartition(ret.ssd, 1)
        if ret.ssdEspParti != bootDev:
            raise StorageLayoutParseError(StorageLayoutEfiBcacheLvm.name, "SSD is not boot device")
        if util.getBlkDevSize(ret.ssdEspParti) != util.getEspSizeInMb() * 1024 * 1024:
            raise StorageLayoutParseError(StorageLayoutEfiBcacheLvm.name, "%s has an invalid size" % (ret.ssdEspParti))

        # ret.ssdSwapParti
        ret._bSsdSwapParti = util.devPathDiskToPartition(ret.ssd, 2)
        if not os.path.exists(ret._bSsdSwapParti):
            raise StorageLayoutParseError(StorageLayoutEfiBcacheLvm.name, "SSD has no swap partition")
        if util.getBlkDevFsType(ret._bSsdSwapParti) != "swap":
            raise StorageLayoutParseError(StorageLayoutEfiBcacheLvm.name, "swap device %s has an invalid file system" % (ret._bSsdSwapParti))

        # ret.ssdCacheParti
        ret._bSsdCacheParti = util.devPathDiskToPartition(ret.ssd, 3)
        if not os.path.exists(ret._bSsdCacheParti):
            raise StorageLayoutParseError(StorageLayoutEfiBcacheLvm.name, "SSD has no cache partition")

        for pvHdd, bcacheDev in ret.lvmPvHddDict.items():
            tlist = util.bcacheGetSlaveDevPathList(bcacheDev)
            if len(tlist) < 2:
                raise StorageLayoutParseError(StorageLayoutEfiBcacheLvm.name, "%s(%s) has no cache device" % (pvHdd, bcacheDev))
            if len(tlist) > 2:
                raise StorageLayoutParseError(StorageLayoutEfiBcacheLvm.name, "%s(%s) has multiple cache devices" % (pvHdd, bcacheDev))
            if tlist[0] != ret._bSsdCacheParti:
                raise StorageLayoutParseError(StorageLayoutEfiBcacheLvm.name, "%s(%s) has invalid cache device" % (pvHdd, bcacheDev))
        if True:
            partName, partId = util.devPathPartitionToDiskAndPartitionId(ret._bSsdCacheParti)
            nextPartName = util.devPathDiskToPartition(partName, partId + 1)
            if os.path.exists(nextPartName):
                raise StorageLayoutParseError(StorageLayoutEfiBcacheLvm.name, "redundant partition exists on %s" % (ret.ssd))
    else:
        ret.ssd = None

    # ret.bootHdd
    if ret.ssd is None:
        ret.bootHdd = util.devPathPartitionToDisk(bootDev)

    return ret