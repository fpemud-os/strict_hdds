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


class StorageLayoutImpl(StorageLayout):
    """Layout:
           /dev/sda                 SSD, GPT (cache-disk)
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
           5. cache-disk can have no swap partition, /dev/sda2 would be the cache device then
           6. extra LVM-LV is allowed to exist
           7. extra harddisk is allowed to exist
    """

    def __init__(self):
        self._ssd = None
        self._ssdEspParti = None
        self._ssdSwapParti = None
        self._ssdCacheParti = None
        self._hddDict = dict()           # dict<hddDev,bcacheDev>
        self._bootHdd = None             # boot harddisk name, must be None if ssd exists

    @property
    def boot_mode(self):
        return StorageLayout.BOOT_MODE_EFI

    @property
    def dev_rootfs(self):
        return util.rootLvDevPath

    @property
    def dev_swap(self):
        return self._ssdSwapParti

    def get_boot_disk(self):
        return self._ssd if self._ssd is not None else self._bootHdd

    def check_swap_size(self):
        assert self._ssdSwapParti is not None
        return util.getBlkDevSize(self._ssdSwapParti) >= util.getSwapSize()

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

    def get_ssd(self):
        return self._ssd

    def get_ssd_esp_partition(self):
        assert self._ssd is not None
        return self._ssdEspParti

    def get_ssd_swap_partition(self):
        assert self._ssd is not None
        return self._ssdSwapParti

    def get_ssd_cache_partition(self):
        assert self._ssd is not None
        return self._ssdCacheParti

    def get_disk_list(self):
        if self._ssd is not None:
            return [self._ssd] + list(self._hddDict.keys())
        else:
            return list(self._hddDict.keys())

    def add_disk(self, devpath):
        assert devpath is not None
        assert devpath not in self.get_disk_list()

        if devpath not in util.getDevPathListForFixedHdd():
            raise StorageLayoutAddDiskError(devpath, "not a harddisk")

        if util.isBlkDevSsdOrHdd(devpath):
            assert self._ssd is None
            return self._addSsdEfiBcacheLvm(devpath)
        else:
            return self._addHddEfiBcacheLvm(devpath)

    def release_disk(self, devpath):
        assert devpath is not None
        assert devpath in self.get_disk_list()

        if devpath == self._ssd:
            return

        parti = util.devPathDiskToPartition(devpath, 2)
        bcacheDev = util.bcacheFindByBackingDevice(parti)
        rc, out = util.cmdCallWithRetCode("/sbin/lvm", "pvmove", bcacheDev)
        if rc != 5:
            raise StorageLayoutReleaseDiskError("failed")

    def remove_disk(self, devpath):
        assert devpath is not None
        assert devpath in self.get_disk_list()

        if devpath == self._ssd:
            return self._removeSsdEfiBcacheLvm()
        else:
            return self._removeHddEfiBcacheLvm(devpath)

    def _addSsdEfiBcacheLvm(self, devpath):
        # create partitions
        util.initializeDisk(devpath, "gpt", [
            ("%dMiB" % (util.getEspSizeInMb()), "esp"),
            (self.swapPartiSizeStr, util.fsTypeSwap),
            ("*", "bcache"),
        ])
        self._ssd = devpath

        # sync partition1 as boot partition
        parti = util.devPathDiskToPartition(devpath, 1)
        util.cmdCall("/usr/sbin/mkfs.vfat", parti)
        util.syncBlkDev(util.devPathDiskToPartition(self._bootHdd, 1), parti, mountPoint1=util.bootDir)
        self._ssdEspParti = parti

        # make partition2 as swap partition
        parti = util.devPathDiskToPartition(devpath, 2)
        util.cmdCall("/sbin/mkswap", parti)
        self._ssdSwapParti = parti

        # make partition3 as cache partition
        parti = util.devPathDiskToPartition(devpath, 3)
        util.bcacheMakeDevice(parti, False)
        self._ssdCacheParti = parti

        # enable cache partition
        with open("/sys/fs/bcache/register", "w") as f:
            f.write(parti)
        setUuid = util.bcacheGetSetUuid(self._ssdCacheParti)
        for bcacheDev in self._hddDict.values():
            with open("/sys/block/%s/bcache/attach" % (os.path.basename(bcacheDev)), "w") as f:
                f.write(str(setUuid))

        # change boot device
        util.cmdCall("/bin/umount", util.bootDir)
        util.gptToggleEspPartition(util.devPathDiskToPartition(self._bootHdd, 1), False)
        util.cmdCall("/bin/mount", self._ssdEspParti, util.bootDir, "-o", "ro")
        self._bootHdd = None

        return True

    def _addHddEfiBcacheLvm(self, devpath):
        if devpath == self._ssd or devpath in self._hddDict:
            raise Exception("the specified device is already managed")
        if devpath not in util.getDevPathListForFixedHdd():
            raise Exception("the specified device is not a fixed harddisk")

        if util.isBlkDevSsdOrHdd(devpath):
            print("WARNING: \"%s\" is an SSD harddisk, perhaps you want to add it as mainboot device?" % (devpath))

        # create partitions
        util.initializeDisk(devpath, "gpt", [
            ("%dMiB" % (util.getEspSizeInMb()), util.fsTypeFat),
            ("*", "bcache"),
        ])

        # fill partition1
        parti = util.devPathDiskToPartition(devpath, 1)
        util.cmdCall("/usr/sbin/mkfs.vfat", parti)
        if self._ssd is not None:
            util.syncBlkDev(self._ssdEspParti, parti, mountPoint1=util.bootDir)
        else:
            util.syncBlkDev(util.devPathDiskToPartition(self._bootHdd, 1), parti, mountPoint1=util.bootDir)

        # add partition2 to bcache
        parti = util.devPathDiskToPartition(devpath, 2)
        util.bcacheMakeDevice(parti, True)
        with open("/sys/fs/bcache/register", "w") as f:
            f.write(parti)
        bcacheDev = util.bcacheFindByBackingDevice(parti)
        if self._ssd is not None:
            setUuid = util.bcacheGetSetUuid(self._ssdCacheParti)
            with open("/sys/block/%s/bcache/attach" % os.path.basename(bcacheDev), "w") as f:
                f.write(str(setUuid))

        # create lvm physical volume on bcache device and add it to volume group
        util.cmdCall("/sbin/lvm", "pvcreate", bcacheDev)
        util.cmdCall("/sbin/lvm", "vgextend", util.vgName, bcacheDev)
        self._hddDict[devpath] = bcacheDev

        return False

    def _removeSsdEfiBcacheLvm(self):
        assert self._ssd is not None
        assert len(self._hddDict) > 0

        # check
        if util.systemdFindSwapService(self._ssdSwapParti) is not None:
            raise Exception("swap partition is in use, please use \"sysman disable-swap\" first")

        # remove cache partition
        setUuid = util.bcacheGetSetUuid(self._ssdCacheParti)
        with open("/sys/fs/bcache/%s/unregister" % (setUuid), "w") as f:
            f.write(self._ssdCacheParti)
        self._ssdCacheParti = None

        # remove swap partition
        self._ssdSwapParti = None

        # change boot device
        util.cmdCall("/bin/umount", util.bootDir)
        self._bootHdd = list(self._hddDict.keys())[0]
        util.gptToggleEspPartition(util.devPathDiskToPartition(self._bootHdd, 1), True)
        util.cmdCall("/bin/mount", util.devPathDiskToPartition(self._bootHdd, 1), util.bootDir, "-o", "ro")
        self._ssdEspParti = None

        # wipe disk
        util.wipeHarddisk(self._ssd)
        self._ssd = None

        return True

    def _removeHddEfiBcacheLvm(self, devpath):
        assert devpath in self._hddDict

        if len(self._hddDict) <= 1:
            raise Exception("can not remove the last physical volume")

        # change boot device if needed
        ret = False
        if self._bootHdd is not None and self._bootHdd == devpath:
            util.cmdCall("/bin/umount", util.bootDir)
            del self._hddDict[devpath]
            self._bootHdd = list(self._hddDict.keys())[0]
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
        if self._ssd is not None:
            return self._ssdEspParti
        else:
            return util.devPathDiskToPartition(self._bootHdd, 1)

    def _getOtherEspList(self):
        ret = []
        for hdd in self._hddDict:
            if self._bootHdd is None or hdd != self._bootHdd:
                ret.append(util.devPathDiskToPartition(hdd, 1))
        return ret


def create_layout(ssd=None, hdd_list=None, create_swap=True, dry_run=False):
    if ssd is None and hdd_list is None:
        ssdList = []
        for devpath in util.getDevPathListForFixedHdd():
            if util.isBlkDevSsdOrHdd(devpath):
                ssdList.append(devpath)
            else:
                hdd_list.append(devpath)
        if len(ssdList) == 0:
            pass
        elif len(ssdList) == 1:
            ssd = ssdList[0]
        else:
            raise StorageLayoutCreateError("multiple SSD harddisks")
        if len(hdd_list) == 0:
            raise StorageLayoutCreateError("no HDD harddisk")
    else:
        assert hdd_list is not None and len(hdd_list) > 0

    ret = StorageLayoutImpl()
    if ssd is not None:
        ret._ssd = ssd
        ret._ssdEspParti = util.devPathDiskToPartition(ssd, 1)
        if create_swap:
            ret._ssdSwapParti = util.devPathDiskToPartition(ssd, 2)
            ret._ssdCacheParti = util.devPathDiskToPartition(ssd, 3)
        else:
            ret._ssdCacheParti = util.devPathDiskToPartition(ssd, 2)
    else:
        ret._bootHdd = hdd_list[0]
    for i in range(0, len(hdd_list)):
        ret._hddDict[hdd_list[i]] = "/dev/bcache%d" % (i)       # would be overwrited if not dry-run

    if not dry_run:
        setUuid = None

        if ssd is not None:
            # create partitions
            util.initializeDisk(ssd, "gpt", [
                ("%dMiB" % (util.getEspSizeInMb()), "esp"),
                ("%dGiB" % (util.getSwapSizeInGb()), util.fsTypeSwap),
                ("*", "bcache"),
            ])

            # esp partition
            util.cmdCall("/usr/sbin/mkfs.vfat", ret._ssdEspParti)

            # swap partition
            if ret._ssdSwapParti is not None:
                util.cmdCall("/sbin/mkswap", ret._ssdSwapParti)

            # cache partition
            util.bcacheMakeDevice(ret._ssdCacheParti, False)
            with open("/sys/fs/bcache/register", "w") as f:
                f.write(ret._ssdCacheParti)
            setUuid = util.bcacheGetSetUuid(ret._ssdCacheParti)

        for devpath in ret._hddDict:
            # create partitions
            util.initializeDisk(devpath, "gpt", [
                ("%dMiB" % (util.getEspSizeInMb()), util.fsTypeFat),
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
            if not util.cmdCallTestSuccess("/sbin/lvm", "vgdisplay", util.vgName):
                util.cmdCall("/sbin/lvm", "vgcreate", util.vgName, bcacheDev)
            else:
                util.cmdCall("/sbin/lvm", "vgextend", util.vgName, bcacheDev)

            # record to return value
            ret._hddDict[devpath] = bcacheDev

        # create root lv
        out = util.cmdCall("/sbin/lvm", "vgdisplay", "-c", util.vgName)
        freePe = int(out.split(":")[15])
        util.cmdCall("/sbin/lvm", "lvcreate", "-l", "%d" % (freePe // 2), "-n", util.rootLvName, util.vgName)

    return ret


def parse_layout(bootDev, rootDev):
    ret = StorageLayoutImpl()

    if not util.gptIsEspPartition(bootDev):
        raise StorageLayoutParseError(ret.name, "boot device is not an ESP partitiion")

    # vg
    if not util.cmdCallTestSuccess("/sbin/lvm", "vgdisplay", util.vgName):
        raise StorageLayoutParseError(ret.name, "volume group \"%s\" does not exist" % (util.vgName))

    # pv list
    out = util.cmdCall("/sbin/lvm", "pvdisplay", "-c")
    for m in re.finditer("(/dev/\\S+):%s:.*" % (util.vgName), out, re.M):
        if re.fullmatch("/dev/bcache[0-9]+", m.group(1)) is None:
            raise StorageLayoutParseError(ret.name, "volume group \"%s\" has non-bcache physical volume" % (util.vgName))
        bcacheDev = m.group(1)
        tlist = util.bcacheGetSlaveDevPathList(bcacheDev)
        hddDev, partId = util.devPathPartitionToDiskAndPartitionId(tlist[-1])
        if partId != 2:
            raise StorageLayoutParseError(ret.name, "physical volume partition of %s is not %s" % (hddDev, util.devPathDiskToPartition(hddDev, 2)))
        if os.path.exists(util.devPathDiskToPartition(hddDev, 3)):
            raise StorageLayoutParseError(ret.name, "redundant partition exists on %s" % (hddDev))
        ret._hddDict[hddDev] = bcacheDev

    # root lv
    out = util.cmdCall("/sbin/lvm", "lvdisplay", "-c")
    if re.search("/dev/hdd/root:%s:.*" % (util.vgName), out, re.M) is not None:
        fs = util.getBlkDevFsType(util.rootLvDevPath)
        if fs != util.fsTypeExt4:
            raise StorageLayoutParseError(ret.name, "root partition file system is \"%s\", not \"ext4\"" % (fs))
    else:
        raise StorageLayoutParseError(ret.name, "logical volume \"%s\" does not exist" % (util.rootLvDevPath))

    # ssd
    ret._ssd = util.devPathPartitionToDisk(bootDev)
    if ret._ssd not in ret._hddDict:
        ret._ssdEspParti = util.devPathDiskToPartition(ret._ssd, 1)
        if os.path.exists(util.devPathDiskToPartition(ret._ssd, 3)):
            ret._ssdSwapParti = util.devPathDiskToPartition(ret._ssd, 2)
            ret._ssdCacheParti = util.devPathDiskToPartition(ret._ssd, 3)
            if os.path.exists(util.devPathDiskToPartition(ret._ssd, 4)):
                raise StorageLayoutParseError(ret.name, "redundant partition exists on %s" % (ret._ssd))
        else:
            ret._ssdCacheParti = util.devPathDiskToPartition(ret._ssd, 2)

        # ret._ssdEspParti
        if ret._ssdEspParti != bootDev:
            raise StorageLayoutParseError(ret.name, "SSD is not boot device")
        if util.getBlkDevSize(ret._ssdEspParti) != util.getEspSize():
            raise StorageLayoutParseError(ret.name, "%s has an invalid size" % (ret._ssdEspParti))

        # ret._ssdSwapParti
        if ret._ssdSwapParti is not None:
            if not os.path.exists(ret._ssdSwapParti):
                raise StorageLayoutParseError(ret.name, "SSD has no swap partition")
            if util.getBlkDevFsType(ret._ssdSwapParti) != util.fsTypeSwap:
                raise StorageLayoutParseError(ret.name, "swap device %s has an invalid file system" % (ret._ssdSwapParti))

        # ret._ssdCacheParti
        if not os.path.exists(ret._ssdCacheParti):
            raise StorageLayoutParseError(ret.name, "SSD has no cache partition")

        for pvHdd, bcacheDev in ret._hddDict.items():
            tlist = util.bcacheGetSlaveDevPathList(bcacheDev)
            if len(tlist) < 2:
                raise StorageLayoutParseError(ret.name, "%s(%s) has no cache device" % (pvHdd, bcacheDev))
            if len(tlist) > 2:
                raise StorageLayoutParseError(ret.name, "%s(%s) has multiple cache devices" % (pvHdd, bcacheDev))
            if tlist[0] != ret._ssdCacheParti:
                raise StorageLayoutParseError(ret.name, "%s(%s) has invalid cache device" % (pvHdd, bcacheDev))
        if True:
            partName, partId = util.devPathPartitionToDiskAndPartitionId(ret._ssdCacheParti)
            nextPartName = util.devPathDiskToPartition(partName, partId + 1)
            if os.path.exists(nextPartName):
                raise StorageLayoutParseError(ret.name, "redundant partition exists on %s" % (ret._ssd))
    else:
        ret._ssd = None

    # boot harddisk
    if ret._ssd is None:
        ret._bootHdd = util.devPathPartitionToDisk(bootDev)

    return ret
