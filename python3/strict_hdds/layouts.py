#!/usr/bin/env python3

# strict_hdds.py - strict harddisks
#
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


class StorageLayoutBiosSimple(StorageLayout):
    """Layout:
           /dev/sda          MBR, BIOS-GRUB
               /dev/sda1     root device, EXT4
       Description:
           1. partition number of /dev/sda1 and /dev/sda2 is irrelevant
           2. use optional swap file /var/swap.dat
           3. extra partition is allowed to exist
    """

    name = "bios-simple"

    def __init__(self):
        self._hdd = None
        self._hddRootParti = None
        self.swapFile = None

    @property
    def boot_mode(self):
        return StorageLayout.BOOT_MODE_BIOS

    def is_ready(self):
        assert self._hdd is not None
        assert self._hddRootParti is not None
        assert self.swapFile is None or self.swapFile == _swapFilename
        return True

    def get_rootdev(self):
        assert self.is_ready()
        return self._hddRootParti

    def optimize_rootdev(self):
        assert self.is_ready()
        return      # no-op

    def get_boot_disk(self):
        assert self.is_ready()
        return self._hdd


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
        self.lvmVg = None
        self.lvmPvHddList = []
        self.lvmRootLv = None
        self.lvmSwapLv = None
        self.bootHdd = None

    @property
    def boot_mode(self):
        return StorageLayout.BOOT_MODE_BIOS

    def is_ready(self):
        assert self.lvmVg == "hdd"
        assert len(self.lvmPvHddList) > 0
        assert self.lvmRootLv == "root"
        assert self.lvmSwapLv is None or self.lvmSwapLv == "swap"
        assert self.bootHdd is not None and self.bootHdd in self.lvmPvHddList
        return True

    def get_rootdev(self):
        assert self.is_ready()

        # return "/dev/mapper/%s.%s" % (self.lvmVg, self.lvmRootLv)
        ret = "/dev/mapper/%s.%s" % (self.lvmVg, self.lvmRootLv)
        if os.path.exists(ret):
            return ret
        else:
            ret = "/dev/mapper/%s-%s" % (self.lvmVg, self.lvmRootLv)    # compatible with old lvm version
            if os.path.exists(ret):
                return ret
        assert False

    def optimize_rootdev(self):
        assert self.is_ready()
        _helperAdjust(self)

    def get_boot_disk(self):
        assert self.is_ready()
        return self.bootHdd

    def add_disk(self, devpath):
        assert devpath not in self.lvmPvHddList
        assert devpath in util.getDevPathListForFixedHdd()
        assert False

    def release_disk(self, devpath):
        assert devpath in self.lvmPvHddList and len(self.lvmPvHddList) > 1

        parti = util.devPathDiskToPartition(devpath, 1)
        rc, out = util.cmdCallWithRetCode("/sbin/lvm", "pvmove", parti)
        if rc != 5:
            raise Exception("failed")

    def remove_disk(self, devpath):
        assert devpath in self.lvmPvHddList and len(self.lvmPvHddList) > 1

        # change boot device if needed
        ret = False
        if self.bootHdd == devpath:
            self.lvmPvHddList.remove(devpath)
            self.bootHdd = self.lvmPvHddList[0]
            # FIXME: add Boot Code for self.bootHdd?
            ret = True

        # remove harddisk
        parti = util.devPathDiskToPartition(devpath, 1)
        util.cmdCall("/sbin/lvm", "vgreduce", self.lvmVg, parti)
        util.wipeHarddisk(devpath)

        return ret


class StorageLayoutEfiSimple(StorageLayout):
    """Layout:
           /dev/sda          GPT, EFI-GRUB
               /dev/sda1     ESP partition
               /dev/sda2     root device, EXT4
       Description:
           1. the 3 partition in /dev/sda is order-insensitive
           2. use optional swap file /var/swap.dat
           3. extra partition is allowed to exist
    """

    name = "efi-simple"

    def __init__(self):
        self.hdd = None
        self.hddEspParti = None
        self.hddRootParti = None
        self.swapFile = None

    @property
    def boot_mode(self):
        return StorageLayout.BOOT_MODE_EFI

    def is_ready(self):
        assert self.hdd is not None
        assert self.hddEspParti == util.devPathDiskToPartition(self.hdd, 1)
        assert self.hddRootParti == util.devPathDiskToPartition(self.hdd, 2)
        assert self.swapFile is None or self.swapFile == _swapFilename
        return True

    def get_rootdev(self):
        assert self.is_ready()
        return self.hddRootParti

    def optimize_rootdev(self):
        assert self.is_ready()
        return      # no-op

    def get_esp(self):
        assert self.is_ready()
        return self.hddEspParti


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
        self.lvmVg = None
        self.lvmPvHddList = []
        self.lvmRootLv = None
        self.lvmSwapLv = None
        self.bootHdd = None

    @property
    def boot_mode(self):
        return StorageLayout.BOOT_MODE_EFI

    def is_ready(self):
        assert self.lvmVg == "hdd"
        assert len(self.lvmPvHddList) > 0
        assert self.lvmRootLv == "root"
        assert self.lvmSwapLv is None or self.lvmSwapLv == "swap"
        assert self.bootHdd is not None and self.bootHdd in self.lvmPvHddList
        return True

    def get_rootdev(self):
        assert self.is_ready()

        # return "/dev/mapper/%s.%s" % (self.lvmVg, self.lvmRootLv)
        ret = "/dev/mapper/%s.%s" % (self.lvmVg, self.lvmRootLv)
        if os.path.exists(ret):
            return ret
        else:
            ret = "/dev/mapper/%s-%s" % (self.lvmVg, self.lvmRootLv)    # compatible with old lvm version
            if os.path.exists(ret):
                return ret
        assert False

    def optimize_rootdev(self):
        assert self.is_ready()
        _helperAdjust(self)

    def get_esp(self):
        assert self.is_ready()
        return util.devPathDiskToPartition(self.bootHdd, 1)

    def get_esp_sync_info(self):
        assert self.is_ready()

        src = util.devPathDiskToPartition(self.bootHdd, 1)

        dstList = []
        for hdd in self.lvmPvHddList:
            if hdd != self.bootHdd:
                dstList.append(util.devPathDiskToPartition(hdd, 1))

        return (src, dstList)

    def sync_esp(self, src, dst):
        assert self.is_ready()
        _helperSyncEsp(src, dst)

    def add_disk(self, devpath):
        assert devpath not in self.lvmPvHddList
        assert devpath in util.getDevPathListForFixedHdd()

        # create partitions
        util.initializeDisk(devpath, "gpt", [
            (self.espPartiSizeStr, "vfat"),
            ("*", "lvm"),
        ])

        # fill partition1, mount boot device if needed
        parti = util.devPathDiskToPartition(devpath, 1)
        util.cmdCall("/usr/sbin/mkfs.vfat", parti)
        util.syncBlkDev(util.devPathDiskToPartition(self.bootHdd, 1), parti, mountPoint1=_bootDir)

        # create lvm physical volume on partition2 and add it to volume group
        parti = util.devPathDiskToPartition(devpath, 2)
        util.cmdCall("/sbin/lvm", "pvcreate", parti)
        util.cmdCall("/sbin/lvm", "vgextend", self.lvmVg, parti)
        self.lvmPvHddList.append(devpath)

        return False

    def release_disk(self, devpath):
        assert devpath in self.lvmPvHddList and len(self.lvmPvHddList) > 1

        parti = util.devPathDiskToPartition(devpath, 2)
        rc, out = util.cmdCallWithRetCode("/sbin/lvm", "pvmove", parti)
        if rc != 5:
            raise Exception("failed")
        return

    def remove_disk(self, devpath):
        assert devpath in self.lvmPvHddList and len(self.lvmPvHddList) > 1

        # change boot device if needed
        ret = False
        if self.bootHdd == devpath:
            util.cmdCall("/bin/umount", _bootDir)
            self.lvmPvHddList.remove(devpath)
            self.bootHdd = self.lvmPvHddList[0]
            util.gptToggleEspPartition(util.devPathDiskToPartition(self.bootHdd, 1), True)
            util.cmdCall("/bin/mount", util.devPathDiskToPartition(self.bootHdd, 1), _bootDir, "-o", "ro")
            ret = True

        # remove harddisk
        parti = util.devPathDiskToPartition(devpath, 2)
        util.cmdCall("/sbin/lvm", "vgreduce", self.lvmVg, parti)
        util.wipeHarddisk(devpath)

        return ret


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
        self.ssd = None
        self.ssdEspParti = None
        self.ssdSwapParti = None
        self.ssdCacheParti = None
        self.lvmVg = None
        self.lvmPvHddDict = {}          # dict<hddDev,bcacheDev>
        self.lvmRootLv = None
        self.bootHdd = None

    @property
    def boot_mode(self):
        return StorageLayout.BOOT_MODE_EFI

    def is_ready(self):
        assert self.lvmVg == "hdd"
        assert len(self.lvmPvHddDict) > 0
        assert self.lvmRootLv == "root"
        if self.ssd is not None:
            assert self.ssdEspParti == util.devPathDiskToPartition(self.ssd, 1)
            assert self.ssdSwapParti == util.devPathDiskToPartition(self.ssd, 2)
            assert self.ssdCacheParti == util.devPathDiskToPartition(self.ssd, 3)
            assert self.bootHdd is None
        else:
            assert self.bootHdd is not None and self.bootHdd in self.lvmPvHddDict
        return True

    def get_rootdev(self):
        assert self.is_ready()

        # return "/dev/mapper/%s.%s" % (self.lvmVg, self.lvmRootLv)
        ret = "/dev/mapper/%s.%s" % (self.lvmVg, self.lvmRootLv)
        if os.path.exists(ret):
            return ret
        else:
            ret = "/dev/mapper/%s-%s" % (self.lvmVg, self.lvmRootLv)    # compatible with old lvm version
            if os.path.exists(ret):
                return ret
        assert False

    def optimize_rootdev(self):
        _helperAdjust(self)

    def get_esp(self):
        assert self.is_ready()

        if self.ssd is not None:
            return self.ssdEspParti
        else:
            return util.devPathDiskToPartition(self.bootHdd, 1)

    def get_esp_sync_info(self):
        assert self.is_ready()

        if self.ssd is not None:
            src = self.ssdEspParti
        else:
            src = util.devPathDiskToPartition(self.bootHdd, 1)

        dstList = []
        for hdd in self.lvmPvHddDict:
            if self.bootHdd is None or hdd != self.bootHdd:
                dstList.append(util.devPathDiskToPartition(hdd, 1))

        return (src, dstList)

    def sync_esp(self, src, dst):
        assert self.is_ready()
        _helperSyncEsp(src, dst)

    def add_disk(self, devpath):
        # FIXME: only one ssd is allowed, and sdd must be main-disk
        if False:
            return self._addSsdEfiBcacheLvm(devpath)
        else:
            return self._addHddEfiBcacheLvm(devpath)

    def release_disk(self, devpath):
        if self.ssd is not None and self.ssd == devpath:
            return
        if devpath not in self.lvmPvHddDict:
            raise Exception("the specified device is not managed")
        parti = util.devPathDiskToPartition(devpath, 2)
        bcacheDev = util.bcacheFindByBackingDevice(parti)
        rc, out = util.cmdCallWithRetCode("/sbin/lvm", "pvmove", bcacheDev)
        if rc != 5:
            raise Exception("failed")
        return

    def remove_disk(self, devpath):
        if devpath == self.ssd:
            return self._removeSsdEfiBcacheLvm()
        else:
            return self._removeHddEfiBcacheLvm(devpath)

    def _addSsdEfiBcacheLvm(self, devpath):
        if self.ssd is not None:
            raise Exception("mainboot device already exists")
        if devpath in self.lvmPvHddDict:
            raise Exception("the specified device is already managed")
        if devpath not in util.getDevPathListForFixedHdd() or not util.isBlkDevSsdOrHdd(devpath):
            raise Exception("the specified device is not a fixed SSD harddisk")

        # create partitions
        util.initializeDisk(devpath, "gpt", [
            (self.espPartiSizeStr, "esp"),
            (self.swapPartiSizeStr, "swap"),
            ("*", "bcache"),
        ])
        self.ssd = devpath

        # sync partition1 as boot partition
        parti = util.devPathDiskToPartition(devpath, 1)
        util.cmdCall("/usr/sbin/mkfs.vfat", parti)
        util.syncBlkDev(util.devPathDiskToPartition(self.bootHdd, 1), parti, mountPoint1=_bootDir)
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
        for bcacheDev in self.lvmPvHddDict.values():
            with open("/sys/block/%s/bcache/attach" % (os.path.basename(bcacheDev)), "w") as f:
                f.write(str(setUuid))

        # change boot device
        util.cmdCall("/bin/umount", _bootDir)
        util.gptToggleEspPartition(util.devPathDiskToPartition(self.bootHdd, 1), False)
        util.cmdCall("/bin/mount", self.ssdEspParti, _bootDir, "-o", "ro")
        self.bootHdd = None

        return True

    def _addHddEfiBcacheLvm(self, devpath):
        if devpath == self.ssd or devpath in self.lvmPvHddDict:
            raise Exception("the specified device is already managed")
        if devpath not in util.getDevPathListForFixedHdd():
            raise Exception("the specified device is not a fixed harddisk")

        if util.isBlkDevSsdOrHdd(devpath):
            print("WARNING: \"%s\" is an SSD harddisk, perhaps you want to add it as mainboot device?" % (devpath))

        # create partitions
        util.initializeDisk(devpath, "gpt", [
            (self.espPartiSizeStr, "vfat"),
            ("*", "bcache"),
        ])

        # fill partition1
        parti = util.devPathDiskToPartition(devpath, 1)
        util.cmdCall("/usr/sbin/mkfs.vfat", parti)
        if self.ssd is not None:
            util.syncBlkDev(self.ssdEspParti, parti, mountPoint1=_bootDir)
        else:
            util.syncBlkDev(util.devPathDiskToPartition(self.bootHdd, 1), parti, mountPoint1=_bootDir)

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
        util.cmdCall("/sbin/lvm", "vgextend", self.lvmVg, bcacheDev)
        self.lvmPvHddDict[devpath] = bcacheDev

        return False

    def _removeSsdEfiBcacheLvm(self):
        assert self.ssd is not None
        assert len(self.lvmPvHddDict) > 0

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
        util.cmdCall("/bin/umount", _bootDir)
        self.bootHdd = list(self.lvmPvHddDict.keys())[0]
        util.gptToggleEspPartition(util.devPathDiskToPartition(self.bootHdd, 1), True)
        util.cmdCall("/bin/mount", util.devPathDiskToPartition(self.bootHdd, 1), _bootDir, "-o", "ro")
        self.ssdEspParti = None

        # wipe disk
        util.wipeHarddisk(self.ssd)
        self.ssd = None

        return True

    def _removeHddEfiBcacheLvm(self, devpath):
        assert devpath in self.lvmPvHddDict

        if len(self.lvmPvHddDict) <= 1:
            raise Exception("can not remove the last physical volume")

        # change boot device if needed
        ret = False
        if self.bootHdd is not None and self.bootHdd == devpath:
            util.cmdCall("/bin/umount", _bootDir)
            del self.lvmPvHddDict[devpath]
            self.bootHdd = list(self.lvmPvHddDict.keys())[0]
            util.gptToggleEspPartition(util.devPathDiskToPartition(self.bootHdd, 1), True)
            util.cmdCall("/bin/mount", util.devPathDiskToPartition(self.bootHdd, 1), _bootDir, "-o", "ro")
            ret = True

        # remove harddisk
        bcacheDev = util.bcacheFindByBackingDevice(util.devPathDiskToPartition(devpath, 2))
        util.cmdCall("/sbin/lvm", "vgreduce", self.lvmVg, bcacheDev)
        with open("/sys/block/%s/bcache/stop" % (os.path.basename(bcacheDev)), "w") as f:
            f.write("1")
        util.wipeHarddisk(devpath)

        return ret


def _helperAdjust(layout):
    total, used = util.getBlkDevCapacity(layout.get_rootdev())
    if used / total < 0.9:
        return
    added = int(used / 0.7) - total
    added = (added // 1024 + 1) * 1024      # change unit from MB to GB
    util.cmdCall("/sbin/lvm", "lvextend", "-L+%dG" % (added), layout.get_rootdev())
    util.cmdExec("/sbin/resize2fs", layout.get_rootdev())


def _helperSyncEsp(src, dst, syncInfo):
    assert src == syncInfo[0] and dst in syncInfo[1]
    util.syncBlkDev(src, dst, mountPoint1=_bootDir)


_bootDir = "/boot"
_swapFilename = "/var/swap.dat"
