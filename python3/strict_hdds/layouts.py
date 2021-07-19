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
from . import StorageLayout


class StorageLayoutBiosSimple(StorageLayout):
    """Layout:
           /dev/sda          MBR, BIOS-GRUB
               /dev/sda1     root device, EXT4
       Description:
           1. partition number of /dev/sda1 and /dev/sda2 is irrelevant
           2. no swap partition
           3. extra partition is allowed to exist
    """

    name = "bios-simple"

    def __init__(self):
        self._hdd = None
        self._hddRootParti = None

    @property
    def boot_mode(self):
        return StorageLayout.BOOT_MODE_BIOS

    def is_ready(self):
        assert self._hdd is not None
        assert self._hddRootParti is not None
        return True

    def get_boot_disk(self):
        assert self.is_ready()
        return self._hdd

    def get_rootdev(self):
        assert self.is_ready()
        return self._hddRootParti


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

    def get_boot_disk(self):
        assert self.is_ready()

        return self.bootHdd

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

    def add_disk(devpath):
        assert False

    def release_disk(devpath):
        assert False

    def remove_disk(devpath):
        assert False


class StorageLayoutEfiSimple(StorageLayout):
    """Layout:
           /dev/sda          GPT, EFI-GRUB
               /dev/sda1     ESP partition
               /dev/sda2     root device, EXT4
       Description:
           1. the 3 partition in /dev/sda is order-insensitive
           2. no swap partition
           3. extra partition is allowed to exist
    """

    name = "efi-simple"

    def __init__(self):
        self.hdd = None
        self.hddEspParti = None
        self.hddRootParti = None

    @property
    def boot_mode(self):
        return StorageLayout.BOOT_MODE_EFI

    def is_ready(self):
        assert self.hdd is not None
        assert self.hddEspParti == FmUtil.devPathDiskToPartition(self.hdd, 1)
        assert self.hddRootParti == FmUtil.devPathDiskToPartition(self.hdd, 2)
        return True

    def get_esp(self):
        assert self.is_ready()
        return self.hddEspParti

    def get_rootdev(self):
        assert self.is_ready()
        return self.hddRootParti


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

    def get_esp(self):
        assert self.is_ready()

        return FmUtil.devPathDiskToPartition(self.bootHdd, 1)

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

    def add_disk(devpath):
        assert False

    def release_disk(devpath):
        assert False

    def remove_disk(devpath):
        assert False

    def get_esp_sync_info(self):
        assert self.is_ready()

        src = FmUtil.devPathDiskToPartition(self.bootHdd, 1)

        dstList = []
        for hdd in self.lvmPvHddList:
            if hdd != self.bootHdd:
                dstList.append(FmUtil.devPathDiskToPartition(hdd, 1))

        return (src, dstList)


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
            assert self.ssdEspParti == FmUtil.devPathDiskToPartition(self.ssd, 1)
            assert self.ssdSwapParti == FmUtil.devPathDiskToPartition(self.ssd, 2)
            assert self.ssdCacheParti == FmUtil.devPathDiskToPartition(self.ssd, 3)
            assert self.bootHdd is None
        else:
            assert self.bootHdd is not None and self.bootHdd in self.lvmPvHddDict
        return True

    def get_esp(self):
        assert self.is_ready()

        if self.ssd is not None:
            return self.ssdEspParti
        else:
            return FmUtil.devPathDiskToPartition(self.bootHdd, 1)

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

    def add_disk(self, devpath):
        assert False

    def release_disk(self, devpath):
        assert False

    def remove_disk(self, devpath):
        assert False

    def get_esp_sync_info(self):
        assert self.is_ready()

        if self.ssd is not None:
            src = self.ssdEspParti
        else:
            src = FmUtil.devPathDiskToPartition(self.bootHdd, 1)
        
        dstList = []
        for hdd in self.lvmPvHddDict:
            if self.bootHdd is None or hdd != self.bootHdd:
                dstList.append(FmUtil.devPathDiskToPartition(hdd, 1))

        return (src, dstList)
