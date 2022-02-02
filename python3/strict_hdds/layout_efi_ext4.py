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


from .util import Util, PartiUtil, GptUtil
from .handy import SwapFile, MountEfi, DisksChecker, HandyUtil
from . import errors
from . import StorageLayout, MountParam


class StorageLayoutImpl(StorageLayout):
    """Layout:
           /dev/sda          GPT
               /dev/sda1     ESP partition
               /dev/sda2     root device, EXT4
       Description:
           1. the 3 partition in /dev/sda is order-insensitive
           2. use optional swap file /var/cache/swap.dat
           3. extra partition is allowed to exist
    """

    def __init__(self):
        self._hdd = None              # boot harddisk name
        self._hddEspParti = None      # ESP partition name
        self._hddRootParti = False    # root partition name
        self._swap = None             # SwapFile
        self._mnt = None              # MountEfi

    @property
    def boot_mode(self):
        return StorageLayout.BOOT_MODE_EFI

    @property
    def dev_rootfs(self):
        return self._hddRootParti

    @property
    def dev_boot(self):
        raise self._hddEspParti

    @SwapFile.proxy
    @property
    def dev_swap(self):
        pass

    @property
    def boot_disk(self):
        return self._hdd

    @MountEfi.proxy
    @property
    def mount_point(self):
        pass

    def umount_and_dispose(self):
        if True:
            Util.mntUmount(self.mount_point, ["/boot", "/"])
            del self._mnt
        del self._swap
        del self._hddRootParti
        del self._hddEspParti
        del self._hdd

    @MountEfi.proxy
    def get_bootdir_rw_controller(self):
        pass

    def get_params_for_mount(self, **kwargs):
        return [
            MountParam(self.dev_rootfs, "/", "ext4", ""),
            MountParam(self.dev_boot, "/boot", "vfat", "ro"),
        ]

    def get_esp(self):
        return self._hddEspParti

    @SwapFile.proxy
    def create_swap_file(self):
        pass

    @SwapFile.proxy
    def remove_swap_file(self):
        pass

    def _check_impl(self, check_item, *kargs, auto_fix=False, error_callback=None):
        if check_item == Util.checkItemBasic:
            dc = DisksChecker([self._hdd])
            dc.check_partition_type("gpt", auto_fix, error_callback)
            dc.check_boot_sector(auto_fix, error_callback)
            dc.check_logical_sector_size(auto_fix, error_callback)
        elif check_item == "swap":
            self._swap.check(auto_fix, error_callback)
        else:
            assert False


def parse(boot_dev, root_dev, mount_dir):
    if PartiUtil.partiToDisk(boot_dev) != PartiUtil.partiToDisk(root_dev):
        raise errors.StorageLayoutParseError(StorageLayoutImpl.name, "boot device and root device are not on the same harddisk")
    if not GptUtil.isEspPartition(boot_dev):
        raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.BOOT_DEV_IS_NOT_ESP)
    if Util.getBlkDevFsType(root_dev) != Util.fsTypeExt4:
        raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.ROOT_PARTITION_FS_SHOULD_BE(Util.fsTypeExt4))

    # return
    ret = StorageLayoutImpl()
    ret._hdd = PartiUtil.partiToDisk(boot_dev)
    ret._hddEspParti = boot_dev
    ret._hddRootParti = root_dev
    ret._swap = HandyUtil.swapFileDetectAndNew(StorageLayoutImpl.name, mount_dir)
    ret._mnt = MountEfi(mount_dir)
    return ret


def detect_and_mount(disk_list, mount_dir, mount_options):
    # scan for ESP and root partition
    espAndRootPartitionList = []
    for disk in disk_list:
        espParti = PartiUtil.diskToParti(disk, 1)
        rootParti = PartiUtil.diskToParti(disk, 2)
        if not PartiUtil.partiExists(espParti):
            continue
        if not PartiUtil.partiExists(rootParti):
            continue
        if not GptUtil.isEspPartition(espParti):
            continue
        if Util.getBlkDevFsType(rootParti) != Util.fsTypeExt4:
            continue
        espAndRootPartitionList.append((disk, espParti, rootParti))
    if len(espAndRootPartitionList) == 0:
        raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.DISK_NOT_FOUND)
    if len(espAndRootPartitionList) > 1:
        raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.DISK_TOO_MANY)

    # return
    ret = StorageLayoutImpl()
    ret._hdd = espAndRootPartitionList[0][0]
    ret._hddEspParti = espAndRootPartitionList[0][1]
    ret._hddRootParti = espAndRootPartitionList[0][2]
    ret._swap = HandyUtil.swapFileDetectAndNew(StorageLayoutImpl.name, mount_dir)
    ret._mnt = MountEfi(mount_dir)

    # mount
    Util.mntMount(mount_dir, Util.optimizeMntParamList(ret.get_params_for_mount(), mount_options))
    return ret


def create_and_mount(disk_list, mount_dir, mount_options):
    # create partitions
    hdd = HandyUtil.checkAndGetHdd(disk_list)
    Util.initializeDisk(hdd, Util.diskPartTableGpt, [
        ("%dMiB" % (Util.getEspSizeInMb()), Util.fsTypeFat),
        ("*", Util.fsTypeExt4),
    ])

    # get esp partition and root partition
    espParti = PartiUtil.diskToParti(hdd, 1)
    rootParti = PartiUtil.diskToParti(hdd, 2)

    # return
    ret = StorageLayoutImpl()
    ret._hdd = hdd
    ret._hddEspParti = espParti
    ret._hddRootParti = rootParti
    ret._swap = SwapFile(False)
    ret._mnt = MountEfi(mount_dir)

    # mount
    Util.mntMount(mount_dir, Util.optimizeMntParamList(ret.get_params_for_mount(), mount_options))
    return ret
