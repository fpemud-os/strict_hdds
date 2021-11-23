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


from .util import Util, PartiUtil, MbrUtil
from .handy import SwapFile, MountBios, HandyUtil
from . import errors
from . import StorageLayout, MountParam


class StorageLayoutImpl(StorageLayout):
    """Layout:
           /dev/sda          MBR, BIOS-GRUB
               /dev/sda1     root device, EXT4
       Description:
           1. partition number of /dev/sda1 and /dev/sda2 is irrelevant
           2. use optional swap file /var/cache/swap.dat
           3. extra partition is allowed to exist
    """

    def __init__(self):
        self._hdd = None              # boot harddisk name
        self._hddRootParti = False    # root partition name
        self._swap = None             # SwapFile
        self._mnt = None              # MountBios

    @property
    def boot_mode(self):
        return StorageLayout.BOOT_MODE_BIOS

    @property
    def dev_rootfs(self):
        return self._hddRootParti

    @property
    def dev_boot(self):
        assert False

    @SwapFile.proxy
    @property
    def dev_swap(self):
        pass

    @property
    def boot_disk(self):
        return self._hdd

    @MountBios.proxy
    @property
    def mount_point(self):
        pass

    def umount_and_dispose(self):
        if True:
            Util.mntUmount(self.mount_point, ["/"])
            del self._mnt
        del self._swap
        del self._hddRootParti
        del self._hdd

    @MountBios.proxy
    def get_bootdir_rw_controller(self):
        pass

    def get_params_for_mount(self, **kwargs):
        return [MountParam(self.dev_rootfs, "/", "")]

    @SwapFile.proxy
    def create_swap_file(self):
        pass

    @SwapFile.proxy
    def remove_swap_file(self):
        pass

    def _check_impl(self, check_item, *kargs, auto_fix=False, error_callback=None):
        if check_item == Util.checkItemBasic:
            pass
        elif check_item == "swap":
            self._swap.check(auto_fix, error_callback)
        else:
            assert False


def parse(boot_dev, root_dev):
    if boot_dev is not None:
        raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.BOOT_DEV_SHOULD_NOT_EXIST)
    if Util.getBlkDevFsType(root_dev) != Util.fsTypeExt4:
        raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.ROOT_PARTITION_FS_SHOULD_BE(Util.fsTypeExt4))

    # hdd
    hdd = PartiUtil.partiToDisk(root_dev)
    if Util.getBlkDevPartitionTableType(hdd) != Util.diskPartTableMbr:
        raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.PARTITION_TYPE_SHOULD_BE(hdd, Util.diskPartTableMbr))

    # return
    ret = StorageLayoutImpl()
    ret._hdd = hdd
    ret._hddRootParti = root_dev
    ret._swap = HandyUtil.swapFileDetectAndNew(StorageLayoutImpl.name, "/")
    ret._mnt = MountBios("/")
    return ret


def detect_and_mount(disk_list, mount_dir, mount_options):
    # scan for root partition
    rootPartitionList = []
    for disk in disk_list:
        if not MbrUtil.hasBootCode(disk):                                       # no boot code, ignore unbootable disk
            continue
        if Util.getBlkDevPartitionTableType(disk) != Util.diskPartTableMbr:     # only accept disk with MBR partition table
            continue
        i = 1
        while True:
            parti = PartiUtil.diskToParti(disk, i)
            if not PartiUtil.partiExists(parti):
                break
            if Util.getBlkDevFsType(parti) == Util.fsTypeExt4:
                rootPartitionList.append(parti)
            i += 1
    if len(rootPartitionList) == 0:
        raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.ROOT_PARTITION_NOT_FOUND)
    if len(rootPartitionList) > 1:
        raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.ROOT_PARTITIONS_TOO_MANY)

    # return
    ret = StorageLayoutImpl()
    ret._hdd = PartiUtil.partiToDisk(rootPartitionList[0])
    ret._hddRootParti = rootPartitionList[0]
    ret._swap = HandyUtil.swapFileDetectAndNew(StorageLayoutImpl.name, mount_dir)
    ret._mnt = MountBios(mount_dir)

    # mount
    Util.mntMount(mount_dir, [MountParam(ret.dev_rootfs, "/", mount_options)])
    return ret


def create_and_mount(disk_list, mount_dir, mount_options):
    # create partitions
    hdd = HandyUtil.checkAndGetHdd(disk_list)
    Util.initializeDisk(hdd, Util.diskPartTableMbr, [
        ("*", Util.fsTypeExt4),
    ])

    # root partition
    rootParti = PartiUtil.diskToParti(hdd, 1)

    # return
    ret = StorageLayoutImpl(mount_dir)
    ret._hdd = hdd
    ret._hddRootParti = rootParti
    ret._swap = SwapFile(False)
    ret._mnt = MountBios(mount_dir)

    # mount
    Util.mntMount(mount_dir, [MountParam(ret.dev_rootfs, "/", mount_options)])
    return ret
