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


from .util import Util, BcacheUtil, BtrfsUtil
from .handy import EfiCacheGroup, BcacheRaid, Snapshot, SnapshotBtrfs, MountEfi, HandyCg, HandyBcache, DisksChecker
from . import errors
from . import StorageLayout, MountParam


class StorageLayoutImpl(StorageLayout):
    """Layout:
           /dev/sda                      SSD, GPT (cache-disk)
               /dev/sda1                 ESP partition
               /dev/sda2                 swap device
               /dev/sda3                 bcache cache device
           /dev/sdb                      Non-SSD, GPT
               /dev/sdb1                 reserved ESP partition
               /dev/sdb2                 bcache backing device
           /dev/sdc                      Non-SSD, GPT
               /dev/sdc1                 reserved ESP partition
               /dev/sdc2                 bcache backing device
           /dev/bcache0:/dev/bcache1     root device, btrfs
              /dev/bcache0               corresponds to /dev/sdb2, btrfs device
              /dev/bcache1               corresponds to /dev/sdc2, btrfs device
       Description:
           1. /dev/sda1 and /dev/sd{b,c}1 must has the same size
           2. /dev/sda1, /dev/sda2 and /dev/sda3 is order-sensitive, no extra partition is allowed
           3. /dev/sd{b,c}1 and /dev/sd{b,c}2 is order-sensitive, no extra partition is allowed
           4. cache-disk is optional, and only one cache-disk is allowed at most
           5. cache-disk can have no swap partition, /dev/sda2 would be the cache device then
           6. extra harddisk is allowed to exist
    """

    def __init__(self):
        self._cg = None                     # EfiCacheGroup
        self._bcache = None                 # BcacheRaid
        self._snapshot = None               # SnapshotBtrfs
        self._mnt = None                    # MountEfi

    @property
    def boot_mode(self):
        return StorageLayout.BOOT_MODE_EFI

    @property
    def dev_rootfs(self):
        return self._bcache.get_all_bcache_dev_list()[0]

    @EfiCacheGroup.proxy
    @property
    def dev_boot(self):
        pass

    @EfiCacheGroup.proxy
    @property
    def dev_swap(self):
        pass

    @EfiCacheGroup.proxy
    @property
    def boot_disk(self):
        pass

    @Snapshot.proxy
    @property
    def snapshot(self):
        pass

    @MountEfi.proxy
    @property
    def mount_point(self):
        pass

    @MountEfi.proxy
    @property
    def mount_params(self):
        pass

    def umount_and_dispose(self):
        if True:
            self._mnt.umount()
            del self._mnt
        if True:
            self._bcache.stopAll()
            del self._bcache
        if True:
            del self._cg

    @MountEfi.proxy
    def get_bootdir_rw_controller(self):
        pass

    def optimize_rootdev(self):
        # FIXME: btrfs balance
        pass

    @EfiCacheGroup.proxy
    def get_esp(self):
        pass

    @EfiCacheGroup.proxy
    def get_pending_esp_list(self):
        pass

    @EfiCacheGroup.proxy
    def sync_esp(self, dst):
        pass

    @EfiCacheGroup.proxy
    def get_disk_list(self):
        pass

    @EfiCacheGroup.proxy
    def get_ssd(self):
        pass

    @EfiCacheGroup.proxy
    def get_ssd_esp_partition(self):
        pass

    @EfiCacheGroup.proxy
    def get_ssd_swap_partition(self):
        pass

    @EfiCacheGroup.proxy
    def get_ssd_cache_partition(self):
        pass

    @EfiCacheGroup.proxy
    def get_hdd_list(self):
        pass

    @EfiCacheGroup.proxy
    def get_hdd_esp_partition(self, disk):
        pass

    @EfiCacheGroup.proxy
    def get_hdd_data_partition(self, disk):
        pass

    def get_hdd_bcache_dev(self, disk):
        return self._bcache.get_bcache_dev(disk)

    @Snapshot.proxy
    def get_snapshot_list(self):
        pass

    def add_disk(self, disk):
        assert disk is not None

        if disk not in Util.getDevPathListForFixedDisk():
            raise errors.StorageLayoutAddDiskError(disk, errors.NOT_DISK)

        if Util.isBlkDevSsdOrHdd(disk):
            if self._cg.boot_disk is not None:
                self._mnt.umount_esp(self._cg.get_hdd_esp_partition(self._cg.boot_disk))
            self._cg.add_ssd(disk, "bcache")
            self._bcache.add_cache(self._cg.get_ssd_cache_partition())
            self._mnt.mount_esp(self._cg.get_ssd_esp_partition())
            return True
        else:
            self._cg.add_hdd(disk, "bcache")
            self._bcache.add_backing(self._cg.get_ssd_cache_partition(), disk, self._cg.get_hdd_data_partition(disk))
            Util.cmdCall("btrfs", "device", "add", self._bcache.get_bcache_dev(disk), self._mnt.mount_point)
            if disk == self._cg.boot_disk:
                self._mnt.mount_esp(self._cg.get_hdd_esp_partition(disk))
                return True
            else:
                return False

    def remove_disk(self, disk):
        assert disk is not None

        if disk == self._cg.get_ssd():
            # check if swap is in use
            if self._cg.get_ssd_swap_partition() is not None:
                if Util.isSwapFileOrPartitionBusy(self._cg.get_ssd_swap_partition()):
                    raise errors.StorageLayoutRemoveDiskError(errors.SWAP_IS_IN_USE)

            # remove
            self._mnt.umount_esp(self._cg.get_ssd_esp_partition())
            self._bcache.remove_cache(self._cg.get_ssd_cache_partition())
            self._cg.remove_ssd(disk)

            # boot disk change
            if self._cg.boot_disk is not None:
                self._mnt.mount_esp(self._cg.get_hdd_esp_partition(self._cg.boot_disk))
                return True
            else:
                return False

        if disk in self._cg.get_hdd_list():
            # check for last hdd
            if len(self._cg.get_hdd_list()) <= 1:
                raise errors.StorageLayoutRemoveDiskError(errors.CAN_NOT_REMOVE_LAST_HDD)

            # boot disk change
            if disk == self._cg.boot_disk:
                self._mnt.umount_esp(self._cg.get_hdd_esp_partition(self._cg.boot_disk))
                bChange = True
            else:
                bChange = False

            # remove
            Util.cmdCall("btrfs", "device", "delete", self._bcache.get_bcache_dev(disk), self._mnt.mount_point)
            self._bcache.remove_backing(disk)
            self._cg.remove_hdd(disk)

            # boot disk change
            if bChange:
                assert self._cg.boot_disk is not None
                self._mnt.mount_esp(self._cg.get_disk_esp_partition(self._cg.boot_disk))
                return True
            else:
                return False

        assert False

    @Snapshot.proxy
    def create_snapshot(self, snapshot_name):
        pass

    @Snapshot.proxy
    def remove_snapshot(self, snapshot_name):
        pass

    def _check_impl(self, check_item, *kargs, auto_fix=False, error_callback=None):
        if check_item == Util.checkItemBasic:
            if True:
                dc = DisksChecker(self._cg.get_disk_list())
                dc.check_partition_type("gpt", auto_fix, error_callback)
                dc.check_boot_sector(auto_fix, error_callback)
                dc.check_logical_sector_size(auto_fix, error_callback)
            self._cg.check_ssd(auto_fix, error_callback)
            self._cg.check_esp(auto_fix, error_callback)
            self._bcache.check(auto_fix, error_callback)
            self._snapshot.check(auto_fix, error_callback)
        elif check_item == "swap":
            self._cg.check_swap(auto_fix, error_callback)
        elif check_item == "bcache-write-mode":
            self._bcache.check_write_mode(kargs[0], auto_fix, error_callback)
        else:
            assert False


def parse(boot_dev, root_dev, mount_dir):
    if boot_dev is None:
        raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.BOOT_DEV_NOT_EXIST)
    if Util.getBlkDevFsType(root_dev) != Util.fsTypeBtrfs:
        raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.ROOT_PARTITION_FS_SHOULD_BE(Util.fsTypeBtrfs))

    # bcache device list
    slaveDevPathList = BtrfsUtil.getSlaveDevPathList(mount_dir)
    for slaveDevPath in slaveDevPathList:
        if BcacheUtil.getBcacheDevFromDevPath(slaveDevPath) is None:
            raise errors.StorageLayoutParseError(StorageLayoutImpl.name, "\"%s\" has non-bcache slave device" % (root_dev))

    # ssd, hdd_list, boot_disk
    ssd, hddList = HandyBcache.getSsdAndHddListFromBcacheDevPathList(StorageLayoutImpl.name, slaveDevPathList)
    ssdEspParti, ssdSwapParti, ssdCacheParti = HandyCg.checkAndGetSsdPartitions(StorageLayoutImpl.name, ssd)
    bootHdd = HandyCg.checkAndGetBootHddFromBootDev(StorageLayoutImpl.name, boot_dev, ssdEspParti, hddList)

    # FIXME: check mount options
    pass

    # return
    ret = StorageLayoutImpl()
    ret._cg = EfiCacheGroup(ssd=ssd, ssdEspParti=ssdEspParti, ssdSwapParti=ssdSwapParti, ssdCacheParti=ssdCacheParti, hddList=hddList, bootHdd=bootHdd)
    ret._bcache = BcacheRaid(keyList=hddList, bcacheDevPathList=slaveDevPathList)
    ret._snapshot = SnapshotBtrfs(mount_dir)
    ret._mnt = MountEfi(mount_dir, "", _params_for_mount(ret))
    return ret


def detect_and_mount(disk_list, mount_dir, mount_options):
    # scan
    bcacheDevPathList = BcacheUtil.scanAndRegisterAll()
    bcacheDevPathList = [x for x in bcacheDevPathList if Util.getBlkDevFsType(x) == Util.fsTypeBtrfs]
    if len(bcacheDevPathList) == 0:
        raise errors.StorageLayoutParseError(StorageLayoutImpl.name, errors.DISK_NOT_FOUND)

    # ssd, hdd_list, boot_disk
    ssd, hddList = HandyBcache.getSsdAndHddListFromBcacheDevPathList(StorageLayoutImpl.name, bcacheDevPathList)
    HandyCg.checkExtraDisks(StorageLayoutImpl.name, ssd, hddList, disk_list)
    ssdEspParti, ssdSwapParti, ssdCacheParti = HandyCg.checkAndGetSsdPartitions(StorageLayoutImpl.name, ssd)
    bootHdd = HandyCg.checkAndGetBootHddAndBootDev(StorageLayoutImpl.name, ssdEspParti, hddList)[0]

    # return
    ret = StorageLayoutImpl()
    ret._cg = EfiCacheGroup(ssd=ssd, ssdEspParti=ssdEspParti, ssdSwapParti=ssdSwapParti, ssdCacheParti=ssdCacheParti, hddList=hddList, bootHdd=bootHdd)
    ret._bcache = BcacheRaid(keyList=hddList, bcacheDevPathList=bcacheDevPathList)
    ret._snapshot = SnapshotBtrfs(mount_dir)
    ret._mnt = MountEfi(mount_dir, "", _params_for_mount(ret))

    # mount
    ret._mnt.mount()
    return ret


def create_and_mount(disk_list, mount_dir, mount_options):
    # add disks to cache group
    cg = EfiCacheGroup()
    HandyCg.checkAndAddDisks(cg, *Util.splitSsdAndHddFromFixedDiskDevPathList(disk_list), "bcache")

    bcache = BcacheRaid()
    for hdd in cg.get_hdd_list():
        # hdd partition 2: make them as backing device
        bcache.add_backing(None, hdd, cg.get_hdd_data_partition(hdd))
    if cg.get_ssd() is not None:
        # ssd partition 3: make it as cache device
        bcache.add_cache(cg.get_ssd_cache_partition())

    # create btrfs
    Util.cmdCall("mkfs.btrfs", "-f", "-d", "single", "-m", "single", *bcache.get_all_bcache_dev_list())
    SnapshotBtrfs.initializeFs(bcache.get_all_bcache_dev_list()[0])

    # return
    ret = StorageLayoutImpl()
    ret._cg = cg
    ret._bcache = bcache
    ret._snapshot = SnapshotBtrfs(mount_dir)
    ret._mnt = MountEfi(mount_dir, "", _params_for_mount(ret))

    # mount
    ret._mnt.mount()
    return ret


def _params_for_mount(obj):
    ret = []
    for dirPath, dirMode, dirUid, dirGid, mntOpts in obj._snapshot.getParamsForMount():
        ret.append(MountParam(dirPath, dirMode, dirUid, dirGid, target=obj.dev_rootfs, fs_type=Util.fsTypeBtrfs, mnt_opts=mntOpts))
    ret.append(MountParam(Util.bootDir, 0o0755, 0, 0, target=obj.dev_boot, fs_type=Util.fsTypeFat, mnt_opts="ro"))
    return ret
