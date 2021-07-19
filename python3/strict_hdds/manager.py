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
import re
from . import util
from .layouts import StorageLayoutBiosSimple
from .layouts import StorageLayoutBiosLvm
from .layouts import StorageLayoutEfiSimple
from .layouts import StorageLayoutEfiLvm
from .layouts import StorageLayoutEfiBcacheLvm


def get_supported_storage_layouts():
    return [
        StorageLayoutBiosSimple.name,
        StorageLayoutBiosLvm.name,
        StorageLayoutEfiSimple.name,
        StorageLayoutEfiLvm.name,
        StorageLayoutEfiBcacheLvm.name,
    ]


def create_storage_layout(layout_name):
    if layout_name == StorageLayoutBiosSimple.name:
        _StorageLayoutCreator.createLayoutBiosSimple()
    elif layout_name == StorageLayoutBiosLvm.name:
        _StorageLayoutCreator.createLayoutBiosLvm()
    elif layout_name == StorageLayoutEfiSimple.name:
        _StorageLayoutCreator.createLayoutEfiSimple()
    elif layout_name == StorageLayoutEfiLvm.name:
        _StorageLayoutCreator.createLayoutEfiLvm()
    elif layout_name == StorageLayoutEfiBcacheLvm.name:
        _StorageLayoutCreator.createLayoutEfiBcacheLvm()
    else:
        assert False


def parse_storage_layout():
    return _StorageLayoutParser.getStorageLayout()


class ParseStorageLayoutError(Exception):

    def __init__(self, layout_class, message):
        self._layout_name = layout_class.name
        self._message = message


class _StorageLayoutCreator:

    @staticmethod
    def createLayoutBiosSimple(hdd=None):
        if hdd is None:
            hddList = util.getDevPathListForFixedHdd()
            if len(hddList) == 0:
                raise Exception("no harddisks")
            if len(hddList) > 1:
                raise Exception("multiple harddisks")
            hdd = hddList[0]

        # create partitions
        util.initializeDisk(hdd, "mbr", [
            ("*", "ext4"),
        ])

    @staticmethod
    def createLayoutBiosLvm(hddList=None):
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

    @staticmethod
    def createLayoutEfiSimple(hdd=None):
        if hdd is None:
            hddList = util.getDevPathListForFixedHdd()
            if len(hddList) == 0:
                raise Exception("no harddisks")
            if len(hddList) > 1:
                raise Exception("multiple harddisks")
            hdd = hddList[0]

        # create partitions
        util.initializeDisk(hdd, "gpt", [
            (_espPartiSizeStr, "vfat"),
            ("*", "ext4"),
        ])

    @staticmethod
    def createLayoutEfiLvm(hddList=None):
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
                (_espPartiSizeStr, "vfat"),
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

    @staticmethod
    def createLayoutEfiBcacheLvm(ssd=None, hddList=None):
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
                (_espPartiSizeStr, "esp"),
                (_swapPartiSizeStr, "swap"),
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
                (_espPartiSizeStr, "vfat"),
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


class _StorageLayoutParser:

    @staticmethod
    def getStorageLayout():
        rootDev = util.getMountDeviceForPath("/")
        bootDev = util.getMountDeviceForPath("/boot")

        assert rootDev is not None
        if bootDev is not None:
            lvmInfo = util.getBlkDevLvmInfo(rootDev)
            if lvmInfo is not None:
                tlist = util.lvmGetSlaveDevPathList(lvmInfo[0])
                if any(re.fullmatch("/dev/bcache[0-9]+", x) is not None for x in tlist):
                    ret = _StorageLayoutParser._getEfiBcacheLvmLayout(bootDev)
                else:
                    ret = _StorageLayoutParser._getEfiLvmLayout(bootDev)
            else:
                ret = _StorageLayoutParser._getEfiSimpleLayout(bootDev, rootDev)
        else:
            if util.getBlkDevLvmInfo(rootDev) is not None:
                ret = _StorageLayoutParser._getBiosLvmLayout()
            else:
                ret = _StorageLayoutParser._getBiosSimpleLayout(rootDev)

        assert ret.is_ready()
        return ret

    @staticmethod
    def _getEfiSimpleLayout(bootDev, rootDev):
        if not util.gptIsEspPartition(bootDev):
            raise ParseStorageLayoutError(StorageLayoutEfiSimple, "boot device is not ESP partitiion")

        ret = StorageLayoutEfiSimple()

        # ret.hdd
        ret.hdd = util.devPathPartitionToDisk(bootDev)
        if ret.hdd != util.devPathPartitionToDisk(rootDev):
            raise ParseStorageLayoutError(StorageLayoutEfiSimple, "boot device and root device is not the same")

        # ret.hddEspParti
        ret.hddEspParti = bootDev

        # ret.hddRootParti
        ret.hddRootParti = rootDev
        if True:
            fs = util.getBlkDevFsType(ret.hddRootParti)
            if fs != "ext4":
                raise ParseStorageLayoutError(StorageLayoutEfiSimple, "root partition file system is \"%s\", not \"ext4\"" % (fs))

        # ret.swapFile
        if os.path.exists(_swapFilename) and util.cmdCallTestSuccess("/sbin/swaplabel", _swapFilename):
            ret.swapFile = _swapFilename

        return ret

    @staticmethod
    def _getEfiLvmLayout(bootDev):
        if not util.gptIsEspPartition(bootDev):
            raise ParseStorageLayoutError(StorageLayoutEfiLvm, "boot device is not ESP partitiion")

        ret = StorageLayoutEfiLvm()

        # ret.bootHdd
        ret.bootHdd = util.devPathPartitionToDisk(bootDev)

        # ret.lvmVg
        if not util.cmdCallTestSuccess("/sbin/lvm", "vgdisplay", "hdd"):
            raise ParseStorageLayoutError(StorageLayoutEfiLvm, "volume group \"hdd\" does not exist")
        ret.lvmVg = "hdd"

        # ret.lvmPvHddList
        out = util.cmdCall("/sbin/lvm", "pvdisplay", "-c")
        for m in re.finditer("(/dev/\\S+):hdd:.*", out, re.M):
            hdd, partId = util.devPathPartitionToDiskAndPartitionId(m.group(1))
            if util.getBlkDevPartitionTableType(hdd) != "gpt":
                raise ParseStorageLayoutError(StorageLayoutEfiLvm, "partition type of %s is not \"gpt\"" % (hdd))
            if partId != 2:
                raise ParseStorageLayoutError(StorageLayoutEfiLvm, "physical volume partition of %s is not %s" % (hdd, util.devPathDiskToPartition(hdd, 2)))
            if util.getBlkDevSize(util.devPathDiskToPartition(hdd, 1)) != _espPartiSize:
                raise ParseStorageLayoutError(StorageLayoutEfiLvm, "%s has an invalid size" % (util.devPathDiskToPartition(hdd, 1)))
            if os.path.exists(util.devPathDiskToPartition(hdd, 3)):
                raise ParseStorageLayoutError(StorageLayoutEfiLvm, "redundant partition exists on %s" % (hdd))
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
                    raise ParseStorageLayoutError(StorageLayoutEfiLvm, "root partition file system is \"%s\", not \"ext4\"" % (fs))
            else:
                raise ParseStorageLayoutError(StorageLayoutEfiLvm, "logical volume \"/dev/mapper/hdd.root\" does not exist")

            # ret.lvmSwapLv
            if re.search("/dev/hdd/swap:hdd:.*", out, re.M) is not None:
                ret.lvmSwapLv = "swap"
                if os.path.exists("/dev/mapper/hdd.swap"):
                    if util.getBlkDevFsType("/dev/mapper/hdd.swap") != "swap":
                        raise ParseStorageLayoutError(StorageLayoutEfiLvm, "/dev/mapper/hdd.swap has an invalid file system")
                elif os.path.exists("/dev/mapper/hdd-swap"):                    # compatible with old lvm version
                    if util.getBlkDevFsType("/dev/mapper/hdd-swap") != "swap":
                        raise ParseStorageLayoutError(StorageLayoutEfiLvm, "/dev/mapper/hdd.swap has an invalid file system")
                else:
                    assert False

        # ret.swapFile
        if os.path.exists(_swapFilename) and util.cmdCallTestSuccess("/sbin/swaplabel", _swapFilename):
            ret.swapFile = _swapFilename

        return ret

    @staticmethod
    def _getEfiBcacheLvmLayout(bootDev):
        if not util.gptIsEspPartition(bootDev):
            raise ParseStorageLayoutError(StorageLayoutEfiBcacheLvm, "boot device is not ESP partitiion")

        ret = StorageLayoutEfiBcacheLvm()

        # ret.lvmVg
        if not util.cmdCallTestSuccess("/sbin/lvm", "vgdisplay", "hdd"):
            raise ParseStorageLayoutError(StorageLayoutEfiBcacheLvm, "volume group \"hdd\" does not exist")
        ret.lvmVg = "hdd"

        # ret.lvmPvHddDict
        out = util.cmdCall("/sbin/lvm", "pvdisplay", "-c")
        for m in re.finditer("(/dev/\\S+):hdd:.*", out, re.M):
            if re.fullmatch("/dev/bcache[0-9]+", m.group(1)) is None:
                raise ParseStorageLayoutError(StorageLayoutEfiBcacheLvm, "volume group \"hdd\" has non-bcache physical volume")
            bcacheDev = m.group(1)
            tlist = util.bcacheGetSlaveDevPathList(bcacheDev)
            hddDev, partId = util.devPathPartitionToDiskAndPartitionId(tlist[-1])
            if partId != 2:
                raise ParseStorageLayoutError(StorageLayoutEfiBcacheLvm, "physical volume partition of %s is not %s" % (hddDev, util.devPathDiskToPartition(hddDev, 2)))
            if os.path.exists(util.devPathDiskToPartition(hddDev, 3)):
                raise ParseStorageLayoutError(StorageLayoutEfiBcacheLvm, "redundant partition exists on %s" % (hddDev))
            ret.lvmPvHddDict[hddDev] = bcacheDev

        # ret.lvmRootLv
        out = util.cmdCall("/sbin/lvm", "lvdisplay", "-c")
        if re.search("/dev/hdd/root:hdd:.*", out, re.M) is not None:
            ret.lvmRootLv = "root"
            if os.path.exists("/dev/mapper/hdd.root"):
                fs = util.getBlkDevFsType("/dev/mapper/hdd.root")
            elif os.path.exists("/dev/mapper/hdd-root"):                    # compatible with old lvm version
                fs = util.getBlkDevFsType("/dev/mapper/hdd-root")
            else:
                assert False
            if fs != "ext4":
                raise ParseStorageLayoutError(StorageLayoutEfiBcacheLvm, "root partition file system is \"%s\", not \"ext4\"" % (fs))
        else:
            raise ParseStorageLayoutError(StorageLayoutEfiBcacheLvm, "logical volume \"/dev/mapper/hdd.root\" does not exist")

        # ret.ssd
        ret.ssd = util.devPathPartitionToDisk(bootDev)
        if ret.ssd not in ret.lvmPvHddDict:
            # ret.ssdEspParti
            ret.ssdEspParti = util.devPathDiskToPartition(ret.ssd, 1)
            if ret.ssdEspParti != bootDev:
                raise ParseStorageLayoutError(StorageLayoutEfiBcacheLvm, "SSD is not boot device")
            if util.getBlkDevSize(ret.ssdEspParti) != _espPartiSize:
                raise ParseStorageLayoutError(StorageLayoutEfiBcacheLvm, "%s has an invalid size" % (ret.ssdEspParti))

            # ret.ssdSwapParti
            ret.ssdSwapParti = util.devPathDiskToPartition(ret.ssd, 2)
            if not os.path.exists(ret.ssdSwapParti):
                raise ParseStorageLayoutError(StorageLayoutEfiBcacheLvm, "SSD has no swap partition")
            if util.getBlkDevFsType(ret.ssdSwapParti) != "swap":
                raise ParseStorageLayoutError(StorageLayoutEfiBcacheLvm, "swap device %s has an invalid file system" % (ret.ssdSwapParti))

            # ret.ssdCacheParti
            ret.ssdCacheParti = util.devPathDiskToPartition(ret.ssd, 3)
            if not os.path.exists(ret.ssdCacheParti):
                raise ParseStorageLayoutError(StorageLayoutEfiBcacheLvm, "SSD has no cache partition")

            for pvHdd, bcacheDev in ret.lvmPvHddDict.items():
                tlist = util.bcacheGetSlaveDevPathList(bcacheDev)
                if len(tlist) < 2:
                    raise ParseStorageLayoutError(StorageLayoutEfiBcacheLvm, "%s(%s) has no cache device" % (pvHdd, bcacheDev))
                if len(tlist) > 2:
                    raise ParseStorageLayoutError(StorageLayoutEfiBcacheLvm, "%s(%s) has multiple cache devices" % (pvHdd, bcacheDev))
                if tlist[0] != ret.ssdCacheParti:
                    raise ParseStorageLayoutError(StorageLayoutEfiBcacheLvm, "%s(%s) has invalid cache device" % (pvHdd, bcacheDev))
            if True:
                partName, partId = util.devPathPartitionToDiskAndPartitionId(ret.ssdCacheParti)
                nextPartName = util.devPathDiskToPartition(partName, partId + 1)
                if os.path.exists(nextPartName):
                    raise ParseStorageLayoutError(StorageLayoutEfiBcacheLvm, "redundant partition exists on %s" % (ret.ssd))
        else:
            ret.ssd = None

        # ret.bootHdd
        if ret.ssd is None:
            ret.bootHdd = util.devPathPartitionToDisk(bootDev)

        return ret

    @staticmethod
    def _getBiosSimpleLayout(rootDev):
        ret = StorageLayoutBiosSimple()

        # ret.hdd
        ret.hdd = util.devPathPartitionToDisk(rootDev)
        if util.getBlkDevPartitionTableType(ret.hdd) != "dos":
            raise ParseStorageLayoutError(StorageLayoutBiosSimple, "partition type of %s is not \"dos\"" % (ret.hdd))

        # ret.hddRootParti
        ret.hddRootParti = rootDev
        fs = util.getBlkDevFsType(ret.hddRootParti)
        if fs != "ext4":
            raise ParseStorageLayoutError(StorageLayoutBiosSimple, "root partition file system is \"%s\", not \"ext4\"" % (fs))

        return ret

    @staticmethod
    def _getBiosLvmLayout():
        ret = StorageLayoutBiosLvm()

        # ret.lvmVg
        if not util.cmdCallTestSuccess("/sbin/lvm", "vgdisplay", "hdd"):
            raise ParseStorageLayoutError(StorageLayoutBiosLvm, "volume group \"hdd\" does not exist")
        ret.lvmVg = "hdd"

        # ret.lvmPvHddList
        out = util.cmdCall("/sbin/lvm", "pvdisplay", "-c")
        for m in re.finditer("(/dev/\\S+):hdd:.*", out, re.M):
            hdd = util.devPathPartitionToDisk(m.group(1))
            if util.getBlkDevPartitionTableType(hdd) != "dos":
                raise ParseStorageLayoutError(StorageLayoutBiosLvm, "partition type of %s is not \"dos\"" % (hdd))
            if os.path.exists(util.devPathDiskToPartition(hdd, 2)):
                raise ParseStorageLayoutError(StorageLayoutBiosLvm, "redundant partition exists on %s" % (hdd))
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
                    raise ParseStorageLayoutError(StorageLayoutBiosLvm, "root partition file system is \"%s\", not \"ext4\"" % (fs))
            else:
                raise ParseStorageLayoutError(StorageLayoutBiosLvm, "logical volume \"/dev/mapper/hdd.root\" does not exist")

            # ret.lvmSwapLv
            if re.search("/dev/hdd/swap:hdd:.*", out, re.M) is not None:
                ret.lvmSwapLv = "swap"
                if os.path.exists("/dev/mapper/hdd.swap"):
                    if util.getBlkDevFsType("/dev/mapper/hdd.swap") != "swap":
                        raise ParseStorageLayoutError(StorageLayoutBiosLvm, "/dev/mapper/hdd.swap has an invalid file system")
                elif os.path.exists("/dev/mapper/hdd-swap"):                # compatible with old lvm version
                    if util.getBlkDevFsType("/dev/mapper/hdd-swap") != "swap":
                        raise ParseStorageLayoutError(StorageLayoutBiosLvm, "/dev/mapper/hdd.swap has an invalid file system")
                else:
                    assert False

        # ret.bootHdd
        for hdd in ret.lvmPvHddList:
            with open(hdd, "rb") as f:
                if not util.isBufferAllZero(f.read(440)):
                    if ret.bootHdd is not None:
                        raise ParseStorageLayoutError(StorageLayoutBiosLvm, "boot-code exists on multiple harddisks")
                    ret.bootHdd = hdd
        if ret.bootHdd is None:
            raise ParseStorageLayoutError(StorageLayoutBiosLvm, "no harddisk has boot-code")

        return ret


_espPartiSize = 512 * 1024 * 1024
_espPartiSizeStr = "512MiB"

_swapSizeInGb = util.getPhysicalMemorySize() * 2
_swapSize = _swapSizeInGb * 1024 * 1024 * 1024
_swapPartiSizeStr = "%dGiB" % (_swapSizeInGb)

_swapFilename = "/var/swap.dat"
