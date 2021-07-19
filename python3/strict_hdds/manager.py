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


from . import ParseStorageLayoutError
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
        self.layout_name = layout_class.name
        self.message = message


class _StorageLayoutCreator:

    @staticmethod
    def createLayoutBiosSimple(self, hdd=None):
        if hdd is None:
            hddList = _Util.getDevPathListForFixedHdd()
            if len(hddList) == 0:
                raise Exception("no harddisks")
            if len(hddList) > 1:
                raise Exception("multiple harddisks")
            hdd = hddList[0]

        # create partitions
        _Util.initializeDisk(hdd, "mbr", [
            ("*", "ext4"),
        ])

    @staticmethod
    def createLayoutBiosLvm(self, hddList=None):
        if hddList is None:
            hddList = _Util.getDevPathListForFixedHdd()
            if len(hddList) == 0:
                raise Exception("no harddisks")
        else:
            assert len(hddList) > 0

        vgCreated = False

        for devpath in hddList:
            # create partitions
            _Util.initializeDisk(devpath, "mbr", [
                ("*", "lvm"),
            ])

            # create lvm physical volume on partition1 and add it to volume group
            parti = _Util.devPathDiskToPartition(devpath, 1)
            _Util.cmdCall("/sbin/lvm", "pvcreate", parti)
            if not vgCreated:
                _Util.cmdCall("/sbin/lvm", "vgcreate", "hdd", parti)
                vgCreated = True
            else:
                _Util.cmdCall("/sbin/lvm", "vgextend", "hdd", parti)

        # create root lv
        out = _Util.cmdCall("/sbin/lvm", "vgdisplay", "-c", "hdd")
        freePe = int(out.split(":")[15])
        _Util.cmdCall("/sbin/lvm", "lvcreate", "-l", "%d" % (freePe // 2), "-n", "root", "hdd")

    @staticmethod
    def createLayoutEfiSimple(self, hdd=None):
        if hdd is None:
            hddList = _Util.getDevPathListForFixedHdd()
            if len(hddList) == 0:
                raise Exception("no harddisks")
            if len(hddList) > 1:
                raise Exception("multiple harddisks")
            hdd = hddList[0]

        # create partitions
        _Util.initializeDisk(hdd, "gpt", [
            (self.espPartiSizeStr, "vfat"),
            ("*", "ext4"),
        ])

    @staticmethod
    def createLayoutEfiLvm(self, hddList=None):
        if hddList is None:
            hddList = _Util.getDevPathListForFixedHdd()
            if len(hddList) == 0:
                raise Exception("no harddisks")
        else:
            assert len(hddList) > 0

        vgCreated = False

        for devpath in hddList:
            # create partitions
            _Util.initializeDisk(devpath, "gpt", [
                (self.espPartiSizeStr, "vfat"),
                ("*", "lvm"),
            ])

            # fill partition1
            parti = _Util.devPathDiskToPartition(devpath, 1)
            _Util.cmdCall("/usr/sbin/mkfs.vfat", parti)

            # create lvm physical volume on partition2 and add it to volume group
            parti = _Util.devPathDiskToPartition(devpath, 2)
            _Util.cmdCall("/sbin/lvm", "pvcreate", parti)
            if not vgCreated:
                _Util.cmdCall("/sbin/lvm", "vgcreate", "hdd", parti)
                vgCreated = True
            else:
                _Util.cmdCall("/sbin/lvm", "vgextend", "hdd", parti)

        # create root lv
        out = _Util.cmdCall("/sbin/lvm", "vgdisplay", "-c", "hdd")
        freePe = int(out.split(":")[15])
        _Util.cmdCall("/sbin/lvm", "lvcreate", "-l", "%d" % (freePe // 2), "-n", "root", "hdd")

    @staticmethod
    def createLayoutEfiBcacheLvm(self, ssd=None, hddList=None):
        if ssd is None and hddList is None:
            ssdList = []
            for devpath in _Util.getDevPathListForFixedHdd():
                if _Util.isBlkDevSsdOrHdd(devpath):
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
            _Util.initializeDisk(ssd, "gpt", [
                (self.espPartiSizeStr, "esp"),
                (self.swapPartiSizeStr, "swap"),
                ("*", "bcache"),
            ])

            # sync partition1 as boot partition
            parti = _Util.devPathDiskToPartition(ssd, 1)
            _Util.cmdCall("/usr/sbin/mkfs.vfat", parti)

            # make partition2 as swap partition
            parti = _Util.devPathDiskToPartition(ssd, 2)
            _Util.cmdCall("/sbin/mkswap", parti)

            # make partition3 as cache partition
            parti = _Util.devPathDiskToPartition(ssd, 3)
            _Util.bcacheMakeDevice(parti, False)
            with open("/sys/fs/bcache/register", "w") as f:
                f.write(parti)
            setUuid = _Util.bcacheGetSetUuid(parti)

        for devpath in hddList:
            # create partitions
            _Util.initializeDisk(devpath, "gpt", [
                (self.espPartiSizeStr, "vfat"),
                ("*", "bcache"),
            ])

            # fill partition1
            parti = _Util.devPathDiskToPartition(devpath, 1)
            _Util.cmdCall("/usr/sbin/mkfs.vfat", parti)

            # add partition2 to bcache
            parti = _Util.devPathDiskToPartition(devpath, 2)
            _Util.bcacheMakeDevice(parti, True)
            with open("/sys/fs/bcache/register", "w") as f:
                f.write(parti)
            bcacheDev = _Util.bcacheFindByBackingDevice(parti)
            if ssd is not None:
                with open("/sys/block/%s/bcache/attach" % (os.path.basename(bcacheDev)), "w") as f:
                    f.write(str(setUuid))

            # create lvm physical volume on bcache device and add it to volume group
            _Util.cmdCall("/sbin/lvm", "pvcreate", bcacheDev)
            if not vgCreated:
                _Util.cmdCall("/sbin/lvm", "vgcreate", "hdd", bcacheDev)
                vgCreated = True
            else:
                _Util.cmdCall("/sbin/lvm", "vgextend", "hdd", bcacheDev)

        # create root lv
        out = _Util.cmdCall("/sbin/lvm", "vgdisplay", "-c", "hdd")
        freePe = int(out.split(":")[15])
        _Util.cmdCall("/sbin/lvm", "lvcreate", "-l", "%d" % (freePe // 2), "-n", "root", "hdd")


class _StorageLayoutParser:

    @staticmethod
    def getStorageLayout(self):
        rootDev = FmUtil.getMountDeviceForPath("/")
        bootDev = FmUtil.getMountDeviceForPath("/boot")

        assert rootDev is not None
        if bootDev is not None:
            try:
                lvmInfo = FmUtil.getBlkDevLvmInfo(rootDev)
                if lvmInfo is not None:
                    tlist = FmUtil.lvmGetSlaveDevPathList(lvmInfo[0])
                    if any(re.fullmatch("/dev/bcache[0-9]+", x) is not None for x in tlist):
                        ret = self._getEfiBcacheLvmLayout(bootDev)
                    else:
                        ret = self._getEfiLvmLayout(bootDev)
                else:
                    ret = self._getEfiSimpleLayout(bootDev, rootDev)
            except ParseStorageLayoutError as e:
                return StorageLayoutNonStandard(True, None, bootDev, rootDev, e.layoutName, e.message)
        else:
            try:
                if FmUtil.getBlkDevLvmInfo(rootDev) is not None:
                    ret = self._getBiosLvmLayout()
                else:
                    ret = self._getBiosSimpleLayout(rootDev)
            except ParseStorageLayoutError as e:
                if e.layoutName == StorageLayoutBiosLvm.name:
                    # get harddisk for lvm volume group
                    diskSet = set()
                    lvmInfo = FmUtil.getBlkDevLvmInfo(rootDev)
                    for slaveDev in FmUtil.lvmGetSlaveDevPathList(lvmInfo[0]):
                        if FmUtil.devPathIsDiskOrPartition(slaveDev):
                            diskSet.add(slaveDev)
                        else:
                            diskSet.add(FmUtil.devPathPartitionToDisk(slaveDev))

                    # check which disk has Boot Code
                    # return the first disk if no disk has Boot Code
                    bootHdd = None
                    for d in sorted(list(diskSet)):
                        with open(d, "rb") as f:
                            if not FmUtil.isBufferAllZero(f.read(440)):
                                bootHdd = d
                                break
                    if bootHdd is None:
                        bootHdd = sorted(list(diskSet))[0]
                elif e.layoutName == StorageLayoutBiosSimple.name:
                    bootHdd = FmUtil.devPathPartitionToDisk(rootDev)
                else:
                    assert False
                return StorageLayoutNonStandard(False, bootHdd, None, rootDev, e.layoutName, e.message)

        assert ret.isReady()
        return ret

    @staticmethod
    def _getEfiSimpleLayout(self, bootDev, rootDev):
        if not FmUtil.gptIsEspPartition(bootDev):
            raise ParseStorageLayoutError(StorageLayoutEfiSimple, "boot device is not ESP partitiion")

        ret = StorageLayoutEfiSimple()

        # ret.hdd
        ret.hdd = FmUtil.devPathPartitionToDisk(bootDev)
        if ret.hdd != FmUtil.devPathPartitionToDisk(rootDev):
            raise ParseStorageLayoutError(StorageLayoutEfiSimple, "boot device and root device is not the same")

        # ret.hddEspParti
        ret.hddEspParti = bootDev

        # ret.hddRootParti
        ret.hddRootParti = rootDev
        if True:
            fs = FmUtil.getBlkDevFsType(ret.hddRootParti)
            if fs != "ext4":
                raise ParseStorageLayoutError(StorageLayoutEfiSimple, "root partition file system is \"%s\", not \"ext4\"" % (fs))

        # ret.swapFile
        if os.path.exists(_swapFilename) and FmUtil.cmdCallTestSuccess("/sbin/swaplabel", _swapFilename):
            ret.swapFile = _swapFilename

        return ret

    @staticmethod
    def _getEfiLvmLayout(self, bootDev):
        if not FmUtil.gptIsEspPartition(bootDev):
            raise ParseStorageLayoutError(StorageLayoutEfiLvm, "boot device is not ESP partitiion")

        ret = StorageLayoutEfiLvm()

        # ret.bootHdd
        ret.bootHdd = FmUtil.devPathPartitionToDisk(bootDev)

        # ret.lvmVg
        if not FmUtil.cmdCallTestSuccess("/sbin/lvm", "vgdisplay", "hdd"):
            raise ParseStorageLayoutError(StorageLayoutEfiLvm, "volume group \"hdd\" does not exist")
        ret.lvmVg = "hdd"

        # ret.lvmPvHddList
        out = FmUtil.cmdCall("/sbin/lvm", "pvdisplay", "-c")
        for m in re.finditer("(/dev/\\S+):hdd:.*", out, re.M):
            hdd, partId = FmUtil.devPathPartitionToDiskAndPartitionId(m.group(1))
            if FmUtil.getBlkDevPartitionTableType(hdd) != "gpt":
                raise ParseStorageLayoutError(StorageLayoutEfiLvm, "partition type of %s is not \"gpt\"" % (hdd))
            if partId != 2:
                raise ParseStorageLayoutError(StorageLayoutEfiLvm, "physical volume partition of %s is not %s" % (hdd, FmUtil.devPathDiskToPartition(hdd, 2)))
            if FmUtil.getBlkDevSize(FmUtil.devPathDiskToPartition(hdd, 1)) != self.espPartiSize:
                raise ParseStorageLayoutError(StorageLayoutEfiLvm, "%s has an invalid size" % (FmUtil.devPathDiskToPartition(hdd, 1)))
            if os.path.exists(FmUtil.devPathDiskToPartition(hdd, 3)):
                raise ParseStorageLayoutError(StorageLayoutEfiLvm, "redundant partition exists on %s" % (hdd))
            ret.lvmPvHddList.append(hdd)

        out = FmUtil.cmdCall("/sbin/lvm", "lvdisplay", "-c")
        if True:
            # ret.lvmRootLv
            if re.search("/dev/hdd/root:hdd:.*", out, re.M) is not None:
                ret.lvmRootLv = "root"
                if os.path.exists("/dev/mapper/hdd.root"):
                    fs = FmUtil.getBlkDevFsType("/dev/mapper/hdd.root")
                elif os.path.exists("/dev/mapper/hdd-root"):                # compatible with old lvm version
                    fs = FmUtil.getBlkDevFsType("/dev/mapper/hdd-root")
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
                    if FmUtil.getBlkDevFsType("/dev/mapper/hdd.swap") != "swap":
                        raise ParseStorageLayoutError(StorageLayoutEfiLvm, "/dev/mapper/hdd.swap has an invalid file system")
                elif os.path.exists("/dev/mapper/hdd-swap"):                    # compatible with old lvm version
                    if FmUtil.getBlkDevFsType("/dev/mapper/hdd-swap") != "swap":
                        raise ParseStorageLayoutError(StorageLayoutEfiLvm, "/dev/mapper/hdd.swap has an invalid file system")
                else:
                    assert False
        return ret

    @staticmethod
    def _getEfiBcacheLvmLayout(self, bootDev):
        if not FmUtil.gptIsEspPartition(bootDev):
            raise ParseStorageLayoutError(StorageLayoutEfiBcacheLvm, "boot device is not ESP partitiion")

        ret = StorageLayoutEfiBcacheLvm()

        # ret.lvmVg
        if not FmUtil.cmdCallTestSuccess("/sbin/lvm", "vgdisplay", "hdd"):
            raise ParseStorageLayoutError(StorageLayoutEfiBcacheLvm, "volume group \"hdd\" does not exist")
        ret.lvmVg = "hdd"

        # ret.lvmPvHddDict
        out = FmUtil.cmdCall("/sbin/lvm", "pvdisplay", "-c")
        for m in re.finditer("(/dev/\\S+):hdd:.*", out, re.M):
            if re.fullmatch("/dev/bcache[0-9]+", m.group(1)) is None:
                raise ParseStorageLayoutError(StorageLayoutEfiBcacheLvm, "volume group \"hdd\" has non-bcache physical volume")
            bcacheDev = m.group(1)
            tlist = FmUtil.bcacheGetSlaveDevPathList(bcacheDev)
            hddDev, partId = FmUtil.devPathPartitionToDiskAndPartitionId(tlist[-1])
            if partId != 2:
                raise ParseStorageLayoutError(StorageLayoutEfiBcacheLvm, "physical volume partition of %s is not %s" % (hddDev, FmUtil.devPathDiskToPartition(hddDev, 2)))
            if os.path.exists(FmUtil.devPathDiskToPartition(hddDev, 3)):
                raise ParseStorageLayoutError(StorageLayoutEfiBcacheLvm, "redundant partition exists on %s" % (hddDev))
            ret.lvmPvHddDict[hddDev] = bcacheDev

        # ret.lvmRootLv
        out = FmUtil.cmdCall("/sbin/lvm", "lvdisplay", "-c")
        if re.search("/dev/hdd/root:hdd:.*", out, re.M) is not None:
            ret.lvmRootLv = "root"
            if os.path.exists("/dev/mapper/hdd.root"):
                fs = FmUtil.getBlkDevFsType("/dev/mapper/hdd.root")
            elif os.path.exists("/dev/mapper/hdd-root"):                    # compatible with old lvm version
                fs = FmUtil.getBlkDevFsType("/dev/mapper/hdd-root")
            else:
                assert False
            if fs != "ext4":
                raise ParseStorageLayoutError(StorageLayoutEfiBcacheLvm, "root partition file system is \"%s\", not \"ext4\"" % (fs))
        else:
            raise ParseStorageLayoutError(StorageLayoutEfiBcacheLvm, "logical volume \"/dev/mapper/hdd.root\" does not exist")

        # ret.ssd
        ret.ssd = FmUtil.devPathPartitionToDisk(bootDev)
        if ret.ssd not in ret.lvmPvHddDict:
            # ret.ssdEspParti
            ret.ssdEspParti = FmUtil.devPathDiskToPartition(ret.ssd, 1)
            if ret.ssdEspParti != bootDev:
                raise ParseStorageLayoutError(StorageLayoutEfiBcacheLvm, "SSD is not boot device")
            if FmUtil.getBlkDevSize(ret.ssdEspParti) != self.espPartiSize:
                raise ParseStorageLayoutError(StorageLayoutEfiBcacheLvm, "%s has an invalid size" % (ret.ssdEspParti))

            # ret.ssdSwapParti
            ret.ssdSwapParti = FmUtil.devPathDiskToPartition(ret.ssd, 2)
            if not os.path.exists(ret.ssdSwapParti):
                raise ParseStorageLayoutError(StorageLayoutEfiBcacheLvm, "SSD has no swap partition")
            if FmUtil.getBlkDevFsType(ret.ssdSwapParti) != "swap":
                raise ParseStorageLayoutError(StorageLayoutEfiBcacheLvm, "swap device %s has an invalid file system" % (ret.ssdSwapParti))

            # ret.ssdCacheParti
            ret.ssdCacheParti = FmUtil.devPathDiskToPartition(ret.ssd, 3)
            if not os.path.exists(ret.ssdCacheParti):
                raise ParseStorageLayoutError(StorageLayoutEfiBcacheLvm, "SSD has no cache partition")

            for pvHdd, bcacheDev in ret.lvmPvHddDict.items():
                tlist = FmUtil.bcacheGetSlaveDevPathList(bcacheDev)
                if len(tlist) < 2:
                    raise ParseStorageLayoutError(StorageLayoutEfiBcacheLvm, "%s(%s) has no cache device" % (pvHdd, bcacheDev))
                if len(tlist) > 2:
                    raise ParseStorageLayoutError(StorageLayoutEfiBcacheLvm, "%s(%s) has multiple cache devices" % (pvHdd, bcacheDev))
                if tlist[0] != ret.ssdCacheParti:
                    raise ParseStorageLayoutError(StorageLayoutEfiBcacheLvm, "%s(%s) has invalid cache device" % (pvHdd, bcacheDev))
            if True:
                partName, partId = FmUtil.devPathPartitionToDiskAndPartitionId(ret.ssdCacheParti)
                nextPartName = FmUtil.devPathDiskToPartition(partName, partId + 1)
                if os.path.exists(nextPartName):
                    raise ParseStorageLayoutError(StorageLayoutEfiBcacheLvm, "redundant partition exists on %s" % (ret.ssd))
        else:
            ret.ssd = None

        # ret.bootHdd
        if ret.ssd is None:
            ret.bootHdd = FmUtil.devPathPartitionToDisk(bootDev)

        return ret

    @staticmethod
    def _getBiosSimpleLayout(self, rootDev):
        ret = StorageLayoutBiosSimple()

        # ret.hdd
        ret.hdd = FmUtil.devPathPartitionToDisk(rootDev)
        if FmUtil.getBlkDevPartitionTableType(ret.hdd) != "dos":
            raise ParseStorageLayoutError(StorageLayoutBiosSimple, "partition type of %s is not \"dos\"" % (ret.hdd))

        # ret.hddRootParti
        ret.hddRootParti = rootDev
        fs = FmUtil.getBlkDevFsType(ret.hddRootParti)
        if fs != "ext4":
            raise ParseStorageLayoutError(StorageLayoutBiosSimple, "root partition file system is \"%s\", not \"ext4\"" % (fs))

        # ret.swapFile
        if os.path.exists(_swapFilename) and FmUtil.cmdCallTestSuccess("/sbin/swaplabel", _swapFilename):
            ret.swapFile = _swapFilename

        return ret

    @staticmethod
    def _getBiosLvmLayout(self):
        ret = StorageLayoutBiosLvm()

        # ret.lvmVg
        if not FmUtil.cmdCallTestSuccess("/sbin/lvm", "vgdisplay", "hdd"):
            raise ParseStorageLayoutError(StorageLayoutBiosLvm, "volume group \"hdd\" does not exist")
        ret.lvmVg = "hdd"

        # ret.lvmPvHddList
        out = FmUtil.cmdCall("/sbin/lvm", "pvdisplay", "-c")
        for m in re.finditer("(/dev/\\S+):hdd:.*", out, re.M):
            hdd = FmUtil.devPathPartitionToDisk(m.group(1))
            if FmUtil.getBlkDevPartitionTableType(hdd) != "dos":
                raise ParseStorageLayoutError(StorageLayoutBiosLvm, "partition type of %s is not \"dos\"" % (hdd))
            if os.path.exists(FmUtil.devPathDiskToPartition(hdd, 2)):
                raise ParseStorageLayoutError(StorageLayoutBiosLvm, "redundant partition exists on %s" % (hdd))
            ret.lvmPvHddList.append(hdd)

        out = FmUtil.cmdCall("/sbin/lvm", "lvdisplay", "-c")
        if True:
            # ret.lvmRootLv
            if re.search("/dev/hdd/root:hdd:.*", out, re.M) is not None:
                ret.lvmRootLv = "root"
                if os.path.exists("/dev/mapper/hdd.root"):
                    fs = FmUtil.getBlkDevFsType("/dev/mapper/hdd.root")
                elif os.path.exists("/dev/mapper/hdd-root"):                # compatible with old lvm version
                    fs = FmUtil.getBlkDevFsType("/dev/mapper/hdd-root")
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
                    if FmUtil.getBlkDevFsType("/dev/mapper/hdd.swap") != "swap":
                        raise ParseStorageLayoutError(StorageLayoutBiosLvm, "/dev/mapper/hdd.swap has an invalid file system")
                elif os.path.exists("/dev/mapper/hdd-swap"):                # compatible with old lvm version
                    if FmUtil.getBlkDevFsType("/dev/mapper/hdd-swap") != "swap":
                        raise ParseStorageLayoutError(StorageLayoutBiosLvm, "/dev/mapper/hdd.swap has an invalid file system")
                else:
                    assert False

        # ret.bootHdd
        for hdd in ret.lvmPvHddList:
            with open(hdd, "rb") as f:
                if not FmUtil.isBufferAllZero(f.read(440)):
                    if ret.bootHdd is not None:
                        raise ParseStorageLayoutError(StorageLayoutBiosLvm, "boot-code exists on multiple harddisks")
                    ret.bootHdd = hdd
        if ret.bootHdd is None:
            raise ParseStorageLayoutError(StorageLayoutBiosLvm, "no harddisk has boot-code")

        return ret
