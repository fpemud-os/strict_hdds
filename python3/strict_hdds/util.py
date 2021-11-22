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
import glob
import uuid
import time
import stat
import psutil
import crcmod
import parted
import struct
import pathlib
import tempfile
import subprocess


class Util:

    bootDir = "/boot"

    swapFilepath = "/var/cache/swap.dat"

    diskPartTableMbr = "mbr"
    diskPartTableGpt = "gpt"

    fsTypeExt4 = "ext4"
    fsTypeFat = "vfat"
    fsTypeBtrfs = "btrfs"
    fsTypeBcachefs = "bcachefs"
    fsTypeSwap = "swap"

    @staticmethod
    def listRemoveNoValueError(tlist, value):
        try:
            tlist.remove(value)
        except ValueError:
            pass

    @staticmethod
    def keyValueListToDict(keyList, valueList):
        assert len(keyList) == len(valueList)
        ret = dict()
        for i in range(0, len(keyList)):
            ret[keyList[i]] = valueList[i]
        return ret

    @staticmethod
    def anyIn(list1, list2):
        for i in list1:
            if i in list2:
                return True
        return False

    @staticmethod
    def modName2layoutName(modName):
        assert modName.startswith("layout_")
        return modName[len("layout_"):].replace("_", "-")

    @staticmethod
    def layoutName2modName(layoutName):
        return "layout_" + layoutName.replace("-", "_")

    @staticmethod
    def mntOptsStrToList(mntOptsStr):
        if mntOptsStr == "":
            return []
        else:
            return mntOptsStr.split(",")

    @staticmethod
    def mntOptsListToStr(mntOptsList):
        if len(mntOptsList) == 0:
            return ""
        else:
            return ",".join(mntOptsList)

    @staticmethod
    def mntGetSubVol(mountPoint):
        for pobj in psutil.disk_partitions():
            if pobj.mountpoint == mountPoint:
                for mo in pobj.opts.split(","):
                    m = re.fullmatch("subvol=(.+)", mo)
                    if m is not None:
                        return m.group(1)
                return None
        return None

    @staticmethod
    def getPhysicalMemorySizeInGb():
        with open("/proc/meminfo", "r") as f:
            # We return memory size in GB.
            # Since the memory size shown in /proc/meminfo is always a
            # little less than the real size because various sort of
            # reservation, so we do a "+1"
            m = re.search("^MemTotal:\\s+(\\d+)", f.read())
            return int(m.group(1)) // 1024 // 1024 + 1

    @staticmethod
    def cmdCall(cmd, *kargs):
        # call command to execute backstage job
        #
        # scenario 1, process group receives SIGTERM, SIGINT and SIGHUP:
        #   * callee must auto-terminate, and cause no side-effect
        #   * caller must be terminated by signal, not by detecting child-process failure
        # scenario 2, caller receives SIGTERM, SIGINT, SIGHUP:
        #   * caller is terminated by signal, and NOT notify callee
        #   * callee must auto-terminate, and cause no side-effect, after caller is terminated
        # scenario 3, callee receives SIGTERM, SIGINT, SIGHUP:
        #   * caller detects child-process failure and do appopriate treatment

        ret = subprocess.run([cmd] + list(kargs),
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                             universal_newlines=True)
        if ret.returncode > 128:
            # for scenario 1, caller's signal handler has the oppotunity to get executed during sleep
            time.sleep(1.0)
        if ret.returncode != 0:
            print(ret.stdout)
            ret.check_returncode()
        return ret.stdout.rstrip()

    @staticmethod
    def cmdCallWithRetCode(cmd, *kargs):
        ret = subprocess.run([cmd] + list(kargs),
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                             universal_newlines=True)
        if ret.returncode > 128:
            time.sleep(1.0)
        return (ret.returncode, ret.stdout.rstrip())

    @staticmethod
    def cmdCallTestSuccess(cmd, *kargs):
        ret = subprocess.run([cmd] + list(kargs),
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                             universal_newlines=True)
        if ret.returncode > 128:
            time.sleep(1.0)
        return (ret.returncode == 0)

    @staticmethod
    def cmdExec(cmd, *kargs):
        # call command to execute frontend job
        #
        # scenario 1, process group receives SIGTERM, SIGINT and SIGHUP:
        #   * callee must auto-terminate, and cause no side-effect
        #   * caller must be terminate AFTER child-process, and do neccessary finalization
        #   * termination information should be printed by callee, not caller
        # scenario 2, caller receives SIGTERM, SIGINT, SIGHUP:
        #   * caller should terminate callee, wait callee to stop, do neccessary finalization, print termination information, and be terminated by signal
        #   * callee does not need to treat this scenario specially
        # scenario 3, callee receives SIGTERM, SIGINT, SIGHUP:
        #   * caller detects child-process failure and do appopriate treatment
        #   * callee should print termination information

        # FIXME, the above condition is not met, FmUtil.shellExec has the same problem

        ret = subprocess.run([cmd] + list(kargs), universal_newlines=True)
        if ret.returncode > 128:
            time.sleep(1.0)
        ret.check_returncode()

    @staticmethod
    def shellExec(cmd):
        ret = subprocess.run(cmd, shell=True, universal_newlines=True)
        if ret.returncode > 128:
            time.sleep(1.0)
        ret.check_returncode()

    @staticmethod
    def wipeHarddisk(devpath):
        pass
        # with open(devpath, 'wb') as f:
        #     f.write(bytearray(1024))

    @staticmethod
    def isHarddiskClean(devpath):
        with open(devpath, 'rb') as f:
            return Util.isBufferAllZero(f.read(1024))

    @staticmethod
    def isBlkDevSsdOrHdd(devPath):
        bn = os.path.basename(devPath)
        with open("/sys/block/%s/queue/rotational" % (bn), "r") as f:
            buf = f.read().strip("\n")
            if buf == "1":
                return False
        return True

    @staticmethod
    def getBlkDevSize(devPath):
        out = Util.cmdCall("/sbin/blockdev", "--getsz", devPath)
        return int(out) * 512        # unit is byte

    @staticmethod
    def getBlkDevPartitionTableType(devPath):
        if not PartiUtil.isDiskOrParti(devPath):
            devPath = PartiUtil.partiToDisk(devPath)

        ret = Util.cmdCall("/sbin/blkid", "-o", "export", devPath)
        m = re.search("^PTTYPE=(\\S+)$", ret, re.M)
        if m is not None:
            if m.group(1) == "gpt":
                return Util.diskPartTableGpt
            elif m.group(1) == "dos":
                return Util.diskPartTableMbr
            else:
                return m.group(1)
        else:
            return ""

    @staticmethod
    def getBlkDevFsType(devPath):
        ret = Util.cmdCall("/sbin/blkid", "-o", "export", devPath)
        m = re.search("^TYPE=(\\S+)$", ret, re.M)
        if m is not None:
            return m.group(1).lower()
        else:
            return ""

    @staticmethod
    def getBlkDevLvmInfo(devPath):
        """Returns (vg-name, lv-name)
            Returns None if the device is not lvm"""

        if "-" in devPath:
            splitter = "-"
        elif "." in devPath:
            splitter = "."
        else:
            return None

        rc, ret = Util.cmdCallWithRetCode("/sbin/dmsetup", "info", devPath)
        if rc != 0:
            return None

        m = re.search("^Name: *(\\S+)$", ret, re.M)
        assert m is not None
        return m.group(1).split(splitter)

    @staticmethod
    def getBlkDevCapacity(devPath):
        ret = Util.cmdCall("/bin/df", "-BM", devPath)
        m = re.search("%s +(\\d+)M +(\\d+)M +\\d+M", ret, re.M)
        total = int(m.group(1))
        used = int(m.group(2))
        return (total, used)        # unit: MB

    @staticmethod
    def syncBlkDev(devPath1, devPath2, mountPoint1=None, mountPoint2=None):
        if Util.getBlkDevSize(devPath1) != Util.getBlkDevSize(devPath2):
            raise Exception("%s and %s have different size" % (devPath1, devPath2))
        if Util.getBlkDevFsType(devPath1) != Util.getBlkDevFsType(devPath2):
            raise Exception("%s and %s have different filesystem" % (devPath1, devPath2))

        cmd = "/usr/bin/rsync -q -a --delete \"%s/\" \"%s\""        # SRC parameter has "/" postfix so that whole directory is synchronized
        if mountPoint1 is not None and mountPoint2 is not None:
            Util.shellExec(cmd % (mountPoint1, mountPoint2))
        elif mountPoint1 is not None and mountPoint2 is None:
            with TmpMount(devPath2) as mp2:
                Util.shellExec(cmd % (mountPoint1, mp2.mountpoint))
        elif mountPoint1 is None and mountPoint2 is not None:
            with TmpMount(devPath1, "ro") as mp1:
                Util.shellExec(cmd % (mp1.mountpoint, mountPoint2))
        else:
            with TmpMount(devPath1, "ro") as mp1:
                with TmpMount(devPath2) as mp2:
                    Util.shellExec(cmd % (mp1.mountpoint, mp2.mountpoint))

    @staticmethod
    def createSwapFile(path):
        Util.cmdCall("/bin/dd", "if=/dev/zero", "of=%s" % (path), "bs=%d" % (1024 * 1024), "count=%d" % (Util.getSwapSizeInGb() * 1024))
        Util.cmdCall("/bin/chmod", "600", path)
        Util.cmdCall("/sbin/mkswap", "-f", path)

    @staticmethod
    def getSwapSizeInGb():
        sz = Util.getPhysicalMemorySizeInGb()
        if sz <= 4:
            return sz * 2               # 3GB -> 6GB, 4GB -> 8GB
        else:
            return (sz + 3) // 2 * 2    # 5GB -> 8GB, 6GB -> 8GB, 7GB -> 10GB, 8GB -> 10GB

    @staticmethod
    def getSwapSize():
        return Util.getSwapSizeInGb() * 1024 * 1024 * 1024

    @staticmethod
    def getEspSizeInMb():
        return 512

    @staticmethod
    def getEspSize():
        return Util.getEspSizeInMb() * 1024 * 1024

    @staticmethod
    def initializeDisk(devPath, partitionTableType, partitionInfoList):
        assert partitionTableType in ["mbr", "gpt"]
        assert len(partitionInfoList) >= 1

        if partitionTableType == "mbr":
            partitionTableType = "msdos"

        def _getFreeRegion(disk):
            region = None
            for r in disk.getFreeSpaceRegions():
                if r.length <= disk.device.optimumAlignment.grainSize:
                    continue                                                # ignore alignment gaps
                if region is not None:
                    assert False                                            # there should be only one free region
                region = r
            if region.start < 2048:
                region.start = 2048
            return region

        def _addPartition(disk, pType, pStart, pEnd):
            region = parted.Geometry(device=disk.device, start=pStart, end=pEnd)
            if pType == "":
                partition = parted.Partition(disk=disk, type=parted.PARTITION_NORMAL, geometry=region)
            elif pType == "esp":
                assert partitionTableType == "gpt"
                partition = parted.Partition(disk=disk,
                                             type=parted.PARTITION_NORMAL,
                                             fs=parted.FileSystem(type="fat32", geometry=region),
                                             geometry=region)
                partition.setFlag(parted.PARTITION_ESP)     # which also sets flag parted.PARTITION_BOOT
            elif pType == "bcache":
                assert partitionTableType == "gpt"
                partition = parted.Partition(disk=disk, type=parted.PARTITION_NORMAL, geometry=region)
            elif pType == "swap":
                partition = parted.Partition(disk=disk, type=parted.PARTITION_NORMAL, geometry=region)
                if partitionTableType == "mbr":
                    partition.setFlag(parted.PARTITION_SWAP)
                elif partitionTableType == "gpt":
                    pass            # don't know why, it says gpt partition has no way to setFlag(SWAP)
                else:
                    assert False
            elif pType == "lvm":
                partition = parted.Partition(disk=disk, type=parted.PARTITION_NORMAL, geometry=region)
                partition.setFlag(parted.PARTITION_LVM)
            elif pType == "vfat":
                partition = parted.Partition(disk=disk,
                                             type=parted.PARTITION_NORMAL,
                                             fs=parted.FileSystem(type="fat32", geometry=region),
                                             geometry=region)
            elif pType in ["ext2", "ext4", "btrfs"]:
                partition = parted.Partition(disk=disk,
                                             type=parted.PARTITION_NORMAL,
                                             fs=parted.FileSystem(type=pType, geometry=region),
                                             geometry=region)
            else:
                assert False
            disk.addPartition(partition=partition,
                              constraint=disk.device.optimalAlignedConstraint)

        def _erasePartitionSignature(devPath, pStart, pEnd):
            # fixme: this implementation is very limited
            with open(devPath, "wb") as f:
                f.seek(pStart * 512)
                if pEnd - pStart + 1 < 32:
                    f.write(bytearray((pEnd - pStart + 1) * 512))
                else:
                    f.write(bytearray(32 * 512))

        # partitionInfoList => preList & postList
        preList = None
        postList = None
        for i in range(0, len(partitionInfoList)):
            pSize, pType = partitionInfoList[i]
            if pSize == "*":
                assert preList is None
                preList = partitionInfoList[:i]
                postList = partitionInfoList[i:]
        if preList is None:
            preList = partitionInfoList
            postList = []

        # delete all partitions
        disk = parted.freshDisk(parted.getDevice(devPath), partitionTableType)
        disk.commit()

        # process preList
        for pSize, pType in preList:
            region = _getFreeRegion(disk)
            constraint = parted.Constraint(maxGeom=region).intersect(disk.device.optimalAlignedConstraint)
            pStart = constraint.startAlign.alignUp(region, region.start)
            pEnd = constraint.endAlign.alignDown(region, region.end)

            m = re.fullmatch("([0-9]+)(MiB|GiB|TiB)", pSize)
            assert m is not None
            sectorNum = parted.sizeToSectors(int(m.group(1)), m.group(2), disk.device.sectorSize)
            if pEnd < pStart + sectorNum - 1:
                raise Exception("not enough space")

            _addPartition(disk, pType, pStart, pStart + sectorNum - 1)
            _erasePartitionSignature(devPath, pStart, pEnd)

        # process postList
        for pSize, pType in postList:
            region = _getFreeRegion(disk)
            constraint = parted.Constraint(maxGeom=region).intersect(disk.device.optimalAlignedConstraint)
            pStart = constraint.startAlign.alignUp(region, region.start)
            pEnd = constraint.endAlign.alignDown(region, region.end)

            if pSize == "*":
                _addPartition(disk, pType, pStart, pEnd)
                _erasePartitionSignature(devPath, pStart, pEnd)
            else:
                assert False

        # commit and notify kernel (don't wait kernel picks up this change by itself)
        disk.commit()
        Util.cmdCall("/sbin/partprobe")

    @staticmethod
    def toggleEspPartition(devPath, espOrRegular):
        assert isinstance(espOrRegular, bool)

        diskDevPath, partId = PartiUtil.partiToDiskAndPartiId(devPath)

        diskObj = parted.newDisk(parted.getDevice(diskDevPath))
        partObj = diskObj.partitions[partId - 1]
        if espOrRegular:
            partObj.setFlag(parted.PARTITION_ESP)
        else:
            partObj.unsetFlag(parted.PARTITION_ESP)
        diskObj.commit()

    @staticmethod
    def isBufferAllZero(buf):
        for b in buf:
            if b != 0:
                return False
        return True

    @staticmethod
    def getDevPathListForFixedDisk():
        ret = []
        for line in Util.cmdCall("/bin/lsblk", "-o", "NAME,TYPE", "-n").split("\n"):
            m = re.fullmatch("(\\S+)\\s+(\\S+)", line)
            if m is None:
                continue
            if m.group(2) != "disk":
                continue
            if re.search("/usb[0-9]+/", os.path.realpath("/sys/block/%s/device" % (m.group(1)))) is not None:      # USB device
                continue
            ret.append("/dev/" + m.group(1))
        return ret

    @staticmethod
    def getDevPathListForFixedSsdAndHdd():
        return Util.getSsdAndHddListFromFixedDiskList(Util.getDevPathListForFixedDisk())

    @staticmethod
    def splitSsdAndHddFromFixedDiskDevPathList(diskList):
        ssdList = []
        hddList = []
        for devpath in diskList:
            if Util.isBlkDevSsdOrHdd(devpath):
                ssdList.append(devpath)
            else:
                hddList.append(devpath)
        return (ssdList, hddList)

    @staticmethod
    def swapServiceName2Path(serviceName):
        serviceName = serviceName[:-5]                          # item[:-5] is to remove ".swap"
        path = Util.cmdCall("/bin/systemd-escape", "-u", serviceName)
        path = os.path.join("/", path)
        return path

    @staticmethod
    def systemdFindSwapService(path):
        for f in os.listdir("/etc/systemd/system"):
            fullf = os.path.join("/etc/systemd/system", f)
            if os.path.isfile(fullf) and fullf.endswith(".swap"):
                if os.path.realpath(path) == os.path.realpath(Util.swapServiceName2Path(f)):
                    return f
        return None

    @staticmethod
    def swapDeviceIsBusy(path):
        buf = pathlib.Path(path).read_text()
        for line in buf.split("\n")[1:]:
            if line.startswith(path + " "):
                return True
        return False


class PartiUtil:

    @staticmethod
    def isDiskOrParti(devPath):
        if re.fullmatch("/dev/sd[a-z]", devPath) is not None:
            return True
        if re.fullmatch("(/dev/sd[a-z])([0-9]+)", devPath) is not None:
            return False
        if re.fullmatch("/dev/xvd[a-z]", devPath) is not None:
            return True
        if re.fullmatch("(/dev/xvd[a-z])([0-9]+)", devPath) is not None:
            return False
        if re.fullmatch("/dev/vd[a-z]", devPath) is not None:
            return True
        if re.fullmatch("(/dev/vd[a-z])([0-9]+)", devPath) is not None:
            return False
        if re.fullmatch("/dev/nvme[0-9]+n[0-9]+", devPath) is not None:
            return True
        if re.fullmatch("(/dev/nvme[0-9]+n[0-9]+)p([0-9]+)", devPath) is not None:
            return False
        assert False

    @staticmethod
    def partiToDiskAndPartiId(partitionDevPath):
        m = re.fullmatch("(/dev/sd[a-z])([0-9]+)", partitionDevPath)
        if m is not None:
            return (m.group(1), int(m.group(2)))
        m = re.fullmatch("(/dev/xvd[a-z])([0-9]+)", partitionDevPath)
        if m is not None:
            return (m.group(1), int(m.group(2)))
        m = re.fullmatch("(/dev/vd[a-z])([0-9]+)", partitionDevPath)
        if m is not None:
            return (m.group(1), int(m.group(2)))
        m = re.fullmatch("(/dev/nvme[0-9]+n[0-9]+)p([0-9]+)", partitionDevPath)
        if m is not None:
            return (m.group(1), int(m.group(2)))
        assert False

    @staticmethod
    def partiToDisk(partitionDevPath):
        return PartiUtil.partiToDiskAndPartiId(partitionDevPath)[0]

    @staticmethod
    def diskToParti(diskDevPath, partitionId):
        m = re.fullmatch("/dev/sd[a-z]", diskDevPath)
        if m is not None:
            return diskDevPath + str(partitionId)
        m = re.fullmatch("/dev/xvd[a-z]", diskDevPath)
        if m is not None:
            return diskDevPath + str(partitionId)
        m = re.fullmatch("/dev/vd[a-z]", diskDevPath)
        if m is not None:
            return diskDevPath + str(partitionId)
        m = re.fullmatch("/dev/nvme[0-9]+n[0-9]+", diskDevPath)
        if m is not None:
            return diskDevPath + "p" + str(partitionId)
        assert False

    @staticmethod
    def diskHasParti(diskDevPath, partitionId):
        partiDevPath = PartiUtil.diskToParti(diskDevPath, partitionId)
        return os.path.exists(partiDevPath)

    @staticmethod
    def diskHasMoreParti(diskDevPath, partitionId):
        for fn in os.listdir("/dev"):
            m = re.fullmatch(os.path.basename(diskDevPath) + "([0-9]+)", fn)
            if m is not None and int(m.group(1)) > partitionId:
                return True
        return False

    @staticmethod
    def partiExists(partitionDevPath):
        return os.path.exists(partitionDevPath)


class MbrUtil:

    @staticmethod
    def hasBootCode(devPath):
        with open(devPath, "rb") as f:
            return not Util.isBufferAllZero(f.read(440))

    @staticmethod
    def wipeBootCode(devPath):
        pass
        # with open(devPath, "wb") as f:
        #     f.write(bytearray(440))


class GptUtil:

    @staticmethod
    def newGuid(guidStr):
        assert len(guidStr) == 36
        assert guidStr[8] == "-" and guidStr[13] == "-" and guidStr[18] == "-" and guidStr[23] == "-"

        # struct gpt_guid {
        #     uint32_t   time_low;
        #     uint16_t   time_mid;
        #     uint16_t   time_hi_and_version;
        #     uint8_t    clock_seq_hi;
        #     uint8_t    clock_seq_low;
        #     uint8_t    node[6];
        # };
        gptGuidFmt = "IHHBB6s"
        assert struct.calcsize(gptGuidFmt) == 16

        guidStr = guidStr.replace("-", "")

        # really obscure behavior of python3
        # see http://stackoverflow.com/questions/1463306/how-does-exec-work-with-locals
        ldict = {}
        exec("n1 = 0x" + guidStr[0:8], globals(), ldict)
        exec("n2 = 0x" + guidStr[8:12], globals(), ldict)
        exec("n3 = 0x" + guidStr[12:16], globals(), ldict)
        exec("n4 = 0x" + guidStr[16:18], globals(), ldict)
        exec("n5 = 0x" + guidStr[18:20], globals(), ldict)
        exec("n6 = bytearray()", globals(), ldict)
        for i in range(0, 6):
            exec("n6.append(0x" + guidStr[20 + i * 2:20 + (i + 1) * 2] + ")", globals(), ldict)

        return struct.pack(gptGuidFmt, ldict["n1"], ldict["n2"], ldict["n3"], ldict["n4"], ldict["n5"], ldict["n6"])

    @staticmethod
    def isEspPartition(devPath):
        # struct mbr_partition_record {
        #     uint8_t  boot_indicator;
        #     uint8_t  start_head;
        #     uint8_t  start_sector;
        #     uint8_t  start_track;
        #     uint8_t  os_type;
        #     uint8_t  end_head;
        #     uint8_t  end_sector;
        #     uint8_t  end_track;
        #     uint32_t starting_lba;
        #     uint32_t size_in_lba;
        # };
        mbrPartitionRecordFmt = "8BII"
        assert struct.calcsize(mbrPartitionRecordFmt) == 16

        # struct mbr_header {
        #     uint8_t                     boot_code[440];
        #     uint32_t                    unique_mbr_signature;
        #     uint16_t                    unknown;
        #     struct mbr_partition_record partition_record[4];
        #     uint16_t                    signature;
        # };
        mbrHeaderFmt = "440sIH%dsH" % (struct.calcsize(mbrPartitionRecordFmt) * 4)
        assert struct.calcsize(mbrHeaderFmt) == 512

        # struct gpt_entry {
        #     struct gpt_guid type;
        #     struct gpt_guid partition_guid;
        #     uint64_t        lba_start;
        #     uint64_t        lba_end;
        #     uint64_t        attrs;
        #     uint16_t        name[GPT_PART_NAME_LEN];
        # };
        gptEntryFmt = "16s16sQQQ36H"
        assert struct.calcsize(gptEntryFmt) == 128

        # struct gpt_header {
        #     uint64_t            signature;
        #     uint32_t            revision;
        #     uint32_t            size;
        #     uint32_t            crc32;
        #     uint32_t            reserved1;
        #     uint64_t            my_lba;
        #     uint64_t            alternative_lba;
        #     uint64_t            first_usable_lba;
        #     uint64_t            last_usable_lba;
        #     struct gpt_guid     disk_guid;
        #     uint64_t            partition_entry_lba;
        #     uint32_t            npartition_entries;
        #     uint32_t            sizeof_partition_entry;
        #     uint32_t            partition_entry_array_crc32;
        #     uint8_t             reserved2[512 - 92];
        # };
        gptHeaderFmt = "QIIIIQQQQ16sQIII420s"
        assert struct.calcsize(gptHeaderFmt) == 512

        # do checking
        diskDevPath, partId = PartiUtil.partiToDiskAndPartiId(devPath)
        with open(diskDevPath, "rb") as f:
            # get protective MBR
            mbrHeader = struct.unpack(mbrHeaderFmt, f.read(struct.calcsize(mbrHeaderFmt)))

            # check protective MBR header
            if mbrHeader[4] != 0xAA55:
                return False

            # check protective MBR partition entry
            found = False
            for i in range(0, 4):
                pRec = struct.unpack_from(mbrPartitionRecordFmt, mbrHeader[3], struct.calcsize(mbrPartitionRecordFmt) * i)
                if pRec[4] == 0xEE:
                    found = True
            if not found:
                return False

            # get the specified GPT partition entry
            gptHeader = struct.unpack(gptHeaderFmt, f.read(struct.calcsize(gptHeaderFmt)))
            f.seek(gptHeader[10] * 512 + struct.calcsize(gptEntryFmt) * (partId - 1))
            partEntry = struct.unpack(gptEntryFmt, f.read(struct.calcsize(gptEntryFmt)))

            # check partition GUID
            if partEntry[0] != GptUtil.newGuid("C12A7328-F81F-11D2-BA4B-00A0C93EC93B"):
                return False

        return True


class BcacheUtil:

    @staticmethod
    def getBcacheDevFromDevPath(bcacheDevPath):
        m = re.fullmatch("/dev/(bcache[0-9]+)", bcacheDevPath)
        if m is not None:
            return m.group(1)
        else:
            return None

    @staticmethod
    def makeDevice(devPath, backingDeviceOrCacheDevice, blockSize=None, bucketSize=None, dataOffset=None):
        assert isinstance(backingDeviceOrCacheDevice, bool)
        assert blockSize is None or (isinstance(blockSize, int) and blockSize > 0)
        assert bucketSize is None or (isinstance(bucketSize, int) and bucketSize > 0)
        assert dataOffset is None or (isinstance(dataOffset, int) and dataOffset > 0)

        #######################################################################
        # code from bcache-tools-1.0.8
        #######################################################################
        # struct cache_sb {
        #     uint64_t        csum;
        #     uint64_t        offset;    /* sector where this sb was written */
        #     uint64_t        version;
        #     uint8_t         magic[16];
        #     uint8_t         uuid[16];
        #     union {
        #         uint8_t     set_uuid[16];
        #         uint64_t    set_magic;
        #     };
        #     uint8_t         label[SB_LABEL_SIZE];
        #     uint64_t        flags;
        #     uint64_t        seq;
        #     uint64_t        pad[8];
        #     union {
        #         struct {
        #             /* Cache devices */
        #             uint64_t    nbuckets;      /* device size */
        #             uint16_t    block_size;    /* sectors */
        #             uint16_t    bucket_size;   /* sectors */
        #             uint16_t    nr_in_set;
        #             uint16_t    nr_this_dev;
        #         };
        #         struct {
        #             /* Backing devices */
        #             uint64_t    data_offset;
        #             /*
        #             * block_size from the cache device section is still used by
        #             * backing devices, so don't add anything here until we fix
        #             * things to not need it for backing devices anymore
        #             */
        #         };
        #     };
        #     uint32_t        last_mount;        /* time_t */
        #     uint16_t        first_bucket;
        #     union {
        #         uint16_t    njournal_buckets;
        #         uint16_t    keys;
        #     };
        #     uint64_t        d[SB_JOURNAL_BUCKETS];    /* journal buckets */
        # };
        bcacheSbFmt = "QQQ16B16B16B32BQQ8QQHHHHIHH"     # without cache_sb.d

        bcacheSbMagic = [0xc6, 0x85, 0x73, 0xf6, 0x4e, 0x1a, 0x45, 0xca,
                         0x82, 0x65, 0xf5, 0x7f, 0x48, 0xba, 0x6d, 0x81]

        if blockSize is None:
            st = os.stat(devPath)
            if stat.S_ISBLK(st.st_mode):
                out = Util.cmdCall("/sbin/blockdev", "--getss", devPath)
                blockSize = int(out) // 512
            else:
                blockSize = st.st_blksize // 512

        if bucketSize is None:
            bucketSize = 1024
        if bucketSize < blockSize:
            raise Exception("bucket size (%d) cannot be smaller than block size (%d)", bucketSize, blockSize)

        devUuid = uuid.uuid4()
        setUuid = uuid.uuid4()

        bcacheSb = bytearray(struct.calcsize(bcacheSbFmt))
        offset_content = None
        offset_version = None

        # cache_sb.csum
        p = struct.calcsize("Q")
        offset_content = p

        # cache_sb.offset
        value = 8               # SB_SECTOR
        struct.pack_into("Q", bcacheSb, p, value)
        p += struct.calcsize("Q")

        # cache_sb.version
        if backingDeviceOrCacheDevice:
            value = 1           # BCACHE_SB_VERSION_BDEV
        else:
            value = 0           # BCACHE_SB_VERSION_CDEV
        offset_version = p
        struct.pack_into("Q", bcacheSb, p, value)
        p += struct.calcsize("Q")

        # cache_sb.magic
        struct.pack_into("16B", bcacheSb, p, *bcacheSbMagic)
        p += struct.calcsize("16B")

        # cache_sb.uuid
        struct.pack_into("16B", bcacheSb, p, *devUuid.bytes)
        p += struct.calcsize("16B")

        # cache_sb.set_uuid
        struct.pack_into("16B", bcacheSb, p, *setUuid.bytes)
        p += struct.calcsize("16B")

        # cache_sb.label
        p += struct.calcsize("32B")

        # cache_sb.flags
        if backingDeviceOrCacheDevice:
            value = 0x01                        # CACHE_MODE_WRITEBACK
        else:
            value = 0x00
        struct.pack_into("Q", bcacheSb, p, value)
        p += struct.calcsize("Q")

        # cache_sb.seq
        p += struct.calcsize("Q")

        # cache_sb.pad
        p += struct.calcsize("8Q")

        if backingDeviceOrCacheDevice:
            if dataOffset is not None:
                # modify cache_sb.version
                value = 4                       # BCACHE_SB_VERSION_BDEV_WITH_OFFSET
                struct.pack_into("Q", bcacheSb, offset_version, value)

                # cache_sb.data_offset
                struct.pack_into("Q", bcacheSb, p, dataOffset)
                p += struct.calcsize("Q")
            else:
                # cache_sb.data_offset
                p += struct.calcsize("Q")
        else:
            # cache_sb.nbuckets
            value = Util.getBlkDevSize(devPath) // 512 // bucketSize
            if value < 0x80:
                raise Exception("not enough buckets: %d, need %d", value, 0x80)
            struct.pack_into("Q", bcacheSb, p, value)
            p += struct.calcsize("Q")

        # cache_sb.block_size
        struct.pack_into("H", bcacheSb, p, blockSize)
        p += struct.calcsize("H")

        # cache_sb.bucket_size
        struct.pack_into("H", bcacheSb, p, bucketSize)
        p += struct.calcsize("H")

        # cache_sb.nr_in_set
        if not backingDeviceOrCacheDevice:
            value = 1
            struct.pack_into("H", bcacheSb, p, value)
            p += struct.calcsize("H")

        # cache_sb.nr_this_dev
        p += struct.calcsize("H")

        # cache_sb.last_mount
        p += struct.calcsize("I")

        # cache_sb.first_bucket
        value = (23 // bucketSize) + 1
        struct.pack_into("H", bcacheSb, p, value)
        p += struct.calcsize("H")

        # cache_sb.csum
        crc64 = crcmod.predefined.Crc("crc-64-we")
        crc64.update(bcacheSb[offset_content:])
        struct.pack_into("Q", bcacheSb, 0, crc64.crcValue)

        with open(devPath, "r+b") as f:
            f.write(bytearray(8 * 512))
            f.write(bcacheSb)
            f.write(bytearray(256 * 8))         # cacbe_sb.d

        return (devUuid, setUuid)

    @staticmethod
    def isBackingDevice(devPath):
        return BcacheUtil._isBackingDeviceOrCachDevice(devPath, True)

    @staticmethod
    def isCacheDevice(devPath):
        return BcacheUtil._isBackingDeviceOrCachDevice(devPath, False)

    @staticmethod
    def registerBackingDevice(devPath):
        BcacheUtil._registerDevice(devPath)

    @staticmethod
    def registerCacheDevice(devPath):
        BcacheUtil._registerDevice(devPath)

    @staticmethod
    def makeAndRegisterBackingDevice(devPath):
        BcacheUtil.makeDevice(devPath, True)
        BcacheUtil.registerBackingDevice(devPath)

    @staticmethod
    def makeAndRegisterCacheDevice(devPath):
        BcacheUtil.makeDevice(devPath, False)
        BcacheUtil.registerCacheDevice(devPath)

    @staticmethod
    def attachCacheDevice(backingDevPathList, cacheDevPath):
        if len(backingDevPathList) > 0:
            setUuid = BcacheUtil.getSetUuid(cacheDevPath)
            for backingDevPath in backingDevPathList:
                with open("/sys/block/%s/bcache/attach" % (os.path.basename(backingDevPath)), "w") as f:
                    f.write(str(setUuid))

    @staticmethod
    def stopBackingDevice(devPath):
        with open("/sys/block/%s/bcache/stop" % (os.path.basename(devPath)), "w") as f:
            f.write("1")

    @staticmethod
    def unregisterCacheDevice(devPath):
        setUuid = BcacheUtil.getSetUuid(devPath)
        with open("/sys/fs/bcache/%s/unregister" % (setUuid), "w") as f:
            f.write(devPath)

    @staticmethod
    def getSetUuid(devPath):
        # see C struct definition in makeDevice()
        bcacheSbSetUuidPreFmt = "QQQ16B16B"
        bcacheSbSetUuidFmt = "16B"

        assert BcacheUtil.isCacheDevice(devPath)

        with open(devPath, "rb") as f:
            f.seek(8 * 512 + struct.calcsize(bcacheSbSetUuidPreFmt))
            buf = f.read(struct.calcsize(bcacheSbSetUuidFmt))
            return uuid.UUID(bytes=buf)

    @staticmethod
    def getSlaveDevPathList(bcacheDevPath):
        """Last element in the returned list is the backing device, others are cache device"""

        retList = []

        slavePath = "/sys/block/" + os.path.basename(bcacheDevPath) + "/slaves"
        for slaveDev in os.listdir(slavePath):
            retList.append(os.path.join("/dev", slaveDev))

        bcachePath = os.path.realpath("/sys/block/" + os.path.basename(bcacheDevPath) + "/bcache")
        backingDev = os.path.basename(os.path.dirname(bcachePath))
        backingDevPath = os.path.join("/dev", backingDev)

        retList.remove(backingDevPath)
        retList.append(backingDevPath)
        return retList

    @staticmethod
    def findByBackingDevice(devPath):
        for fullfn in glob.glob("/dev/bcache*"):
            if re.fullmatch("/dev/bcache[0-9]+", fullfn):
                bcachePath = os.path.realpath("/sys/block/" + os.path.basename(devPath) + "/bcache")
                backingDev = os.path.basename(os.path.dirname(bcachePath))
                if os.path.basename(devPath) == backingDev:
                    return fullfn
        return None

    @staticmethod
    def scanAndRegisterAll():
        # FIXME

        ret = []
        for fn in os.listdir("/dev"):
            if re.fullmatch("bcache[0-9]+", fn) is not None:
                ret.append(os.path.join("/dev", fn))
        return ret

    @staticmethod
    def _isBackingDeviceOrCachDevice(devPath, backingDeviceOrCacheDevice):
        # see C struct definition in makeDevice()
        bcacheSbMagicPreFmt = "QQQ"
        bcacheSbMagicFmt = "16B"
        bcacheSbVersionPreFmt = "QQ"
        bcacheSbVersionFmt = "Q"

        bcacheSbMagic = [0xc6, 0x85, 0x73, 0xf6, 0x4e, 0x1a, 0x45, 0xca,
                         0x82, 0x65, 0xf5, 0x7f, 0x48, 0xba, 0x6d, 0x81]
        if backingDeviceOrCacheDevice:
            versionValueList = [
                1,           # BCACHE_SB_VERSION_BDEV
                4,           # BCACHE_SB_VERSION_BDEV_WITH_OFFSET
            ]
        else:
            versionValueList = [
                0,           # BCACHE_SB_VERSION_CDEV
                3,           # BCACHE_SB_VERSION_CDEV_WITH_UUID
            ]

        with open(devPath, "rb") as f:
            f.seek(8 * 512 + struct.calcsize(bcacheSbMagicPreFmt))
            buf = f.read(struct.calcsize(bcacheSbMagicFmt))
            if list(buf) != bcacheSbMagic:
                return False

            f.seek(8 * 512 + struct.calcsize(bcacheSbVersionPreFmt))
            buf = f.read(struct.calcsize(bcacheSbVersionFmt))
            value = struct.unpack(bcacheSbVersionFmt, buf)[0]
            if value not in versionValueList:
                return False

            return True

    @staticmethod
    def _registerDevice(devPath):
        with open("/sys/fs/bcache/register", "w") as f:
            f.write(devPath)


class BcachefsUtil:

    @staticmethod
    def getSlaveSsdDevPatListAndHddDevPathList(devPath):
        return [], []

    @staticmethod
    def createBcachefs(ssdList, hddList):
        assert len(hddList) > 0

        cmdList = ["/sbin/bcachefs", "format"]
        if len(ssdList) > 0:
            cmdList.append("--group=ssd")
            cmdList += ssdList
        if True:
            cmdList.append("--group=hdd")
            cmdList += hddList
        cmdList += ["--data_replicas=1", "--metadata_replicas=1", "--foreground_target=ssd", "--background_target=hdd", "--promote_target=ssd"]

        Util.cmdCall(*cmdList)

    @staticmethod
    def addSsdToBcachefs(ssd):
        Util.cmdCall("")
        cmdList = ["/sbin/bcachefs", "device", "add", "--group=ssd", "/mnt", ssd]

    @staticmethod
    def addHddToBcachefs(hdd):
        pass


class BtrfsUtil:

    @staticmethod
    def getSlaveDevPathList(devPath):
        return []


class LvmUtil:

    vgName = "hdd"

    rootLvName = "root"
    rootLvDevPath = "/dev/mapper/hdd-root"      # FIXME

    swapLvName = "swap"
    swapLvDevPath = "/dev/mapper/hdd-swap"      # FIXME

    @staticmethod
    def getSlaveDevPathList(vgName):
        ret = []
        out = Util.cmdCall("/sbin/lvm", "pvdisplay", "-c")
        for m in re.finditer("^\\s*(\\S+):%s:.*" % (vgName), out, re.M):
            if m.group(1) == "[unknown]":
                raise LvmUtilException("volume group %s not fully loaded" % (vgName))
            ret.append(m.group(1))
        return ret

    @staticmethod
    def addPvToVg(pvDevPath, vgName, mayCreate=False):
        Util.cmdCall("/sbin/lvm", "pvcreate", pvDevPath)
        if mayCreate and not Util.cmdCallTestSuccess("/sbin/lvm", "vgdisplay", vgName):
            Util.cmdCall("/sbin/lvm", "vgcreate", vgName, pvDevPath)
        else:
            Util.cmdCall("/sbin/lvm", "vgextend", vgName, pvDevPath)

    @staticmethod
    def removePvFromVg(pvDevPath, vgName):
        rc, out = Util.cmdCallWithRetCode("/sbin/lvm", "pvmove", pvDevPath)
        if rc != 5:
            raise LvmUtilException("failed")

        if pvDevPath in LvmUtil.getSlaveDevPathList(vgName):
            Util.cmdCall("/sbin/lvm", "vgreduce", vgName, pvDevPath)

    @staticmethod
    def createLvWithDefaultSize(vgName, lvName):
        out = Util.cmdCall("/sbin/lvm", "vgdisplay", "-c", vgName)
        freePe = int(out.split(":")[15])
        Util.cmdCall("/sbin/lvm", "lvcreate", "-l", "%d" % (freePe // 2), "-n", lvName, vgName)

    @staticmethod
    def activateAll():
        Util.cmdCall("/sbin/lvm", "vgchange", "-ay")

    @staticmethod
    def getVgList():
        out = Util.cmdCall("/sbin/lvm", "vgdisplay", "-s")
        return [x for x in out.split("\n") if x != ""]

    @staticmethod
    def autoExtendLv(lvDevPath):
        total, used = Util.getBlkDevCapacity(lvDevPath)
        if used / total < 0.9:
            return
        added = int(used / 0.7) - total
        added = (added // 1024 + 1) * 1024      # change unit from MB to GB
        Util.cmdCall("/sbin/lvm", "lvextend", "-L+%dG" % (added), lvDevPath)


class LvmUtilException(Exception):
    pass


class EfiMultiDisk:

    @staticmethod
    def proxy(func):
        if isinstance(func, property):
            def f_get(self):
                return getattr(self._md, func.fget.__name__)
            f_get.__name__ = func.fget.__name__
            return property(f_get)
        else:
            def f(self, *args):
                return getattr(self._md, func.__name__)(*args)
            return f

    def __init__(self, diskList=[], bootHdd=None):
        # assign self._hddList
        assert diskList is not None
        self._hddList = sorted(diskList)

        # assign self._bootHdd
        if len(self._hddList) > 0:
            if bootHdd is None:
                bootHdd = self._hddList[0]
            else:
                assert bootHdd in self._hddList
        else:
            assert bootHdd is None
        self._bootHdd = bootHdd

    @property
    def dev_boot(self):
        return self.get_esp()

    @property
    def boot_disk(self):
        return self._bootHdd

    def get_esp(self):
        if self._bootHdd is not None:
            return PartiUtil.diskToParti(self._bootHdd, 1)
        else:
            return None

    def get_pending_esp_list(self):
        ret = []
        for hdd in self._hddList:
            if self._bootHdd is None or hdd != self._bootHdd:
                ret.append(PartiUtil.diskToParti(hdd, 1))
        return ret

    def sync_esp(self, dst):
        assert self.get_esp() is not None
        assert dst is not None and dst in self.get_pending_esp_list()
        Util.syncBlkDev(self.get_esp(), dst, mountPoint1=Util.bootDir)

    def get_suggestted_esp_size(self):
        return Util.getEspSize()

    def get_disk_list(self):
        return self._hddList

    def get_disk_esp_partition(self, disk):
        assert disk in self._hddList
        return PartiUtil.diskToParti(disk, 1)

    def get_disk_data_partition(self, disk):
        assert disk in self._hddList
        return PartiUtil.diskToParti(disk, 2)

    def add_disk(self, disk):
        assert disk is not None and disk not in self._hddList

        # create partitions
        Util.initializeDisk(disk, "gpt", [
            ("%dMiB" % (Util.getEspSizeInMb()), Util.fsTypeFat),
            ("*", "bcache"),
        ])

        # partition1: pending ESP partition
        parti = PartiUtil.diskToParti(disk, 1)
        Util.cmdCall("/usr/sbin/mkfs.vfat", parti)
        if self._bootHdd is not None:
            Util.syncBlkDev(PartiUtil.diskToParti(self._bootHdd, 1), parti, mountPoint1=Util.bootDir)
        else:
            pass

        # partition2: data partition, leave it to user
        pass

        # record result
        self._hddList.append(disk)
        self._hddList.sort()

        # change boot disk if needed
        if self._bootHdd is None:
            self._mountFirstHddAsBootHdd()

    def remove_disk(self, hdd):
        assert hdd is not None and hdd in self._hddList

        # change boot device if needed
        if self._bootHdd is not None and self._bootHdd == hdd:
            self._unmountCurrentBootHdd()
            self._hddList.remove(hdd)
            self._mountFirstHddAsBootHdd()
        else:
            self._hddList.remove(hdd)

        # wipe disk
        Util.wipeHarddisk(hdd)

    def _mountFirstHddAsBootHdd(self):
        self._bootHdd = self._hddList[0]
        Util.toggleEspPartition(PartiUtil.diskToParti(self._bootHdd, 1), True)
        Util.cmdCall("/bin/mount", PartiUtil.diskToParti(self._bootHdd, 1), Util.bootDir, "-o", "ro")

    def _unmountCurrentBootHdd(self):
        Util.cmdCall("/bin/umount", Util.bootDir)
        Util.toggleEspPartition(PartiUtil.diskToParti(self._bootHdd, 1), False)
        self._bootHdd = None


class EfiCacheGroup:

    @staticmethod
    def proxy(func):
        if isinstance(func, property):
            def f_get(self):
                return getattr(self._cg, func.fget.__name__)
            f_get.__name__ = func.fget.__name__
            return property(f_get)
        else:
            def f(self, *args):
                return getattr(self._cg, func.__name__)(*args)
            return f

    def __init__(self, ssd=None, ssdEspParti=None, ssdSwapParti=None, ssdCacheParti=None, hddList=[], bootHdd=None):
        # assign self._ssd and friends
        self._ssd = ssd
        if self._ssd is not None:
            self._ssdEspParti = PartiUtil.diskToParti(ssd, 1)
            if ssdSwapParti is not None:
                self._ssdSwapParti = PartiUtil.diskToParti(ssd, 2)
                self._ssdCacheParti = PartiUtil.diskToParti(ssd, 3)
            else:
                self._ssdSwapParti = None
                self._ssdCacheParti = PartiUtil.diskToParti(ssd, 2)
        else:
            self._ssdEspParti is None
            self._ssdSwapParti is None
            self._ssdCacheParti is None
        assert self._ssdEspParti == ssdEspParti
        assert self._ssdSwapParti == ssdSwapParti
        assert self._ssdCacheParti == ssdCacheParti

        # assign self._hddList
        assert hddList is not None
        self._hddList = sorted(hddList)

        # assign self._bootHdd
        if self._ssd is not None:
            assert bootHdd is None
        else:
            if len(self._hddList) > 0:
                if bootHdd is None:
                    bootHdd = self._hddList[0]
                else:
                    assert bootHdd in self._hddList
            else:
                assert bootHdd is None
        self._bootHdd = bootHdd

    @property
    def dev_boot(self):
        return self.get_esp()

    @property
    def dev_swap(self):
        return self.get_ssd_swap_partition()

    @property
    def boot_disk(self):
        return self._ssd if self._ssd is not None else self._bootHdd

    def get_esp(self):
        if self._ssd is not None:
            return self._ssdEspParti
        elif self._bootHdd is not None:
            return PartiUtil.diskToParti(self._bootHdd, 1)
        else:
            return None

    def get_pending_esp_list(self):
        ret = []
        for hdd in self._hddList:
            if self._bootHdd is None or hdd != self._bootHdd:
                ret.append(PartiUtil.diskToParti(hdd, 1))
        return ret

    def sync_esp(self, dst):
        assert self.get_esp() is not None
        assert dst is not None and dst in self.get_pending_esp_list()
        Util.syncBlkDev(self.get_esp(), dst, mountPoint1=Util.bootDir)

    def get_suggestted_esp_size(self):
        return Util.getEspSize()

    def get_disk_list(self):
        if self._ssd is not None:
            return [self._ssd] + self._hddList
        else:
            return self._hddList

    def get_ssd(self):
        return self._ssd

    def get_ssd_esp_partition(self):
        assert self._ssd is not None
        assert self._ssdEspParti is not None
        assert self._bootHdd is None
        return self._ssdEspParti

    def get_ssd_swap_partition(self):
        assert self._ssd is not None
        assert self._bootHdd is None
        return self._ssdSwapParti

    def get_ssd_cache_partition(self):
        assert self._ssd is not None
        assert self._ssdCacheParti is not None
        assert self._bootHdd is None
        return self._ssdCacheParti

    def get_suggestted_swap_size(self):
        return Util.getSwapSize()

    def get_hdd_list(self):
        return self._hddList

    def get_hdd_esp_partition(self, disk):
        assert disk in self._hddList
        return PartiUtil.diskToParti(disk, 1)

    def get_hdd_data_partition(self, disk):
        assert disk in self._hddList
        return PartiUtil.diskToParti(disk, 2)

    def add_ssd(self, ssd):
        assert self._ssd is None
        assert ssd is not None and ssd not in self._hddList

        self._ssd = ssd
        self._ssdEspParti = PartiUtil.diskToParti(ssd, 1)
        self._ssdSwapParti = PartiUtil.diskToParti(ssd, 2)
        self._ssdCacheParti = PartiUtil.diskToParti(ssd, 3)

        # create partitions
        Util.initializeDisk(self._ssd, "gpt", [
            ("%dMiB" % (Util.getEspSizeInMb()), "esp"),
            ("%dGiB" % (Util.getSwapSizeInGb()), Util.fsTypeSwap),
            ("*", "bcache"),
        ])

        # partition1: ESP partition
        Util.cmdCall("/usr/sbin/mkfs.vfat", self._ssdEspParti)
        if self._bootHdd is not None:
            Util.syncBlkDev(PartiUtil.diskToParti(self._bootHdd, 1), self._ssdEspParti, mountPoint1=Util.bootDir)
        else:
            pass

        # partition2: swap partition
        Util.cmdCall("/sbin/mkswap", self._ssdSwapParti)

        # partition3: cache partition, leave it to caller
        pass

        # change boot device
        if self._bootHdd is not None:
            self._unmountCurrentBootHdd()
        Util.cmdCall("/bin/mount", self._ssdEspParti, Util.bootDir, "-o", "ro")

    def remove_ssd(self):
        assert self._ssd is not None

        # partition3: cache partition, the caller should have processed it
        self._ssdCacheParti = None

        # partition2: swap partition
        if self._ssdSwapParti is not None:
            assert not Util.swapDeviceIsBusy(self._ssdSwapParti)
            self._ssdSwapParti = None

        # partition1: ESP partition
        Util.cmdCall("/bin/umount", Util.bootDir)
        self._ssdEspParti = None

        # change boot device
        if len(self._hddList) > 0:
            self._mountFirstHddAsBootHdd()

        # wipe disk
        Util.wipeHarddisk(self._ssd)
        self._ssd = None

    def add_hdd(self, hdd):
        assert hdd is not None and hdd not in self._hddList

        # create partitions
        Util.initializeDisk(hdd, "gpt", [
            ("%dMiB" % (Util.getEspSizeInMb()), Util.fsTypeFat),
            ("*", "bcache"),
        ])

        # partition1: pending ESP partition
        parti = PartiUtil.diskToParti(hdd, 1)
        Util.cmdCall("/usr/sbin/mkfs.vfat", parti)
        if self._ssd is not None:
            Util.syncBlkDev(self._ssdEspParti, parti, mountPoint1=Util.bootDir)
        elif self._bootHdd is not None:
            Util.syncBlkDev(PartiUtil.diskToParti(self._bootHdd, 1), parti, mountPoint1=Util.bootDir)
        else:
            pass

        # partition2: data partition, leave it to user
        pass

        # record result
        self._hddList.append(hdd)
        self._hddList.sort()

        # change boot disk if needed
        if self._bootHdd is None:
            self._mountFirstHddAsBootHdd()

    def remove_hdd(self, hdd):
        assert hdd is not None and hdd in self._hddList

        # change boot device if needed
        if self._bootHdd is not None and self._bootHdd == hdd:
            self._unmountCurrentBootHdd()
            self._hddList.remove(hdd)
            self._mountFirstHddAsBootHdd()
        else:
            self._hddList.remove(hdd)

        # wipe disk
        Util.wipeHarddisk(hdd)

    def _mountFirstHddAsBootHdd(self):
        self._bootHdd = self._hddList[0]
        Util.toggleEspPartition(PartiUtil.diskToParti(self._bootHdd, 1), True)
        Util.cmdCall("/bin/mount", PartiUtil.diskToParti(self._bootHdd, 1), Util.bootDir, "-o", "ro")

    def _unmountCurrentBootHdd(self):
        Util.cmdCall("/bin/umount", Util.bootDir)
        Util.toggleEspPartition(PartiUtil.diskToParti(self._bootHdd, 1), False)
        self._bootHdd = None


class TmpMount:

    def __init__(self, path, options=None):
        self._path = path
        self._tmppath = tempfile.mkdtemp()

        try:
            cmd = ["/bin/mount"]
            if options is not None:
                cmd.append("-o")
                cmd.append(options)
            cmd.append(self._path)
            cmd.append(self._tmppath)
            subprocess.run(cmd, check=True, universal_newlines=True)
        except BaseException:
            os.rmdir(self._tmppath)
            raise

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    @property
    def mountpoint(self):
        return self._tmppath

    def close(self):
        subprocess.run(["/bin/umount", self._tmppath], check=True, universal_newlines=True)
        os.rmdir(self._tmppath)
