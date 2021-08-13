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


import re
import anytree
from . import util
from . import StorageLayoutParseError


class DiskStackNode(anytree.node.nodemixin.NodeMixin):

    def __init__(self, dev_path, parent=None):
        super().__init__(parent=parent)
        self.dev_path = dev_path


class DiskStackNodeLvmLv(DiskStackNode):

    def __init__(self, dev_path, vg_name, lv_name, parent=None):
        super().__init__(dev_path, parent=parent)
        self.vg_name = vg_name
        self.lv_name = lv_name


class DiskStackNodeBcache(DiskStackNode):

    def __init__(self, dev_path, cache_dev_list, backing_dev, parent=None):
        super().__init__(dev_path, parent=parent)
        self.cache_dev_list = cache_dev_list
        self.backing_dev = backing_dev


class DiskStackNodeHarddisk(DiskStackNode):

    DEV_TYPE_SCSI = 1
    DEV_TYPE_NVME = 2
    DEV_TYPE_XEN = 3
    DEV_TYPE_VIRTIO = 4

    def __init__(self, dev_path, dev_type, parent=None):
        assert self.DEV_TYPE_SCSI <= dev_type <= self.DEV_TYPE_VIRTIO
        super().__init__(dev_path, parent=parent)
        self.dev_type = self.dev_type


class DiskStackNodePartition(DiskStackNode):

    PART_TYPE_MBR = 1
    PART_TYPE_GPT = 2

    def __init__(self, dev_path, part_type, parent=None):
        assert self.PART_TYPE_MBR <= part_type <= self.PART_TYPE_GPT
        super().__init__(dev_path, parent=parent)
        self.part_type = part_type
        self.part_id = util.devPathPartitionToDiskAndPartitionId(dev_path)[1]


class DiskStackUtil:

    @staticmethod
    def getBlkDevType(layoutName, devPath):
        m = re.fullmatch("/dev/sd[a-z]", devPath)
        if m is not None:
            return DiskStackNodeHarddisk.DEV_TYPE_SCSI

        m = re.fullmatch("/dev/xvd[a-z]", devPath)
        if m is not None:
            return DiskStackNodeHarddisk.DEV_TYPE_XEN

        m = re.fullmatch("/dev/vd[a-z]", devPath)
        if m is not None:
            return DiskStackNodeHarddisk.DEV_TYPE_VIRTIO

        m = re.fullmatch("/dev/nvme[0-9]+n[0-9]+", devPath)
        if m is not None:
            return DiskStackNodeHarddisk.DEV_TYPE_NVME

        raise StorageLayoutParseError(layoutName, "unknown type for block device \"%s\"" % (devPath))
