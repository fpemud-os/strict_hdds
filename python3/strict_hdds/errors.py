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

from enum import Enum, auto


class CheckCode(Enum):
    TRIVIAL = auto()
    ESP_SIZE_INVALID = auto()
    SWAP_NOT_ENABLED = auto()
    SWAP_SIZE_TOO_SMALL = auto()


def checkErrorCallback(error_callback, check_code, *kargs):
    if error_callback is None:
        return

    errDict = {
        CheckCode.TRIVIAL: (1, "{0}"),
        CheckCode.ESP_SIZE_INVALID: (1, "Invalid size for ESP partition \"{0}\"."),
        CheckCode.SWAP_NOT_ENABLED: (0, "Swap is not enabled."),
        CheckCode.SWAP_SIZE_TOO_SMALL: (1, "Swap {0} size is too small."),
    }

    argNum, fstr = errDict[check_code]
    assert len(kargs) == argNum
    error_callback(check_code, fstr % kargs)


class StorageLayoutError(Exception):
    pass


class StorageLayoutCreateError(StorageLayoutError):
    pass


class StorageLayoutMountError(StorageLayoutError):
    pass


class StorageLayoutAddDiskError(StorageLayoutError):

    def __init__(self, disk_devpath, message):
        self.disk_devpath = disk_devpath
        self.message = message


class StorageLayoutReleaseDiskError(StorageLayoutError):

    def __init__(self, disk_devpath, message):
        self.disk_devpath = disk_devpath
        self.message = message


class StorageLayoutRemoveDiskError(StorageLayoutError):

    def __init__(self, disk_devpath, message):
        self.disk_devpath = disk_devpath
        self.message = message


class StorageLayoutParseError(StorageLayoutError):

    def __init__(self, layout_name, message):
        self.layout_name = layout_name
        self.message = message


# common messages for StorageLayoutCreateError
NO_DISK_WHEN_CREATE = "no fixed harddisk"
MULTIPLE_DISKS_WHEN_CREATE = "multiple fixed harddisks found while we need only one"
MULTIPLE_SSD = "multiple SSD harddisks"

# common messages for StorageLayoutAddDiskError
NOT_DISK = "not a fixed harddisk"

# common messages for StorageLayoutReleaseDiskError and StorageLayoutRemoveDiskError
SWAP_IS_IN_USE = "swap partition is in use"
CAN_NOT_REMOVE_LAST_HDD = "can not release/remove the last harddisk"

# common messages for StorageLayoutParseError
NO_DISK_WHEN_PARSE = "no fixed harddisk"
NO_VALID_LAYOUT = "no valid storage layout found"
ROOT_DEV_MUST_BE = lambda root_dev: f"root device must be \"{root_dev!s}\""
ROOT_PARTITION_NOT_FOUND = "no valid root partition"
ROOT_PARTITIONS_TOO_MANY = "multiple valid root partitions found while we need one and only one"
ROOT_PARTITION_FS_SHOULD_BE = lambda expected_fs: f"file system of root partition is not \"{expected_fs!s}\""
DISK_NOT_FOUND = "no valid harddisk"
DISK_TOO_MANY = "multiple valid harddisks found while we need one and only one"
DISK_HAS_REDUNDANT_PARTITION = lambda devpath: f"redundant partition exists on {devpath!s}"
DISK_SIZE_INVALID = lambda devpath: f"{devpath!s} has an invalid size"
DISK_NOT_CLEAN = lambda devpath: f"{devpath!s} is not clean"
PARTITION_SIZE_INVALID = lambda devpath: f"{devpath!s} has an invalid size"
PARTITION_TYPE_SHOULD_BE = lambda devpath, expected_part_type: f"partition type of {devpath!s} is not \"{expected_part_type!s}\""
BOOT_DISK_MUST_IN_SLAVE_DISK_LIST = "boot disk must be one of the root device's slave disks"
BOOT_DEV_NOT_EXIST = "boot device does not exist"
BOOT_DEV_SHOULD_NOT_EXIST = "/boot should not be mounted"
BOOT_DEV_IS_NOT_ESP = "boot device is not an ESP partitiion"
BOOT_DEV_MUST_BE = lambda boot_dev: f"boot device must be \"{boot_dev!s}\""
BOOT_DEV_INVALID = "invalid boot device"
SWAP_DEV_HAS_INVALID_FS_FLAG = lambda devpath: f"swap device {devpath!s} has an invalid file system"
LVM_VG_NOT_FOUND = lambda vg_name: f"volume group \"{vg_name!s}\" does not exist"
LVM_LV_NOT_FOUND = lambda lv_name: f"logical volume \"{lv_name!s}\" does not exist"
BOOT_CODE_NOT_FOUND = "no harddisk has boot-code"
BOOT_CODE_ON_MULTIPLE_DISKS = "boot-code exists on multiple harddisks"
