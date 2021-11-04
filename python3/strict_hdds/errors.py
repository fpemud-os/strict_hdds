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


class StorageLayoutError(Exception):
    pass


class StorageLayoutCreateError(StorageLayoutError):
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
MULTIPLE_SSD = "multiple SSD harddisks"
NO_HDD = "no HDD harddisk"

# common messages for StorageLayoutAddDiskError
NOT_DISK = "not a fixed harddisk"

# common messages for StorageLayoutReleaseDiskError
SWAP_IS_IN_USE = "swap partition is in use"
CAN_NOT_RELEASE_LAST_HDD = "can not release the last physical volume"

# common messages for StorageLayoutRemoveDiskError
CAN_NOT_REMOVE_LAST_HDD = "can not remove the last physical volume"

# common messages for StorageLayoutParseError
BOOT_DEV_IS_NOT_ESP = "boot device is not an ESP partitiion"
DISK_HAS_REDUNDANT_PARTITION = lambda devpath: f"redundant partition exists on {devpath!s}"
DISK_HAS_INVALID_SIZE = lambda devpath: f"{devpath!s} has an invalid size"
SWAP_DEV_HAS_INVALID_FS_FLAG = lambda devpath: f"swap device {devpath!s} has an invalid file system"
