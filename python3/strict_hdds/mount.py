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
import psutil
from .util import Util
from . import BootDirRwController


class MountBios:

    class BootDirRwController(BootDirRwController):

        @property
        def is_writable(self):
            return True

        def to_read_write(self):
            pass

        def to_read_only(self):
            pass

    @staticmethod
    def proxy(func):
        def f(self, *args):
            return getattr(self._mnt, func.__name__)(*args)
        return f

    def __init__(self, mountDir):
        self._mountDir = mountDir
        self._rwCtrl = self.BootDirRwController()

    def mount_point(self):
        return self._mountDir

    def umount(self):
        Util.cmdCall("/bin/umount", self._mountDir)

    def remount_rootfs(self, mount_options):
        Util.cmdCall("/bin/mount", "--remount", ",".join(mount_options))

    def get_bootdir_rw_controller(self):
        return self._rwCtrl


class MountEfi:

    class BootDirRwController(BootDirRwController):

        def __init__(self, mountDir):
            self._mountDir = mountDir

        @property
        def is_writable(self):
            for pobj in psutil.disk_partitions():
                if pobj.mountpoint == self._mountDir:
                    return ("rw" in pobj.opts.split(","))
            assert False

        def to_read_write(self):
            assert not self.is_writable
            Util.cmdCall("/bin/mount", self._mountDir, "-o", "rw,remount")

        def to_read_only(self):
            assert self.is_writable
            Util.cmdCall("/bin/mount", self._mountDir, "-o", "ro,remount")

    @staticmethod
    def proxy(func):
        def f(self, *args):
            return getattr(self._mnt, func.__name__)(*args)
        return f

    def __init__(self, mountDir):
        self._mountDir = mountDir
        self._rwCtrl = self.BootDirRwController()

    def mount_point(self):
        return self._mountDir

    def umount(self):
        Util.cmdCall("/bin/umount", os.path.join(self._mountDir, "boot"))
        Util.cmdCall("/bin/umount", self._mountDir)

    def remount_rootfs(self, mount_options):
        Util.cmdCall("/bin/mount", "--remount", ",".join(mount_options))

    def get_bootdir_rw_controller(self):
        return self._rwCtrl
