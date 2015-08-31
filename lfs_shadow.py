#!/usr/bin/python3 -tt

# Support classes for two different types of shadow files, before direct
# kernel support of mmap().  Shadowing must accommodate graceless restarts
# including book_register.py rewrites.

import errno
import os
import tempfile

from pdb import set_trace

from fuse import FuseOSError

class shadow_directory(object):

    def shadowpath(self, shelf_name):
        return '%s/%s' % (self._shadowpath, shelf_name)

    def __init__(self, args):
        self._shadowpath = args.shadow_dir
        assert os.path.isdir(args.shadow_dir), 'No such directory %s' % args.shadow_dir
        self._fd2obj = { }  # now this object doubles as a dict
        try:
            probe = tempfile.TemporaryFile(dir=args.shadow_dir)
            probe.close()
        except OSError as e:
            raise RuntimeError('%s is not writeable' % args.shadow_dir)

    def __getitem__(self, index):
        return self._fd2obj[index]

    def __setitem__(self, index, obj):
        self._fd2obj[index] = obj

    def __contains__(self, index):
        return index in self._fd2obj

    def __delitem__(self, index):
        del self._fd2obj[index]

    def values(self):
        return self._fd2obj.values()

    def unlink(self, shelf_name):
        try:
            os.unlink(self.shadowpath(shelf_name))
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise FuseOSError(e.errno)

    def open(self, shelf_name, flags, mode=None):
        if mode is None:
            mode = 0o666
        try:    # shadow first
            fd = os.open(self.shadowpath(shelf_name), flags, mode=mode)
            return fd
        except Exception as e:
            raise FuseOSError(errno.ENOENT)

    def create(self, shelf_name, mode):
        try:
            fd = os.open(self.shadowpath(shelf_name), os.O_CREAT, mode=mode)
            return fd
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise FuseOSError(e.errno)
        return None

    def read(self, path, length, offset, fh):
        os.lseek(fh, offset, os.SEEK_SET)
        return os.read(fh, length)

    def write(self, path, buf, offset, fh):
        os.lseek(fh, offset, os.SEEK_SET)
        return os.write(fh, buf)

    def truncate(self, shelf_name, length, fh):
        try: # Shadow first, before hitting the librararian DB
            os.truncate(
                fh if fh is not None else self.shadowpath(shelf_name),
                length)
            return 0
        except OSError as e:
            raise FuseOSError(e.errno)

    def release(self, fh):
        os.close(fh)

#--------------------------------------------------------------------------

class shadow_ivshmem(object):

    def __init__(self, args):
        assert os.path.exists(args.shadow_ivshmem), '%s does not exist' % args.shadow_ivshmem
        raise NotImplementedError

def the_shadow_knows(args):
    '''args is command-line arguments from lfs_fuse.py'''
    if args.shadow_dir:
        return shadow_directory(args)
    elif args.shadow_ivshmem:
        return shadow_ivshmem(args)
    else:
        raise ValueError('One of shadow_dir or shadow_ivshmem must be used')
