#!/usr/bin/python3 -tt

# Support classes for two different types of shadow files, before direct
# kernel support of mmap().  Shadowing must accommodate graceless restarts
# including book_register.py rewrites.

import errno
import math
import os
import stat
import sys
import tempfile
import mmap
from subprocess import getoutput

from pdb import set_trace

from tm_fuse import TmfsOSError

from descmgmt import DescriptorManagement

#--------------------------------------------------------------------------


class shadow_support(object):
    '''Provide private data storage for subclasses.'''

    _mode_rw_file = stat.S_IFREG + 0o600  # regular file

    def __init__(self, args, lfs_globals):
        self.verbose = args.verbose
        self.book_size = lfs_globals['book_size_bytes']
        # Originally did it by fd, then getxattr piggyback only does name.
        # Grand unification!
        self._shelfcache = { }

        # Replaces ig_gap calculation.

        offset = 0
        self._igstart = {}
        for igstr in sorted(lfs_globals['books_per_IG'].keys()):
            ig = int(igstr)
            print('IG %2d flatspace offset @ %d (0x%x)' % (ig, offset, offset))
            self._igstart[ig] = offset
            books = int(lfs_globals['books_per_IG'][igstr])
            offset += books * self.book_size
        self.book_shift = int(math.log(self.book_size, 2))

    # Duck-type a dict, with multiple entries.  It's supposed to be a shelf!
    def __setitem__(self, index, shelf):
        self._shelfcache[index] = shelf
        try:
            if isinstance(index, int):
                self._shelfcache[shelf.name] = shelf
            else:
                self._shelfcache[shelf.open_handle] = shelf
        except Exception as e:
            pass

    def __getitem__(self, index):
        return self._shelfcache.get(index, None)

    def __contains__(self, index):
        return index in self._shelfcache

    def __delitem__(self, index):
        if index not in self._shelfcache:
            return
        tmp = self._shelfcache[index]
        del self._shelfcache[index]
        try:
            if isinstance(index, int):   # Always true?
                del self._shelfcache[tmp.name]
            else:
                del self._shelfcache[tmp.open_handle]
        except KeyError:
            pass

    def keys(self):
        return tuple(self._shelfcache.keys())

    def items(self):
        return self._shelfcache.items()

    # End of dictionary duck typing, now use that cache

    def shadow_offset(self, shelf_name, shelf_offset):
        '''Translate shelf-relative offset to flat shadow file offset'''
        bos = self[shelf_name].bos
        bos_index = shelf_offset // self.book_size  # (0..n)

        # Stop FS read ahead past shelf, but what about writes?  Later.
        try:
            book = bos[bos_index]
        except Exception as e:
            return -1

        # Offset into flat space has several contributors.  Oddly enough
        # this doesn't neet the concatenated LZA field.
        intlv_group = book['intlv_group']
        book_num = book['book_num']
        book_start = book_num * self.book_size
        book_offset = shelf_offset % self.book_size
        tmp = self._igstart[intlv_group] + book_start + book_offset
        return tmp

    # Provide ABC noop defaults.  Note they're not all actually noop.
    # FIXME: currently this only supports a single open per node.
    # Multiple opens from one process and/or multiple processes opening
    # one shelf will behave in strange and undesireable ways.  We've got
    # top men working on it.   TOP...............men.  Best guess:
    # we'll have to hash on a tuple, not just open_handle.

    def truncate(self, shelf, length, fd):
        self[shelf.open_handle] = shelf
        return 0

    def unlink(self, shelf_name):
        del self[shelf_name]
        return 0

    # Piggybacked during mmap fault handling.  If the kernel receives
    # 'FALLBACK' it will use legacy, generic cache-based handler with stock
    # read() and write() spill and fill.  Override to do true mmaps.
    def getxattr(self, shelf_name, xattr):
        return 'FALLBACK'

    def read(self, shelf_name, length, offset, fd):
        raise TmfsOSError(errno.ENOSYS)

    def write(self, shelf_name, buf, offset, fd):
        raise TmfsOSError(errno.ENOSYS)

#--------------------------------------------------------------------------


class shadow_directory(shadow_support):
    '''Create a regular file for each shelf as backing store.  These files
       will exist in the file system of the entity running lfs_fuse,
       ie, if you're on a VM, the file storage is in the VM disk image.'''

    def __init__(self, args, lfs_globals):
        super(self.__class__, self).__init__(args, lfs_globals)
        assert os.path.isdir(
            args.shadow_dir), 'No such directory %s' % args.shadow_dir
        self._shadowpath = args.shadow_dir
        try:
            probe = tempfile.TemporaryFile(dir=args.shadow_dir)
            probe.close()
        except OSError as e:
            raise RuntimeError('%s is not writeable' % args.shadow_dir)

    def shadowpath(self, shelf_name):
        return '%s/%s' % (self._shadowpath, shelf_name)

    def unlink(self, shelf_name):
        for k, v in self.items():
            if v[0].name == shelf_name:
                del self[k]
                break
        try:
            return os.unlink(self.shadowpath(shelf_name))
        except OSError as e:
            if e.errno == errno.ENOENT:
                return 0
            raise TmfsOSError(e.errno)

    def _create_open_common(self, shelf, flags, mode):
        if mode is None:
            mode = 0o666
        try:
            fd = os.open(self.shadowpath(shelf.name), flags, mode=mode)
        except OSError as e:
            if flags & os.O_CREAT:
                if e.errno != errno.EEXIST:
                    raise TmfsOSError(e.errno)
            else:
                raise TmfsOSError(e.errno)
        self[fd] = shelf
        return fd

    def open(self, shelf, flags, mode=None):
        return self._create_open_common(shelf, flags, mode)

    def create(self, shelf, mode=None):
        flags = os.O_CREAT | os.O_RDWR | os.O_CLOEXEC
        return self._create_open_common(shelf, flags, mode)

    def read(self, shelf_name, length, offset, fd):
        os.lseek(fd, offset, os.SEEK_SET)
        return os.read(fd, length)

    def write(self, shelf_name, buf, offset, fd):
        os.lseek(fd, offset, os.SEEK_SET)
        return os.write(fd, buf)

    def truncate(self, shelf, length, fd):
        try:
            if fd:
                assert shelf == self[fd], 'Oops'
            os.truncate(
                fd if fd is not None else self.shadowpath(shelf.name),
                length)
            if fd is not None:
                self[fd].size_bytes = length
            return 0
        except OSError as e:
            raise TmfsOSError(e.errno)

    def release(self, fd):
        shelf = self[fd]
        del self[fd]
        os.close(fd)    # I don't think this ever raises....
        return shelf

#--------------------------------------------------------------------------


class shadow_file(shadow_support):
    '''Use one (large) shadow file indexed by "normalized" LZA (ie,
       discontiguous holes in LZA are made smooth for the file.  This
       file lives in the file system of the entity running lfs_fuse.'''

    def __init__(self, args, lfs_globals):
        super(self.__class__, self).__init__(args, lfs_globals)

        (head, tail) = os.path.split(args.shadow_file)

        assert os.path.isdir(head), 'No such directory %s' % head

        try:
            probe = tempfile.TemporaryFile(dir=head)
            probe.close()
        except OSError as e:
            raise RuntimeError('%s is not writeable' % head)

        if os.path.isfile(args.shadow_file):
            fd = os.open(args.shadow_file, os.O_RDWR)
        else:
            fd = os.open(args.shadow_file, os.O_RDWR | os.O_CREAT)
            size = lfs_globals['nvm_bytes_total']
            os.ftruncate(fd, size)

        # Compare node requirements to file size
        statinfo = os.stat(args.shadow_file)
        assert self._mode_rw_file == self._mode_rw_file & statinfo.st_mode, \
            '%s is not RW'
        assert statinfo.st_size >= lfs_globals['nvm_bytes_total']

        self._shadow_fd = fd

    def open(self, shelf, flags, mode=None):
        self[shelf.open_handle] = shelf
        return shelf.open_handle

    def create(self, shelf, mode):
        self[shelf.open_handle] = shelf
        return shelf.open_handle

    def read(self, shelf_name, length, offset, fd):

        if ((offset % self.book_size) + length) <= self.book_size:
            shadow_offset = self.shadow_offset(shelf_name, offset)
            os.lseek(self._shadow_fd, shadow_offset, os.SEEK_SET)
            return os.read(self._shadow_fd, length)

        # Read overlaps books, split into multiple chunks

        buf = b''
        cur_offset = offset
        tot_length = length

        while (tot_length > 0):
            cur_length = min((self.book_size - (cur_offset % self.book_size)),
                             tot_length)
            shadow_offset = self.shadow_offset(shelf_name, cur_offset)

            if shadow_offset == -1:
                break

            os.lseek(self._shadow_fd, shadow_offset, os.SEEK_SET)
            buf += os.read(self._shadow_fd, cur_length)

            if self.verbose > 2:
                print("READ: co = %d, tl = %d, cl = %d, so = %d, bl = %d" % (
                      cur_offset, tot_length, cur_length,
                      shadow_offset, len(buf)))

            offset += cur_length
            cur_offset += cur_length
            tot_length -= cur_length

        return buf

    def write(self, shelf_name, buf, offset, fd):

        if ((offset % self.book_size) + len(buf)) <= self.book_size:
            shadow_offset = self.shadow_offset(shelf_name, offset)
            os.lseek(self._shadow_fd, shadow_offset, os.SEEK_SET)
            return os.write(self._shadow_fd, buf)

        # Write overlaps books, split into multiple chunks

        tbuf = b''
        buf_offset = 0
        cur_offset = offset
        tot_length = len(buf)
        wsize = 0

        while (tot_length > 0):
            cur_length = min((self.book_size - (cur_offset % self.book_size)),
                             tot_length)
            shadow_offset = self.shadow_offset(shelf_name, cur_offset)

            assert shadow_offset != -1, "shadow_offset -1 during write"

            # chop buffer in pieces
            buf_end = buf_offset + cur_length
            tbuf = buf[buf_offset:buf_end]

            os.lseek(self._shadow_fd, shadow_offset, os.SEEK_SET)
            wsize += os.write(self._shadow_fd, tbuf)

            if self.verbose > 2:
                print("WRITE: co = %d, tl = %d, cl = %d, so = %d,"
                      " bl = %d, bo = %d, wsize = %d, be = %d" % (
                          cur_offset, tot_length, cur_length, shadow_offset,
                          len(tbuf), buf_offset, wsize, buf_end))

            offset += cur_length
            cur_offset += cur_length
            tot_length -= cur_length
            buf_offset += cur_length
            tbuf = b''

        return wsize

    def release(self, fd):
        shelf = self[fd]
        del self[fd]
        return shelf

#--------------------------------------------------------------------------
# Original class did read/write against the resource file by mmapping slices
# against /sys/bus/pci/..../resource2.  mmap in kernel was handled by
# legacy generic fault handles that used read/write.  There was a lot of
# overhead but it was functional against true global shared memory.

# 'class fam' supported true mmap in the kernel against the physical
# address of the IVSHMEM device, but it couldn't do read and write.  Merging
# the two classes (actually, just getxattr() from the previous "class fam")
# gets the best of both worlds.

class shadow_ivshmem(shadow_support):

    def __init__(self, args, lfs_globals):

        super(self.__class__, self).__init__(args, lfs_globals)

        # Retrieve ivshmem information.  Our convention states the first
        # IVSHMEM device found is used as fabric-attached memory.  Parse the
        # first block of lines of lspci -vv for Bus-Device-Function and
        # BAR2 information.

        lspci = getoutput('lspci -vv -d1af4:1110').split('\n')[:11]
        assert lspci[0].endswith('Red Hat, Inc Inter-VM shared memory'), \
            'IVSHMEM device not found'
        bdf = lspci[0].split()[0]
        print('IVSHMEM device at %s used as fabric-attached memory' % bdf)
        mf = '/sys/devices/pci0000:00/0000:%s/resource2' % bdf
        assert (os.path.isfile(mf)), '%s is not a file' % mf

        region2 = [ l for l in lspci if 'Region 2:' in l ][0]
        assert ('(64-bit, prefetchable)' in region2), \
            'IVSHMEM region 2 not found for device %s' % bdf
        self.aperture_base = int(region2.split('Memory at')[1].split()[0], 16)
        assert self.aperture_base, \
            'Could not retrieve base address of IVSHMEM device at %s' % bdf

        # Compare requirements to file size
        statinfo = os.stat(mf)
        assert self._mode_rw_file == self._mode_rw_file & statinfo.st_mode, \
            '%s is not RW' % mf
        assert statinfo.st_size >= lfs_globals['nvm_bytes_total'], \
            'st_size (%d) < nvm_bytes_total (%d)' % \
            (statinfo.st_size, lfs_globals['nvm_bytes_total'])

        # Paranoia check in face of multiple IVSHMEMS: zbridge emulation
        # has firewall table of 32M.  Make sure this is bigger.
        assert statinfo.st_size > 64 * 1 << 20, \
            'IVSHMEM at %s is not big enough, possible collision?' % bdf
        self.aperture_size = statinfo.st_size

        # os.open vs. built-in allows all the low-level stuff I need.
        self._shadow_fd = os.open(mf, os.O_RDWR)
        self._mmap = mmap.mmap(
            self._shadow_fd, 0, prot=mmap.PROT_READ | mmap.PROT_WRITE)

        print('IVSHMEM max offset is 0x%x; physical addresses 0x%x - 0x%x' % (
              self.aperture_size - 1,
              self.aperture_base, self.aperture_base + self.aperture_size - 1))

        self.descriptors = DescriptorManagement()

    def open(self, shelf, flags, mode=None):
        self[shelf.open_handle] = shelf
        return shelf.open_handle

    def create(self, shelf, mode):
        self[shelf.open_handle] = shelf
        return shelf.open_handle

    def read(self, shelf_name, length, offset, fd):

        if ((offset % self.book_size) + length) <= self.book_size:
            shadow_offset = self.shadow_offset(shelf_name, offset)
            self._mmap.seek(shadow_offset, 0)
            return self._mmap.read(length)

        # Read overlaps books, split into multiple chunks

        buf = b''
        cur_offset = offset
        tot_length = length

        while (tot_length > 0):
            cur_length = min((self.book_size - (cur_offset % self.book_size)),
                             tot_length)
            shadow_offset = self.shadow_offset(shelf_name, cur_offset)

            if shadow_offset == -1: break

            self._mmap.seek(shadow_offset, 0)
            buf += self._mmap.read(cur_length)

            if self.verbose > 2:
                print("READ: co = %d, tl = %d, cl = %d, so = %d, bl = %d" % (
                      cur_offset, tot_length, cur_length,
                      shadow_offset, len(buf)))

            offset += cur_length
            cur_offset += cur_length
            tot_length -= cur_length

        return buf

    def write(self, shelf_name, buf, offset, fd):

        if ((offset % self.book_size) + len(buf)) <= self.book_size:
            shadow_offset = self.shadow_offset(shelf_name, offset)
            self._mmap.seek(shadow_offset, 0)
            # write to mmap file always returns "None"
            self._mmap.write(buf)
            return len(buf)

        # Write overlaps books, split into multiple chunks

        tbuf = b''
        buf_offset = 0
        cur_offset = offset
        tot_length = len(buf)
        wsize = 0

        while (tot_length > 0):
            cur_length = min((self.book_size - (cur_offset % self.book_size)),
                             tot_length)
            shadow_offset = self.shadow_offset(shelf_name, cur_offset)

            assert shadow_offset != -1, "shadow_offset -1 during write"

            # chop buffer in pieces
            buf_end = buf_offset + cur_length
            tbuf = buf[buf_offset:buf_end]

            self._mmap.seek(shadow_offset, 0)
            # write to mmap file always returns "None"
            self._mmap.write(tbuf)
            wsize += len(tbuf)

            if self.verbose > 2:
                print("WRITE: co = %d, tl = %d, cl = %d, so = %d,"
                      " bl = %d, bo = %d, wsize = %d, be = %d" % (
                          cur_offset, tot_length, cur_length, shadow_offset,
                          len(tbuf), buf_offset, wsize, buf_end))

            offset += cur_length
            cur_offset += cur_length
            tot_length -= cur_length
            buf_offset += cur_length
            tbuf = b''

        return wsize

    def release(self, fd):
        shelf = self[fd]
        del self[fd]
        return shelf

    def getxattr(self, shelf_name, xattr):
        # Called during fault handler in kernel, don't die here :-)
        try:
            bos = self[shelf_name].bos
            cmd, comm, pid, offset, userVA = xattr.split(':')
            pid = int(pid)
            offset = int(offset)
            userVA = int(userVA)
            book_num = offset // self.book_size  # (0..n-1)
            book_offset = offset % self.book_size
            if book_num >= len(bos):
                return 'ERROR'
            LZA = bos[book_num]['lza']

            evictLZA = self.descriptors.allocate(LZA, pid, userVA)
            if evictLZA is not None:
                # Contains a list of PIDs whose PTEs need to be invalidated
                # over this physical range.
                print('---> EVICT %s: %s' % (evictLZA, ','.join(
                    [str(k) for k in evictLZA.pids.keys()])))

            # FAME physical offset for virtual to physical mapping during fault
            ivshmem_offset = self.shadow_offset(shelf_name, offset)
            if ivshmem_offset == -1:
                return 'ERROR'
            physaddr = self.aperture_base + ivshmem_offset

            # Save this construct for future "class aperture"
            # data = ':'.join(str(x) for x in (
            # LZA, book_offset, self.book_size, self.aperture_base, physaddr))

            # ivshmem does not have real apertures even though calculations
            # are being done.   Mapping is direct.
            data = 'direct:%s:%s' % (physaddr, self.book_size)

            if self.verbose > 3:
                print('Process %s[%d] shelf = %s, offset = %d (0x%x)' % (
                    comm, pid, shelf_name, offset, offset))
                print('shelf book seq=%d, LZA=0x%x -> IG=%d, IGoffset=%d' % (
                    book_num, LZA, LZA >> 13, LZA & ((1 << 13) -1 )))
                print('physaddr = %d (0x%x)' % (physaddr, physaddr))
                print('IVSHMEM backing file offset = %d (0x%x)' % (
                    ivshmem_offset, ivshmem_offset))
                print('data returned to fault handler = %s' % (data))

            return data

        except Exception as e:
            print('!!! ERROR IN FAULT HANDLER: %s' % str(e), file=sys.stderr)
            return ''

#--------------------------------------------------------------------------


def the_shadow_knows(args, lfs_globals):
    '''args is command-line arguments from lfs_fuse.py'''
    try:
        if args.shadow_dir:
            return shadow_directory(args, lfs_globals)
        elif args.shadow_file:
            return shadow_file(args, lfs_globals)
        elif args.shadow_ivshmem:
            return shadow_ivshmem(args, lfs_globals)
        elif args.fam:
            return fam(args, lfs_globals)
        else:
            raise ValueError('Illegal shadow setting "%s"' % args.shadow_dir)
    except Exception as e:
        msg = str(e)
    # seems to be ignored, as is SystemExit
    raise OSError(errno.EINVAL, 'lfs_shadow: %s' % msg)
