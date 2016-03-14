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
import ctypes

from copy import deepcopy
from subprocess import getoutput
from pdb import set_trace
from tm_fuse import TmfsOSError, tmfs_get_context
import tm_ioctl_opt as IOCTL

#--------------------------------------------------------------------------
# _shelfcache is essentially a copy of the Librarian's "opened_shelves"
# table data generated on the fly.  The goal was to avoid a round trip
# to the Librarian for many FuSE interactions.  This class duck-types a
# dict with multiple keys (name and file handles).  The value is a single,
# modified TMShelf object that holds all open-related data.  The data
# will assist PTE management for processes holding the shelf open.
# Account for multiple opens by same PID as well as different PIDs.
# Multinode support will require OOB support and a few more Librarian calls.


class shadow_support(object):
    '''Provide private data storage for subclasses.'''

    _mode_rw_file = stat.S_IFREG + 0o600  # regular file

    def __init__(self, args, lfs_globals):
        self.verbose = args.verbose
        self.book_size = lfs_globals['book_size_bytes']
        self._shelfcache = { }
        self.zero_on_unlink = True

        # These are only set after _detect_address_space which isn't called
        # for shadow_[dir|file]
        for attr in ('addr_mode', 'aperture_base', 'aperture_size', 'isFAME', ):
            setattr(self, attr, getattr(args, attr, 0))

        # Backing store (shadow_file or FAME direct) is contiguous but IG
        # ranges are not.  Map IG ranges onto areas of backing store.  Not
        # used for shadow_dir but it's not worth the effort to suppress.
        self._igstart = {}
        offset = 0
        for igstr in sorted(lfs_globals['books_per_IG'].keys()):
            ig = int(igstr)
            if self.verbose > 2:
                print('IG %2d flatspace offset @ %d (0x%x)' % (ig, offset, offset))
            self._igstart[ig] = offset
            books = int(lfs_globals['books_per_IG'][igstr])
            offset += books * self.book_size
        self.book_shift = int(math.log(self.book_size, 2))

    def _consistent(self, cached):
        try:
            all_fh = [ ]
            for vlist in cached.open_handle.values():
                all_fh += vlist
            for v_fh in all_fh:
                assert v_fh in self._shelfcache, 'Inconsistent list members'
        except Exception as e:
            print('Shadow cache is corrupt:', str(e), file=sys.stderr)
            if self.verbose > 3:
                set_trace()
            raise TmfsOSError(errno.EBADFD)

    # Most uses send an open handle fh (integer) as key.  truncate by name
    # is the exception.  An update needs to be reflected for all keys.
    def __setitem__(self, key, shelf):
        '''Part of the support for duck-typing a dict with multiple keys.'''
        fh = shelf.open_handle
        if fh is None:
            assert key == shelf.name, 'Might take more thought on this'
        cached = self._shelfcache.get(shelf.name, None)
        pid = tmfs_get_context()[2]

        # Is it a completely new addtion?  Remember, only cache open shelves.
        if cached is None:
            if fh is None:
                return
            # Create a copy because "open_handle" will be redefined.  This
            # single copy will be retrievable by the shelf name and all of
            # its open handles.  The copy itself has a list of the fh keys
            # indexed by pid, so open_handle.keys() is all the pids.
            cached = deepcopy(shelf)
            self._shelfcache[cached.name] = cached
            self._shelfcache[key] = cached
            cached.open_handle = { }
            cached.open_handle[pid] = [ key, ]
            return

        self._consistent(cached)    # As long as I'm here...

        # Has the shelf changed somehow?  If so, replace the cached copy
        # and perhaps take other steps.  Break down the comparisons in
        # book_shelf_bos.py::TMShelf::__eq__()
        if cached != shelf:
            invalidate = True   # and work to make it false
            assert cached.id == shelf.id, 'Shelf aliasing error?'  # TSNH :-)

            # If it grew OR remained the same size, are the first "n" books
            # still the same?  Order matters.
            if shelf.size_bytes >= cached.size_bytes:
                for i, book in enumerate(cached.bos):
                    if book != shelf.bos[i]:
                        break
                else:
                    invalidate = False  # the first "n" books match

            # Update cached object variant fields.  Beware references.
            cached.size_bytes = shelf.size_bytes
            cached.bos = deepcopy(shelf.bos)
            cached.book_count = shelf.book_count
            cached.mtime = shelf.mtime
            if invalidate:
                print('\n\tNEED TO INVALIDATE PTES!!!\n')

        # fh is unique (created by Librarian as table index).  Does it
        # need to be appended?
        if not isinstance(fh, int) or fh in self._shelfcache:
            return
        self._shelfcache[key] = cached
        try:
            cached.open_handle[pid].append(key)
        except KeyError as e:
            cached.open_handle[pid] = [ key, ]  # new pid

    def __getitem__(self, key):
        '''Part of the support for duck-typing a dict with multiple keys.
           Suppress KeyError, returning None if no value exists.'''
        return self._shelfcache.get(key, None)

    def __contains__(self, key):
        '''Part of the support for duck-typing a dict with multiple keys.'''
        return key in self._shelfcache

    def __delitem__(self, key):
        '''Part of the support for duck-typing a dict with multiple keys.'''
        is_fh = isinstance(key, int)
        try:
            cached = self._shelfcache[key]
        except KeyError as e:
            # Not currently open, something like "rm somefile"
            if not is_fh:
                return
            raise AssertionError('Deleting a missing fh?')
        self._consistent(cached)    # As long as I'm here...

        del self._shelfcache[key]   # always
        if is_fh:
            # Remove this direct shelf reference plus the back link
            open_handles = cached.open_handle
            for pid, fhlist in open_handles.items():
                if key in fhlist:
                    fhlist.remove(key)
                    if not fhlist:
                        del open_handles[pid]
                    if not open_handles:            # Last reference
                        del self._shelfcache[cached.name]
                    return
            # There has to be one
            raise AssertionError('Cannot find fh to delete')

        # It's a string so remove the whole thing.  This is only called
        # from unlink; so VFS has done the filtering job on open handles.
        if cached.open_handle is None:  # probably "unlink"ing
            return
        all_fh = [ ]
        open_handles = cached.open_handle
        if open_handles is not None:
            for vlist in cached.open_handle.values():
                all_fh += vlist
            all_fh = frozenset(all_fh)  # paranoid: remove dupes
            for fh in all_fh:
                del self._shelfcache[fh]

    def keys(self):
        '''Part of the support for duck-typing a dict with multiple keys.'''
        return self._shelfcache.keys()

    def items(self):
        '''Part of the support for duck-typing a dict with multiple keys.'''
        return self._shelfcache.items()

    def values(self):
        '''Part of the support for duck-typing a dict with multiple keys.'''
        return self._shelfcache.values()

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
        assert tmp < self.aperture_size, 'BAD SHADOW OFFSET'
        return tmp

    # Provide ABC noop defaults.  Note they're not all actually noop.
    # Top men are insuring this works with multiple opens of a shelf.

    def truncate(self, shelf, length, fh):
        if fh is not None:
            # This is an update, but there's no good way to flag that to
            # __setitem__.  Do an idiot check here.
            assert fh in self._shelfcache, 'VFS thinks %s is open but LFS does not' % shelf.name
        self[shelf.name] = shelf
        return 0

    def unlink(self, shelf_name):
        try:
            del self[shelf_name]
        except Exception as e:
            set_trace()
            raise
        return 0

    # "man fuse" regarding "hard_remove": an "rm" of a file with active
    # opens tries to rename it.
    def rename(self, old, new):
        try:
            # Retrieve shared object, fix it, and rebind to new name.
            cached = self._shelfcache[old]
            cached.name = new
            del self._shelfcache[old]
            self._shelfcache[new] = cached
        except KeyError as e:
            if new.startswith('.tmfs_hidden'):
                # VFS thinks it's there so I should too
                raise TmfsOSError(errno.ESTALE)
        return 0

    # Idiot checking and caching: shadow_support.  These routines should
    # be called LAST, after any localized ops on the shelf, like _fd.
    def open(self, shelf, flags, mode=None):
        assert isinstance(shelf.open_handle, int), 'Bad handle in open()'
        self[shelf.open_handle] = shelf
        return shelf.open_handle

    # Idiot checking and caching: shadow_support
    def create(self, shelf, mode=None):
        assert isinstance(shelf.open_handle, int), 'Bad handle in create()'
        assert self[shelf.name] is None and self[shelf.open_handle] is None, 'Cache inconsistency'
        self[shelf.open_handle] = shelf     # should be first one
        return shelf.open_handle

    # Idiot checking and UNcaching: shadow_support
    def release(self, fh):                  # Must be an fh, not an fd
        cached = self[fh]
        retval = deepcopy(cached)
        retval.open_handle = fh             # could be larger list
        del self[cached.name]
        return retval

    # Piggybacked for kernel to ask for stuff.  Even in --shadow_[dir|file]
    # it wants globals, handle that here.  During mmap fault handling it
    # wants more.  In shadow modes return 'FALLBACK' to get legacy, generic
    # cache-based handler with stock read() and write() spill and fill.
    # Overridden in class apertures to do true mmaps.
    def getxattr(self, shelf_name, xattr):
        try:
            if xattr == '_obtain_booksize_addrmode_aperbase':
                data = '%s,%d,%s' % (
                    self.book_size, self.addr_mode, self.aperture_base)
                return data
            return 'FALLBACK'   # might be circumvented by subclass
        except Exception as e:
            print('!!! ERROR IN GENERIC KERNEL XATTR HANDLER (%d): %s' % (
                sys.exc_info()[2].tb_lineno, str(e)),
                file=sys.stderr)
            return 'ERROR'

    def read(self, shelf_name, length, offset, fd):
        raise TmfsOSError(errno.ENOSYS)

    def write(self, shelf_name, buf, offset, fd):
        raise TmfsOSError(errno.ENOSYS)

    def ioctl(self, shelf_name, cmd, arg, fh, flags, data):
        return -1

#--------------------------------------------------------------------------


class shadow_directory(shadow_support):
    '''Create a regular file for each shelf as backing store.  These files
       will exist in the file system of the entity running lfs_fuse,
       ie, if you're on a VM, the file storage is in the VM disk image.'''

    def __init__(self, args, lfs_globals):
        super(self.__class__, self).__init__(args, lfs_globals)
        self.zero_on_unlink = False   # OS "clears" per-shelf backing file
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
        # FIXME: not tested since unlink was expanded to do zeroing
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
            shelf._fd = os.open(self.shadowpath(shelf.name), flags, mode=mode)
        except OSError as e:
            if flags & os.O_CREAT:
                if e.errno != errno.EEXIST:
                    raise TmfsOSError(e.errno)
            else:
                raise TmfsOSError(e.errno)

    def open(self, shelf, flags, mode=None):
        self._create_open_common(shelf, flags, mode)
        super(self.__class__, self).open(shelf, flags, mode)    # caching
        return shelf._fd    # so kernel sees a real fd for mmap under FALLBACK

    def create(self, shelf, mode=None):
        flags = os.O_CREAT | os.O_RDWR | os.O_CLOEXEC
        self._create_open_common(shelf, flags, mode)
        super(self.__class__, self).create(shelf, mode)  # caching
        return shelf._fd    # so kernel sees a real fd for mmap under FALLBACK

    def read(self, shelf_name, length, offset, fd):
        assert self[shelf_name]._fd == fd, 'fd mismatch on read'
        os.lseek(fd, offset, os.SEEK_SET)
        return os.read(fd, length)

    def write(self, shelf_name, buf, offset, fd):
        assert self[shelf_name]._fd == fd, 'fd mismatch on write'
        os.lseek(fd, offset, os.SEEK_SET)
        return os.write(fd, buf)

    def truncate(self, shelf, length, fd):  # shadow_dir, yes an fd
        try:
            if fd:  # It's an open shelf
                assert self[shelf.name]._fd == fd, 'fd mismatch on truncate'
            os.truncate(
                shelf._fd if shelf._fd >= 0 else self.shadowpath(shelf.name),
                length)
            shelf.size_bytes = length
            if shelf.open_handle is None:
                shelf = self[shelf.name]
                if shelf is not None:
                    shelf.size_bytes = length
            return 0
        except OSError as e:
            raise TmfsOSError(e.errno)

    def release(self, fd):              # shadow_dir: yes this is an fd
        os.close(fd)                    # never a raise()
        for shelf in self.values():     # search for fd to uncache shelf
            if shelf._fd == fd:
                for v in shelf.open_handle.values():
                    fh = v[0]
                    break
                break
        else:
            raise RuntimeError('release: fd=%s is not cached' % fd)
        super(self.__class__, self).release(fh)
        shelf.open_handle = fh
        shelf._fd = -1
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
        self.aperture_size = statinfo.st_size

        self._shadow_fd = fd

    # open() and create() only need to do caching as handled by superclass

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

#--------------------------------------------------------------------------
# Original IVSHMEM class did read/write against the resource file by mmapping
# slices # against /sys/bus/pci/..../resource2.  mmap in kernel was handled
# by legacy generic fault handles that used read/write.  There was a lot of
# overhead but it was functional against true global shared memory.

# 'class fam' supported true mmap in the kernel against the physical
# address of the IVSHMEM device, but it couldn't do read and write.  Merging
# the two classes (actually, just getxattr() from the previous "class fam")
# gets the best of both worlds (IVHSMEM and HW FAM) in one class.
# FIXME: open() and create() here and in shadow_[dir|file] can be coalesced
# so they all call super(shadow_support).  First of all, bring the two
# shadows up to speed with respect to the shadow_cache.  Then other
# mods will make sense.

class apertures(shadow_support):

    _MODE_NONE = 0          # See tmfs::lfs.c ADDRESS_MODES
    _MODE_DIRECT = 1        # FAME flat area only.
    _MODE_DIRECT_DESC = 2   # FAME, but talk to zbridge.
    _MODE_1906_DESC = 3     # TMAS: Zbridge directly programs DESBK, no chat
    _MODE_FULL_DESC = 4     # TMAS: full operation

    _NDESCRIPTORS = 1906            # Non-secure starting at the BAR...
    _NVM_BK = 0x01600000000         # Thus speaketh the chipset ERS

    _IG_SHIFT = 46                  # Bits of offset for 7 bit IG
    _IG_MASK = ((1 << 7) - 1)       # Mask for 7 bit IG
    _BOOK_SHIFT = 33                # Bits of offset for 20 bit book number
    _BOOK_MASK = ((1 << 13) - 1)    # Mask for 13 bit book number
    _BOOKLET_SHIFT = 16             # Bits of offset for 17 bit booklet number
    _BOOKLET_MASK = ((1 << 17) - 1) # Mask for 17 bit booklet number

    def __init__(self, args, lfs_globals):
        '''args needs to have valid attributes for IVSHMEM and descriptors.'''

        super(self.__class__, self).__init__(args, lfs_globals)

        # The field is called aperture_base even if direct mapping is used.
        self.aperture_base = args.aperture_base
        self.aperture_size = args.aperture_size
        self.isFAME = args.isFAME

    # open() and create() only need to do caching as handled by superclass

    def getxattr(self, shelf_name, xattr):
        # Called from kernel (fault, RW, atomics), don't die here :-)
        try:
            data = super(self.__class__, self).getxattr(shelf_name, xattr)
            if data != 'FALLBACK':
                return data

            bos = self[shelf_name].bos
            cmd, comm, pid, PABO = xattr.split(',')
            pid = int(pid)
            PABO = int(PABO)  # page-aligned byte offset into shelf
            book_num = PABO // self.book_size  # (0..n-1)
            if book_num >= len(bos):
                return 'ERROR'
            baseLZA = bos[book_num]['lza']

            if self.verbose > 3:  # Since this IS in the kernel :-)
                reason = xattr.split('_for_')[1]
                print('Get LZA: %s: process %s[%d] shelf=%s, PABO=%d (0x%x)' %
                    (reason, comm, pid, shelf_name, PABO, PABO))
                print('shelf book seq=%d, LZA=0x%x -> IG=%d, IGoffset=%d' % (
                    book_num,
                    baseLZA,
                    ((baseLZA >> self._IG_SHIFT) & self._IG_MASK),
                    ((baseLZA >> self._BOOK_SHIFT) & self._BOOK_MASK)))

            # If descriptors are NOT enabled, no further work needs to be
            # done.  This can only be true for the IVHSHMEM platform.

            # If total # of books <= total number of descriptors: there should
            # never be any evictions.  Right now zbridge driver is hardcoding
            # descriptors directly in the "3.9 nodes of NVM" scenario.  That
            # would be detected by this __init__ and can be
            # used to flesh out "evictionless" behavior.

            if self.addr_mode == self._MODE_DIRECT:   # "Legacy" IVSHMEM mode.
                phys_offset = self.shadow_offset(shelf_name, PABO)
                if phys_offset == -1:
                    return 'ERROR'
                map_addr = self.aperture_base + phys_offset
            elif self.addr_mode == self._MODE_1906_DESC:  # creep up on it
                map_addr = 0   # no-op here, kernel will calc aperture
            else:
                raise RuntimeError('Unimplemented mode %d' % self.addr_mode)

            data = '%d,%s,%s,%s' % (self.addr_mode, book_num, baseLZA, map_addr)
            if self.verbose > 3:
                print('data returned to fault handler = %s' % (data))
            return data

        except Exception as e:
            print('!!! ERROR IN LZA LOOKUP HANDLER (%d): %s' % (
                sys.exc_info()[2].tb_lineno, str(e)),
                file=sys.stderr)
            return 'ERROR'

    def ioctl(self, shelf_name, cmd, arg, fh, flags, data):

        LFS_GET_PHYS_FROM_OFFSET = IOCTL.IOWR(ord("L"), 0x01, ctypes.c_ulong)

        if (cmd == LFS_GET_PHYS_FROM_OFFSET):

            # data ---
            #   in : byte offset into shelf
            #   out: physical address for book at offset

            # read data sent in by user
            inbuf = ctypes.create_string_buffer(8)
            ctypes.memmove(inbuf, data, 8)
            offset = (int.from_bytes(inbuf, byteorder='little'))

            lfs_offset = self.shadow_offset(shelf_name, offset)
            if lfs_offset == -1:
                return -1
            physaddr = self.aperture_base + lfs_offset

            # send data back to user
            outbuf = physaddr.to_bytes(8, byteorder='little')
            ctypes.memmove(data, outbuf, 8)

            if self.verbose > 1:
                print("LFS_GET_PHYS_FROM_OFFSET: shelf_name = %s" % shelf_name)
                print("offset = %d (0x%x), physaddr = %d (0x%x)" %
                    (offset, offset, physaddr, physaddr))

            return 0
        else:
            return -1

#--------------------------------------------------------------------------


def _detect_memory_space(args, lfs_globals):
    # Discern ivshmem information.  Our convention states the first
    # IVSHMEM device found is used as fabric-attached memory.  Parse the
    # first block of lines of lspci -vv for Bus-Device-Function and
    # BAR2 information.

    if args.shadow_dir or args.shadow_file:
        assert not args.descriptors, 'Cannot use descriptors in this mode'
        args.addr_mode = apertures._MODE_NONE
        args.isFAME = False
        args.aperture_base = 0  # will get tweaked later
        args.aperture_size = 0
        return

    try:
        lspci = getoutput('lspci -vv -d1af4:1110').split('\n')[:11]
        if 'not found' in lspci[0]:
            print('lspci(1) is missing, assuming TM(AS)', file=sys.stderr)
    except Exception as e:
        lspci = ( '', '')
        pass

    # If not FAME/IVSHMEM, ass-u-me it's TMAS or real TM.  Hardcode the
    # direct descriptor mode for now, Zbridge preloads all 1906.
    if not lspci[0].endswith('Red Hat, Inc Inter-VM shared memory'):
        args.isFAME = False
        if args.verbose > 1:
            print('IVSHMEM cannot be found, assuming TM(AS) and fixed 1906')
        assert lfs_globals['books_total'] <= args.descriptors, \
            'Only supporting "Direct Descriptors" for now'
        assert args.descriptors <= apertures.NDESCRIPTORS, \
            'Descriptor count out of range'
        args.addr_mode = apertures._MODE_1906_DESC
        args.aperture_base = apertures._NVM_BK
        args.aperture_size = apertures._NDESCRIPTORS * lfs_globals['book_size_bytes']
        return

    args.isFAME = True

    # Parse lspci output
    bdf = lspci[0].split()[0]
    if args.verbose > 1:
        print('IVSHMEM device at %s used as fabric-attached memory' % bdf)
    memoryfile = '/sys/devices/pci0000:00/0000:%s/resource2' % bdf
    assert (os.path.isfile(memoryfile)), '%s is not a file' % memoryfile

    region2 = [ l for l in lspci if 'Region 2:' in l ][0]
    assert ('(64-bit, prefetchable)' in region2), \
        'IVSHMEM region 2 not found for device %s' % bdf
    args.aperture_base = int(region2.split('Memory at')[1].split()[0], 16)
    assert args.aperture_base, \
        'Could not retrieve base address of IVSHMEM device at %s' % bdf

    # Compare requirements to file size
    statinfo = os.stat(memoryfile)
    assert shadow_support._mode_rw_file == shadow_support._mode_rw_file & statinfo.st_mode, \
        '%s is not RW' % memoryfile
    assert statinfo.st_size >= lfs_globals['nvm_bytes_total'], \
        'st_size (%d) < nvm_bytes_total (%d)' % \
        (statinfo.st_size, lfs_globals['nvm_bytes_total'])

    # Paranoia check in face of multiple IVSHMEMS: zbridge emulation
    # has firewall table of 32M.  Make sure this is bigger.
    assert statinfo.st_size > 64 * 1 << 20, \
        'IVSHMEM at %s is not big enough, possible collision?' % bdf
    args.aperture_size = statinfo.st_size
    args.addr_mode = apertures._MODE_DIRECT

    if args.verbose > 2:
        print('IVSHMEM max offset is 0x%x; physical addresses 0x%x - 0x%x' % (
              args.aperture_size - 1,
              args.aperture_base, args.aperture_base + args.aperture_size - 1))

#--------------------------------------------------------------------------


def the_shadow_knows(args, lfs_globals):
    '''This is a factory.  args is command-line arguments from
       lfs_fuse.py and lfs_globals was received from the librarian.'''
    try:
        if args.shadow_dir:
            return shadow_directory(args, lfs_globals)
        elif args.shadow_file:
            return shadow_file(args, lfs_globals)

        _detect_memory_space(args, lfs_globals)     # Modifies args
        return apertures(args, lfs_globals)
    except Exception as e:
        msg = str(e)
        print('!!! ERROR IN PLATFORM DETERMINATION (%d): %s' % (
            sys.exc_info()[2].tb_lineno, msg),
            file=sys.stderr)

    # seems to be ignored, as is SystemExit
    raise OSError(errno.EINVAL, 'lfs_shadow: %s' % msg)
