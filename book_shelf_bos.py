#!/usr/bin/python3 -tt

# Copyright 2017 Hewlett Packard Enterprise Development LP

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2 as
# published by the Free Software Foundation.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License along
# with this program.  If not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA

import time
from pdb import set_trace

#########################################################################


class BookShelfStuff(object):      # could become a mixin

    # If not specified here, the mechanism doesn't work in subclasses.
    # Obviously this needs an override.  Unfortunately, it doesn't
    # work to set it in this __init__,  There's probably some way
    # to do it with metaclasses, that's another day.

    __slots__ = ()

    _sorted = None

    def _msg(self, basemsg):
        return '%s: %s' % (self.__class__.__name__, basemsg)

    def __init__(self, *args, **kwargs):
        if not self._sorted:
            self.__class__._sorted = tuple(sorted(self._ordered_schema))
        assert not (args and kwargs), self._msg(
            'full tuple or kwargs, not both')
        if args and isinstance(args[0], dict):
            kwargs = args[0]
            args = None
        if args:
            assert len(args) == len(
                self._ordered_schema), self._msg('bad arg count')
            submitted = dict(zip(self._ordered_schema, args))
            missing = {}
        else:
            submitted = frozenset(kwargs.keys())
            missing = self.__slots__ - submitted - set((self._MFname,))
            if False and not self.__slots__.issubset(submitted):
                print('Missing fields "%s"' % (
                    ', '.join(sorted([k for k in missing]))))
            submitted = kwargs
            missing = dict(zip(missing, (0, ) * len(missing)))

        for src in (submitted, missing):
            for k, v in src.items():
                try:    # __slots__ is in play
                    setattr(self, k, v)
                except AttributeError as e:
                    pass
        setattr(self, self._MFname, None)

    def __eq__(self, other):
        for k in self._ordered_schema:  # not ids
            if getattr(self, k) != getattr(other, k):
                return False
        return True

    def __str__(self):
        s = []
        for k in sorted(self.__slots__):
            if k[0] != '_':
                val = getattr(self, k)
                if k.endswith('time'):
                    val = time.ctime(val)
                s.append('{}: {}'.format(k, val))
        return '\n'.join(s)

    def __repr__(self):         # makes "p" work better in pdb
        return self.__str__()

    def __getitem__(self, key):    # and now I'm a dict
        return getattr(self, key)

    # for (re)conversion to send back across the wire
    @property
    def dict(self):
        d = {}
        for k in self.__slots__:
            if k[0] != '_':
                val = getattr(self, k)
                d[k] = val
        return d

    # Be liberal in what I take, versus expecting people to remember
    # to *expand existing tuples.
    def tuple(self, *args):
        if args:
            if isinstance(args[0], tuple):  # probably matchfields
                args = args[0]
        else:
            args = self._ordered_schema
        return tuple([getattr(self, a) for a in args])

    @property
    def schema(self):
        return self._ordered_schema

    # Used for DB searches.  Align with next two property names.
    _MFname = '_matchfields'

    @property
    def matchfields(self):
        return getattr(self, self._MFname)

    # Liberal in what you accept
    @matchfields.setter
    def matchfields(self, infields):
        if isinstance(infields, str):
            infields = (infields, )
        for f in infields:
            assert f in self._ordered_schema, self._msg('Bad field %s' % f)
        setattr(self, self._MFname, infields)

#########################################################################


class TMBook(BookShelfStuff):

    ALLOC_FREE = 0    # available for allocation
    ALLOC_INUSE = 1   # allocated in shelves
    ALLOC_ZOMBIE = 2  # being prepared for allocation
    ALLOC_OFFLINE = 3  # unavailable for allocation

    _ordered_schema = (  # a little dodgy
        'id',
        'intlv_group',
        'book_num',
        'allocated',
        'attributes',
    )

    # Gotta do this here or the mechanism doesn't work.
    __slots__ = frozenset((_ordered_schema) + (BookShelfStuff._MFname, ))

#########################################################################


class TMShelf(BookShelfStuff):

    _ordered_schema = (  # a little dodgy
        'id',
        'creator_id',
        'size_bytes',
        'book_count',
        'ctime',
        'mtime',
        'name',
        'mode',
        'parent_id',
        'link_count'
    )

    # Gotta do this here or the mechanism doesn't work.  "bos" will
    # probably only be used on the client(s) as this type of info
    # is reasonably ephemeral in The Librarian.  _fd is only used
    # by shadow_dir as the fd of the per-shelf backing file.
    __slots__ = frozenset((_ordered_schema) + (BookShelfStuff._MFname,
                                               'bos',
                                               'open_handle',
                                               '_fd'))

    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        # super.__init__ sets things to zero if the constructor dict is
        # missing them.  Some fields need a different "missing" value.
        if self.bos == 0:
            self.bos = [ ]
        if self.open_handle == 0:
            self.open_handle = None
        if self._fd == 0:
            self._fd = -1

    def __eq__(self, other):
        # If size_bytes match, then len(bos) must match.
        if self.id != other.id or self.size_bytes != other.size_bytes:
            return False
        for book in self.bos:
            if book not in other.bos:
                return False
        return True

#########################################################################


class TMBos(BookShelfStuff):

    _ordered_schema = (  # a little dodgy
        'shelf_id',
        'book_id',
        'seq_num'
    )

    # Gotta do this here or the mechanism doesn't work.
    __slots__ = frozenset((_ordered_schema) + (BookShelfStuff._MFname, ))

#########################################################################


class TMOpenedShelves(BookShelfStuff):

    _ordered_schema = (  # a little dodgy
        'id',
        'shelf_id',
        'node_id',
        'pid'
    )

    # Gotta do this here or the mechanism doesn't work.
    __slots__ = frozenset((_ordered_schema) + (BookShelfStuff._MFname, ))
