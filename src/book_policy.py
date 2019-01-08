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

#---------------------------------------------------------------------------
# Assistance routines for handling book allocation policy.
# Designed for "Full Rack Demo" (FRD) to be launched in the summer of 2016.
#---------------------------------------------------------------------------

import errno
import os
import random
import sys
from pdb import set_trace
from collections import defaultdict

from book_shelf_bos import TMBook
from frdnode import BooksIGInterpretation as BII    # for constants

#--------------------------------------------------------------------------
# lfs_fuse.py does a little syntax/error checking before calling *_xattr
# routines.  xattr_assist does more and does the math for some of the
# user.LFS.xxxxx intrinsics.  Start with general logic checks.


def _node_id2ig(node_id, mode=BII.MODE_LZA):
    '''Right now nodes go from 1-80 but IGs are 0-79'''
    assert 0 < node_id <= 80, 'Bad node value'
    # Overload IG field with BIImode before DB query
    return (node_id - 1) | (mode << BII.MODE_SHIFT)


class BookPolicy(object):

    _policies = ('RandomBooks', 'LocalNode', 'LocalEnc', 'NonLocal_Enc',
                 'Nearest', 'NearestRemote', 'NearestEnc', 'NearestRack',
                 'LZAascending', 'LZAdescending', 'RequestIG')

    DEFAULT_ALLOCATION_POLICY = 'RandomBooks'    # mutable

    XATTR_ALLOCATION_POLICY =         'user.LFS.AllocationPolicy'
    XATTR_ALLOCATION_POLICY_DEFAULT = 'user.LFS.AllocationPolicyDefault'
    XATTR_ALLOCATION_POLICY_LIST =    'user.LFS.AllocationPolicyList'
    XATTR_IG_REQ = 'user.LFS.InterleaveRequest'
    XATTR_IG_REQ_POS = 'user.LFS.InterleaveRequestPos'

    @classmethod
    def xattr_assist(cls, LCEobj, cmdict, setting=False, removing=False):
        '''More error checking and legwork for user.LFS.xxxxx.  LCEobj is
           both data to be used and returns the error on a Raise.'''
        LCEobj.errno = errno.EINVAL
        xattr = cmdict['xattr']
        value = cmdict.get('value', None)
        if setting:
            assert value is not None, 'Trying to set a null value'
        elems = xattr.split('.')
        # If it's not a special LFS variable, let the caller handle it
        if elems[1] != 'LFS':
            return (xattr, value)

        assert len(elems) == 3, 'LFS xattrs are of form "user.LFS.xxx"'
        assert not removing, 'Removal of LFS xattrs is prohibited'
        try:
            shelf = LCEobj.cmd_get_shelf(cmdict)
        except AssertionError as e:
            if cmdict['name']:
                raise
            # It's the root of the file system.  Keep going.

        no_set = 'Setting %s is prohibited' % xattr
        LCEobj.errno = errno.ENOTSUP
        if xattr == cls.XATTR_IG_REQ:
            # Value can just fall through but there might be extra work
            if setting:
                reqIGs = [ord(value[i:i+1]) for i in range(0, len(value), 1)]

                # as originally written:
                # interleave_groups = LCEobj.db.get_interleave_groups()
                # currentIGs = [ig.groupId for ig in interleave_groups]
                # optimization noticed during MODE_PHYSICAL work:
                currentIGs = [ig.groupId for ig in LCEobj.IGs]

                assert set(reqIGs).issubset(currentIGs), \
                    'Requested IGs not subset of known IGs'
                # Reset current position in pattern.
                LCEobj.db.modify_xattr(shelf, cls.XATTR_IG_REQ_POS, 0)
        elif xattr == cls.XATTR_IG_REQ_POS:
            assert not setting, no_set
        elif xattr == cls.XATTR_ALLOCATION_POLICY:
            if setting:
                assert value in cls._policies, \
                    'Bad AllocationPolicy "%s"' % value
        elif xattr == cls.XATTR_ALLOCATION_POLICY_LIST:
            assert not setting, no_set
            value = ','.join(cls._policies)
        elif elems[2] == 'Interleave':
            assert not setting, no_set
            bos = LCEobj.db.get_books_on_shelf(shelf)
            value = bytes([ b.intlv_group & BII.IG_MASK for b in bos ]).decode()
        elif xattr == cls.XATTR_ALLOCATION_POLICY_DEFAULT:
            if setting:
                legal = frozenset(cls._policies) - frozenset(('RequestIG',))
                assert value in legal, 'Bad %s value: %s' % (xattr, value)
                cls.DEFAULT_ALLOCATION_POLICY = value
            else:
                value = cls.DEFAULT_ALLOCATION_POLICY
        else:
            raise AssertionError('Bad LFS attribute "%s"' % xattr)
        return (xattr, value)

   #-----------------------------------------------------------------------
    # Return a list of books or raise an error.

    def __init__(self, LCEobj, shelf, context):
        '''LCEobj members are used to enforce the 1:1 IG:node assumption'''
        if LCEobj.BII.is_MODE_LZA:
            assert len(LCEobj.IGs) == len(LCEobj.nodes), 'IG:node != 1:1'
        LCEobj.errno = errno.EINVAL
        self.LCEobj = LCEobj
        self.shelf = shelf
        self.context = context
        self.name = LCEobj.db.get_xattr(shelf, 'user.LFS.AllocationPolicy')
        assert self.name in self._policies, 'Unknown policy "%s"' % self.name

    def __str__(self):
        return '%s policy=%s' % (self.shelf.name, self.name)

    def __repr__(self):
        return self.__str__()

    def _IGs2books(self, books_needed, IGs, exclude=False, shuffle=True):
        db = self.LCEobj.db
        # Randomization needs to work with EVERY possible book
        tmp = 999999 if shuffle else books_needed
        books = db.get_books_by_intlv_group(tmp, IGs, exclude=exclude)
        if shuffle:
            random.shuffle(books)
        return books[:books_needed]

    # For "NUMA" distance calculations there are three sources
    # (Node | Enc | OffEnc aka Rack), eight states.
    # Node  Enc Rack
    # T     T   T   Full "Nearest", serves as the basis for others
    # T     T   F   NearestEnclosure, starting with myself
    # T     F   T   LocalNodeRack
    # T     F   F   LocalNode only
    # F     T   T   NearestRemote
    # F     T   F   NonLocal_Enc: Off node but this enc:
    # F     F   T   Away from this node and this enc: NearestRack
    # F     F   F   noop state, not sane, not used

    # T T T: Nearest, below

    # T T F
    def _policy_NearestEnc(self, books_needed):
        return self._policy_Nearest(books_needed,
            fromRack=False)

    # T F T
    def _policy_LocalNodeRack(self, books_needed):
        return self._policy_Nearest(books_needed,
            fromEnc=False)

    # T F F
    def _policy_LocalNode(self, books_needed):
        return self._policy_Nearest(books_needed,
            fromEnc=False, fromRack=False)

    # F T T
    def _policy_NearestRemote(self, books_needed):
        return self._policy_Nearest(books_needed,
            fromLocal=False)

    # F T F
    def _policy_NonLocal_Enc(self, books_needed):
        return self._policy_Nearest(books_needed,
            fromLocal=False, fromRack=False)

    # F F T
    def _policy_NearestRack(self, books_needed):
        return self._policy_Nearest(books_needed,
            fromLocal=False, fromEnc=False)

    # F F F: noop, not used

    def _node_ids2books(self, books_needed, node_ids, shuffle=True):
        '''This should ONLY be called from _policy_Nearest()'''
        # Get books from a set of nodes.   Grab candidate books, maybe
        # randomize, and return what's requested.
        if isinstance(node_ids, int):
            node_ids = (node_ids, )
        if not node_ids:
            return []
        IGs = [ _node_id2ig(n, self.LCEobj.BII()) for n in node_ids ]
        return self._IGs2books(books_needed, IGs, shuffle=shuffle)

    def _policy_Nearest(self, books_needed,
            fromLocal=True, fromEnc=True, fromRack=True):
        '''Get books closest to calling SoC.  Stop when enough books are
           found OR no more can be found (ie, return a short list).'''

        assert fromLocal or fromEnc or fromRack, \
            '_policy_Nearest(): nothing selected'

        caller_id = int(self.context['node_id'])
        caller = [n for n in self.LCEobj.nodes if n.node_id == caller_id][0]
        extant_node_ids = frozenset(n.node_id for n in self.LCEobj.nodes)

        # Assume all SD Flex partitions are in a single rack/enc
        if self.LCEobj.BII.is_MODE_PHYSADDR:
            enc_node_ids = frozenset(n.node_id for n in self.LCEobj.nodes)
        else:
            enc_node_ids = frozenset((n.node_id for n in self.LCEobj.nodes
                if n.enc == caller.enc))

        localbooks = []
        encbooks = []
        nonencbooks = []

        if fromLocal:
            localbooks = self._node_ids2books(
                books_needed, caller_id, shuffle=False)

        if not (fromEnc or fromRack):
            return localbooks   # stop now regardless of len(localbooks)

        # Are there enough local books?
        books_needed -= len(localbooks)
        assert books_needed >= 0, '"Nearest" policy error: node'
        if not books_needed:
            return localbooks

        # Where does the next batch come from?
        if fromEnc:
            candidate_ids = enc_node_ids - frozenset((caller_id, ))
            encbooks = self._node_ids2books(books_needed, candidate_ids)

            # Are there enough additional books in this enclosure?
            books_needed -= len(encbooks)
            assert books_needed >= 0, '"Nearest" policy error: enclosure'
            if not books_needed:
                return localbooks + encbooks

        # How about the final batch?
        if fromRack:
            candidate_ids = extant_node_ids - enc_node_ids
            nonencbooks = self._node_ids2books(books_needed, candidate_ids)

            # It doesn't really matter if there are enough, this is it
            books_needed -= len(nonencbooks)
            assert books_needed >= 0, '"Nearest" policy error: rack'

        return localbooks + encbooks + nonencbooks

    def _policy_RandomBooks(self, books_needed):
        '''Using all IGs, select random books from all of FAM.'''
        notIGs = (99999,)
        return self._IGs2books(books_needed, notIGs, exclude=True, shuffle=True)

    def _policy_LZAascending(self, books_needed, ascending=True):
        '''Using all IGs, select books from all of FAM in specified order.'''
        db = self.LCEobj.db
        freebooks = db.get_books_by_intlv_group(
            books_needed, (999999, ), exclude=True, ascending=ascending)
        return freebooks

    def _policy_LZAdescending(self, books_needed):
        freebooks = self._policy_LZAascending(books_needed, ascending=False)
        return freebooks

    def _policy_RequestIG(self, books_needed):
        '''Select books from IGs specified in interleave_request attribute.
           If interleave_request_pos is present use it as the starting point.'''
        db = self.LCEobj.db
        ig_req = db.get_xattr(self.shelf, self.XATTR_IG_REQ)
        self.LCEobj.errno = errno.ERANGE
        assert ig_req is not None, \
            'RequestIG policy requires prior %s' % self.XATTR_IG_REQ
        assert len(ig_req), \
            'RequestIG policy requires prior %s' % self.XATTR_IG_REQ

        # Get a starting position for the interleave_request list
        self.LCEobj.errno = errno.ENOSPC
        pos = db.get_xattr(self.shelf, self.XATTR_IG_REQ_POS)
        try:
            ig_pos = int(pos)
            if ig_pos < 0 or ig_pos > (len(ig_req) - 1):
                ig_pos = 0
        except TypeError as err:    # TSNH, see create_shelf.  Legacy paranoia.
            ig_pos = 0
            resp = db.create_xattr(self.shelf, self.XATTR_IG_REQ_POS, ig_pos)
        except ValueError as err:
            ig_pos = 0

        reqIGs = [ord(ig_req[i:i+1]) for i in range(0, len(ig_req), 1)]

        # Determine number of books needed from each IG
        igCnt = defaultdict(int)
        cur = ig_pos
        for cnt in range(0, books_needed):
            ig = reqIGs[cur % len(reqIGs)]
            igCnt[ig] += 1
            cur += 1

        # Allocate specified number of books from each selected IG
        booksIG = {}
        for ig in igCnt.keys():
            # Overload IG field with BIImode before DB query
            mod_ig = ig | self.LCEobj.BII() << BII.MODE_SHIFT
            booksIG[ig] = db.get_books_by_intlv_group(
                igCnt[ig], (mod_ig, ), exclude=False)

        # Build list of books using request_interleave pattern
        self.LCEobj.errno = errno.ENOSPC
        bookList = []
        cur = ig_pos
        for cnt in range(0, books_needed):
            ig = reqIGs[cur % len(reqIGs)]
            assert len(booksIG[ig]) != 0, 'Not enough books remaining in IG'
            bookList.append(booksIG[ig].pop(0))
            cur += 1

        # Save current position in interleave_request list
        db.modify_xattr(self.shelf, self.XATTR_IG_REQ_POS, cur % len(reqIGs))

        return bookList

    def __call__(self, books_needed):
        '''Look up the appropriate routine or throw an error'''
        self.LCEobj.errno = errno.ENOSYS
        try:
            policy_func = self.__class__.__dict__['_policy_' + self.name]
            return policy_func(self, books_needed)
        except KeyError as e:
            # AssertionError is a "gentler" reporting path back to user
            raise AssertionError('"%s" is not implemented' % self.name)

###########################################################################
# This is NOT for testing, just a quick entry without the full Librarian.


if __name__ == '__main__':
    set_trace()
    policy = BookPolicy(None, None, None)
