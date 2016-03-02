#!/usr/bin/python3 -tt
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
from frdnode import FRDnode, FRDintlv_group

#--------------------------------------------------------------------------
# lfs_fuse.py does a little syntax/error checking before calling *_xattr
# routines.  xattr_assist does more and does the math for some of the
# user.LFS.xxxxx intrinsics.  Start with general logic checks.


def _node2ig(node):
    '''Right now nodes go from 1-80 but IGs are 0-79'''
    assert 0 < node <= 80, 'Bad node value'
    return node - 1


class BookPolicy(object):

    POLICY_DEFAULT = 'RandomBooks'
    _policies = (POLICY_DEFAULT, 'LocalNode', 'Nearest',
                 'LZAascending', 'LZAdescending', 'RequestIG')

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

        # Simple request special values

        if elems[1] == 'interleave_request' and setting:
            reqIGs = [ord(value[i:i+1]) for i in range(0, len(value), 1)]
            interleave_groups = LCEobj.db.get_interleave_groups()
            currentIGs = [ig.num for ig in interleave_groups]
            assert set(reqIGs).issubset(currentIGs), \
                'Requested IGs not subset of existing IGs'

        if elems[1] != 'LFS':       # simple get or set, leave it to caller
            return (xattr, value)

        # LFS special values

        assert len(elems) == 3, 'LFS xattrs are of form "user.LFS.xxx"'
        assert not removing, 'Removal of LFS xattrs is prohibited'

        shelf = LCEobj.cmd_get_shelf(cmdict)

        if elems[2] == 'AllocationPolicy':
            if setting:
                assert value in cls._policies, \
                    'Bad AllocationPolicy "%s"' % value
                if value != 'RequestIG':
                    # Remove RequestIG specific attributes
                    LCEobj.db.remove_xattr(shelf, 'user.interleave_request')
                    LCEobj.db.remove_xattr(shelf, 'user.interleave_request_pos')
        elif elems[2] == 'AllocationPolicyList':
            assert not setting, 'Setting AllocationPolicyList is prohibited'
            value = ','.join(cls._policies)
        elif elems[2] == 'Interleave':
            assert not setting, 'Setting Interleave is prohibited'
            bos = LCEobj.db.get_books_on_shelf(shelf)
            value = bytes([ b.intlv_group for b in bos ]).decode()
        else:
            raise AssertionError('Bad LFS attribute "%s"' % xattr)
        return (xattr, value)

   #-----------------------------------------------------------------------
    # Return a list of books or raise an error.

    def __init__(self, LCEobj, shelf, context):
        '''LCEobj members are used to enforce the 1:1 IG:node assumption'''
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

    def _policy_LocalNode(self, books_needed):
        return self._policy_Nearest(books_needed, LocalNode=True)

    def _policy_Nearest(self, books_needed, LocalNode=False):
        '''Get books starting with "this" node, perhaps stopping there.'''

        def _nodes2books(books_needed, nodes, shuffle=True):
            # Get books from a set of nodes.   Grab candidate books, maybe
            # randomize, and return what's requested.  "self" is upscope.
            if isinstance(nodes, int):
                nodes = (nodes, )
            IGs = [ _node2ig(n) for n in nodes ]
            return self._IGs2books(books_needed, IGs, shuffle=shuffle)

        node = int(self.context['node_id'])
        localbooks = _nodes2books(books_needed, node, shuffle=False)
        if LocalNode:
            return localbooks   # stop now regardless of len(localbooks)

        # Are there enough local books?
        books_needed -= len(localbooks)
        assert books_needed >= 0, '"Nearest" policy internal error: node'
        if not books_needed:
            return localbooks

        # Get the next batch from elsewhere in this enclosure.
        enc = FRDnode(node).enc
        lo = ((enc - 1) * 10) + 1
        encnodes = frozenset(range(lo, lo + 10))
        encbooks = _nodes2books(books_needed, encnodes - frozenset((node,)))

        # Are there enough additional books in this enclosure?
        books_needed -= len(encbooks)
        assert books_needed >= 0, '"Nearest" policy internal error: enclosure'
        if not books_needed:
            return localbooks + encbooks

        # Get the next batch from OUTSIDE this enclosure.
        allnodes = frozenset(range(1, len(self.LCEobj.nodes) + 1))
        nonencbooks = _nodes2books(books_needed, allnodes - encnodes)

        # It doesn't really matter if there are enough, this is it
        books_needed -= len(nonencbooks)
        assert books_needed >= 0, '"Nearest" policy internal error: rack'
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
        self.LCEobj.errno = errno.ENOSPC
        ig_req = db.get_xattr(self.shelf, 'user.interleave_request')
        assert ig_req is not None, 'RequestIG policy requires interleave_request attribute'

        # Get a starting position for the interleave_request list
        pos = db.get_xattr(self.shelf, 'user.interleave_request_pos')
        try:
            ig_pos = int(pos)
            if ig_pos < 0 or ig_pos > (len(ig_req)-1):
                ig_pos = 0
        except TypeError as err:
            ig_pos = 0
            resp = db.create_xattr(self.shelf, 'user.interleave_request_pos', 0)
        except ValueError as err:
            ig_pos = 0

        reqIGs = [ord(ig_req[i:i+1]) for i in range(0, len(ig_req), 1)]

        # Determine number of books needed from each IG
        igCnt = defaultdict(int)
        cur = ig_pos
        for cnt in range(0, books_needed):
            ig = reqIGs[cur%len(reqIGs)]
            igCnt[ig] += 1
            cur += 1

        # Allocate specified number of books from each selected IG
        booksIG = {}
        for ig in igCnt.keys():
            booksIG[ig] = db.get_books_by_intlv_group(igCnt[ig], (ig, ), exclude=False)

        # Build list of books using request_interleave pattern
        self.LCEobj.errno = errno.ENOSPC
        bookList = []
        cur = ig_pos
        for cnt in range(0, books_needed):
            ig = reqIGs[cur%len(reqIGs)]
            assert len(booksIG[ig]) != 0, 'Not enough books in IG to satisfy request'
            bookList.append(booksIG[ig].pop(0))
            cur += 1

        # Save current position in interleave_request list
        resp = db.modify_xattr(self.shelf, 'user.interleave_request_pos', cur%len(reqIGs))

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
