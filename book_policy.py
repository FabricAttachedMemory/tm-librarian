#!/usr/bin/python3 -tt
#---------------------------------------------------------------------------
# Assistance routines for handling book allocation policy.
# Designed for "Full Rack Demo" (FRD) to be launched in the summer of 2016.
#---------------------------------------------------------------------------

import errno
import os
import sys
from pdb import set_trace
from random import randint, shuffle

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

    POLICY_DEFAULT = 'LocalNode'
    _policies = (POLICY_DEFAULT, 'RandomBook', 'Nearest',
                 'LZAascending', 'LZAdescending')

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
        if elems[1] != 'LFS':       # simple get or set, leave it to caller
            return (xattr, value)

        # LFS special values
        assert len(elems) == 3, 'LFS xattrs are of form "user.LFS.xxx"'
        assert not removing, 'Removal of LFS xattrs is prohibited'

        if elems[2] == 'AllocationPolicy':
            if setting:
                assert value in cls._policies, \
                    'Bad AllocationPolicy "%s"' % value
        elif elems[2] == 'AllocationPolicyList':
            assert not setting, 'Setting AllocationPolicyList is prohibited'
            value = ','.join(cls._policies)
        elif elems[2] == 'Interleave':
            assert not setting, 'Setting Interleave is prohibited'
            shelf = LCEobj.cmd_get_shelf(cmdict)
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

    def _IGlist2books(self, books_needed, IGs, exclude=False):
        db = self.LCEobj.db
        freebooks = db.get_books_by_intlv_group(
            books_needed, IGs, exclude=exclude)
        return freebooks

    def _policy_LocalNode(self, books_needed):
        return self._policy_Nearest(books_needed, LocalNode=True)

    def _policy_Nearest(self, books_needed, LocalNode=False):
        node = int(self.context['node_id'])
        IGs = ( _node2ig(node), )
        localbooks = self._IGlist2books(books_needed, IGs)
        if LocalNode:
            return localbooks

        # Are there enough?
        books_needed -= len(localbooks)
        assert books_needed >= 0, '"Nearest" policy internal error: node'
        if not books_needed:
            return localbooks

        # Helper to get books from a set of nodes.  Grab all candidate
        # books, randomize, and return what's needed.
        def _nodeset2candidates(books_needed, nodeset):
            IGs = [ _node2ig(n) for n in nodeset ]
            books = self._IGlist2books(999999, IGs)
            shuffle(books)
            return books[:books_needed]

        # Get the next batch from elsewhere in this enclosure.
        enc = FRDnode(node).enc
        lo = ((enc - 1) * 10) + 1
        encnodes = frozenset(range(lo, lo + 10))
        set_trace()
        encbooks = _nodeset2candidates(
            books_needed, encnodes - frozenset((node,)))

        # Are there enough?
        books_needed -= len(encbooks)
        assert books_needed >= 0, '"Nearest" policy internal error: enclosure'
        if not books_needed:
            return localbooks + encbooks

        # Get the next batch from OUTSIDE this enclosure.
        nonencnodes = frozenset(range(1, 81)) - encnodes
        nonencbooks = _nodeset2candidates(books_needed, nonencnodes)

        # It doesn't really matter if there are enough, this is it
        books_needed -= len(nonencbooks)
        assert books_needed >= 0, '"Nearest" policy internal error: rack'
        return localbooks + encbooks + nonencbooks

    def _policy_LZAascending(self, books_needed, ascending=True):
        # using IGs 0-79 on nodes 1-80
        IG = 99999
        db = self.LCEobj.db
        freebooks = db.get_books_by_intlv_group(
            IG, TMBook.ALLOC_FREE, books_needed,
            inverse=True, ascending=ascending)
        return freebooks

    def _policy_LZAdescending(self, books_needed):
        freebooks = self._policy_LZAascending(books_needed, ascending=False)
        return freebooks

    def _policy_RandomBook(self, books_needed, excludeIG=9999):
        # using IGs 0-79 on nodes 1-80
        # select random books from the entire FAM pool
        book_pool = 99999
        random_books = [ ]
        db = self.LCEobj.db
        freebooks = db.get_books_by_intlv_group(
            excludeIG, TMBook.ALLOC_FREE, book_pool, inverse=True)

        while books_needed > 0:
            index = randint(0, len(freebooks) - 1)
            random_books.append(freebooks.pop(index))
            books_needed -= 1

        return random_books

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
