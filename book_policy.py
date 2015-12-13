#!/usr/bin/python3 -tt
#---------------------------------------------------------------------------
# Assistance routines for handling book allocation policy.
# Designed for "Full Rack Demo" (FRD) to be launched in the summer of 2016.
#---------------------------------------------------------------------------

import errno
import os
import sys
from pdb import set_trace

from book_shelf_bos import TMBook
from frdnode import FRDnode, FRDintlv_group

#--------------------------------------------------------------------------
# lfs_fuse.py does a little syntax/error checking before calling *_xattr
# routines.  xattr_assist does more and does the math for some of the
# user.LFS.xxxxx intrinsics.  Start with general logic checks.

BOOK_POLICY_DEFAULT = 'LocalOnly'
_policies = (BOOK_POLICY_DEFAULT, 'LocalFirst', 'Random')

def xattr_assist(LCEobj, cmdict, setting=False, removing=False):
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
            assert value in _policies, 'Bad AllocationPolicy "%s"' % value
    elif elems[2] == 'Interleave':
        assert not setting, 'Setting Interleave is prohibited'
        shelf = LCEobj.cmd_get_shelf(cmdict)
        bos = LCEobj.db.get_books_on_shelf(shelf)
        value = bytes([ b.intlv_group for b in bos ]).decode()
    else:
        raise AssertionError('Bad LFS attribute')
    return (xattr, value)

###########################################################################
# Return a list of books or raise an error.  LCEobj has an IGs member, BTW.


def _policy_LocalOnly(LCEobj, shelf, cmdict, books_needed, inverse=False):
    # using IGs 0-79 on nodes 1-80
    IG = node_id = cmdict['context']['node_id'] - 1
    freebooks = LCEobj.db.get_books_by_intlv_group(
        IG, TMBook.ALLOC_FREE, books_needed, inverse)
    return freebooks


def _policy_LocalFirst(LCEobj, shelf, cmdict, books_needed):
    localbooks = _policy_LocalOnly(LCEobj, shelf, cmdict, books_needed)
    books_needed -= len(localbooks)
    assert books_needed >= 0, 'LocalFirst policy internal error'
    if not books_needed:
        return localbooks
    nonlocalbooks = _policy_LocalOnly(LCEobj, shelf, cmdict, books_needed,
                                      inverse=True)
    return localbooks.extend(nonlocalbooks)


def get_books_by_policy(LCEobj, shelf, cmdict, books_needed):
    LCEobj.errno = errno.EINVAL
    policy = LCEobj.db.get_xattr(shelf, 'user.LFS.AllocationPolicy')
    assert policy in _policies, 'Unknown policy "%s"' % policy

    if policy == BOOK_POLICY_DEFAULT:
        freebooks = _policy_LocalOnly(LCEobj, shelf, cmdict, books_needed)
    elif policy == 'LocalFirst':
        freebooks = _policy_LocalFirst(LCEobj, shelf, cmdict, books_needed)
    else:
        LCEobj.errno = errno.EINVAL
        raise AssertionError('%s not implemented yet' % policy)

    return freebooks
