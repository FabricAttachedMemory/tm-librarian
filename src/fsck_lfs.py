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

import os
import sys
import stat

from argparse import Namespace  # result of an argparse sequence
from pdb import set_trace

from backend_sqlite3 import LibrarianDBackendSQLite3
from book_shelf_bos import TMBook, TMShelf, TMBos

###########################################################################


def _prompt(msg, defaultY=True):
    hint = 'Y/n' if defaultY else 'y/N'
    while True:
        junk = input('%s? (%s) ' % (msg, hint))
        if not junk:
            return defaultY
        if junk.upper().startswith('Y'):
            return True
        if junk.upper().startswith('N'):
            return False

###########################################################################
# Account for oddities:
# 1. Somehow opened_shelves points to a non-existent shelf (LEFT JOIN)

def _10_stale_handles(db):
    '''Remove stale file handles (dangling opens)'''
    sql = '''SELECT name, shelf_id, node_id, pid
             FROM opened_shelves LEFT JOIN shelves
             WHERE shelf_id = shelves.id'''
    db.execute(sql)
    db.iterclass = 'default'
    stale = [ s for s in db ]
    print('%d detected' % len(stale))
    if not stale:
        return
    for s in stale:
        print('\tclosing %s' % s.name)     # let them see
        db.DELETE('opened_shelves', 'shelf_id=?', (s.shelf_id, ))
    db.execute('DELETE FROM opened_shelves')
    db.commit()

    db.execute(sql)
    db.iterclass = None
    stale = [ s for s in db ]
    print('\t %d stale file handle(s) remain(s)' % len(stale))

###########################################################################
# The "unlink workflow" renames a file to 'tmfs_hidden_NNN (pure FuSE).
# LFS then renames it to .lfs_pending_zero_NNN, marks all books as
# ZOMBIE, then zeroes them via dd, truncates to zero, then kills the shelf.

def _20_finish_unlink(db):
    '''Finalize partially-unlinked files'''

    patterns = ('.lfs_pending_zero_%', '.tmfs_hidden%')
    sql = '''SELECT * FROM shelves
             WHERE name LIKE ? OR name LIKE ? '''
    db.execute(sql, patterns)
    db.iterclass = TMShelf
    partial = [ p for p in db ]
    print('%d detected' % len(partial))
    if not partial:
        return
    for shelf in partial:
        print('\tremoving %s' % (shelf.name))
        # get_bos(shelf) returns TMBook objects, but I want TMBos
        bos = db.get_bos_by_shelf_id(shelf.id)
        for thisbos in bos:
            db.delete_bos(thisbos)
            book = db.get_book_by_id(thisbos.book_id)
            if book.allocated == TMBook.ALLOC_INUSE:
                book.allocated = TMBook.ALLOC_ZOMBIE
                book.matchfields = 'allocated'
                db.modify_book(book)
        db.delete_shelf(shelf)
        db.commit()

###########################################################################


def _30_zombie_sith(db):
    '''Return zombie books to free pool'''
    db.execute('SELECT * FROM books where allocated=?', TMBook.ALLOC_ZOMBIE)
    db.iterclass = TMBook
    zombie_books = [ z for z in db ]
    print('%d detected' % len(zombie_books))
    if not zombie_books:
        return
    for book in zombie_books:
        book.allocated = TMBook.ALLOC_FREE
        book.matchfields = 'allocated'
        db.modify_book(book)
    db.commit()
    pass

###########################################################################


def _40_verify_shelves_return_orphaned_books(db):
    '''Verify book ownership & return orphan books to free pool'''
    # Run this AFTER zombie clears so DB contains only INUSE and FREE
    # books.  Make set of all allocated books.  Then for each shelf:
    #   Insure all shelf books are in all_books
    #   Remove shelf books from all_books set
    # Any leftovers are orphans
    db.execute('SELECT book_size_bytes FROM globals')
    book_size_bytes = db.fetchone()[0]
    db.execute('SELECT id FROM books where allocated=?', TMBook.ALLOC_INUSE)
    used_books = [ u[0] for u in db ]
    used_books = frozenset(used_books)
    print('%d book(s) in use' % len(used_books))
    shelves = db.get_shelf_all()    # Keep going even if empty, you'll see

    # Seq nums okay?  Eliminate dupes and check
    fatal = False
    for shelf in shelves:
        shelf.bos = db.get_bos_by_shelf_id(shelf.id)
        seqs = frozenset(bos.seq_num for bos in shelf.bos)
        if len(seqs) != len(shelf.bos):
            print('\t duplicate sequence numbers in', shelf.name)
            fatal = True
    if fatal:
        raise RuntimeError('Problems detected; no repair automation exists')

    # Book allocations
    for shelf in shelves:
        shelf.bos = db.get_books_on_shelf(shelf)
        bookset = set(b.id for b in shelf.bos)
        if used_books.intersection(bookset) != bookset:
            unallocated = bookset - used_books
            # FIXME: seq_nums are valid and there are no zombies
            # This may recover truncated but unreleased books
            for u in unallocated:
                book = db.get_book_by_id(u)
                book.allocated = TMBook.ALLOC_INUSE
                book.matchfields = 'allocated'
                db.modify_book(book)
                bookset = bookset.union(set((book.id,)))
            db.commit()
        used_books = used_books - bookset

        if len(bookset) != shelf.book_count:
            print('\tAdjusting %s book count %d -> %d' % (
                shelf.name, shelf.book_count, len(bookset)))
            shelf.book_count = len(bookset)
            shelf.matchfields = 'book_count'
            db.modify_shelf(shelf)

        tmp = shelf.book_count * book_size_bytes
        if shelf.size_bytes > tmp:
            print('\tAdjusting %s book size %d -> %d' % (
                shelf.name, shelf.size_bytes, tmp))
            shelf.size_bytes = tmp
            shelf.matchfields = 'size_bytes'
            db.modify_shelf(shelf)
            pass

    # All shelves have been scanned.  Anything left?
    if used_books:
        print('\tClearing %d book(s) marked as allocated but shelfless' %
            len(used_books))
        for book_id in used_books:
            notused = db.get_book_by_id(book_id)
            notused.allocated = TMBook.ALLOC_FREE
            notused.matchfields = 'allocated'
            db.modify_book(notused)
        db.commit()

###########################################################################


def _50_clear_orphaned_xattrs(db):
    '''Remove orphaned extended attributes'''
    db.execute('SELECT id FROM shelves')
    shelf_ids = frozenset(r[0] for r in db.fetchall())
    db.execute('SELECT shelf_id FROM shelf_xattrs')
    xattr_ids = frozenset(r[0] for r in db.fetchall())
    orphans = xattr_ids - shelf_ids
    print('%d detected' % len(orphans))
    for orphan in orphans:
        db.DELETE('shelf_xattrs', 'shelf_id=?', (orphan, ))
    db.commit()

###########################################################################


# TODO should these variables be used globally?
_GARBAGE_SHELF_ID = 1
_ROOT_SHELF_ID = 2
_LOST_FOUND_SHELF_ID = 3

def _60_find_lost_shelves(db):
    '''Move orphan files/directories to lost+found'''

    # get shelves and their ids from databse
    shelves = db.get_shelf_all()
    shelf_ids = [s.id for s in shelves]

    lost_shelves_count = 0

    for shelf in shelves:
        # ignore garbage shelf
        if (shelf.id != _GARBAGE_SHELF_ID) and (shelf.parent_id not in shelf_ids):
            lost_shelves_count += 1
            # move orphan shelf (and therefore all its children) to lost+found
            shelf.parent_id = _LOST_FOUND_SHELF_ID
            # add "_<shelf_id>" to shelf's name to eliminate name conflicts
            shelf.name = shelf.name + '_' + str(shelf.id)
            # update parent_id and name all at once
            shelf.matchfields = ('parent_id', 'name')
            db.modify_shelf(shelf)

    print('%s found' % lost_shelves_count)
    db.commit()

###########################################################################


def _70_fix_link_counts(db):
    '''Fix any inconsistent link_counts of directories'''
    # run after _60_find_lost_shelves b/c lost+found link_count
    # will be wrong if any directories were moved there

    # get shelves from database
    shelves = db.get_shelf_all()
    link_counts_wrong_count = 0

    # remove all shelves that are not directories;
    # they should not be counted when shelf_parent_ids.count() is called
    tmp = list(shelves)
    # loop through temp list so no shelves are skipped
    for s in tmp:
        if not stat.S_ISDIR(s.mode):
            shelves.remove(s)

    # only get parent_ids after non-directory shelves have been removed
    shelf_parent_ids = [s.parent_id for s in shelves]

    # loop through shelves again to correct link_counts
    for shelf in shelves:
        wrong = False
        # skip garbage shelf
        if shelf.id != _GARBAGE_SHELF_ID:
            # check shelf's link_count
            children = shelf_parent_ids.count(shelf.id)
            if shelf.id == _ROOT_SHELF_ID:
                # root directory is a little different cuz it's its own parent
                if shelf.link_count != children + 1:
                    link_counts_wrong_count += 1
                    shelf.link_count = children + 1
                    shelf.matchfields = 'link_count'
                    db.modify_shelf(shelf)
            elif shelf.link_count != children + 2:
                wrong = True
            elif shelf.link_count < 2:
                # given the above elif, this may be unnecessary, but shouldn't hurt
                wrong = True
            # don't have duplicate code to modify db inside each if/elif
            if wrong:
                link_counts_wrong_count += 1
                shelf.link_count = children + 2
                shelf.matchfields = 'link_count'
                db.modify_shelf(shelf)

    print('%s inconsistency(ies)' % link_counts_wrong_count)
    db.commit()

###########################################################################


def capacity(db):
    '''Print stats until a problem occurs'''
    db.execute('SELECT books_total FROM globals')
    g_books_total = db.fetchone()[0]
    db.execute('SELECT COUNT(*) FROM books')
    book_count = db.fetchone()[0]
    if g_books_total != book_count:
        raise SystemExit('Book count mismatch: no recovery is possible')
    db.execute(
        'SELECT COUNT(*) FROM books where allocated=?',
        TMBook.ALLOC_FREE)
    free_books = db.fetchone()[0]
    print('free books / total books = %d / %d (%d%%)\n' % (
        free_books, g_books_total, (100 * free_books) / g_books_total))

###########################################################################


if __name__ == '__main__':

    try:
        # Using this instead of SQLite3assist gets higher level ops
        db = LibrarianDBackendSQLite3(Namespace(db_file=sys.argv[1]))
    except Exception as e:
        raise SystemExit(str(e))

    capacity(db)

    # Order matters.
    for f in (_10_stale_handles,
              _20_finish_unlink,
              _30_zombie_sith,
              _40_verify_shelves_return_orphaned_books,
              _50_clear_orphaned_xattrs,
              _60_find_lost_shelves,
              _70_fix_link_counts):
        try:
            print(f.__doc__, end=': ')
            f(db)
            print('')
        except Exception as e:
            db.rollback()
            print(str(e), file=sys.stderr)
            raise SystemExit(
                'Send %s to rocky.craig@hpe.com' % sys.argv[1])

    print('Finalizing database')    # FIXME: compaction, ????

    capacity(db)
    db.close()
    raise SystemExit(0)
