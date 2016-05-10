#!/usr/bin/python3 -tt

import os
import sys

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
    print('\t %d stale file handles remain' % len(stale))

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
    db.execute('SELECT id FROM books where allocated=?', TMBook.ALLOC_INUSE)
    used_books = [ u[0] for u in db ]
    used_books = frozenset(used_books)
    print('%d books in use' % len(used_books))
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
        bookset = frozenset(b.id for b in shelf.bos)
        if used_books.intersection(bookset) != bookset:
            unallocated = bookset - used_books
            # FIXME: seq_nums are valid and there are no zombies
            # This may recover truncated but unreleased books
            for u in unallocated:
                book = db.get_book_by_id(u)
                book.allocated = TMBook.ALLOC_INUSE
                book.matchfields = 'allocated'
                db.modify_book(book)
                book_set = book_set + frozenset(book.id)
            db.commit()
        used_books = used_books - bookset

    if used_books:
        print('\tClearing %d book(s) marked as allocated but shelfless')
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
        raise SystemExit('Please supply a valid DB file name')

    capacity(db)

    # Order matters.
    for f in (_10_stale_handles,
              _20_finish_unlink,
              _30_zombie_sith,
              _40_verify_shelves_return_orphaned_books,
              _50_clear_orphaned_xattrs):
        try:
            print(f.__doc__, end=': ')
            f(db)
            print('')
        except Exception as e:
            db.rollback()
            print('Send %s to rocky.craig@hpe.com' % sys.argv[1],
                file=sys.stderr)
            raise SystemExit(str(e))

    print('Finalizing database')    # FIXME: compaction, ????

    capacity(db)
    db.close()
    raise SystemExit(0)
