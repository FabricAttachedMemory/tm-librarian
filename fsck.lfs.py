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

def stale_handles(db):
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


def finish_unlink(db):
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
            # book = db.get_book_by_id(thisbos.book_id)     # pedantic
            book = TMBook(id=thisbos.book_id)               # good enough
            book.allocated = TMBook.ALLOC_FREE
            book.matchfields = 'allocated'
            db.modify_book(book)
            pass
        db.delete_shelf(shelf)
        db.commit()

def zombie_sith(db):
    '''Return all zombie books to free pool'''
    print('not implemented')
    pass

def orphaned_books(db):
    '''Return all orphan books to free pool'''
    print('not implemented')
    pass

def verify_shelves(db):
    '''Verify all assigned books on a shelf are allocated'''
    print('not implemented')
    pass

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

    for f in (stale_handles, finish_unlink, zombie_sith,
              orphaned_books, verify_shelves):
        try:
            print(f.__doc__, end=': ')
            f(db)
            print('')
        except Exception as e:
            db.rollback()
            raise SystemExit(str(e))

    capacity(db)
    db.close()
    raise SystemExit(0)
