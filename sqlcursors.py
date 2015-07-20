#!/usr/bin/python3 -tt

import os
import sys
import getpass
from datetime import date, datetime
from decimal import Decimal
from pdb import set_trace
from genericobj import GenericObject

###########################################################################


class Schema(object):

    def __init__(self, cursor, table, SQLschema):
        """Return an object with orderedFields and conversion functions.
           This will destroy an active query/fetch so do it OOB."""

        raise RuntimeError('This is how to do it for MySQL, not sqlite')
        cursor._cursor.execute(SQLschema.format())
        self.orderedFields = tuple([f[0] for f in cursor.fetchall()])
        assert len(self.orderedFields) > 1, \
            'Unexpected field count for %s (1)' % table

        cursor.execute('SELECT * FROM %s LIMIT 1' % table)
        junk = cursor.fetchone()     # might be superfluous; all I want is...
        assert junk is not None, 'Unexpected field count for %s (2)' % table
        desc = cursor.description
        assert len(desc) == len(self.orderedFields), 'Field length mismatch'

        # Return the CONVERSION function for each field.  Values are from
        # Python DBI spec and direct 'describe tables'
        type2conv = {
            2:   int,       # MySQL: smallint   TOTALLY UNTESTED SWAG
            3:   Decimal,   # integer(xx)
            5:   Decimal,   # double            TOTALLY UNTESTED SWAG
            7:   datetime,  # timestamp         TOTALLY UNTESTED SWAG
            10:  date,      # date              TOTALLY UNTESTED SWAG
            246: Decimal,   # float
            252: str,       # text              TOTALLY UNTESTED SWAG
            253: str        # smallint
        }

        for i, d in enumerate(desc):
            fname = d[0]
            assert self.orderedFields[i] == fname, 'Field name mismatch'
            try:
                setattr(self, fname, type2conv[d[1]])
            except KeyError:
                raise RuntimeError('unknown field spec %d' % d[1])

    def __len__(self):
        return len(self.orderedFields)

###########################################################################


class SQLcursor(object):
    """Abstract Base Class, or mostly an interface.  Inherit from this
       and define a DBconect() method that sets _conn and _cursor."""

    _defaults = {
        'DBname':           None,
        'getSchemas':       False,
        'user':             None,
        'passwd':           None,
    }

    def DBconnect(self):
        self._conn = None
        self._cursor = None
        raise NotImplementedError   # Left as an exercise to the reader

    def schema(self, table):
        raise NotImplementedError   # Left as an exercise to the reader

    def __init__(self, **kwargs):
        for k in self._defaults:
            if k not in kwargs:
                kwargs[k] = self._defaults[k]
        for k, v in kwargs.items():
            setattr(self, k, v)
        self.DBconnect()            # In the derived class
        self._iterclass = None

        return  # needs more work

        self.schemas = {}
        if self.getSchemas:
            tmp = self.execute(self._SQLshowtables)
            tables = self.fetchall()    # need a copy
            for t in tables:
                table = t[0]
                self.schemas[table] = Schema(self, table, self._SQLshowschema)

    def __str__(self):
        s = []
        for k in sorted(self.__dict__.keys()):
            if not k.startswith('_'):
                s.append(k + ' = ' + str(self.__dict__[k]))
        s.append('Iteration class = ' + str(self._iterclass))
        return '\n'.join(s)

    @property
    def iterclass(self, cls):
        return self._iterclass

    @iterclass.setter
    def iterclass(self, cls):
        '''Set to None, 'default', or a class with __init__(..., **kwargs)'''
        if cls is None or cls == 'raw':
            self._iterclass = None
        elif cls == 'default':
            self._iterclass = GenericObject
        elif hasattr(cls, '__init__'):
            self._iterclass = cls
        else:
            raise ValueError('must be None, "default", or a class name')

    def __iter__(self):
        return self

    def __next__(self):
        '''Fancier than fetchone/many'''
        r = self._cursor.fetchone()
        if not r:
            raise StopIteration
        if self._iterclass is None:
            return r
        asdict = dict(zip([f[0] for f in cur.description], r))
        return self._iterclass(**asdict)

    # Act like a cursor, except for the commit() method, because a cursor
    # doesn't have one.   Don't invoke methods here; rather, return a
    # callable to be invoked by the caller.

    def __getattr__(self, name):
        if name == 'close':
            cur = self._cursor
            conn = self._conn
            self._cursor = self._conn = None
            return conn.close if cur is not None else (lambda: False)

        if not hasattr(self, '_cursor'):
            raise RuntimeError(self.status)

        if name == 'commit':
            return self._conn.commit

        if self._cursor is None:
            raise AttributeError
            ('DB has no cursor')

        realattr = self._cursor.__getattribute__(name)
        if name != 'execute':
            return realattr

        # Wrap it to use the internal attrs in a callback, hidden from user
        def exec_wrapper(query, parms=None):
            try:
                if parms is None:
                    tmp = self._cursor.execute(query)
                else:
                    # Insure a tuple.  DON'T call tuple() as it iterates.
                    if not isinstance(parms, tuple):
                        parms = (parms, )
                    tmp = self._cursor.execute(query, parms)
            except Exception as e:
                print('SQL execute failed:', str(e))
                tmp = None
            return tmp
        return exec_wrapper

###########################################################################

import sqlite3


class SQLiteCursor(SQLcursor):

    _SQLshowtables = 'SELECT name FROM main.sqlite_master WHERE type="table";'
    _SQLshowschema = 'PRAGMA table_info({});'

    def DBconnect(self):
        try:
            self._conn = sqlite3.connect(self.DBfile)
            self._cursor = self._conn.cursor()
        except Exception as e:
            raise
        pass

    def __init__(self, **kwargs):
        if not 'DBfile' in kwargs:
            kwargs['DBfile'] = ':memory:'
        super(self.__class__, self).__init__(**kwargs)

    def schema(self, table):
        self.execute(self._SQLshowschema.format(table))
        tmp = tuple([str(row[1]) for row in self.fetchall()])
        assert tmp, 'Empty schema'
        return tmp

###########################################################################

if __name__ == '__main__':

    import time
    from book_register import create_empty_db
    from bookshelves import TMBook, TMShelf, TMBos

    print("--> Setup empty database, create and check table schemas")
    cur = SQLiteCursor()
    create_empty_db(cur)
    print("    Table (Globals):", cur.schema('Globals'))
    print("    Table (Books):", cur.schema('Books'))
    print("    Table (Shelves):", cur.schema('Shelves'))
    print("    Table (Books_on_shelf):", cur.schema('Books_on_shelf'))

    print("--> Write book size to the globals table, then print table")
    bs_size = (1024 * 1024 * 1024 * 8)
    cur.execute('INSERT INTO globals VALUES(?)', bs_size)
    cur.commit()
    cur.execute('SELECT * FROM globals')
    cur.iterclass = 'default'
    for r in cur:
        print("   ", r)

    print("--> Add five books to database, then retrieve and print them")
    node1 = 0x000000000000AAAA
    node2 = 0x000000000000BBBB
    book_id1 = 0x1111111111111111
    book_id2 = 0x2222222222222222
    book_id3 = 0x3333333333333333
    book_id4 = 0x4444444444444444
    book_id5 = 0x5555555555555555
    book1 = TMBook(book_id1, node1, 0, 0)
    book2 = TMBook(book_id2, node1, 0, 0)
    book3 = TMBook(book_id3, node1, 0, 0)
    book4 = TMBook(book_id4, node2, 0, 0)
    book5 = TMBook(book_id5, node2, 0, 0)
    cur.execute('INSERT INTO books VALUES(?, ?, ?, ?)', book1.tuple())
    cur.execute('INSERT INTO books VALUES(?, ?, ?, ?)', book2.tuple())
    cur.execute('INSERT INTO books VALUES(?, ?, ?, ?)', book3.tuple())
    cur.execute('INSERT INTO books VALUES(?, ?, ?, ?)', book4.tuple())
    cur.execute('INSERT INTO books VALUES(?, ?, ?, ?)', book5.tuple())
    cur.commit()
    cur.execute('SELECT * FROM books')
    cur.iterclass = 'default'
    print("    all books ---")
    for r in cur:
        print("     ", r)
    cur.execute('SELECT * FROM books WHERE node_id = ?', node1)
    cur.iterclass = 'default'
    print("    node1 books ---")
    for r in cur:
        print("     ", r)

    print("--> Delete one book and modify one book then print them")
    book_id1 = 0x1111111111111111
    book_id2 = 0x2222222222222222
    cur.execute('DELETE FROM books WHERE book_id = ?', book_id1)
    cur.execute('UPDATE books SET allocated = ? WHERE book_id = ?',
                (1, book_id2))
    cur.commit()
    cur.execute('SELECT * FROM books')
    cur.iterclass = 'default'
    print("    all books ---")
    for r in cur:
        print("     ", r)

    print("--> Create three shelves, then print them")
    shelf_id1 = 0x00000000AAAA0000
    shelf_id2 = 0x00000000BBBB0000
    shelf_id3 = 0x00000000CCCC0000
    c_id1 = 0x1111000011110000
    c_id2 = 0x2222000022220000
    c_id3 = 0x3333000033330000
    c_time = time.time()
    m_time = time.time()
    shelf1 = TMShelf(shelf_id1, c_id1, 0, 0, 0, c_time, m_time, "shelf_1")
    shelf2 = TMShelf(shelf_id2, c_id2, 0, 0, 0, c_time, m_time, "shelf_2")
    shelf3 = TMShelf(shelf_id3, c_id3, 0, 0, 0, c_time, m_time, "shelf_3")
    cur.execute('INSERT INTO shelves VALUES(?, ?, ?, ?, ?, ?, ?, ?)',
                shelf1.tuple())
    cur.execute('INSERT INTO shelves VALUES(?, ?, ?, ?, ?, ?, ?, ?)',
                shelf2.tuple())
    cur.execute('INSERT INTO shelves VALUES(?, ?, ?, ?, ?, ?, ?, ?)',
                shelf3.tuple())
    cur.commit()
    cur.execute('SELECT * FROM shelves')
    cur.iterclass = 'default'
    print("    all shelves ---")
    for r in cur:
        print("     ", r)

    print("--> Delete one shelf and modify one shelf then print them all")
    shelf_id1 = 0x00000000AAAA0000
    shelf_id2 = 0x00000000BBBB0000
    cur.execute('DELETE FROM shelves WHERE shelf_id = ?', shelf_id1)
    cur.execute('UPDATE shelves SET open_count = ? WHERE shelf_id = ?',
               (1, shelf_id2))
    cur.commit()
    cur.execute('SELECT * FROM shelves')
    cur.iterclass = 'default'
    print("    all shelves ---")
    for r in cur:
        print("     ", r)

    print("--> Create four BOS records, then print them")
    shelf_id1 = 0x00000000AAAA0000
    shelf_id2 = 0x00000000BBBB0000
    shelf_id3 = 0x00000000CCCC0000
    book_id1 = 0x1111111111111111
    book_id2 = 0x2222222222222222
    book_id3 = 0x3333333333333333
    book_id4 = 0x4444444444444444
    book_id5 = 0x5555555555555555
    bos1 = TMBos(shelf_id1, book_id1, 1)
    bos2 = TMBos(shelf_id1, book_id2, 2)
    bos3 = TMBos(shelf_id2, book_id3, 1)
    bos4 = TMBos(shelf_id2, book_id4, 2)
    bos5 = TMBos(shelf_id3, book_id5, 1)
    cur.execute('INSERT INTO books_on_shelf VALUES(?, ?, ?)', bos1.tuple())
    cur.execute('INSERT INTO books_on_shelf VALUES(?, ?, ?)', bos2.tuple())
    cur.execute('INSERT INTO books_on_shelf VALUES(?, ?, ?)', bos3.tuple())
    cur.execute('INSERT INTO books_on_shelf VALUES(?, ?, ?)', bos4.tuple())
    cur.execute('INSERT INTO books_on_shelf VALUES(?, ?, ?)', bos5.tuple())
    cur.commit()
    cur.execute('SELECT * FROM books_on_shelf')
    cur.iterclass = 'default'
    print("    all bos ---")
    for r in cur:
        print("     ", r)

    print("--> Delete all shelf1 bos, then print them all")
    shelf_id1 = 0x00000000AAAA0000
    shelf_id2 = 0x00000000BBBB0000
    cur.execute('DELETE FROM books_on_shelf WHERE shelf_id = ?', shelf_id1)
    cur.commit()
    cur.execute('SELECT * FROM books_on_shelf')
    cur.iterclass = 'default'
    print("    all shelves ---")
    for r in cur:
        print("     ", r)

    cur.close()

    raise SystemExit(0)
