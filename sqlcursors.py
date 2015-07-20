#!/usr/bin/python3 -tt

import getpass, os, sys
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
        self.orderedFields = tuple([ f[0] for f in cursor.fetchall() ])
        assert len(self.orderedFields) > 1, \
            'Unexpected field count for %s (1)' % table

        cursor.execute('SELECT * FROM %s LIMIT 1' % table)
        junk = cursor.fetchone()     # might be superfluous; all I want is...
        assert junk is not None, 'Unexpected field count for %s (2)' % table
        set_trace()
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

        self.schemas = { }
        if self.getSchemas:
            tmp = self.execute(self._SQLshowtables)
            tables = self.fetchall()    # need a copy
            for t in tables:
                table = t[0]
                self.schemas[table] = Schema(self, table, self._SQLshowschema)

    def __str__(self):
        s = [ ]
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
                set_trace()
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
        tmp = tuple([ str(row[1]) for row in self.fetchall()])
        assert tmp, 'Empty schema'
        return tmp

###########################################################################

if __name__ == '__main__':

    from bookshelves import TMBook, mem_db_init
    cur = SQLiteCursor()
    mem_db_init(cur)
    print(cur.schema('Shelves'))

    # You caan't use qmark parmstyle on sqlite 'objects'.
    book1 = TMBook()
    sql = 'INSERT INTO books VALUES (?, ?, ?, ?, ?)'
    set_trace()
    cur.execute(sql, book1.tuple())

    sql = 'SELECT * FROM {} LIMIT 5'.format('books')

    # Direct: klunky but okay
    cur.execute(sql)
    cur.iterclass = None    # default on instantiation
    for r in cur.fetchall():
        print(r)

    # As the default object
    cur.execute(sql)
    cur.iterclass = 'default'
    for r in cur:
        print(r)

    # Might be subject to some module path juggling
    cur.execute(sql)
    cur.iterclass = TMBook
    for r in cur:
        print(r)

    set_trace()
    cur.close()
    raise SystemExit(0)
