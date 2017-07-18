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

# Two convenience modules are exported: lower and higher levels of abstraction.
# Some of this (INSERT, UPDATE, DELETE) might be better in sqlassist.
# In SQLite3, cursor.rowcount doesn't seem to be valid after SELECT.

from pdb import set_trace

import sqlite3
import os

from sqlassist import SQLassist
from sqlbackend import LibrarianDBackendSQL


class SQLite3assist(SQLassist):
    '''Defines a cursor class based on SQLite and helpers from sqlassist.
       This is a "lower-level" module.'''

    _SQLshowtables = 'SELECT name FROM main.sqlite_master WHERE type="table";'
    _SQLshowschema = 'PRAGMA table_info({});'

    # Crossover data so all "base" classes have it.
    SCHEMA_VERSION = LibrarianDBackendSQL.SCHEMA_VERSION

    def DBconnect(self):
        try:
            uri = 'file:%s' % self.db_file
            if self.ro:
                uri += '?mode=ro'
            self._conn = sqlite3.connect(uri,
                                         uri=True,
                                         isolation_level='EXCLUSIVE')
            self._cursor = self._conn.cursor()
        except Exception as e:
            raise RuntimeError('Cannot open %s: %s' % (self.db_file, str(e)))

        # WAL: https://www.sqlite.org/wal.html
        try:
            self.execute('PRAGMA journal_mode=WAL')
        except Exception as e:
            dir = os.path.dirname(os.path.realpath(self.db_file))
            raise RuntimeError('Cannot open WAL journal in %s' % dir)

    def __init__(self, **kwargs):
        super(self.__class__, self).__init__(**kwargs)
        self.DBname = kwargs['db_file']

    def schema(self, table):
        self.execute(self._SQLshowschema.format(table))
        tmp = tuple([str(row[1]) for row in self.fetchall()])
        assert tmp, 'Empty schema'
        return tmp

    def _execAndCheck(self, sql, values, commit):
        assert values, 'missing values for %s' % sql
        self.execute(sql, values)
        # First is explicit failure like UNIQUE collision or bad SQL,
        # second is more generic
        if self.execfail:
            raise AssertionError('%s failed: %s' % (sql, self.execfail))
        if self.rowcount != 1 and not sql.startswith('DELETE'):
            self.rollback()
            raise AssertionError('%s failed: rowcount mismatch' % (sql))
        if commit:
            self.commit()

    def INSERT(self, table, values, commit=True):
        '''Values are a COMPLETE tuple following ordered schema.  Primary
           key (always 'id')  can be "None" for DB to autoselect.'''
        qmarks = ', '.join(['?'] * len(values))
        sql = 'INSERT INTO %s VALUES (%s)' % (table, qmarks)
        self._execAndCheck(sql, values, commit)
        return self.lastrowid   # good with or without commit

    def UPDATE(self, table, setclause, values, commit=False):
        '''setclause is effective key=val, key=val sequence'''
        assert setclause, 'missing setclause for UPDATE'
        sql = 'UPDATE %s SET %s' % (table, setclause)
        self._execAndCheck(sql, values, commit)

    # FIXME: move fields2qmarks in here (makes sense), then just pass
    # the data object?  Or do schema manips belong "one level up" ?
    def DELETE(self, table, where, values, commit=False):
        '''Values are a COMPLETE tuple following ordered schema'''
        assert where, 'missing where for DELETE'
        sql = 'DELETE FROM %s WHERE %s' % (table, where)
        self._execAndCheck(sql, values, commit)
        pass

    #
    # sqlite: if PRIMARY, but not AUTOINC, you get autoinc behavior and
    # hole-filling.  Explicitly setting id overrides that.
    #
    def getnextid(self, table):
        self.execute('SELECT MAX(id) FROM %s' % table)
        id = self.fetchone()
        if isinstance(id[0], int):
            return id[0] + 1
        # no rows? double check
        self.execute('SELECT COUNT(id) FROM %s' % table)
        id = self.fetchone()[0]
        if id == 0:
            return 1    # first id is non-zero
        raise RuntimeError('Cannot calculate nextid for %s' % table)

    #
    # To assist testing
    #

    def check_tables(self):
        print("Checking data tables()")

        total_tables = 0
        table_names = []
        tables_to_ignore = ( 'sqlite_sequence', )
        self.execute("""SELECT name FROM sqlite_master
                             WHERE type='table'
                             ORDER BY Name""")
        tables = [ t[0] for t in self.fetchall() if
                   t[0] not in tables_to_ignore ]

        for table in tables:

            db_query = "PRAGMA table_info(%s)" % (table)
            self.execute(db_query)
            number_of_columns = len(self.fetchall())

            db_query = "PRAGMA table_info(%s)" % (table)
            self.execute(db_query)
            columns = self.fetchall()

            db_query = "SELECT Count() FROM %s" % (table)
            self.execute(db_query)
            number_of_rows = self.fetchone()[0]

            print("Table: %s (columns = %d, rows = %d)" %
                  (table, number_of_columns, number_of_rows))

            for column in columns:
                print("  ", column)

            table_names.append(table)
            total_tables += 1

        print("Total number of tables: %d" % total_tables)

###########################################################################


class LibrarianDBackendSQLite3(LibrarianDBackendSQL):

    @staticmethod
    def argparse_extend(parser):
        # group = parser.add_mutually_exclusive_group()
        parser.add_argument(
            '--db_file',
            help='SQLite3 database backing store file',
            type=str,
            default="/var/hpetm/librarian.db")

    def __init__(self, args):
        if not os.path.isfile(args.db_file):
            raise RuntimeError('DB file "%s" does not exist' % args.db_file)
        try:
            self._cur = SQLite3assist(db_file=args.db_file)
            self._cur.execute('SELECT schema_version FROM globals')
            tmp = self._cur.fetchone()
        except Exception as e:
            raise RuntimeError('DB "%s": %s' % (args.db_file, str(e)))
        assert tmp is not None and isinstance(tmp, tuple) and tmp, \
            '"%s" is not a valid Librarian database' % self._cur.DBname
        assert tmp[0] == self._cur.SCHEMA_VERSION, \
            'Schema version mismatch in DB "%s"' % self._cur.DBname
