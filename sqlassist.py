#!/usr/bin/python3 -tt
'''The base class for all SQL RDBMS'''

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
import getpass
from datetime import date, datetime
from decimal import Decimal
from pdb import set_trace
from genericobj import GenericObject

###########################################################################


class Schema(object):
    '''Exposes a schema'''

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
# Started as sqlcursor, giving basic connect/execute wrappers.  Some
# higher-level APIs arose during development and got pulled in here.


class SQLassist(object):
    """Abstract Base Class, or mostly an interface.  Inherit from this
       and define a DBconect() method that sets _conn and _cursor."""

    _defaults = {
        'DBname':           None,
        'getSchemas':       False,
        'user':             None,
        'passwd':           None,
        'raiseOnExecFail':  True,
        'ro':               False
    }

    def DBconnect(self, *args):
        raise NotImplementedError

    def schema(self, *args):
        raise NotImplementedError

    def check_tables(self):
        raise NotImplementedError

    def INSERT(self, *args):
        raise NotImplementedError

    def UPDATE(self, *args):
        raise NotImplementedError

    def DELETE(self, *args):
        raise NotImplementedError

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
    def iterclass(self):
        return self._iterclass

    @iterclass.setter
    def iterclass(self, cls):
        '''Set to None, 'default', or a class with __init__(..., **kwargs)'''
        if cls is None or cls == 'raw':
            self._iterclass = None
        elif cls in ('default', 'generic'):
            self._iterclass = GenericObject
        elif not isinstance(cls, str):
            self._iterclass = cls
        else:
            raise ValueError('must be None, "generic", or a class name')

    def __iter__(self):
        return self

    def __next__(self):
        '''Fancier than fetchone/many'''
        r = self._cursor.fetchone()
        if not r:
            self._iterclass = None  # yes, force problems "next time"
            raise StopIteration
        if self._iterclass is None:
            return r
        cur = self._cursor
        asdict = dict(zip([f[0] for f in cur.description], r))
        return self._iterclass(**asdict)

    # Act like a cursor, except for the commit() method, because a cursor
    # doesn't have one.   Don't invoke methods here; rather, return a
    # callable to be invoked by the caller.

    def __getattr__(self, name):
        if self._cursor is None:
            raise AttributeError('DB has no cursor')

        if name == 'close':
            cur = self._cursor
            conn = self._conn
            self._cursor = self._conn = None
            return conn.close if cur is not None else (lambda: False)

        # Connection methods.  'execute' is a special case, handled below.
        if name in ('commit', 'rollback'):
            return getattr(self._conn, name)

        realattr = self._cursor.__getattribute__(name)
        if name != 'execute':
            return realattr

        # Wrap it to use the internal attrs in a callback, hidden from user.
        # This is where actual execute() occurs and errors can be trapped.
        def exec_wrapper(query, parms=None):
            self.execfail = ''
            try:
                if parms is None:
                    self._cursor.execute(query)
                else:
                    # Insure a tuple.  DON'T call tuple() as it iterates.
                    if not isinstance(parms, tuple):
                        parms = (parms, )
                    self._cursor.execute(query, parms)
            except Exception as e:  # includes sqlite3.Error
                self.execfail = str(e)
                if self.raiseOnExecFail:
                    raise
            return
        return exec_wrapper
