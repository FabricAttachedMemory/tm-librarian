#!/usr/bin/python3 -tt

import time
from pdb import set_trace

#########################################################################


class BookShelfStuff(object):      # could become a mixin

    # If not specified here, the mechanism doesn't work in subclasses.
    # Obviously this needs an override.  Unfortunately, it doesn't
    # work to set it in this __init__,  There's probably some way
    # to do it with metaclasses, that's another day.

    __slots__ = ()

    def __init__(self, *args, **kwargs):
        self.__class__._sorted = tuple(sorted(self._ordered_schema))
        if args:
            assert not kwargs, 'full tuple or kwargs, not both'
            assert len(args) == len(self._ordered_schema), 'bad arg count'
            submitted = dict(zip(self._ordered_schema, args))
            missing = {}
        else:
            submitted = frozenset(kwargs.keys())
            missing = self.__slots__ - submitted
            if False and not self.__slots__.issubset(submitted):
                print('Missing fields "%s"' % (
                    ', '.join(sorted([k for k in missing]))))
            submitted = kwargs
            missing = dict(zip(missing, (0, ) * len(missing)))

        for src in (submitted, missing):
            for k, v in src.items():
                setattr(self, k, v)
        setattr(self, self._MFname, None)

    def __eq__(self, other):
        for k in self._ordered_schema:
            if getattr(self, k) != getattr(other, k):
                return False
        return True

    def __str__(self):
        s = []
        for k in self._sorted:
            val = getattr(self, k)
            if k.endswith('time'):
                val = time.ctime(val)
            s.append('{}: {}'.format(k, val))
        return '\n'.join(s)

    def __getitem__(self, key):    # and now I'm a dict
        return getattr(self, key)

    def tuple(self, *args):
        if not args:
            args = self._ordered_schema
        return tuple([getattr(self, a) for a in args])

    @property
    def schema(self):
        return self._ordered_schema

    # Used for DB searches.  Align with next two property names.
    _MFname = '_matchfields'

    @property
    def matchfields(self):
        return getattr(self, self._MFname)

    @matchfields.setter
    def matchfields(self, infields):
        for f in infields:
            assert f in self._ordered_schmea, 'Bad field %s' % f
        sgetattr(self, self._MFname, (infields))

#########################################################################


class TMBook(BookShelfStuff):

    _ordered_schema = (  # a little dodgy
        'id',
        'node_id',
        'allocated',
        'attributes',
    )

    # Gotta do this here or the mechanism doesn't work.
    __slots__ = frozenset((_ordered_schema) + (BookShelfStuff._MFname, ))

#########################################################################


class TMShelf(BookShelfStuff):

    _ordered_schema = (  # a little dodgy
        'id',
        'creator_id',
        'size_bytes',
        'book_count',
        'open_count',
        'ctime',
        'mtime',
        'name'
    )

    # Gotta do this here or the mechanism doesn't work.
    __slots__ = frozenset((_ordered_schema) + (BookShelfStuff._MFname, ))

#########################################################################


class TMBos(BookShelfStuff):

    _ordered_schema = (  # a little dodgy
        'shelf_id',
        'book_id',
        'seq_num'
    )

    # Gotta do this here or the mechanism doesn't work.
    __slots__ = frozenset((_ordered_schema) + (BookShelfStuff._MFname, ))

#########################################################################
# Support testing

if __name__ == '__main__':

    from sqlcursors import SQLiteCursor

    cur = SQLiteCursor()    # no args == :memory:

    book1 = TMBook()
    print(book1)

    shelf1 = TMShelf()
    print(shelf1)
    set_trace()

    fields = cur.schema('books')
    assert set(fields) == set(TMBook._ordered_schema), 'TMBook oopsie'
    fields = cur.schema('shelves')
    assert set(fields) == set(TMShelf._ordered_schema), 'TMShelfoopsie'

    sql = '''INSERT INTO books VALUES (?, ?, ?, ?, ?)'''
    set_trace()
    cur.execute(sql, book1.tuple())
    cur.commit()
    print(cur.rowcount, "row inserted")  # only after updates, not SELECT

    # ways to build objects

    sql = 'SELECT * FROM Books LIMIT 1'
    tmp = cur.execute(sql).fetchone()
    plagiarize = TMBook(*tmp)
    print(book1 == plagiarize)

    pass

    cur.close()

    raise SystemExit(0)
