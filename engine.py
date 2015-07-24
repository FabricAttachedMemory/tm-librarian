#!/usr/bin/python3 -tt
#---------------------------------------------------------------------------
# Librarian engine module
#---------------------------------------------------------------------------

import errno
import uuid
import time
import math
import sys
from pdb import set_trace

from sqlcursors import SQLiteCursor
from bookshelves import TMBook, TMShelf, TMBos
from cmdproto import LibrarianCommandProtocol

class LibrarianCommandExecution(object):

    book_size = 0
    book_columns = ('book_id', 'node_id', 'status', 'attributes', 'size_bytes')
    shelf_columns = ('shelf_id', 'size_bytes', 'book_count', 'open_count',
                 'c_time', 'm_time')
    bos_columns = ('shelf_id', 'book_id', 'seq_num')

    LIBRARIAN_VERSION = "Librarian v0.01"

    # sqlite: if PRIMARY, but not AUTOINC, you get autoinc behavior and
    # hole-filling.  Explicitly setting id overrides that.  Break it out
    # in case we switch to a UUID or something else.
    def _getnextid(self, table):
        self._cur.execute('SELECT MAX(id) FROM %s' % table)
        id = self._cur.fetchone()
        if isinstance(id[0], int):
            return id[0] + 1
        # no rows? double check
        self._cur.execute('SELECT COUNT(id) FROM %s' % table)
        id = self._cur.fetchone()[0]
        if id == 0:
            return 1    # first id is non-zero
        raise RuntimeError('Cannot discern nextid for ' + table)

    def _INSERT(self, table, values):
        assert len(values), 'oopsie'
        qmarks = ', '.join(['?'] * len(values))
        self._cur.execute(
            'INSERT INTO %s VALUES (%s)' % (table, qmarks),
            values
        )
        # First is explicit failure like UNIQUE collision, second is generic
        if self._cur.execfail:
            raise RuntimeError(
                'INSERT %s failed: %s' % (table, self._cur.execfail))
        if self._cur.rowcount != 1:
            self._cur.rollback()
            raise RuntimeError('INSERT %s failed' % table)
        # DO NOT COMMIT, give caller a chance for multiples or rollback

    def _UPDATE(self, table, setclause, values):
        self._cur.execute('UPDATE %s SET %s' % (table, setclause), values)
        if not self._cur.rowcount == 1:
            self._cur.rollback()
            raise RuntimeError('update %s failed' % table)
        # DO NOT COMMIT, give caller a chance for multiples or rollback

    def cmd_version(self):
        """ Return librarian version
            In (dict)---
                None
            Out (dict) ---
                librarian version
        """
        return self.LIBRARIAN_VERSION

    def cmd_create_shelf(self):
        """ Create a new shelf
            In (dict)---
                None
            Out (dict) ---
                shelf data
        """
        id = self._getnextid('shelves')
        tmp = int(time.time())
        shelf = TMShelf(
            id=id,
            ctime=tmp,
            mtime=tmp,
            name=self._cmdict['name'],
        )
        # Since shelves are indexed on 'name', dupes fail nicely.
        self._INSERT('shelves', shelf.tuple())
        self._cur.commit()
        return shelf

    def cmd_list_shelf(self, aux=None):
        """ List a given shelf.
            In (dict)---
                shelf_id
            Out (dict) ---
                shelf data
        """
        self._cur.execute(
            'SELECT * FROM shelves WHERE %s = ?' % field,
            self._cmdict[field])
        self._cur.iterclass = TMShelf
        shelves = [ r for r in self._cur ]
        assert len(shelves) <= 1, 'oopsie'
        return shelves[0] if shelves else None

    def cmd_list_shelves(self):
        self._cur.execute('SELECT * FROM shelves')
        self._cur.iterclass = TMShelf
        shelves = [ r for r in self._cur ]
        return shelves

    def cmd_open_shelf(self):
        """ Open a shelf for access by a node.
            In (dict)---
                shelf_id
                node_id
                uid
                gid
            Out (dict) ---
                shelf data
        """
        shelf = self.cmd_list_shelf()
        if shelf is None:
            return None

        shelf.mtime = int(time.time())
        shelf.open_count += 1
        self._UPDATE(
            'shelves',
            'mtime=?, open_count=? WHERE id=?',
            shelf.tuple('mtime', 'open_count', 'id')
        )
        self._cur.commit()
        return shelf

    def cmd_close_shelf(self):
        """ Close a shelf against access by a node.
            In (dict)---
                shelf_id
                node_id
                uid
                gid
            Out (dict) ---
                shelf data
        """
        # todo: check if node/user really has this shelf open
        # todo: ensure open count does not go below zero

        set_trace()
        shelf = self.cmd_list_shelf(aux='id')
        if shelf is None:
            return None

        shelf.mtime = int(time.time())
        shelf.open_count -= 1

        self._UPDATE(
            'shelves',
            'mtime=?, open_count=? WHERE id=?',
            shelf.tuple('mtime', 'open_count', 'id')
        )
        self._cur.commit()
        return shelf


    def cmd_destroy_shelf(cmd_data):
        """ Destroy a shelf and free any books associated with it.
            In (dict)---
                shelf_id
                node_id
                uid
                gid
            Out (dict) ---
                shelf data
        """
        shelf_id = cmd_data["shelf_id"]
        node_id = cmd_data["node_id"]
        uid = cmd_data["uid"]
        gid = cmd_data["gid"]

        if uid != 0:
            return '{"error":"Permission denied for non-root user"}'

        resp = db.get_shelf(shelf_id)
        # todo: fail if shelf does not exist
        shelf_id, size_bytes, book_count, open_count, c_time, m_time = (resp)

        if open_count != 0:
            return '{"error":"Shelf open count is non-zero"}'

        db_data = db.get_bos_by_shelf(shelf_id)
        for bos in db_data:
            print("bos:", bos)
            shelf_id, book_id, seq_num = (bos)
            db_data = db.delete_bos(bos)
            book_data = db.get_book_by_id(book_id)
            book_id, node_id, status, attributes, size_bytes = (book_data)
            book_data = (book_id, node_id, 0, attributes, size_bytes)
            db_data = db.modify_book(book_data)

        # Delete shelf
        db_data = db.delete_shelf(shelf_id)

        return '{"success":"Shelf destroyed"}'

    def cmd_resize_shelf(self):
        """ Resize given shelf given a shelf and new size in bytes.
            In (dict)---
                shelf_id
                node_id
                size_bytes
            Out (dict) ---
                shelf data
        """
        set_trace()
        shelf = self.cmd_list_shelf(aux='id')
        if shelf is None:
            return None

        new_size_bytes = int(self._cmdict['size_bytes'])
        assert new_size_bytes >= 0, 'Bad size'
        if new_size_bytes == shelf.size_bytes:
            return shelf
        shelf.size_bytes = new_size_bytes
        shelf.mtime = int(time.time())
        self._UPDATE(
            'shelves',
            'mtime=?, size_bytes=? WHERE id=?',
            shelf.tuple('mtime', 'size_bytes', 'id')
        )
        self._cur.commit()
        return shelf

        new_book_count = int(math.ceil(new_size_bytes / self.book_size))
        books_needed = new_book_count - shelf.book_count
        if not new_book_count:
            return shelf
        set_trace()

        if books_needed > 0:
            seq_num = shelf.book_count
            db_data = db.get_book_by_node(node_id, 0, books_needed)
            # todo: check we got back enough books
            for book in db_data:
                # Mark book in use and create BOS entry
                seq_num += 1
                book_id, node_id, status, attributes, size_bytes = (book)
                book_data = (book_id, node_id, 1, attributes, size_bytes)
                db_data = db.modify_book(book_data)
                bos_data = (shelf_id, book_id, seq_num)
                db_data = db.create_bos(bos_data)
        elif books_needed < 0:
            print("remove books")
            books_del = 0
            db_data = db.get_bos_by_shelf(shelf_id)
            for bos in reversed(db_data):
                shelf_id, book_id, seq_num = (bos)
                bos_data = (shelf_id, book_id, seq_num)
                bos_info = db.delete_bos(bos_data)
                book_data = db.get_book_by_id(book_id)
                book_id, node_id, status, attributes, size_bytes = (book_data)
                book_data = (book_id, node_id, 0, attributes, size_bytes)
                book_data = db.modify_book(book_data)
                books_del -= 1
                if books_del == books_needed:
                    break

        shelf_data = (shelf_id, new_size_bytes, new_book_count,
                      open_count, c_time, m_time)
        db_data = db.modify_shelf(shelf_data)

        resp = db.get_shelf(shelf_id)
        recvd = dict(zip(shelf_columns, resp))

        return recvd


    def cmd_get_shelf_zaddr(cmd_data):
        """
            In (dict)---
                ?
            Out (dict) ---
                ?
        """
        return '{"error":"Command not implemented"}'

    def cmd_list_book(cmd_data):
        """ List a given book
            In (dict)---
                book_id
            Out (dict) ---
                book data
        """
        book_id = cmd_data["book_id"]
        resp = db.get_book_by_id(book_id)
        # todo: fail if book does not exist
        recvd = dict(zip(book_columns, resp))
        return recvd

    def cmd_list_bos(cmd_data):
        """ List all the books on a given shelf.
            In (dict)---
                shelf_id
            Out (dict) ---
                bos data
        """
        shelf_id = cmd_data["shelf_id"]
        resp = db.get_bos_by_shelf(shelf_id)
        # todo: fail if shelf does not exist
        recvd = [{'shelf_id': shelf_id, 'book_id': book_id, 'seq_num': seq_num}
                    for shelf_id, book_id, seq_num in resp]
        return recvd

    _handlers = { }

    def __init__(self, cursor):
        # Skip 'cmd_' prefix
        tmp = dict( [ (name[4:], func)
                    for (name, func) in self.__class__.__dict__.items() if
                        name.startswith('cmd_')
                    ]
        )
        self._handlers.update(tmp)
        self._cur = cursor
        self._cur.execute('SELECT book_size_bytes FROM globals')
        self.__class__.book_size = self._cur.fetchone()[0]
        assert self.book_size > 1024*1024, 'Bad book size'

    def __call__(self, cmdict):
        try:
            assert isinstance(cmdict, dict) and 'command' in cmdict
            self._cmdict = cmdict
            handler = self._handlers[self._cmdict['command']]
            ret = handler(self)
            return ret
        except AssertionError as e:
            msg = str(e)
        except Exception as e:
            msg = 'Internal error: ' + str(e)
            pass
        raise RuntimeError(msg)
    @property
    def commandset(self):
        return tuple(sorted(self._handlers.keys()))

def execute_command(cmd_data):
    """ Execute the correct command handler.
    """
    try:
        cmd = cmd_data["command"]
        return(command_handlers[cmd](cmd_data))
    except:
        return '{"error":"No command key"}'

def engine_args_init(parser):
    pass

if __name__ == '__main__':
    '''"recvd" is commands/data that would be received from a client.'''

    import os
    from pprint import pprint

    from genericobj import GenericObject

    def pp(recvd, data):
        print('Original:', dict(recvd))
        print('DB response:')
        if hasattr(data, '__init__'):   # TMBook, GenericObjects, etc
            print(str(data))
        else:
            pprint(data)
        print()

    authdata = GenericObject(
        node_id=1,
        uid=os.geteuid(),
        gid=os.getegid(),
        pid=os.getpid()
    )

    lce = LibrarianCommandExecution(SQLiteCursor(DBfile=sys.argv[1]))
    lcp = LibrarianCommandProtocol()
    print(lcp.commandset)
    print(lce.commandset)
    print()
    print('Engine missing:',set(lcp.commandset) - set(lce.commandset))
    print('Engine extras: ',set(lce.commandset) - set(lcp.commandset))

    recvd = lcp('version')
    data = lce(recvd)
    pp(recvd, data)

    for name in ('xyzzy', 'shelf22', 'coke', 'pepsi'):
        recvd = lcp('create_shelf', name=name)
        try:
            data = lce(recvd)
        except Exception as e:
            data = e
        pp(recvd, data)

    name = 'xyzzy'
    recvd = lcp('list_shelf', name=name)
    data = lce(recvd)
    pp(recvd, data)

    recvd = lcp('list_shelves')
    data = lce(recvd)
    pp(recvd, data)

    recvd = lcp('open_shelf', name=name)
    data = lce(recvd)
    pp(recvd, data)

    # Need to have this fail if shelf is not opened to me
    set_trace()
    new_size = data.size_bytes + 42
    recvd = lcp('resize_shelf', id=data.id, size_bytes=new_size)
    data = lce(recvd)
    pp(recvd, data)

    # Need to have this fail if shelf is not opened to me
    recvd = lcp('close_shelf', id=data.id)
    data = lce(recvd)
    pp(recvd, data)

    print ("close/get shelf -----")
    recvd = {}
    node_id = 0x0A0A0A0A0A0A0A0A
    uid = 0
    gid = 0
    recvd.update({"command": "close_shelf"})
    recvd.update({"shelf_id": shelf_id})
    recvd.update({"node_id": node_id})
    recvd.update({"uid": uid})
    recvd.update({"gid": gid})
    data_in = execute_command(recvd)
    print ("recvd =", recvd)
    print ("data_in =", data_in)
    recvd = {}
    recvd.update({"command": "list_shelf"})
    recvd.update({"shelf_id": shelf_id})
    data_in = execute_command(recvd)
    print ("recvd =", recvd)
    print ("data_in =", data_in)

    # destroy shelf
    print ("destroy/get shelf -----")
    recvd = {}
    node_id = 0x0A0A0A0A0A0A0A0A
    uid = 0
    gid = 0
    recvd.update({"command": "destroy_shelf"})
    recvd.update({"shelf_id": shelf_id})
    recvd.update({"node_id": node_id})
    recvd.update({"uid": uid})
    recvd.update({"gid": gid})
    data_in = execute_command(recvd)
    print ("recvd =", recvd)
    print ("data_in =", data_in)
