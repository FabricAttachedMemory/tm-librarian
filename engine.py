#!/usr/bin/python3 -tt
#---------------------------------------------------------------------------
# Librarian engine module
#---------------------------------------------------------------------------

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

    # version will fail if you don't have a top level definition
    LIBRARIAN_VERSION = "Librarian v0.01"

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
        tmp = time.time()
        shelf = TMShelf(
            shelf_id=int(uuid.uuid1().int >> 65),
            ctime=tmp,
            mtime=tmp,
            name=self._cmdict['name'],
        )
        # Since I indexed shelves on 'name', dupes fail nicely.
        self._cur.execute(
            'INSERT INTO shelves VALUES(?, ?, ?, ?, ?, ?, ?, ?)',
            shelf.tuple())
        # First is explicit failure like UNIQUE collision, second is generic
        assert not self._cur.execfail, self._cur.execfail
        assert self._cur.rowcount == 1, 'Insertion failed'
        self._cur.commit()
        return shelf

    def cmd_open_shelf(cmd_data):
        """ Open a shelf for access by a node.
            In (dict)---
                shelf_id
                node_id
                uid
                gid
            Out (dict) ---
                shelf data
        """
        shelf_id = cmd_data["shelf_id"]
        uid = cmd_data["uid"]
        gid = cmd_data["gid"]

        if uid != 0:
            return '{"error":"Permission denied for non-root user"}'

        resp = db.get_shelf(shelf_id)
        # todo: fail if shelf does not exist
        shelf_id, size_bytes, book_count, open_count, c_time, m_time = (resp)
        m_time = time.time()
        open_count += 1
        shelf_data = (shelf_id, size_bytes, book_count, open_count, c_time, m_time)
        resp = db.modify_shelf(shelf_data)
        resp = db.get_shelf(shelf_id)
        recvd = dict(zip(shelf_columns, resp))
        return recvd

    def cmd_close_shelf(cmd_data):
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
        shelf_id = cmd_data["shelf_id"]
        node_id = cmd_data["node_id"]
        uid = cmd_data["uid"]
        gid = cmd_data["gid"]

        if uid != 0:
            return '{"error":"Permission denied for non-root user"}'

        resp = db.get_shelf(shelf_id)
        # todo: fail if shelf does not exist
        shelf_id, size_bytes, book_count, open_count, c_time, m_time = (resp)
        m_time = time.time()
        open_count -= 1
        shelf_data = (shelf_id, size_bytes, book_count, open_count, c_time, m_time)
        resp = db.modify_shelf(shelf_data)
        resp = db.get_shelf(shelf_id)
        recvd = dict(zip(shelf_columns, resp))
        return recvd

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


    def cmd_resize_shelf(cmd_data):
        """ Resize given shelf given a shelf and new size in bytes.
            In (dict)---
                shelf_id
                node_id
                size_bytes
                uid
                gid
            Out (dict) ---
                shelf data
        """
        shelf_id = cmd_data['shelf_id']
        node_id = cmd_data['node_id']
        uid = cmd_data['uid']
        gid = cmd_data['gid']
        new_size_bytes = int(cmd_data['size_bytes'])
        new_book_count = int(math.ceil(new_size_bytes / book_size))

        if uid != 0:
            return '{"error":"Permission denied for non-root user"}'

        db_data = db.get_shelf(shelf_id)
        # todo: fail if shelf does not exist
        shelf_id, size_bytes, book_count, open_count, c_time, m_time = (db_data)
        print("db_data:", db_data)

        if new_size_bytes == size_bytes:
            return db_data

        books_needed = new_book_count - book_count
        print("size_bytes = %d (0x%016x)" % (size_bytes, size_bytes))
        print("new_size_bytes = %d (0x%016x)" % (new_size_bytes, new_size_bytes))
        print("new_book_count = %d" % new_book_count)
        print("book_size = %d (0x%016x)" % (book_size, book_size))
        print("book_count = %d" % book_count)
        print("books_needed = %d" % books_needed)

        if books_needed > 0:
            print("add books")
            seq_num = book_count
            db_data = db.get_book_by_node(node_id, 0, books_needed)
            # todo: check we got back enough books
            for book in db_data:
                print("book (add):", book)
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
                print("book (del):", bos)
                shelf_id, book_id, seq_num = (bos)
                bos_data = (shelf_id, book_id, seq_num)
                bos_info = db.delete_bos(bos_data)
                book_data = db.get_book_by_id(book_id)
                print(book_data)
                book_id, node_id, status, attributes, size_bytes = (book_data)
                book_data = (book_id, node_id, 0, attributes, size_bytes)
                book_data = db.modify_book(book_data)
                books_del -= 1
                print("books_del = %d" % books_del)
                if books_del == books_needed:
                    break

        m_time = time.time()
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

    def cmd_list_shelf(self):
        """ List a given shelf.
            In (dict)---
                shelf_id
            Out (dict) ---
                shelf data
        """
        set_trace()
        self._cur.execute(
            'SELECT * FROM shelves WHERE name = ?',
            self._cmdict['shelf_name'])
        self._cur.iterclass = TMShelf
        shelves = [ r for r in self._cur ]
        assert len(shelves) <= 1, 'oopsie'
        return shelves[0] if shelves else None

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
        print('Original command:')
        pprint(dict(recvd))
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

    name = 'xyzzy1'
    recvd = lcp('create_shelf', name=name)
    try:
        data = lce(recvd)
    except Exception as e:
        data = e
    pp(recvd, data)

    recvd = lcp('list_shelf', name=name)
    data = lce(recvd)
    pp(recvd, data)

    # open/get shelf
    set_trace()
    print ("open/get shelf -----")
    recvd = {}
    uid = 0
    gid = 0
    node_id = 0x0A0A0A0A0A0A0A0A
    recvd.update({"command": "open_shelf"})
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

    # resize new shelf/get shelf
    print ("resize/get shelf -----")
    recvd = {}
    node_id = 0x0A0A0A0A0A0A0A0A
    size_bytes = (20 * 1024 * 1024 * 1024)
    uid = 0
    gid = 0
    recvd.update({"command": "resize_shelf"})
    recvd.update({"shelf_id": shelf_id})
    recvd.update({"node_id": node_id})
    recvd.update({"size_bytes": size_bytes})
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

    # resize shelf bigger/get shelf
    print ("resize/get shelf -----")
    recvd = {}
    node_id = 0x0A0A0A0A0A0A0A0A
    size_bytes = (50 * 1024 * 1024 * 1024)
    uid = 0
    gid = 0
    recvd.update({"command": "resize_shelf"})
    recvd.update({"shelf_id": shelf_id})
    recvd.update({"node_id": node_id})
    recvd.update({"size_bytes": size_bytes})
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

    # list books on shelf
    print ("list books on shelf -----")
    recvd = {}
    recvd.update({"command": "list_bos"})
    recvd.update({"shelf_id": shelf_id})
    data_in = execute_command(recvd)
    print ("recvd =", recvd)
    print ("data_in =", data_in)
    for book in data_in:
        print("  book:", book)

    # resize shelf smaller/get shelf
    print ("resize/get shelf -----")
    recvd = {}
    node_id = 0x0A0A0A0A0A0A0A0A
    size_bytes = (6 * 1024 * 1024 * 1024)
    uid = 0
    gid = 0
    recvd.update({"command": "resize_shelf"})
    recvd.update({"shelf_id": shelf_id})
    recvd.update({"node_id": node_id})
    recvd.update({"size_bytes": size_bytes})
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

    # list books on shelf
    print ("list books on shelf -----")
    recvd = {}
    recvd.update({"command": "list_bos"})
    recvd.update({"shelf_id": shelf_id})
    data_in = execute_command(recvd)
    print ("recvd =", recvd)
    print ("data_in =", data_in)
    for book in data_in:
        print("  book:", book)

    # close/get shelf
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
