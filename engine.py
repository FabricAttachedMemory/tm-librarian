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

from bookshelves import TMBook, TMShelf, TMBos
from cmdproto import LibrarianCommandProtocol

class LibrarianCommandEngine(object):

    _book_size = 0  # read from DB

    @classmethod
    def args_init(cls, parser): # sets up things for optargs in __init__
        pass

    @classmethod
    def _nbooks(cls, nbytes):
        return int(math.ceil(float(nbytes) / float(cls._book_size)))

    def cmd_version(self):
        """ Return librarian version
            In (dict)---
                None
            Out (dict) ---
                librarian version
        """
        return self.db.get_version()

    def cmd_create_shelf(self):
        """ Create a new shelf
            In (dict)---
                name
            Out (dict) ---
                shelf data
        """
        return self.db.create_shelf(TMSHelf(self._cmdict))

    def cmd_list_shelf(self):        # By name only
        """ List a given shelf.
            In (dict)---
                name
            Out (TMShelf object) ---
                TMShelf object
        """
        shelf = TMShelf(self._cmdict)
        shelf.matchfields = ('name', )
        shelf = self.db.get_shelf(shelf)
        if shelf is not None:
            assert self._nbooks(shelf.size_bytes) == shelf.book_count, '%s size metadata mismatch' % shelf.name
        return shelf

    def cmd_list_shelves(self):
        return self.db.get_shelf_all()

    def cmd_open_shelf(self):
        """ Open a shelf for access by a node.
            In (dict)---
                name
            Out ---
                TMShelf object
        """
        shelf = self.cmd_list_shelf()   # lookup by name
        if shelf is None:
            return None
        shelf.open_count += 1
        shelf.matchfields = 'open_count'
        self.db.modify_shelf(shelf)
        return shelf

    def cmd_close_shelf(self):
        """ Close a shelf against access by a node.
            In (dict)---
                shelf_id
            Out (dict) ---
                TMShelf object
        """
        # todo: check if node/user really has this shelf open
        # todo: ensure open count does not go below zero

        set_trace()
        shelf = TMShelf(self._cmdict)
        shelf.matchfields = ('id')
        shelf = self.db.get_shelf(shelf)
        if shelf is None:
            return None

        shelf.open_count -= 1
        shelf.matchfields = 'open_count'
        self.db.modify_shelf(shelf)
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
        shelf = self.cmd_list_shelf()   # lookup by name
        if shelf is None:
            return None
        assert shelf.open_count <= 0, '%s open count is %d' % (
                                    shelf.name, shelf.open_count)

        set_trace()
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
                name
                id
                size_bytes
            Out (dict) ---
                shelf data
        """

        # Gonna need book details sooner or later.  Since resizing should
        # be reasonably infrequent, do some consistency checking now.
        # Save the idiot checking until later.

        shelf = TMShelf(self._cmdict)   # just for the....
        shelf.matchfields = ('id')
        shelf = self.db.get_shelf(shelf)
        if shelf is None:
            return None

        books = self.db.get_bos_by_shelf(shelf)
        assert len(books) == shelf.book_count, (
            '%s book count mismatch' % shelf.name)

        # other consistency checks
        assert self._nbooks(shelf.size_bytes) == shelf.book_count, (
            '%s size metadata mismatch' % shelf.name)
        new_size_bytes = int(self._cmdict['size_bytes'])
        assert new_size_bytes >= 0, 'Bad size'
        new_book_count = self._nbooks(new_size_bytes)

        # Can I leave real early?
        if new_size_bytes == shelf.size_bytes:
            return shelf

        # How about a little early?
        shelf.size_bytes = new_size_bytes
        shelf.matchfields = 'size_bytes'
        if new_book_count == shelf.book_count:
            self.db.modify_shelf(shelf)
            return shelf

        books_needed = new_book_count - shelf.book_count
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
        set_trace()
        book_id = cmd_data["book_id"]
        resp = db.get_book_by_id(book_id)
        # todo: fail if book does not exist
        recvd = dict(zip(book_columns, resp))
        return recvd

    def cmd_list_bos(self):
        """ List all the books on a given shelf.
            In (dict)---
                shelf_id
            Out (dict) ---
                bos data
        """
        shelf_id = self._cmdict['shelf_id']
        resp = db.get_bos_by_shelf(shelf_id)
        # todo: fail if shelf does not exist
        recvd = [{'shelf_id': shelf_id, 'book_id': book_id, 'seq_num': seq_num}
                    for shelf_id, book_id, seq_num in resp]
        return recvd

    _handlers = { }

    def __init__(self, backend, optargs=None, cooked=False):
        self.db = backend
        self.__class__._book_size = self.db.get_book_size()
        assert self._book_size >= 1024*1024, 'Bad book size in DB'

        # Skip 'cmd_' prefix
        tmp = dict( [ (name[4:], func)
                    for (name, func) in self.__class__.__dict__.items() if
                        name.startswith('cmd_')
                    ]
        )
        self._handlers.update(tmp)
        self._cooked = cooked   # return style: raw = dict, cooked = obj

    def __call__(self, cmdict):
        try:
            self._cmdict = cmdict
            handler = self._handlers[self._cmdict['command']]
        except KeyError as e:
            raise RuntimeError('Bad lookup on "%s"' % str(e))

        try:
            ret = handler(self)
            if self._cooked:
                return ret
            raise NotImplementedError('obj2dict')
        except AssertionError as e:     # idiot checks
            msg = str(e)
        except Exception as e:
            msg = 'INTERNAL ERROR @ %s[%d]: %s' %  (
                self.__class__.__name__, sys.exc_info()[2].tb_lineno,str(e))
            pass
        raise RuntimeError(msg)

    @property
    def commandset(self):
        return tuple(sorted(self._handlers.keys()))

###########################################################################

if __name__ == '__main__':
    '''"recvd" is commands/data that would be received from a client.'''

    import os
    from pprint import pprint

    from database import LibrarianDBackendSQL
    from genericobj import GenericObject

    def pp(recvd, data):
        print('Original:', dict(recvd))
        print('DB results:')
        if hasattr(data, '__init__'):   # TMBook, GenericObjects, etc
            print(str(data))
        else:
            pprint(data)
        print()

    # Someday this will be fancy
    authdata = GenericObject(
        node_id=1,
        uid=os.geteuid(),
        gid=os.getegid(),
        pid=os.getpid()
    )

    # Used to synthesize command dictionaries
    lcp = LibrarianCommandProtocol()
    print(lcp.commandset)

    # For self test, look at prettier results than dictionaries
    lce = LibrarianCommandEngine(
                    LibrarianDBackendSQL(DBfile=sys.argv[1]),
                    cooked=True)
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
            data = lce(recvd)   # only works on fresh DB
        except Exception as e:
            data = e
        pp(recvd, data)

    name = 'xyzzy'
    recvd = lcp('list_shelf', name=name)
    data = lce(recvd)
    pp(recvd, data)

    recvd = lcp('list_shelves')
    data = lce(recvd)
    assert len(data) >= 4, 'not good'
    pp(recvd, data)

    recvd = lcp('open_shelf', name=name)
    data = lce(recvd)
    pp(recvd, data)

    # Need to have this fail if shelf is not opened to me
    data.size_bytes += 42
    recvd = lcp('resize_shelf', data)
    data = lce(recvd)
    pp(recvd, data)

    # Need to have this fail if shelf is not opened to me
    recvd = lcp('close_shelf', data)
    data = lce(recvd)
    pp(recvd, data)

    # destroy shelf
    set_trace()
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
