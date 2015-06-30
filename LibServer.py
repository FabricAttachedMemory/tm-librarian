#!/usr/bin/python3

import sqlite3
import json
import random
import time
import sys
import socketserver
import math
import argparse
import os

VERSION="Librarian 0.1"
LIBDB = './librarian.db'
HOST = 'localhost'
PORT = 9090
BOOK_SIZE = (8 * 1024 * 1024 * 1024)

parser = argparse.ArgumentParser()
parser.add_argument("--perf", action='store_true', help="Run sequential and random performance testing on database")
parser.add_argument("--mem", action='store_true', help="Use in memory database, otherwise use on disk database")
parser.add_argument("--data", help="Specify book data file")

args = parser.parse_args()

def cmd_Version(jdata):
    print("cmd_Version(%s)" % (jdata))
    print("version = %s" % (VERSION))
    print("Send: pass")
    return("pass")

def cmd_ListBook(jdata):
    print("cmd_ListBook(%s)" % (jdata))
    rowid = jdata["rowid"]
    cur.execute('SELECT rowid,* FROM Books WHERE rowid = ?', (rowid,))
    row = cur.fetchone()
    rowid, node_id, node_name, book_offset, book_id, status, shelf = (row)
    #print(row)
    print("rowid = %d, node_id = 0x%016x, node_name = %s, book_offset = %d, book_id = 0x%016x, status = %d, shelf = %s" %
        (rowid, node_id, node_name, book_offset, book_id, status, shelf))
    print("Send: pass")
    return("pass")

def cmd_ListBookAll(jdata):
    print("cmd_ListBookAll(%s)" % (jdata))
    cur.execute('SELECT rowid,* FROM Books')
    rows = cur.fetchall()
    for row in rows:
        rowid, node_id, node_name, book_offset, book_id, status, shelf = (row)
        #print(row)
        print("rowid = %d, node_id = 0x%016x, node_name = %s, book_offset = %d, book_id = 0x%016x, status = %d, shelf = %s" %
            (rowid, node_id, node_name, book_offset, book_id, status, shelf))
    print("Send: pass")
    return("pass")

def cmd_ListShelf(jdata):
    print("cmd_ListShelf(%s)" % (jdata))
    shelf_name = str(jdata['shelf_name'])
    cur.execute('SELECT rowid,* FROM Shelves WHERE shelf_name = ?', [shelf_name])
    row = cur.fetchone()
    rowid, shelf_name, shelf_owner, size_bytes, num_books, open_cnt = (row)
    #print(row)
    print("rowid = %d, shelf_name = %s, shelf_owner = %s, size_bytes = %d (0x%016x), num_books = %d, open_cnt = %d" %
        (rowid, shelf_name, shelf_owner, size_bytes, size_bytes, num_books, open_cnt))
    cur.execute('SELECT rowid,* FROM Books WHERE shelf = ?', [shelf_name])
    brows = cur.fetchall()
    for brow in brows:
        rowid, node_id, node_name, book_offset, book_id, status, shelf = (brow)
        #print(brow)
        print("rowid = %d, node_id = 0x%016x, node_name = %s, book_offset = %d, book_id = 0x%016x, status = %d, shelf = %s" %
            (rowid, node_id, node_name, book_offset, book_id, status, shelf))
    cur.execute('SELECT rowid,* FROM ShelfRes WHERE shelf_name = ?', [shelf_name])
    rrows = cur.fetchall()
    for rrow in rrows:
        rowid, shelf_name, res_owner = (rrow)
        #print(rrow)
        print("rowid = %d, shelf_name = %s, res_owner = %s" %
            (rowid, shelf_name, res_owner))
    print("Send: pass")
    return("pass")

def cmd_ListShelfAll(jdata):
    print("cmd_ListShelfAll(%s)" % (jdata))
    cur.execute('SELECT rowid,* FROM Shelves')
    rows = cur.fetchall()
    for row in rows:
        rowid, shelf_name, shelf_owner, size_bytes, num_books, open_cnt = (row)
        #print(row)
        print("rowid = %d, shelf_name = %s, shelf_owner = %s, size_bytes = %d (0x%016x), num_books = %d, open_cnt = %d" %
            (rowid, shelf_name, shelf_owner, size_bytes, size_bytes, num_books, open_cnt))
        cur.execute('SELECT rowid,* FROM Books WHERE shelf = ?', [shelf_name])
        brows = cur.fetchall()
        for brow in brows:
            rowid, node_id, node_name, book_offset, book_id, status, shelf = (brow)
            #print(brow)
            print("rowid = %d, node_id = 0x%016x, node_name = %s, book_offset = %d, book_id = 0x%016x, status = %d, shelf = %s" %
                (rowid, node_id, node_name, book_offset, book_id, status, shelf))
        cur.execute('SELECT rowid,* FROM ShelfRes WHERE shelf_name = ?', [shelf_name])
        rrows = cur.fetchall()
        for rrow in rrows:
            rowid, shelf_name, res_owner = (rrow)
            #print(rrow)
            print("rowid = %d, shelf_name = %s, res_owner = %s" %
                (rowid, shelf_name, res_owner))
    print("Send: pass")
    return("pass")

def cmd_CreateShelf(jdata):
    print("cmd_CreateShelf(%s)" % (jdata))
    cur.execute('INSERT INTO Shelves VALUES (?,?,?,?,?)', (jdata['shelf_name'], jdata['shelf_owner'], 0, 0, 0))  
    con.commit()
    print("Send: pass")
    return("pass")

def cmd_ResizeShelf(jdata):
    print("cmd_ResizeShelf(%s)" % (jdata))

    shelf_name = str(jdata['shelf_name'])
    new_size_bytes = int(jdata['size_bytes'])
    new_num_books = int(math.ceil(new_size_bytes / BOOK_SIZE))

    cur.execute('SELECT rowid,* FROM Shelves WHERE shelf_name = ?', [shelf_name])
    row = cur.fetchone()
    # ??? rewriting shelf_name
    rowid, shelf_name, shelf_owner, size_bytes, num_books, open_cnt = (row)
    #print(row)
    print("rowid = %d, shelf_name = %s, shelf_owner = %s, size_bytes = %d (0x%016x), num_books = %d, open_cnt = %d" %
        (rowid, shelf_name, shelf_owner, size_bytes, size_bytes, num_books, open_cnt))
    
    if new_num_books == num_books:
        print("No new books required")
    elif new_num_books > num_books:
        books_to_add = new_num_books - num_books
        cur.execute('SELECT rowid,* FROM Books WHERE status = 0 ORDER BY RANDOM() LIMIT ?', (books_to_add,))
        rows = cur.fetchall()
        for row in rows:
            #print(row)
            rowid, node_id, node_name, book_offset, book_id, status, shelf = (row)
            cur.execute('UPDATE Books SET status = ?, shelf = ? WHERE rowid = ?', (1, shelf_name, rowid))
        con.commit()
        print("Added %d new books to shelf" % books_to_add)
    else:
        books_to_remove = num_books - new_num_books
        cur.execute('SELECT rowid,* FROM Books WHERE shelf = ? ORDER BY RANDOM() LIMIT ?', (shelf_name, books_to_remove,))
        rows = cur.fetchall()
        for row in rows:
            print(row)
            rowid, node_id, node_name, book_offset, book_id, status, shelf = (row)
            cur.execute('UPDATE Books SET status = ?, shelf = ? WHERE rowid = ?', (0, "none", rowid))
        con.commit()
        print("Removed %d books from shelf" % books_to_remove)

    cur.execute('UPDATE Shelves SET size_bytes = ?, num_books = ? WHERE shelf_name = ?', (new_size_bytes, new_num_books, shelf_name))
    con.commit()

    print("Send: pass")
    return("pass")

def cmd_DestroyShelf(jdata):
    print("cmd_DestroyShelf(%s)" % (jdata))
    shelf_name = str(jdata['shelf_name'])
    cur.execute('SELECT rowid,* FROM Books WHERE shelf = ?', (shelf_name,))
    rows = cur.fetchall()
    books_removed = 0
    for row in rows:
        print(row)
        rowid, node_id, node_name, book_offset, book_id, status, shelf = (row)
        cur.execute('UPDATE Books SET status = ?, shelf = ? WHERE rowid = ?', (0, "none", rowid))
        books_removed += 1
    con.commit()
    print("Removed %d books from shelf" % books_removed)
    cur.execute('DELETE FROM ShelfRes WHERE shelf_name = ?', [shelf_name])  
    con.commit()
    print("Removed all shelf reservations")
    cur.execute('DELETE FROM Shelves WHERE shelf_name = ?', (jdata['shelf_name'],))  
    con.commit()
    print("Destroyed shelf: %s" % shelf_name)
    return("SUCCESS")

def cmd_OpenShelf(jdata):
    print("cmd_OpenShelf(%s)" % (jdata))
    shelf_name = str(jdata['shelf_name'])
    res_owner = str(jdata['res_owner'])
    cur.execute('SELECT rowid,* FROM Shelves WHERE shelf_name = ?', [shelf_name])
    row = cur.fetchone()
    rowid, shelf_name, shelf_owner, size_bytes, num_books, open_cnt = (row)
    open_cnt += 1
    cur.execute('UPDATE Shelves SET open_cnt = ? WHERE shelf_name = ?', (open_cnt, shelf_name))
    cur.execute('INSERT INTO ShelfRes VALUES (?,?)', (shelf_name, res_owner))  
    con.commit()
    return("SUCCESS")

def cmd_CloseShelf(jdata):
    print("cmd_CloseShelf(%s)" % (jdata))
    shelf_name = str(jdata['shelf_name'])
    res_owner = str(jdata['res_owner'])
    cur.execute('SELECT rowid,* FROM Shelves WHERE shelf_name = ?', [shelf_name])
    row = cur.fetchone()
    rowid, shelf_name, shelf_owner, size_bytes, num_books, open_cnt = (row)
    open_cnt -= 1
    cur.execute('UPDATE Shelves SET open_cnt = ? WHERE shelf_name = ?', (open_cnt, shelf_name))
    cur.execute('DELETE FROM ShelfRes WHERE shelf_name = ? AND res_owner = ?', (shelf_name, res_owner))  
    con.commit()
    return("SUCCESS")

def cmd_ListResAll(jdata):
    print("cmd_ListResAll(%s)" % (jdata))
    cur.execute('SELECT rowid,* FROM ShelfRes')
    rows = cur.fetchall()
    for row in rows:
        rowid, shelf_name, res_owner = (row)
        #print(row)
        print("rowid = %d, shelf_name = %s, res_owner = %s" %
            (rowid, shelf_name, res_owner))
    return("SUCCESS")

CmdHandlers = {
    "version":cmd_Version,           # print server version (args: none)
    "listbook":cmd_ListBook,         # list one book (args: rowid)
    "listbookall":cmd_ListBookAll,   # list all books (args: none)
    "listshelf":cmd_ListShelf,       # list shelf (args: shelf_name)
    "listshelfall":cmd_ListShelfAll, # list all shelves (args: none)
    "createshelf":cmd_CreateShelf,   # create new shelf (args: shelf_name, shelf_owner)
    "resizeshelf":cmd_ResizeShelf,   # resize existing shelf (args: shelf_name, new size in bytes)
    "destroyshelf":cmd_DestroyShelf, # destroy existing shelf (args: shelf_name)
    "openshelf":cmd_OpenShelf,       # open existing shelf for use (args: shelf_name)
    "closeshelf":cmd_CloseShelf,     # close existing shelf for use (args: shelf_name)
    "listresall":cmd_ListResAll      # List all shelf reservations (args: none)
    }

#
# Setup socket server
#

class MyTCPServer(socketserver.TCPServer):
    allow_reuse_address = True

class MyTCPServerHandler(socketserver.BaseRequestHandler):
    def handle(self):
        print('Got connection from: ', self.client_address)
        while True:
            try:
                data = self.request.recv(1024)
                if not data:
                    print('Connection terminated')
                    break
                sdata = data.decode('utf-8')
                jdata = json.loads(sdata)
                print("Recv: jdata %s" % jdata)

                cmd = jdata["cmd"]

                try:
                    msg = CmdHandlers[cmd](jdata)
                except KeyError:
                    msg = "FAIL: Unrecognized command"

                self.request.sendall(msg.encode())
            except:
                print("Unexpected error:", sys.exc_info()[0])
                raise

def setup_db():
    if args.mem == True:
        con = sqlite3.connect(":memory:")
        print("Using in memory Librarian database")
    else:
        print("Using on disk Librarian database: %s" % LIBDB)
        if os.path.exists(LIBDB) == True:
            print("Database file exists, opening")
            con = sqlite3.connect(LIBDB)
        else:
            print("Database file does not exist, creating new one")
            con = sqlite3.connect(LIBDB)
            cur = con.cursor()
            print("Creating Books table in database")
            cur.execute('CREATE TABLE Books (node_id INT, node_name TEXT, book_offset INT, book_id INT, status INT, shelf TEXT)') 
            print("Creating Shelves table in database")
            cur.execute('CREATE TABLE Shelves (shelf_name TEXT, shelf_owner TEXT, size_bytes INT, num_books INT, open_cnt INT)') 
            print("Creating ShelfRes table in database")
            cur.execute('CREATE TABLE ShelfRes (shelf_name TEXT, res_owner TEXT)') 
            con.commit()
            print("Loading Books data from file into database")
            total_books = 0
            t0 = time.time()
            with open(args.data, "r") as data_file:
                for line in data_file:
                    data = json.loads(line)
                    print("NodeID: %d, NodeName: %s, BookNum: %d" % (data['NodeID'], data['NodeName'], data['BookNum']))
                    for book in range(data['BookNum']):
                        book_id = book
                        book_id += data['NodeID'] << 32
                        cur.execute('INSERT INTO Books VALUES (?,?,?,?,?,?)', (data['NodeID'], data['NodeName'], book, book_id, 0, "none"))  
                        total_books += 1
            con.commit()
            t1 = time.time()- t0
            print("Total_books = %d, secs = %.2f" % (total_books, t1))

            if args.perf == True:
                print("Performing database read testing")
                t0 = time.time()
                for row in range(1, (total_books+1)):
                    cur.execute('SELECT rowid,* FROM Books WHERE rowid=?', (row,))
                    row = cur.fetchone()
                    #print(row)
                t1 = time.time()-t0
                print("Sequential read: %d books, %.2f total secs, %d books/sec" % (total_books, t1, total_books/t1))

                t0 = time.time()
                for row in range(1, (total_books+1)):
                    rrow = random.randrange(1, (total_books+1))
                    cur.execute('SELECT rowid,* FROM Books WHERE rowid=?', (rrow,))
                    row = cur.fetchone()
                    #print(row)
                t1 = time.time()-t0
                print("Random read: %d books, %.2f total secs, %d books/sec" % (total_books, t1, total_books/t1))

    return(con)

if __name__ == "__main__":

    con = setup_db()
    cur = con.cursor()

    print("Librarian ready")

    # Start up socket server and listen for requests
    server = MyTCPServer((HOST, PORT), MyTCPServerHandler)
    # terminate with Ctrl-C
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        sys.exit(0)

    con.close()
