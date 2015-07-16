#!/usr/bin/env python3 -tt
""" Module to handle socket communication for Librarian and Clients """
import socket
import select

__author__ = "Justin Vreeland"
__copyright__ = "Copyright 2015 HP?"
__credits__ = ""
__license__ = ""
__version__ = ""
__maintainer__ = "Justin Vreeland"
__email__ = "justin.mcd.vreeland@hp.com"
__status__ = "Development"

class SocketReadWrite():

    _sock = None

    def __init__(self):
        self._sock = socket.socket()

    def __del__(self):
        self._sock.close()

    # should be static helper in base class called by child class
    def send(self, string, sock = None):
        """ Send this string """
        if sock is None:
            sock = self._sock

        sock.send(str.encode(string))

    # should be static helper in base class called by child class
    def recv_all(self, sock = None):
        """ Receive the whole message """
        if sock is None:
            sock = self._sock

        in_json = ""
        in_delta = sock.recv(1024).decode("utf-8").strip()
        in_json += in_delta

        while not len(in_delta) != 1024:
            in_delta = sock.recv(1024).decode("utf-8").strip()
            in_json += in_delta

        # should never have to worry about this
        # python will set the descriptor to -1 when it closes stopping us from
        # having to read 0. But this is select/socket behavior so we need to
        # account for it.
        if len(in_json) == 0:
            sock.close()
            return None

        return in_json

    # should be static helper in base class called by child class
    def send_recv(self, string, sock = None):
        """ Send and receive all data. """
        if sock is None:
            sock = self._sock

        if len(string) == 0:
            sock.close()
            return None

        # This kills the socket
        if string == "":
            return None

        # maybe do a proper error but w/e all of this will be replaced
        # by a proper event loop eventually
        if sock == None:
            return None

        self.send(string, sock)
        return self.recv_all(sock)


class Client(SocketReadWrite):
    """ A simple synchronous client for the Librarian """
    _sock = None

    def __init__(self):
        super().__init__()

    def __del__(self):
        super().__del__()

    def connect(self, host = '', port = 9093):
        """ Connect socket to port on host """
        self._sock.connect((host, port))


def echo_handler(string):
    # does not handle ctrl characters well
    print(string)
    return string


class Server(SocketReadWrite):
    """ A simple asynchronous server for the Librarian """

    _sock = None

    def __init__(self):
        super().__init__()
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    def __del__(self):
        super().__del__()

    def serv(self, handler,interface = '', port = 9093):
        self._sock.bind((interface, port))
        self._sock.listen(0)

        to_read = [self._sock]

        while True:
            # closed sockets get -1 value
            # this may not work if the socket closes between now and the select
            # but i still haven't found documentation as to why it does this or how
            # I can force it to stop.
            to_read = [sock for sock in to_read if sock.fileno() != -1]
            readable, _, _ = select.select(to_read, [], [])

            try:

                if self._sock in readable:
                    (conn, addr) = self._sock.accept()
                    to_read.append(conn)
                    readable.remove(self._sock)

            # Socket was closed python and hates me
            except ValueError:
                # fix this code to conform to bad catching bad sockets
                to_read = [sock for sock in to_read if sock.fileno() != -1]
                continue

            if len(readable) != 0:
                for s in readable:
                    self.send(handler(self.recv_all(s)), s)


def main():
    """ Run simple echo server to exersize the module """

    server = Server()
    server.serv(echo_handler)

if __name__ == "__main__":
    main()

