#!/usr/bin/env python3
""" Module to handle socket communication for Libraian and Clients """
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

DEFAULT_INTERFACE = ''
DEFAULT_HOST = ''
DEFAULT_PORT = 9093

SOCK = None

def server_init(interface = DEFAULT_INTERFACE, port = DEFAULT_PORT):
    """ Initialize server socket.
    :param interface:   Interface to bind to (default '')
    :param port:        Port to listen on (default 9093)
    :type interface:    dictionary
    :type port:         int
    :returns:           valid socket for serving
    :rtype:             socket
    """
    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((DEFAULT_INTERFACE, DEFAULT_PORT))
    sock.listen(0)

    return sock


def read_all(sock):
    """ Read a complete message (hopefully from the socket)
    """
    in_json = ""
    in_delta = sock.recv(1024).decode("utf-8").strip()
    in_json += in_delta

    # It's possible that two messages are recieved verry quickly
    # and this ends up reading the second one in. The alternative is that
    # one isn't read completly.  it should be sub-divided and checked for
    # a single valid json document.
    while not len(in_delta) != 1024:
        print("in loop")
        in_delta = sock.recv(1024).decode("utf-8").strip()
        in_json += in_delta

    print("left loop")

    # should never have to worry about this
    # python will set the descriptor to -1 when it closes stopping us from
    # having to read 0. But this is select/socket behavior so we need to
    # account for it.
    if len(in_json) == 0:
        sock.close()
        # for some reason python sends an empty message on conenct
        # cauases error with callback functions right now
        return None

    print("I'm returning now")
    return in_json


def client_init(host = 'localhost', port = DEFAULT_PORT):

    sock = socket.socket()
    sock.connect((DEFAULT_HOST, DEFAULT_PORT))
    global SOCK
    SOCK = sock


def client_send_recv(string):
    global SOCK
    send(string)
    return read_all(SOCK)


# basic clienty stuff
# So for the foreseable future (i.e. twoish weeks from this writing) there's
# no urgency for having a workable eventloop. instead we'll do completly 100%
# synchonous stuff over our perfect network : )
def send(string):
    global SOCK
    SOCK.send(str.encode(string))


# Maybe refactor into an object
def start_event_loop(sock, handler):

    to_read = [sock]

    while True:
        # closed sockets get -1 value
        # this may not work if the socket closes between now and the select
        to_read = [sock for sock in to_read if sock.fileno() != -1]
        readable, _, _ = select.select(to_read, [], [])

        try:

            if sock in readable:
                (conn, addr) = sock.accept()
                to_read.append(conn)
                readable.remove(sock)

        # Socket was closed python hates me
        except ValueError:
            to_read = [sock for sock in to_read if sock.fileno() != -1]
            continue

        if len(readable) != 0:
            [s.send(str.encode(handler(read_all(s)))) for s in readable]


def echo_handeler(string):
    # does not handle ctrl characters well
    print(string)
    return string


def main():
    """ Run simple echo server to exersize the module """
    sock = server_init()
    start_event_loop(sock, echo_handeler)

if __name__ == "__main__":
    main()

