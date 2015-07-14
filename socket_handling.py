import socket
import select


# Grap these from optparse in the future
DEFAULT_HOST = ''
DEFAULT_PORT = 9093

def server_init(host = DEFAULT_HOST, port = DEFAULT_PORT):

    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((DEFAULT_HOST, DEFAULT_PORT))
    sock.listen(0)

    return sock

def echo_handeler(string):
    # does not handle ctrl characters well
    print(string)

def read_all(sock):
    in_json = sock.recv(1024).decode("utf-8").strip()

    # should never have to worry about this
    # python will set the descriptor to -1 when it closes stopping us from
    # having to read 0. But this is select/socket behavior so we need to
    # account for it.
    if len(in_json) == 0:
        sock.close()
        return None

    return in_json

def serv(handler):

    sock = server_init()
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

        # silly trick with short-circuted booleans
        # does this ever get run have yet to verify
        [handler(read_all(s)) == -1 and readable.remove(s) for s in readable]


def main():
    """ Run simple echo server to exersize the module """
    serv(echo_handeler)

if __name__ == "__main__":
    main()

