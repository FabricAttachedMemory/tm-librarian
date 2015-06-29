import socket
import select


# Grap these from optparse in the future
DEFAULT_HOST = ''
DEFAULT_PORT = 9093

def server_init():

    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((DEFAULT_HOST, DEFAULT_PORT))
    sock.listen(0)

    return sock

def echo_handeler(sock):
    # does not handle ctrl characters well
    print(sock.recv(1024).decode("utf-8").strip())

def serv(handler):

    sock = server_init()
    to_read = [sock]

    while True:
        # closed sockets get -1 value
        # this may not work if the socket closes between now and the select
        to_read = [sock for sock in to_read if sock.fileno() != -1]
        readable, _, _ = select.select(to_read, [], [])

        if sock in readable:
            (conn, addr) = sock.accept()
            to_read.append(conn)
            readable.remove(sock)

        # silly trick with short-circuted booleans
        # does this ever get run have yet to verify
        [handler(s) == -1 and readable.remove(s) for s in readable]


def main():
    """ Run simple echo server to exersize the module """
    serv(echo_handeler)

if __name__ == "__main__":
    main()

