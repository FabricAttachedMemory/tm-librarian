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
    # does not handle bad characters
    print(sock.recv(1024).decode("utf-8").strip())

def serv(handeler):

    sock = server_init()
    to_read = [sock]

    while True:
        readable, _, _ = select.select(to_read, [], [])

        if sock in readable:
            (conn, addr) = sock.accept()
            to_read.append(conn)
            readable.remove(sock)


        [handeler(s) for s in readable]

def main():
    """ Run simple echo server to exersize the module """
    serv(echo_handeler)

if __name__ == "__main__":
    main()

