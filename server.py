import socket
import select


# Grap these from optparse in the future
DEFAULT_HOST = ''
DEFAULT_PORT = 9093


def main():
    """ Run simple echo server to exersize the module """
    sock = socket.socket()
    sock.bind((DEFAULT_HOST, DEFAULT_PORT))
    sock.listen(0);

    conn, addr = sock.accept()

    while True:
        readable, _, _ = select.select([conn], [], [])
        [print(s.recv(1024)) for s in readable]

if __name__ == "__main__":
    main()

