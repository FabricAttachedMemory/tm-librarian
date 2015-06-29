import server
import json_handler

def main():
    """ Run simple echo server to exersize the module """
    server.serv(json_handler.json_handler)

if __name__ == "__main__":
    main()


