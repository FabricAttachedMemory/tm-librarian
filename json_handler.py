from collections import defaultdict
import json

# place string here
def unknown_command_handler():
    r = {}
    # Not implemented errno?
    r["error"] = -1
    return lambda x : json.dumps(r)


def version(in_dict):
   r = {}
   r["version"] = "4.0.1JSON"
   return json.dumps(r)

method_handlers = defaultdict(unknown_command_handler)
method_handlers["version"] = version

def json_handler(sock):

    in_json = sock.recv(1024).decode("utf-8").strip()
    if len(in_json) == 0:
        sock.close()
        return -1

    try:

        in_dict = json.loads(in_json)
        out_string = method_handlers[in_dict["method"]](in_json)

    except ValueError:
        # log error?
        sock.send(b'{"error":"Invalid JSON"}')
        return 0
    except KeyError:
        # log error?
        sock.send(b'{"error":"No method key"}')
        return 0

    return sock.send(str.encode(out_string))
