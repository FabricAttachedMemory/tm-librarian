from collections import defaultdict
import json

# same version as Marks WMB as I'm trying to ensure they function
# together
VERSION = "4.0.1JSON"

# may become usefull later
def json_error_creator(errno):
    r = {}
    r["error"] = errno
    return json.dumps(r)

# place string here
def unknown_command_handler():
    return lambda x : json_error_creator(-1)


def version(in_dict):
   r = {}
   r["version"] = VERSION
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
        # need to define error number later but since mark didn't implement
        # errors on these i want them to be different
        sock.send(b'{"error":"Invalid JSON"}')
        return 0
    except KeyError:
        # log error?
        # need to define error number later but since mark didn't implement
        # errors on these i want them to be different
        sock.send(b'{"error":"No method key"}')
        return 0

    return sock.send(str.encode(out_string))
