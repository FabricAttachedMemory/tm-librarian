from collections import defaultdict
import json
#
# may become usefull later
def json_error_creator(errno):
    r = {}
    r["error"] = errno
    return json.dumps(r)

# place string here
def unknown_command_handler():
    return lambda x : json_error_creator(-1)

method_handlers = defaultdict(unknown_command_handler)
method_handlers["version"] = version

def json_handler(in_json):

    try:

        in_dict = json.loads(in_json)
        out_string = method_handlers[in_dict["method"]](in_json)

    except ValueError:
        # log error?
        # need to define error number later but since mark didn't implement
        # errors on these i want them to be different
        return '{"error":"Invalid JSON"}'
    except KeyError:
        # log error?
        # need to define error number later but since mark didn't implement
        # errors on these i want them to be different
        return '{"error":"No method key"}'

    return out_string

def json_init(handlers):

