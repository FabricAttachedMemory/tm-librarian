#!/usr/bin/python3 -tt
from collections import defaultdict
import json
import functools

class Processor:

    _processors = [lambda x : x]
    _unprocessors = [lambda x : x]

    def __init__(self):
        pass

    def __del__(self):
        pass

    def add_processor(self, func):
        self._processors.append(func)

    def add_unprocessor(self, func):
        self._unprocessors.append(func)

    def process(self, in_obj):
        process = list(self._processors)
        process.insert(0, in_obj)
        return functools.reduce(lambda x,y : y(x), process)

    def unprocess(self, in_obj):
        unprocess = list(self._unprocessors)
        unprocess.insert(0, in_obj)
        return functools.reduce(lambda x,y : y(x), unprocess)


def json_error_creator(errno):
    r = {}
    r["error"] = errno
    return json.dumps(r)

# place string here
def unknown_command_handler():
    return lambda x : json_error_creator(-1)

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
