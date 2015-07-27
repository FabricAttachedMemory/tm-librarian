#!/usr/bin/python3 -tt
from function_chain import Link, Chain
import json

# seperating these for now but maybe have a std_link library? inside of the
# function chain module?
# instance or class? class? who cares really
class Json_Link(Link):

    def apply(self, obj):
        return json.dumps(obj)

    def unapply(self, obj):
        return json.loads(obj)


class Encode_Link(Link):

    def apply(self, obj):
        return str.encode(obj)

    def unapply(self, obj):
        return obj.decode("utf-8")


class Librarian_Chain(Chain):

    @staticmethod
    def argparse_extend(parser):
     pass

    def __init__(self, args):
        super().add_link(Json_Link())
        # temporarily removed to preserve current server code
        #super().add_link(Encode_Link())
