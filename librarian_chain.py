#!/usr/bin/python3 -tt
from function_chain import Link, Chain
import json

# seperating these for now but maybe have a std_link library? inside of the
# function chain module?
# instance or class? class? who cares really
class Json_Link(Link):
    # will python implicitly call parents constructor/destructor?
    def __init__(self):
        super().__init__()

    def __del__(self):
        super().__init__()

    def apply(self, obj):
        return json.dumps(obj)

    def unapply(self, obj):
        return json.loads(obj)


class Encode_Link(Link):

    def __init__(self):
        super().__init__()

    def __del__(self):
        super().__init__()

    def apply(self, obj):
        return str.encode(obj)

    def unapply(self, obj):
        return obj.decode("utf-8")


librarin_chain = Chain()
j_link = Json_Link()
e_link = Encode_Link()

# this could be akward if the encodes and decodes we selected don't match up
librarian_chain.add_link(j_link)
librarian_chain.add_link(e_link)
