""" Chain module for use by the librarian """

# Copyright 2017 Hewlett Packard Enterprise Development LP

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2 as
# published by the Free Software Foundation.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License along
# with this program.  If not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA

from function_chain import Link, Chain
import json

from pdb import set_trace


class JSONDumpsLoadsLink(Link):
    """ Link that converts from dictionaries to json and vise-versa """

    def forward(self, obj):
        """ dictionary -> JSON

        Args:
            obj: Json dictionary

        Returns:
            JSON string
        """
        return json.dumps(obj)

    def reverse(self, obj):
        """ JSON -> dictionary

        Args:
            obj: JSON string

        Returns:
            Python dictionary object
        """
        return json.loads(obj)


class StrEncDecLink(Link):
    """ Encode and decode strings to pass to a socket """
    def forward(self, obj):
        """ Apply an encoding python string -> byte string
        Args:
            obj: Python3 string

        Returns:
            Byte string to be use with Socket.send()
        """
        return str.encode(obj)

    def reverse(self, obj):
        """ Decode encoding
        Args:
            obj: Byte string, like from Socket.recv()

        Returns:
            Python3 string
        """
        return obj.decode("utf-8")


class LibrarianChain(Chain):
    """ Chain for use with librarian code """

    @staticmethod
    def argparse_extend(parser):
        """ does nothing """
        pass

    def __init__(self, parseargs=None):
        super().__init__()
        self.append(JSONDumpsLoadsLink())
        self.append(StrEncDecLink())
