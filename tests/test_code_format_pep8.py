#!/usr/bin/python3

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

""" Run pep8 filter on each python file """

import unittest
import pep8


class TestCodeFormat(unittest.TestCase):

    def test_pep8_conformance(self):
        """Test that we conform to PEP8."""
        pep8style = pep8.StyleGuide(
            ignore=['E121', 'E123', 'E126', 'E133', 'E226',
                    'E241', 'E242', 'E704', 'E265', 'E201','E202'],
#            show_pep8=True,
            show_source=True
        )
        result = pep8style.check_files([
            'backend_sqlite3.py', 
            'book_shelf_bos.py', 
            'function_chain.py', 
            'librarian_chain.py', 
            'repl_client.py', 
            'sqlassist.py', 
            'cmdproto.py', 
            'genericobj.py', 
            'librarian.py', 
            'sqlbackend.py', 
            'lfs_fuse.py', 
            'book_register.py', 
            'engine.py', 
            'lfs_shadow.py', 
            'socket_handling.py'])
        self.assertEqual(result.total_errors, 0,
                         "Found code style errors (and warnings).")

if __name__ == '__main__':
    unittest.main()
