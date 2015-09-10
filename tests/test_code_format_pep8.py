#!/usr/bin/python3
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
