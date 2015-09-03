#!/usr/bin/python3
""" Unit tests for book_register.py """

import unittest
import configparser
import subprocess
import os

from book_register import load_config
from book_register import multiplier
from book_register import load_book_data
from book_register import create_empty_db

from backend_sqlite3 import SQLite3assist

def silent_remove(filename):
    try:
        os.remove(filename)
    except OSError:
        pass

class TestLoadConfig(unittest.TestCase):

    def setUp(self):
        silent_remove('test_ini_0')
        silent_remove('test_ini_1')
        silent_remove('test_ini_2')
        silent_remove('test_ini_3')
        silent_remove('test_ini_4')
        silent_remove('test_ini_5')
        silent_remove('test_ini_6')
        silent_remove('test_ini_7')

        f1 = open('test_ini_1', 'w')
        f1.close()

        f2 = open('test_ini_2', 'w')
        f2.write('blah\n')
        f2.close()

        f3 = open('test_ini_3', 'w')
        f3.write('[blah]\n')
        f3.close()

        f4 = open('test_ini_4', 'w')
        f4.write('[global]\n')
        f4.write('stuff = 0\n')
        f4.close()

        f5 = open('test_ini_5', 'w')
        f5.write('[global]\n')
        f5.write('node_count = 5\n')
        f5.write('nvm_size_per_node = 4T\n')
        f5.close()

        f6 = open('test_ini_6', 'w')
        f6.write('[global]\n')
        f6.write('node_count = 1\n')
        f6.write('book_size_bytes = 8G\n')
        f6.write('[node01]\n')
        f6.write('node_id = 0x0000000000000001\n')
        f6.write('lza_base = 0x0000000000000000\n')
        f6.write('nvm_size = 64G\n')
        f6.close()

        f7 = open('test_ini_7', 'w')
        f7.write('[global]\n')
        f7.write('node_count = 1\n')
        f7.write('book_size_bytes = 8G\n')
        f7.write('nvm_size_per_node = 4T\n')
        f7.close()

    def tearDown(self):
        silent_remove('test_ini_1')
        silent_remove('test_ini_2')
        silent_remove('test_ini_3')
        silent_remove('test_ini_4')
        silent_remove('test_ini_5')
        silent_remove('test_ini_6')
        silent_remove('test_ini_7')

    # Illegal - non-existent file
    def test_load_config_1(self):
        self.assertRaises(SystemExit, load_config, 'test_ini_0')

    # Illegal - empty file
    def test_load_config_1(self):
        self.assertRaises(SystemExit, load_config, 'test_ini_1')

    # Illegal - no sections
    def test_load_config_2(self):
        self.assertRaises(configparser.MissingSectionHeaderError, load_config, 'test_ini_2')

    # Illegal - bad section name
    def test_load_config_3(self):
        self.assertRaises(SystemExit, load_config, 'test_ini_3')

    # Illegal - bad global option
    def test_load_config_4(self):
        self.assertRaises(SystemExit, load_config, 'test_ini_4')

    # Illegal - incomplete global section
    def test_load_config_5(self):
        self.assertRaises(SystemExit, load_config, 'test_ini_5')

    # Basic legal file with node section
    def test_load_config_6(self):
        resp = load_config('test_ini_6')
        self.assertIsInstance(resp, configparser.ConfigParser)

    # Basic legal file with no node section
    def test_load_config_7(self):
        resp = load_config('test_ini_7')
        self.assertIsInstance(resp, configparser.ConfigParser)

class TestMultiplier(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    # global section "M" suffix
    def test_multiplier_1(self):
        book_size_bytes = multiplier('8M', 'global')
        self.assertEqual(book_size_bytes, (8*1024*1024))

    # global section "G" suffix
    def test_multiplier_2(self):
        book_size_bytes = multiplier('8G', 'global')
        self.assertEqual(book_size_bytes, (8*1024*1024*1024))

    # global section "T" suffix
    def test_multiplier_3(self):
        book_size_bytes = multiplier('8T', 'global')
        self.assertEqual(book_size_bytes, (8*1024*1024*1024*1024))

    # non-global section "B" suffix
    def test_multiplier_4(self):
        book_size_bytes = multiplier('32B', 'non-global', (8*1024*1024*1024))
        self.assertEqual(book_size_bytes, (32*8*1024*1024*1024))

    # Illegal suffix "X" global section
    def test_multiplier_5(self):
        self.assertRaises(SystemExit, multiplier, '32X', 'global')

    # Illegal suffix "X" non-global section
    def test_multiplier_6(self):
        self.assertRaises(SystemExit, multiplier, '32X', 'non-global')

    # Illegal suffix "B" in global section
    def test_multiplier_7(self):
        self.assertRaises(SystemExit, multiplier, '32B', 'global')

class TestLoadBookData(unittest.TestCase):

    def setUp(self):

        silent_remove('test_ini_1')
        silent_remove('test_ini_2')
        silent_remove('test_ini_3')
        silent_remove('test_ini_4')
        silent_remove('test_ini_5')
        silent_remove('test_ini_6')

        f1 = open('test_ini_1', 'w')
        f1.write('[global]\n')
        f1.write('node_count = 1\n')
        f1.write('book_size_bytes = 8G\n')
        f1.write('[node01]\n')
        f1.write('node_id = 0x0000000000000001\n')
        f1.write('lza_base = 0x0000000000000000\n')
        f1.write('nvm_size = 64G\n')
        f1.close()

        f2 = open('test_ini_2', 'w')
        f2.write('[global]\n')
        f2.write('node_count = 1\n')
        f2.write('book_size_bytes = 8G\n')
        f2.write('nvm_size_per_node = 4T\n')
        f2.close()

        f3 = open('test_ini_3', 'w')
        f3.write('[global]\n')
        f3.write('node_count = 1\n')
        f3.write('book_size_bytes = 3M\n')
        f3.write('nvm_size_per_node = 4T\n')
        f3.close()

        f4 = open('test_ini_4', 'w')
        f4.write('[global]\n')
        f4.write('node_count = 1\n')
        f4.write('book_size_bytes = 33G\n')
        f4.write('nvm_size_per_node = 4T\n')
        f4.close()

        f5 = open('test_ini_5', 'w')
        f5.write('[global]\n')
        f5.write('node_count = 1\n')
        f5.write('book_size_bytes = 8G\n')
        f5.write('nvm_size_per_node = 15G\n')
        f5.close()

        f6 = open('test_ini_6', 'w')
        f6.write('[global]\n')
        f6.write('node_count = 2\n')
        f6.write('book_size_bytes = 8G\n')
        f6.write('[node01]\n')
        f6.write('node_id = 0x0000000000000001\n')
        f6.write('lza_base = 0x0000000000000000\n')
        f6.write('nvm_size = 64G\n')
        f6.write('[node02]\n')
        f6.write('node_id = 0x0000000000000002\n')
        f6.write('lza_base = 0x0000000FFFFFFFFF\n')
        f6.write('nvm_size = 64G\n')
        f6.close()

    def tearDown(self):
        silent_remove('test_ini_1')
        silent_remove('test_ini_2')
        silent_remove('test_ini_3')
        silent_remove('test_ini_4')
        silent_remove('test_ini_5')
        silent_remove('test_ini_6')

    # Known good ini file with global and node sections
    def test_load_book_data_1(self):
        book_size_bytes, section2books = load_book_data('test_ini_1')
        self.assertEqual(book_size_bytes, (8*1024*1024*1024))
        self.assertIsInstance(section2books, dict)
        self.assertEqual(len(section2books['node01']), 8)

    # Known good ini file with global section only
    def test_load_book_data_2(self):
        book_size_bytes, section2books = load_book_data('test_ini_2')
        self.assertEqual(book_size_bytes, (8*1024*1024*1024))
        self.assertIsInstance(section2books, dict)
        self.assertEqual(len(section2books['node01']), 512)

    # Book size too small
    def test_load_book_data_3(self):
        self.assertRaises(SystemExit, load_book_data, 'test_ini_3')

    # Book size too big
    def test_load_book_data_4(self):
        self.assertRaises(SystemExit, load_book_data, 'test_ini_4')

    # Total NVM size is not a multiple of book size
    def test_load_book_data_5(self):
        self.assertRaises(SystemExit, load_book_data, 'test_ini_5')

    # Overlap of node NVM ranges
    def test_load_book_data_6(self):
        self.assertRaises(SystemExit, load_book_data, 'test_ini_6')

class TestCreateEmptyDB(unittest.TestCase):

    def setUp(self):
        silent_remove('test_create_empty_db_1.db')

    def tearDown(self):
        silent_remove('test_create_empty_db_1.db')

    # Create empty database
    def test_create_empty_db_1(self):
        cur = SQLite3assist(db_file='test_create_empty_db_1.db')
        create_empty_db(cur)
        self.assertTrue(os.path.isfile('test_create_empty_db_1.db'))

class TestBookRegister(unittest.TestCase):

    def setUp(self):
        silent_remove('test_book_register_1.db')
        silent_remove('test_book_register_2.db')

    def tearDown(self):
        silent_remove('test_book_register_1.db')
        silent_remove('test_book_register_2.db')

    # Create database from standard "book_data.ini" file
    def test_book_register_1(self):
        ret = subprocess.call(['./book_register.py','-d','test_book_register_1.db','book_data.ini'])
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.isfile('test_book_register_1.db'))

    # Create database from standard "book_max.ini" file
    def test_book_register_2(self):
        ret = subprocess.call(['./book_register.py','-d','test_book_register_2.db','book_max.ini'])
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.isfile('test_book_register_2.db'))

if __name__ == '__main__':
    unittest.main()
