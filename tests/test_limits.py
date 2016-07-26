#!/usr/bin/python3

import unittest
import subprocess
import os
from test_utils import zero_silent_remove

# While this script can be run on its own and every test may be run, it is
# better to run tests individually or as a class because books may not be 
# fully recovered by the time the next test begins. As such running the 
# tests individually will help ensure result integrity in the case that
# large files have previously consumed all available books.

# EG) 'python3 -m unittest test_limits.TestCMD_mv' (tests mv class)
# EG) 'python3 -m unittest test_limits.TestCMD_mv.test_mv_to_lfs' (tests mv to lfs only)



# Tests are currently supposed to be run with 4T nodes and 8G books
num_nodes = 4 
mem_per_node = 4000 #(G)



class TestCMD_mv(unittest.TestCase):
    def setUp(self):
        zero_silent_remove('/lfs/s1')
        zero_silent_remove('/tmp/s1')
        f = open('/lfs/s1','w+')
        f.close()

    def tearDown(self):
        zero_silent_remove('/lfs/s1')
        zero_silent_remove('/tmp/s1') 

    # simple testing of rudimentry and common command functionallity    
    def test_mv_rename(self):
        ret = subprocess.call(['mv','/lfs/s1','/lfs/s2'])
        self.assertEqual(ret, 0,'Return from mv was: ' + str(ret) + ' not 0')
        self.assertTrue(os.path.isfile('/lfs/s2'), 'File not found at expected location: /lfs/s2')
        self.assertFalse(os.path.isfile('/lfs/s1'),'File /lfs/s1 still exists and shouldn\'t')
        zero_silent_remove('/lfs/s2')

    # This currently has some issues with it and causes two s1 files to appear
    @unittest.skip('Creates two versions of s1 and causes subsequent tests to fail needlessly')
    def test_mv_overwrite(self):
        f = open('/lfs/s2','w+')
        f.close()
        ret = subprocess.call(['mv','/lfs/s2','/lfs/s1'])
        self.assertEqual(ret, 0,'Return from mv was: ' + str(ret) + ' not 0')
        self.assertTrue(os.path.isfile('/lfs/s1'),'File /lfs/s1 was not present')
        self.assertFalse(os.path.isfile('/lfs/s2'),'File /lfs/s2 still exists and shouldn\'t')

    # These tests move small files just to see if the command works
    def test_mv_move_from_lfs(self):
        ret = subprocess.call(['mv','/lfs/s1','/tmp'])
        self.assertEqual(ret, 0,'Return from mv was: ' + str(ret) + ' not 0')
        self.assertTrue(os.path.isfile('/tmp/s1'),'File not found at expected location : /tmp/s1')
        self.assertFalse(os.path.isfile('/lfs/s1'),'File /lfs/s1 still exists and shouldn\'t')

    def test_mv_move_to_lfs(self):
        f = open('/tmp/s1','w+')
        f.close()
        zero_silent_remove('/lfs/s1')
        ret = subprocess.call(['mv','/tmp/s1','/lfs/s1'])
        self.assertEqual(ret, 0,'Return from mv was: ' + str(ret) + ' not 0')
        self.assertTrue(os.path.isfile('/lfs/s1'),'File not found at expected location: /lfs/s1')
        self.assertFalse(os.path.isfile('/tmp/s1'),'File /tmp/s1 exists and shouldn\'t')

    # The large tests move 1T of data around, well within the capabilites of multiple 4T nodes
    def test_mv_move_large_from_lfs(self):
        ret = subprocess.call(['truncate','-s1000G','/lfs/s1'])
        ret1 = subprocess.call(['mv','/lfs/s1','/tmp/s1'])
        self.assertEqual(ret, 0,'Return from truncate was: ' + str(ret) + ' not 0')
        self.assertEqual(ret1, 0,'Return from mv was: ' + str(ret) + ' not 0')
        self.assertTrue(os.path.isfile('/tmp/s1'),'File not found at expected location: /tmp/s1')
        self.assertEqual(os.path.getsize('/tmp/s1'), 1073741824000,"File was not expected Size")
        self.assertFalse(os.path.isfile('/lfs/s1'),'File /lfs/s1 exists and shouldn\'t')
    
    def test_mv_move_large_to_lfs(self):
        f = open('/tmp/s1','w+')
        f.close()
        zero_silent_remove('/lfs/s1')
        ret = subprocess.call(['truncate','-s1000G','/tmp/s1'])
        ret1 = subprocess.call(['mv','/tmp/s1','/lfs/s1'])
        self.assertEqual(ret, 0,'Return from truncate was: ' + str(ret) + ' not 0')
        self.assertEqual(ret1, 0,'Return from mv was: ' + str(ret) + ' not 0')
        self.assertTrue(os.path.isfile('/lfs/s1'),'File not found at expected location: /lfs/s1')
        self.assertEqual(os.path.getsize('/lfs/s1'), 1073741824000,"File was not expected Size")
        self.assertFalse(os.path.isfile('/tmp/s1'),'File /tmp/s1 exists and shouldn\'t')

    # Corner Cases:
    # What happens when a 5T (to big for single node) file stored on /lfs is moved to local memory on a node?
    # What happens if a file that is almost too big is moved? Does the system run out of mem?
    # What happens when file that consumes all the books is moved/renamed? (requires 4 4T nodes)

    def test_mv_move_massive_to_node(self):
        ret = subprocess.call(['truncate','-s5000G','/lfs/s1'])
        ret1 = subprocess.call(['mv','/lfs/s1','/tmp/s1'])
        validate_existence = os.path.isfile('/tmp/s1') 
        validate_size = os.path.getsize('/tmp/s1') == 5368709120000
        validate_destruction = not os.path.isfile('/lfs/s1')
        self.assertTrue(ret and ret1 and validate_existence and validate_size and validate_destruction,'Expected Failure') 

    def test_mv_rename_max_size():
        ret = subprocess.call(['truncate','-s','12000G','/lfs/s1'])
        ret1 = subprocess.call(['mv','/lfs/s1','/lfs/s2'])
        validate_existence = os.path.isfile('/lfs/s2') 
        validate_size = os.path.getsize('/lfs/s2') == 12884901888000
        validate_destruction = not os.path.isfile('/lfs/s1')
        zero_silent_remove('/lfs/s2')
        self.assertTrue(ret and ret1 and validate_existence and validate_size and validate_destruction,'Expected Failure')

    def test_mv_rename_max_books():
        ret = subprocess.call(['truncate','-s16384G' ,'/lfs/s1'])
        ret1 = subprocess.call(['mv','/lfs/s1','/lfs/s2'])
        validate_existence = os.path.isfile('/lfs/s2') 
        validate_size = os.path.getsize('/lfs/s2') == 12884901888000
        validate_destruction = not os.path.isfile('/lfs/s1')
        zero_silent_remove('/lfs/s2')
        self.assertTrue(ret and ret1 and validate_existence and validate_size and validate_destruction,'Expected Failure')


class TestCMD_cp(unittest.TestCase):
    def setUp(self):
        zero_silent_remove('/lfs/s1')
        zero_silent_remove('/tmp/s1')
        f = open('/lfs/s1','w+')
        f.close() 

    def tearDown(self):
        zero_silent_remove('/lfs/s1')
        zero_silent_remove('/tmp/s1')

    # simple testing of rudimentry and common command functionallity 
    def test_cp_on_lfs(self):
        ret = subprocess.call(['cp','/lfs/s1','/lfs/s2'])
        self.assertEqual(ret, 0,'Return from cp was: ' + str(ret) + ' not 0')
        self.assertTrue(os.path.isfile('/lfs/s2'),'File not found at expected location: /lfs/s2')
        self.assertTrue(os.path.isfile('/lfs/s1'),'File not found at expected location: /lfs/s1')
        zero_silent_remove('/lfs/s2')

    def test_cp_overwrite(self):
        file = open('/lfs/s2','w+')
        file.close()
        ret = subprocess.call(['cp','/lfs/s2','/lfs/s1'])
        self.assertEqual(ret, 0,'Return from cp was: ' + str(ret) + ' not 0')
        self.assertTrue(os.path.isfile('/lfs/s1'),'File not found at expected location: /lfs/s1')
        self.assertTrue(os.path.isfile('/lfs/s2'),'File not found at expected location: /lfs/s2')
        zero_silent_remove('/lfs/s2')

    # These tests move small files just to see if the command works
    def test_cp_move_from_lfs(self):
        ret = subprocess.call(['cp','/lfs/s1','/tmp'])
        self.assertEqual(ret, 0,'Return from cp was: ' + str(ret) + ' not 0')
        self.assertTrue(os.path.isfile('/tmp/s1'),'File not found at expected location: /tmp/s1')
        self.assertTrue(os.path.isfile('/lfs/s1'),'File not found at expected location: /tmp/s1')

    def test_cp_move_to_lfs(self):
        f = open('/tmp/s1','w+')
        f.close()
        zero_silent_remove('/lfs/s1')
        ret = subprocess.call(['cp','/tmp/s1','/lfs/s1'])
        self.assertEqual(ret, 0,'Return from cp was: ' + str(ret) + ' not 0')
        self.assertTrue(os.path.isfile('/lfs/s1'),'File not found at expected location: /lfs/s1')
        self.assertTrue(os.path.isfile('/tmp/s1'),'File not found at expected location: /tmp/s1')

    # The large tests move 1T of data around, well within the capabilites of multiple 4T nodes 
    def test_cp_move_large_from_lfs(self):
        ret = subprocess.call(['truncate','-s1000G','/lfs/s1'])
        ret1 = subprocess.call(['cp','/lfs/s1','/tmp/s1'])
        self.assertEqual(ret, 0,'Return from truncate was: ' + str(ret) + ' not 0')
        self.assertEqual(ret1, 0,'Return from cp was: ' + str(ret1) + ' not 0')
        self.assertTrue(os.path.isfile('/tmp/s1'),'File not found at expected location: /tmp/s1')
        self.assertEqual(os.path.getsize('/tmp/s1'), 1073741824000,'File was not expected Size')
        self.assertTrue(os.path.isfile('/lfs/s1'),'File not found at expected location: /lfs/s1')

    def test_cp_move_large_to_lfs(self):
        f = open('/tmp/s1','w+')
        f.close()
        zero_silent_remove('/lfs/s1')
        ret = subprocess.call(['truncate','-s1000G','/tmp/s1'])
        ret1 = subprocess.call(['cp','/tmp/s1','/lfs/s1'])
        self.assertEqual(ret, 0,'Return from truncate was: ' + str(ret) + ' not 0')
        self.assertEqual(ret1, 0,'Return from cp was: ' + str(ret1) + ' not 0')
        self.assertTrue(os.path.isfile('/lfs/s1'),'File not found at expected location: /lfs/s1')
        self.assertEqual(os.path.getsize('/lfs/s1'), 1073741824000,"File was not expected Size")
        self.assertTrue(os.path.isfile('/tmp/s1'),'File not found at expected location: /lfs/s1')

    # Corner Cases:
    # What happens when a 5T (to big for single node) file stored on /lfs is moved to local memory on a node?
    # What happens when a large file is duplicated causing the system to overflow in memory or books?
    
    def test_cp_massive_from_lfs(self):
        ret = subprocess.call(['truncate','-s5000G','/lfs/s1'])
        ret1 = subprocess.call(['cp','/lfs/s1','/tmp/s1'])
        validate_existence = os.path.isfile('/tmp/s1')
        validate_size = os.path.getsize('/tmp/s1') == 5368709120000
        validate_existence_lfs = os.path.isfile('/lfs/s1')
        validate_size_lfs = os.path.getsize('/lfs/s1') == 5368709120000
        self.assertTrue(ret == 0 and ret1 == 0 and validate_existence and validate_size and validate_existence_lfs and validate_size_lfs) 

    def test_cp_massive_on_lfs(self):
        ret = subprocess.call(['truncate','-s10000G','/lfs/s1'])
        ret1 = subprocess.call(['cp','/lfs/s1','/lfs/s2'])
        validate_existence = os.path.isfile('/lfs/s1')
        validate_size = os.path.getsize('/lfs/s1') == 10737418240000
        validate_existence_s2 = os.path.isfile('/lfs/s2')
        validate_size_s2 = os.path.getsize('/lfs/s2') == 10737418240000
        self.assertTrue(ret == 0 and ret1 == 0 and validate_existence and validate_size and validate_existence_s2 and validate_size_s2) 

class TestCMD_truncate(unittest.TestCase):
    def setUp(self):
        zero_silent_remove('/lfs/s1')
        f = open('/lfs/s1','w+')
        f.close()

    def tearDown(self):
        zero_silent_remove('/lfs/s1')

    def test_truncate_bigger(self):
        ret = subprocess.call(['truncate','-s10G','/lfs/s1'])
        self.assertEqual(ret,0)
        self.assertTrue(os.path.isfile('/lfs/s1'),'File not found at expected location: /lfs/s1')
        self.assertTrue(os.path.getsize('/lfs/s1')== 10737418240,'File was not the proper size after truncate')

    def test_truncate_smaller(self):
        f = open('/lfs/s1','w+')
        for i in range(0,10000):
            f.write('This is a test file that I am making bigger before I truncate it down to a smaller size.')
        f.close()
        self.assertTrue(os.path.getsize('/lfs/s1'),'Failed to write to file and expand it')
        subprocess.call(['truncate','-s1K','/lfs/s1'])
        self.assertTrue(os.path.isfile('/lfs/s1'),'File not found at expected location: /lfs/s1')
        self.assertEqual(os.path.getsize('/lfs/s1'),1024,'File was not proper size after truncate')
    def test_truncate_zero(self):
        f = open('/lfs/s1','w+')
        for i in range(0,10000):
            f.write('This is a test file that I am making bigger before I truncate it down to zero')
        f.close()
        self.assertTrue(os.path.getsize('/lfs/s1') > 0,'Failed to write to file and expand it')
        ret = subprocess.call(['truncate','-s0K','/lfs/s1'])
        self.assertEqual(ret,0,'Return from truncate was: '+str(ret)+' not 0')
        self.assertTrue(os.path.isfile('/lfs/s1'),'File was not found at expected location: /lfs/s1')
        self.assertEqual(os.path.getsize('/lfs/s1'),0,'File was not the proper size after truncate')

    # Truncate the file to the max amount of memory avaliable in the system
    def test_truncate_max(self):
        ret = subprocess.call(['truncate','-s'+str(num_nodes * mem_per_node) + 'G','/lfs/s1'])
        self.assertTrue(os.path.isfile('/lfs/s1'))
        self.assertEqual(os.path.getsize('/lfs/s1'),1073741824 * num_nodes*mem_per_node)
    
    # Truncate the file to 1 node greater than the max amount of memory avaliable in the system
    def test_truncate_max_plus(self):
        ret = subprocess.call(['truncate','-s'+str((num_nodes +1 )* mem_per_node) + 'G','/lfs/s1'])
        self.assertTrue(os.path.isfile('/lfs/s1'))
        self.assertFalse(ret == 0)
        self.assertFalse(os.path.getsize('/lfs/s1'),1073741824 * (num_nodes + 1)*mem_per_node)
    
    # Truncate the file to a size that no system will ever support just to make sure truncate throws an error in this case 1 Yotabyte 
    def test_truncate_exeed_limit(self):
        ret = subprocess.call(['truncate','-s1Y','/lfs/s1'])
        self.assertTrue(os.path.isfile('/lfs/s1'))
        self.assertFalse(ret == 0)


if __name__ == '__main__':
	unittest.main()
