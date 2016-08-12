#!/usr/bin/python3

import unittest
import subprocess
import os
import random
import filecmp
import re
import time
from test_utils import silent_remove, zero_silent_remove, clear_lfs

# While this script can be run on its own and every test may be run, it is
# better to run tests individually or as a class because books may not be 
# fully recovered by the time the next test begins. As such running the 
# tests individually will help ensure result integrity in the case that
# large files have previously consumed all available books.


# EG) 'python3 -m unittest test_limits.TestCMD_mv' (tests mv class)
# EG) 'python3 -m unittest test_limits.TestCMD_mv.test_mv_to_lfs' (tests mv to lfs only)



# Tests are currently supposed to be run with 4T nodes and 8G books, on FAME or TMAS
num_nodes = 4 
mem_per_node = 4000 #(G)

#change to false if you want to run tests that use large amounts of memory. Note thought that result integrity may be lost
skip_high_mem_use_tests = True 

class TestCMD_mv(unittest.TestCase):
    def setUp(self):
        zero_silent_remove('/lfs/s1')
        zero_silent_remove('/tmp/s1')
        clear_lfs()
        f = open('/lfs/s1','w+')
        f.close()

    def tearDown(self):
        clear_lfs()
        zero_silent_remove('/lfs/s1')
        zero_silent_remove('/tmp/s1') 

    # simple testing of rudimentry and common command functionallity    
    def test_mv_rename(self):
        ret = subprocess.call(['mv','/lfs/s1','/lfs/s2'])
        self.assertEqual(ret, 0,'Return from mv was: ' + str(ret) + ' not 0')
        self.assertTrue(os.path.isfile('/lfs/s2'), 'File not found at expected location: /lfs/s2')
        self.assertFalse(os.path.isfile('/lfs/s1'),'File /lfs/s1 still exists and shouldn\'t')
        zero_silent_remove('/lfs/s2')

    # This currently has an issue and causes two s1 files to appear
    @unittest.expectedFailure
    def test_mv_overwrite(self):
        f = open('/lfs/test_mv_overwrite','w+')
        f.close()
        ret = subprocess.call(['mv','/lfs/s1','/lfs/test_mv_overwrite'])
        self.assertEqual(ret, 0,'Return from mv was: ' + str(ret) + ' not 0')
        self.assertTrue(os.path.isfile('/lfs/test_mv_overwrite'),'File /lfs/test_mv_overwrite was not present')
        self.assertFalse(os.path.isfile('/lfs/s1'),'File /lfs/s1 still exists and shouldn\'t')
    
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
        
    def test_mv_move_integrity_from_lfs(self):
        f = open('/lfs/s1','w+')
        f_two = open('/lfs/s2','w+')
        for i in range(0,1000):
            char = chr(random.randint(32,128))
            f.write(char)
            f_two.write(char)
        f.close()
        f_two.close()
        ret = subprocess.call(['mv','/lfs/s1','/tmp'])
        self.assertEqual(ret, 0,'Return from mv was: ' + str(ret) + ' not 0')
        self.assertTrue(os.path.isfile('/tmp/s1'),'File not found at expected location : /tmp/s1')
        self.assertFalse(os.path.isfile('/lfs/s1'),'File /lfs/s1 still exists and shouldn\'t')
        self.assertTrue(filecmp.cmp('/lfs/s2','/tmp/s1'),'Moved File did not match original')
        self.assertEqual(os.access('/lfs/s1'),os.access('/lfs/s2'),'File permissions did not match expected')
        zero_silent_remove('/lfs/s2')

    def test_mv_move_integrity_to_lfs(self):
        f = open('/tmp/s1','w+')
        f_two = open('/tmp/s2','w+')
        for i in range(0,1000):
            char = chr(random.randint(32,128))
            f.write(char)
            f_two.write(char)
        f.close()
        f_two.close()
        zero_silent_remove('/lfs/s1')
        ret = subprocess.call(['mv','/tmp/s1','/lfs/s1'])
        self.assertEqual(ret, 0,'Return from mv was: ' + str(ret) + ' not 0')
        self.assertTrue(os.path.isfile('/lfs/s1'),'File not found at expected location: /lfs/s1')
        self.assertTrue(filecmp.cmp('/tmp/s2','/lfs/s1'),'Moved File did not match original')
        self.assertFalse(os.path.isfile('/tmp/s1'),'File /tmp/s1 exists and shouldn\'t')
        self.assertEqual(os.access('/lfs/s1'),os.access('/tmp/s2'),'File permissions did not match expected')
        zero_silent_remove('/tmp/s2')

    # The large tests move 1T of data around, well within the capabilites of multiple 4T nodes
    @unittest.skipIf(skip_high_mem_use_tests, 'Skipping high memory usage test')
    def test_mv_move_large_from_lfs(self):
        ret = subprocess.call(['truncate','-s1000G','/lfs/s1'])
        ret1 = subprocess.call(['mv','/lfs/s1','/tmp/s1'])
        self.assertEqual(ret, 0,'Return from truncate was: ' + str(ret) + ' not 0')
        self.assertEqual(ret1, 0,'Return from mv was: ' + str(ret) + ' not 0')
        self.assertTrue(os.path.isfile('/tmp/s1'),'File not found at expected location: /tmp/s1')
        self.assertEqual(os.path.getsize('/tmp/s1'), 1073741824000,"File was not expected Size")
        self.assertFalse(os.path.isfile('/lfs/s1'),'File /lfs/s1 exists and shouldn\'t')

    @unittest.skipIf(skip_high_mem_use_tests, 'Skipping high memory usage test')
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

    @unittest.skipIf(skip_high_mem_use_tests, 'Skipping high memory usage test')
    def test_mv_move_massive_to_node(self):
        self.assertEqual(subprocess.call(['truncate','-s5000G','/lfs/s1']),0)
        self.assertEqual(subprocess.call(['mv','/lfs/s1','/tmp/s1']),0)
        self.assertTrue(os.path.isfile('/tmp/s1')) 
        self.assertEqual(os.path.getsize('/tmp/s1'), 5368709120000)
        self.assertFalse(os.path.isfile('/lfs/s1'))
        
    @unittest.skipIf(skip_high_mem_use_tests, 'Skipping high memory usage test')
    def test_mv_rename_max_size():
        self.assertEqual(subprocess.call(['truncate','-s','12000G','/lfs/s1']),0)
        self.assertEqual(subprocess.call(['mv','/lfs/s1','/lfs/s2']),0)
        self.assertTrue(os.path.isfile('/lfs/s2'))
        self.assertEqual(os.path.getsize('/lfs/s2'), 12884901888000)
        self.assertFalse(os.path.isfile('/lfs/s1'))
        zero_silent_remove('/lfs/s2')

    @unittest.skipIf(skip_high_mem_use_tests, 'Skipping high memory usage test')   
    def test_mv_rename_max_books():
        self.assertEqual(subprocess.call(['truncate','-s16384G' ,'/lfs/s1']),0)
        self.assertEqual(subprocess.call(['mv','/lfs/s1','/lfs/s2']),0)
        self.assertTrue(os.path.isfile('/lfs/s2'))
        self.assertEqual(os.path.getsize('/lfs/s2'), 12884901888000)
        self.assertFalse(os.path.isfile('/lfs/s1'))
        zero_silent_remove('/lfs/s2')
        

class TestCMD_cp(unittest.TestCase):
    def setUp(self):
        zero_silent_remove('/lfs/s1')
        zero_silent_remove('/tmp/s1')
        clear_lfs()
        f = open('/lfs/s1','w+')
        f.close() 

    def tearDown(self):
        clear_lfs()
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
        f = open('/lfs/s2','w+')
        f.close()
        ret = subprocess.call(['cp','/lfs/s2','/lfs/s1'])
        self.assertEqual(ret, 0,'Return from cp was: ' + str(ret) + ' not 0')
        self.assertTrue(os.path.isfile('/lfs/s1'),'File not found at expected location: /lfs/s1')
        self.assertTrue(os.path.isfile('/lfs/s2'),'File not found at expected location: /lfs/s2')
        zero_silent_remove('/lfs/s2')

    # These tests move small files just to see if the command works
    def test_cp_copy_from_lfs(self):
        ret = subprocess.call(['cp','/lfs/s1','/tmp'])
        self.assertEqual(ret, 0,'Return from cp was: ' + str(ret) + ' not 0')
        self.assertTrue(os.path.isfile('/tmp/s1'),'File not found at expected location: /tmp/s1')
        self.assertTrue(os.path.isfile('/lfs/s1'),'File not found at expected location: /tmp/s1')

    def test_cp_copy_to_lfs(self):
        f = open('/tmp/s1','w+')
        f.close()
        zero_silent_remove('/lfs/s1')
        ret = subprocess.call(['cp','/tmp/s1','/lfs/s1'])
        self.assertEqual(ret, 0,'Return from cp was: ' + str(ret) + ' not 0')
        self.assertTrue(os.path.isfile('/lfs/s1'),'File not found at expected location: /lfs/s1')
        self.assertTrue(os.path.isfile('/tmp/s1'),'File not found at expected location: /tmp/s1')
    
    def test_cp_integrity_from_lfs(self):
        ret = subprocess.call(['cp','/lfs/s1','/tmp'])
        self.assertEqual(ret, 0,'Return from cp was: ' + str(ret) + ' not 0')
        self.assertTrue(os.path.isfile('/tmp/s1'),'File not found at expected location: /tmp/s1')
        self.assertTrue(os.path.isfile('/lfs/s1'),'File not found at expected location: /tmp/s1')
        self.assertTrue(filecmp.cmp('/lfs/s1','/tmp/s1'))
        self.assertEqual(os.access('/lfs/s1'),os.access('/tmp/s1'),'File permissions did not match expected')

    def test_cp_integrity_to_lfs(self):
        f = open('/tmp/s1','w+')
        f.close()
        zero_silent_remove('/lfs/s1')
        ret = subprocess.call(['cp','/tmp/s1','/lfs/s1'])
        self.assertEqual(ret, 0,'Return from cp was: ' + str(ret) + ' not 0')
        self.assertTrue(os.path.isfile('/lfs/s1'),'File not found at expected location: /lfs/s1')
        self.assertTrue(os.path.isfile('/tmp/s1'),'File not found at expected location: /tmp/s1')
        self.assertTrue(filecmp.cmp('/lfs/s1','/tmp/s1'))
        self.assertEqual(os.access('/lfs/s1'),os.access('/tmp/s1'),'File permissions did not match expected')

    # The large tests move 1T of data around, well within the capabilites of multiple 4T nodes 
    @unittest.skipIf(skip_high_mem_use_tests, 'Skipping high memory usage test')
    def test_cp_copy_large_from_lfs(self):
        ret = subprocess.call(['truncate','-s1000G','/lfs/s1'])
        ret1 = subprocess.call(['cp','/lfs/s1','/tmp/s1'])
        self.assertEqual(ret, 0,'Return from truncate was: ' + str(ret) + ' not 0')
        self.assertEqual(ret1, 0,'Return from cp was: ' + str(ret1) + ' not 0')
        self.assertTrue(os.path.isfile('/tmp/s1'),'File not found at expected location: /tmp/s1')
        self.assertEqual(os.path.getsize('/tmp/s1'), 1073741824000,'File was not expected Size')
        self.assertTrue(os.path.isfile('/lfs/s1'),'File not found at expected location: /lfs/s1')

    @unittest.skipIf(skip_high_mem_use_tests, 'Skipping high memory usage test')
    def test_cp_copy_large_to_lfs(self):
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

    @unittest.skipIf(skip_high_mem_use_tests, 'Skipping high memory usage test')
    def test_cp_copy_massive_from_lfs(self):
        ret = subprocess.call(['truncate','-s5000G','/lfs/s1'])
        ret1 = subprocess.call(['cp','/lfs/s1','/tmp/s1'])
        validate_existence = os.path.isfile('/tmp/s1')
        validate_size = os.path.getsize('/tmp/s1') == 5368709120000
        validate_existence_lfs = os.path.isfile('/lfs/s1')
        validate_size_lfs = os.path.getsize('/lfs/s1') == 5368709120000
        self.assertTrue(ret == 0 and ret1 == 0 and validate_existence and validate_size and validate_existence_lfs and validate_size_lfs) 
    
    @unittest.skipIf(skip_high_mem_use_tests, 'Skipping high memory usage test')
    def test_cp_copy_massive_on_lfs(self):
        ret = subprocess.call(['truncate','-s10000G','/lfs/s1'])
        ret1 = subprocess.call(['cp','/lfs/s1','/lfs/s2'])
        validate_existence = os.path.isfile('/lfs/s1')
        validate_size = os.path.getsize('/lfs/s1') == 10737418240000
        validate_existence_s2 = os.path.isfile('/lfs/s2')
        validate_size_s2 = os.path.getsize('/lfs/s2') == 10737418240000
        self.assertTrue(ret == 0 and ret1 == 0 and validate_existence and validate_size and validate_existence_s2 and validate_size_s2) 
        zero_silent_remove('/lfs/s2')

class TestCMD_truncate(unittest.TestCase):
    def setUp(self):
        zero_silent_remove('/lfs/s1')
        clear_lfs()
        f = open('/lfs/s1','w+')
        f.close()

    def tearDown(self):
        zero_silent_remove('/lfs/s1')
        clear_lfs()

    @unittest.skipIf(skip_high_mem_use_tests, 'Skipping high memory usage test')
    def test_truncate_bigger(self):
        ret = subprocess.call(['truncate','-s10G','/lfs/s1'])
        self.assertEqual(ret,0)
        self.assertTrue(os.path.isfile('/lfs/s1'),'File not found at expected location: /lfs/s1')
        self.assertTrue(os.path.getsize('/lfs/s1')== 10737418240,'File was not the proper size after truncate')

    def test_truncate_smaller(self):
        f = open('/lfs/s1','w+')
        for i in range(0,1000):
            f.write('This is a test file that I am making bigger before I truncate it down to a smaller size.')
        f.close()
        self.assertTrue(os.path.getsize('/lfs/s1'),'Failed to write to file and expand it')
        subprocess.call(['truncate','-s1K','/lfs/s1'])
        self.assertTrue(os.path.isfile('/lfs/s1'),'File not found at expected location: /lfs/s1')
        self.assertEqual(os.path.getsize('/lfs/s1'),1024,'File was not proper size after truncate')
    
    def test_truncate_zero(self):
        f = open('/lfs/s1','w+')
        for i in range(0,1000):
            f.write('This is a test file that I am making bigger before I truncate it down to zero')
        f.close()
        self.assertTrue(os.path.getsize('/lfs/s1') > 0,'Failed to write to file and expand it')
        ret = subprocess.call(['truncate','-s0K','/lfs/s1'])
        self.assertEqual(ret,0,'Return from truncate was: '+str(ret)+' not 0')
        self.assertTrue(os.path.isfile('/lfs/s1'),'File was not found at expected location: /lfs/s1')
        self.assertEqual(os.path.getsize('/lfs/s1'),0,'File was not the proper size after truncate')

    # Truncate the file to the max amount of memory avaliable in the syste
    @unittest.skipIf(skip_high_mem_use_tests, 'Skipping high memory usage test')
    def test_truncate_max(self):
        ret = subprocess.call(['truncate','-s'+str(num_nodes * mem_per_node) + 'G','/lfs/s1'])
        self.assertTrue(os.path.isfile('/lfs/s1'))
        self.assertEqual(os.path.getsize('/lfs/s1'),1073741824 * num_nodes*mem_per_node)
    
    # Truncate the file to 1 node greater than the max amount of memory avaliable in the system
    @unittest.skipIf(skip_high_mem_use_tests, 'Skipping high memory usage test')
    def test_truncate_max_plus(self):
        ret = subprocess.call(['truncate','-s'+str((num_nodes +1 )* mem_per_node) + 'G','/lfs/s1'])
        self.assertTrue(os.path.isfile('/lfs/s1'))
        self.assertFalse(ret == 0)
        self.assertFalse(os.path.getsize('/lfs/s1'),1073741824 * (num_nodes + 1)*mem_per_node)
    
    # Truncate the file to a size that no system will ever support just to make sure truncate throws an error in this case 1 Yotabyte 
    @unittest.skipIf(skip_high_mem_use_tests, 'Skipping high memory usage test') 
    def test_truncate_exeed_limit(self):
        ret = subprocess.call(['truncate','-s1Y','/lfs/s1'])
        self.assertTrue(os.path.isfile('/lfs/s1'))
        self.assertFalse(ret == 0)

class TestFileOperations(unittest.TestCase):
    def setUp(self):
        clear_lfs()
        f = open("/lfs/s1",'w+')
        f.close()
    
    def tearDown(self):
        zero_silent_remove('/lfs/s1')
        clear_lfs()
    
    def test_shelf_create(self):
        self.assertTrue(os.path.isfile('/lfs/s1'),'File was not found at expected location')
    
    def test_shelf_remove(self):
        output = os.remove("/lfs/s1")
        self.assertFalse(os.path.isfile('/lfs/s1'),'File failed to be removed - Python')
    
    def test_shelf_create_bash(self):
        output = subprocess.call(['touch','/lfs/s2'])
        file_exists = os.path.isfile('/lfs/s2')
        os.remove('/lfs/s2')
        self.assertTrue(file_exists,'shelf failed to be created with touch')
    
    def test_shelf_remove_bash(self):
        output = subprocess.call(['rm','/lfs/s1'])
        self.assertFalse(os.path.isfile('/lfs/s1'),'shelf failed to be removed - Bash')
    
    @unittest.skipIf(skip_high_mem_use_tests, 'Skipping high memory usage test')
    def test_create_max_shelves(self):
        for i in range(0,2048):
            self.assertTrue(subprocess.call(['touch','/lfs/s'+str(i)]) == 0 )
    
    @unittest.skipIf(skip_high_mem_use_tests, 'Skipping high memory usage test')
    def test_create_max_plus_shelves(self):
        for i in range(0,2048):
            self.assertTrue(subprocess.call(['touch','/lfs/s'+str(i)]) == 0 )
        self.assertFalse(subprocess.call(['touch','/lfs/s2048']) == 0 )
    
    @unittest.skipIf(skip_high_mem_use_tests, 'Skipping high memory usage test')
    def test_create_max_shelves_with_data(self):
        for i in range(0,2048):
            f = open('/lfs/s' + str(i),'w+')
            f.write('test')
            f.close()

    @unittest.skipIf(skip_high_mem_use_tests, 'Skipping high memory usage test') 
    def test_create_max_plus_shelves_with_data(self):
        for i in range(0,2048):
            f = open('/lfs/s' + str(i),'w+')
            f.write('test')
            f.close()
            self.assertTrue(os.path.isfile('/lfs/s' + str(i)))
        try:
            f = open('/lfs/2048','w+')
            f.write('test')
            f.close()
        except:
            pass
        self.assertFalse(os.path.isfile('/lfs/s2048'))
    
    def test_ls(self):
        file_names = []
        for i in range(0,10):
            file_names.append('s' + str(i))
            self.assertTrue(subprocess.call(['touch','/lfs/s'+str(i)]) == 0 ,'Creating file with touch failed')
        output = subprocess.check_output(['ls','/lfs'])
        output = output.decode('utf-8')
        files_found = output.split('\n')
        for f in file_names:
            self.assertTrue(f in files_found,'File was not found by ls command')

    @unittest.expectedFailure
    def test_mkdir(self):
        self.assertEqual(subprocess.call(['mkdir','test_dir'], 0,'Successfully made directory'))
        dir_exists = os.path.isdir('/lfs/test_dir')
        self.assertTrue(dir_exists)
        if(dir_exists):
            os.rmdir('/lfs/test_dir')

    def test_cmp_equal(self):
        f = open('/lfs/s1','w+')
        f.write('test')
        f.close()
        f = open('/lfs/s2','w+')
        f.write('test')
        f.close()
        output = subprocess.call(['cmp','/lfs/s1','/lfs/s2'])
        self.assertEqual(output,0,'Return from cmp was: ' + str(output) + ' not 0')

    def test_cmp_not_equal(self):
        f = open('/lfs/s2','w+')
        f.write('test')
        f.close()
        output = subprocess.call(['cmp','/lfs/s1','/lfs/s2'])
        self.assertTrue(output,0,'Return from cmp was: '+str(output) + ' not 0')
    
    @unittest.expectedFailure
    def test_chmod(self):
        output = subprocess.call(['chmod','+x','/lfs/s1'])
        file_can_execute = os.access('/lfs/s1',os.X_OK)
        self.assertTrue(file_can_execute,'File was seen as executable after chmod, chmod failure expected')
    
    def test_df(self):
        output = subprocess.check_output(['df'])
        output = output.decode('utf-8')
        lines = output.split('\n') 
        total_mem = -1 
        partition = ''
        for line in lines:
            if re.search('LibrarianFS',line):
                words = line.split(' ')
                for word in words:
                    if word == '':
                        words.remove(word)
                total_mem = words[1]
                partition = words[len(words)-1]
        self.assertFalse(total_mem == -1,'Never found a value for the amount of memory available')
        self.assertEqual(mem_per_node * num_nodes * (1024**3)/1000,int(total_mem)) 
        self.assettEqual(partition,'/lfs')

class TestCMD_du(unittest.TestCase): 
    def setUp(self):
        clear_lfs()
        f = open("/lfs/s1",'w+')
        f.close()
    
    def tearDown(self):
        zero_silent_remove('/lfs/s1')
        clear_lfs()

    def test_du_size(self):
        output = subprocess.check_output(['du','/lfs'])
        output = output.decode('utf-8')
        size = output.split('\t')[0]
        self.assertEqual(int(size),0,'Output size of du command did not match predicted value')
    
    def test_du_apparent_size(self):
        output = subprocess.check_output(['du','/lfs','--apparent-size'])
        output = output.decode('utf-8')
        size = output.split('\t')[0]
        self.assertEqual(int(size),4,'Output size of du command did not match predicted value')

    def test_du_truncated_size(self): 
        self.assertEqual(subprocess.call(['truncate','-s1K','/lfs/s1']),0,'Truncate for du test failed')
        output = subprocess.check_output(['du','/lfs',])
        output = output.decode('utf-8')
        size = output.split('\t')[0]
        self.assertEqual(int(size),0,'Output size of du command did not match predicted value')
        #This wait has been added to prevent breaks when tests are run as a suite
        time.sleep(1)

    def test_du_apparent_truncated_size(self):
        self.assertEqual(subprocess.call(['truncate','-s1K','/lfs/s1']),0,'Truncate for du test failed')
        output = subprocess.check_output(['du','/lfs','--apparent-size'])
        output = output.decode('utf-8')
        size = output.split('\t')[0]
        self.assertEqual(int(size),5,'Output size of du command did not match predicted value')
 
    def test_du_written_size(self):
        f = open('/lfs/s1','w+')
        for i in range(0,1000):
            f.write(chr(random.randint(32,128)))
        f.close()
        output = subprocess.check_output(['du','/lfs'])
        output = output.decode('utf-8')
        size = output.split('\t')[0]
        self.assertEqual(int(size),0,'Output size of du command did not match predicted value')
    
    def test_du_apparent_written_size(self):
        f = open('/lfs/s1','w+')
        for i in range(0,1000):
            f.write(chr(random.randint(32,128)))
        f.close()
        output = subprocess.check_output(['du','/lfs','--apparent-size'])
        output = output.decode('utf-8')
        size = output.split('\t')[0]
        self.assertEqual(int(size),5,'Output size of du command did not match predicted value')


# In an LFS partition the tar command allows a user to zip multiples
# shelves worth of files together and put them all on one consolidated shelf.
# For small text files this has no data loss, however larger things will fail to tar or lose data

class TestCMD_tar(unittest.TestCase):
    def setUp(self):
        clear_lfs()

    def tearDown(self): 
        clear_lfs()

    def test_create_archive(self):
        for i in range(1,5):
            f = open('/lfs/s' + str(i),'w+')
            f.close() 
        self.assertEqual(subprocess.call(['tar','-cf','/lfs/archive.tar','/lfs/s1','/lfs/s2','/lfs/s3','/lfs/s4']),0)
        self.assertTrue(os.path.isfile('/lfs/archive.tar'))
    
    def test_extract_archive(self):
        for i in range(1,5):
            f = open('/lfs/s' + str(i),'w+')
            f.close() 
        self.assertEqual(subprocess.call(['tar','-cf','/lfs/archive.tar','/lfs/s1','/lfs/s2','/lfs/s3','/lfs/s4']),0)
        self.assertTrue(os.path.isfile('/lfs/archive.tar'))
        for i in range(1,5):
            os.remove('/lfs/s' + str(i))
        
        subprocess.call(['tar','-xf','/lfs/archive.tar','-C','/lfs','--strip-components=1'])
        for i in range(1,5):
            self.assertTrue(os.path.isfile('/lfs/s' + str(i)))
    
    def test_extract_foreign_archive(self):
        for i in range(1,5):
            f = open('/tmp/s' + str(i),'w+')
            f.close()
        self.assertEqual(subprocess.call(['tar','-cf','/tmp/archive.tar','/tmp/s1','/tmp/s2','/tmp/s3','/tmp/s4']),0)
        self.assertEqual(subprocess.call(['cp','/tmp/archive.tar','/lfs/archive.tar']),0)
        self.assertTrue(os.path.isfile('/lfs/archive.tar'))
        subprocess.call(['tar','-xf','/lfs/archive.tar','-C','/lfs','--strip-components=1']) 
        for i in range(1,5):
            self.assertTrue(os.path.isfile('/lfs/s' + str(i)))
            silent_remove('/tmp/s' + str(i))
        silent_remove('/tmp/archive.tar')
    
    def test_data_integrity(self):
        for i in range(1,5):
            f = open('/tmp/s' + str(i),'w+')
            f.write('Test')
            f.close() 
        self.assertEqual(subprocess.call(['tar','-cf','/tmp/archive.tar','/tmp/s1','/tmp/s2','/tmp/s3','/tmp/s4']),0)
        self.assertEqual(subprocess.call(['cp','/tmp/archive.tar','/lfs/archive.tar']),0)
        self.assertTrue(os.path.isfile('/lfs/archive.tar'))
        subprocess.call(['tar','-xf','/lfs/archive.tar','-C','/lfs','--strip-components=1'])
        for i in range(1,5):
            self.assertTrue(os.path.isfile('/lfs/s' + str(i)))
            self.assertTrue(filecmp.cmp('/lfs/s' + str(i),'/tmp/s' + str(i)))
            silent_remove('/tmp/s' + str(i))
        silent_remove('/tmp/archive.tar')
    
    @unittest.skipIf(skip_high_mem_use_tests, 'Skipping high memory usage test') 
    def test_large_data_integrity(self):
        for i in range(1,5):
            f = open('/tmp/s' + str(i),'w+')
            f.write('Test')
            f.close() 
            subprocess.call(['truncate','-s3G','/lfs/s' + str(i)])
        self.assertEqual(subprocess.call(['tar','-cf','/tmp/archive.tar','/tmp/s1','/tmp/s2','/tmp/s3','/tmp/s4']),0)
        self.assertEqual(subprocess.call(['cp','/tmp/archive.tar','/lfs/archive.tar']),0)
        self.assertTrue(os.path.isfile('/lfs/archive.tar'))
        subprocess.call(['tar','-xf','/lfs/archive.tar','-C','/lfs','--strip-components=1'])
        for i in range(1,5):
            self.assertTrue(os.path.isfile('/lfs/s' + str(i)))
            self.assertTrue(filecmp.cmp('/lfs/s' + str(i),'/tmp/s' + str(i)))
            silent_remove('/tmp/s' + str(i))
        silent_remove('/tmp/archive.tar')
 
class TestCMD_rename(unittest.TestCase):
    def setUp(self):
        clear_lfs()
        f = open('/lfs/s1.txt','w+')
        f_two = open('/lfs/s2.txt','w+')
        f.close()
        f_two.close()
    
    def tearDown(self):
        zero_silent_remove('/lfs/s1')
        clear_lfs()

    def test_rename_remove_extension(self):
        #os.system is deprecated but is used here because the subprocess equivalent was not working 
        os.system('rename \'s/\\.txt$//\' /lfs/*.txt')
        self.assertTrue(os.path.isfile('/lfs/s1'))
        self.assertFalse(os.path.isfile('/lfs/s1.txt'))
        self.assertTrue(os.path.isfile('/lfs/s2'))
        self.assertFalse(os.path.isfile('/lfs/s2.txt'))

#Additional Flags need to be tested
class TestCMD_uname(unittest.TestCase):
    def test_uname(self):
        output = subprocess.check_output(['uname'])
        output=output.decode('utf-8')
        self.assertEqual(output,'Linux\n')

if __name__ == '__main__':
	unittest.main()
