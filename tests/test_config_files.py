#!/usr/bin/python3

#
# This Script tests all .ini and .json files in the tm-librarian folder 
# it will automatically find and create database files for any found config
# files.  
#
# After running book_register against all files this script will then try them
# with the librarian itself.
#


from __future__ import print_function
import unittest
import subprocess
import os
import time
import signal 
import re
import sys

def silent_remove(filename):
    try:
        os.remove(filename)
    except OSError:
        pass
def load_tests_from_dir(directory):
    if not directory.endswith('/'):
        directory = directory + '/'
    for root, dirs, files in os.walk(directory):
        for name in files:
            if name.endswith('.ini') or name.endswith('.json'):
                suite.addTest(ParamaterTestClass.paramaterize(CheckConfigFiles,param=(directory+name)))

class ParamaterTestClass(unittest.TestCase):
    def __init__(self,methodName='runTest',param=None):
        super(ParamaterTestClass,self).__init__(methodName)
        self.param = param

    @staticmethod
    def paramaterize(testcase_class, param=None):
        testloader = unittest.TestLoader()
        testnames = testloader.getTestCaseNames(testcase_class)
        suite = unittest.TestSuite()
        for name in testnames:
            suite.addTest(testcase_class(name,param=param))
        return suite

class CheckConfigFiles(ParamaterTestClass): 
    
    def setUp(self):
        silent_remove('out.txt') 
        silent_remove(((os.path.splitext(str(self.param)))[0]) + '.db')

    def tearDown(self):
        silent_remove('out.txt')
        silent_remove(((os.path.splitext(str(self.param)))[0]) + '.db')
    
    #Run book register on the file 
    def test_book_register(self):
        db_file_name = os.path.splitext(self.param)[0] + '.db'
        ret = subprocess.call(['../book_register.py','-d',db_file_name,self.param])
        self.assertTrue(os.path.isfile(db_file_name),'The Database File: ' +db_file_name + ' was not created' )
        self.assertEqual(ret,0,'Book Register returned a non-zero value')
        
    #Run rebuild a new database and try this file with the librarian
    def test_start_librarian(self):
        db_file_name = os.path.splitext(self.param)[0] + '.db'
        ret = subprocess.call(['../book_register.py','-d',db_file_name,self.param])
        
        output = open('out.txt','w')
        if(os.path.isfile(db_file_name)):
            print("Starting Librarian with db_file: " + db_file_name)
            proc=subprocess.Popen(['../librarian.py','--db_file',db_file_name,'--verbose','3','--port','5799'],universal_newlines=True,stdout=output,creationflags=0)
            time.sleep(10)#set to give the test 10 seconds before checking on the librarian could be more or less
            print("Killing Librarian")
            os.kill(proc.pid,signal.SIGINT)
            output.close()
            check = open('out.txt','r')
            lib_started = False
            for line in check:
                if re.search('Waiting for request...',line):
                    lib_started = True
            self.assertTrue(lib_started,'Failed to confirm proper start of librarian for db_file: ' + db_file_name)
            check.close()

suite = unittest.TestSuite()

for i in range(2,len(sys.argv)-1):
    if(sys.argv[i].startswith('-')):
        pass
    elif os.path.isdir(sys.argv[i]):
        load_tests_from_dir(sys.argv[i])

load_tests_from_dir('../configfiles')
unittest.TextTestRunner(verbosity=0).run(suite)



