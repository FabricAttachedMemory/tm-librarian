#!/usr/bin/python3 -tt 

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
#
# This Script tests all .ini and .json files in the tm-librarian folder 
# it will automatically find and create database files for any found config
# files.  
#
# After running book_register against all files this script will then try them
# with the librarian itself. Since this system is firing up multiple instances 
# of the librarian it may take a few minutes to completely run through its 
# opperations.
#

import unittest
import subprocess
import os
import time
import signal 
import re
import sys

#List of test that are expected to fail all others should succeed
known_failures = ['tmcf-drew.json','tmcf-bristol.json']


def silent_remove(filename):
    try:
        os.remove(filename)
    except OSError:
        pass
def load_tests_from_dir(directory):
    if not directory.endswith('/'):
        directory = directory + '/'
    print("Loading tests from Dir: " + directory)
    for root, dirs, files in os.walk(directory):
        for name in files:
            if os.path.isfile(directory + name):
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
            loading_value = param[param.rfind('/')+1:]
            test = testcase_class(name,param=param) 
            suite.addTest(test)
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
        __unittest_expecting_failure__ = True
        if not self.param == None:
            db_file_name = (os.path.splitext(self.param)[0]) + '.db'
            ret = subprocess.call([librarian_path+'/book_register.py','-d',db_file_name,self.param])
            self.assertTrue(os.path.isfile(db_file_name),'The Database File: ' +db_file_name + ' was not created' )
            self.assertEqual(ret,0,'Book Register returned a non-zero value')
        
    
    
    #Rebuild a new database and try this file with the librarian
    def test_start_librarian(self):
        if not self.param == None:
            db_file_name = (os.path.splitext(self.param)[0]) + '.db'
            ret = subprocess.call([librarian_path+'/book_register.py','-d',db_file_name,self.param])
            if  ret == 0:  
                output = open('out.txt','w') 
                print("Starting Librarian with db_file: " + db_file_name)
                proc=subprocess.Popen([librarian_path+'/librarian.py','--db_file',db_file_name,'--verbose','3','--port','5799'],universal_newlines=True,stdout=output,creationflags=0)
                time.sleep(10)#set to give the test 10 seconds before checking on the librarian could be more or less
                print("Killing Librarian")
                os.kill(proc.pid,signal.SIGINT)# kills the librarian using a simulated keyboard interupt
                output.close()
                sys.stdout.flush()
            
                time.sleep(1) # allocate time for the buffer to flush
            
                check = open('out.txt','r')
                lib_started = False
                if 'Waiting for request...' in open('out.txt').read():
                    lib_started = True
                self.assertTrue(lib_started,'Failed to confirm proper start of librarian for db_file: ' + db_file_name)
                check.close()
            else:
                self.skipTest('\nSkipped Starting Librarian with '+db_file_name+'  because the .db file was not properly created\n')

suite = unittest.TestSuite()

#Load tests from files
for i in range(1,len(sys.argv)):
    if os.path.isdir(sys.argv[i]): 
        load_tests_from_dir(sys.argv[i])


#load tests from tm-librarian/configfiles dir, assumes that script is in tests dir
real_path = os.path.dirname(os.path.realpath(__file__))
librarian_path = real_path[:real_path.rfind('/')]
load_tests_from_dir(librarian_path+'/configfiles')
unittest.TextTestRunner(verbosity=0).run(suite)
print('Currently Known Failures:')
for item in known_failures:
    print(item)
