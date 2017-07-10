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

import unittest
import subprocess
import os

print_progress = False
print_debug = False
run_all_tests = False

def print_prog(arg):
	if print_progress:
		print (arg)
def print_debug(arg):
	if print_debug:
		print (arg)
#
# These tests are based upon the standard that in order for a a test to be
# Considered "Done" or "Useable" it must be able to run on the system when
# the /lfs/ partition is not yet fused. For this reason tests that fail on
# a fused /lfs partition are true failures and should be noted. The tests
# should be run as root or with sudo.
#

# These tests are for common shell file commands
class Test_Basic(unittest.TestCase):
	def setUp(self):
		file = open("/lfs/s1",'w')
		file.close()
	def tearDown(self):
		if os.path.isfile('/lfs/s1'):
			os.remove("/lfs/s1")
	def test_shelf_create(self):
		print_prog("Test: create shelf - Python")
		self.assertTrue(os.path.isfile('/lfs/s1'),'File was not found at expected location')
	def test_shelf_remove(self):
		print_prog("Test: remove shelf - Python")
		output = os.remove("/lfs/s1")
		self.assertFalse(os.path.isfile('/lfs/s1'),'File failed to be removed - Python')
	def test_shelf_create_bash(self):
		print_prog("Test: create shelf - Bash ")
		output = subprocess.call(['touch','/lfs/s2'])
		file_exists = os.path.isfile('/lfs/s2')
		os.remove('/lfs/s2')
		self.assertTrue(file_exists,'shelf failed to be created with touch')
	def test_shelf_remove_bash(self):
		print_prog("Test: remove shelf - Bash")
		output = subprocess.call(['rm','/lfs/s1'])
		self.assertFalse(os.path.isfile('/lfs/s1'),'shelf failed to be removed - Bash')
	@unittest.skip("Test Errors on fused lfs partition, works otherwise. Throws error instead of failure")
	def test_shelf_write(self):
		print_prog("Test: write to shelf")
		file = open('/lfs/s1','w+')
		file.write("Hello World!")
		file.close()
		file = open('/lfs/s1','r')
		file_data = file.read()
		file.close()
		self.assertTrue(file_data == 'Hello World!','File message did not equal expected message')
	def test_shelf_truncate(self):
		print_prog("Test: truncate shelf")
		output = subprocess.call(['truncate', '-s4G', '/lfs/s1'])
		file_size = os.path.getsize('/lfs/s1')
		subprocess.call(['truncate','-s0G','/lfs/s1'])
		self.assertTrue(file_size == 4294967296,'File failed to be truncated to proper size')
	def test_shelf_cp(self):
		print_prog("Test: cp command")
		output = subprocess.call(['cp','/lfs/s1','/lfs/s2'])
		file_copied = os.path.isfile('/lfs/s2')
		os.remove('/lfs/s2')
		self.assertTrue(file_copied, 'file failed to be copied in lfs')
	def test_shelf_cp_offshelf(self):
		print_prog("Test: cp command to offshelf")
		output = subprocess.call(['cp','/lfs/s1','/tmp/s1'])
		file_copied = os.path.isfile('/tmp/s1')
		os.remove('/tmp/s1')
		self.assertTrue(file_copied,'file failed to be copied to tmp')
	def test_shelf_rename(self):
		print_prog("Test: mv command (used to rename)")
		output = subprocess.call(['mv','/lfs/s1','/lfs/s2'])
		file_renamed = os.path.isfile('/lfs/s2')
		subprocess.call(['mv','/lfs/s2','/lfs/s1'])
		self.assertTrue(file_renamed,'File failed to be renamed using mv')
	def test_shelf_mv_offshelf(self):
		print_prog("Test: mv command (used to move)")
		output = subprocess.call(['mv','/lfs/s1','/tmp/s1'])
		file_moved = os.path.isfile('/tmp/s1')
		subprocess.call(['mv','/tmp/s1','/lfs/s1'])
		file_moved2 = os.path.isfile('/lfs/s1')
		self.assertTrue(file_moved and file_moved2, "File failed to be moved to and from destination")
	#@unittest.expectedFailure
	def test_shelf_mkdir(self):
		print_prog("Test: creating a Directory")
		output1 = subprocess.call(['mkdir','/lfs/test_dir'])
		file_exists = os.path.isdir("/lfs/test_dir")
		if file_exists:
			os.rmdir('/lfs/test_dir')
		#self.assertTrue(file_exists,'Test: Create directory, expected to fail')
		self.assertTrue(file_exists,'Failed to create directory')
	def test_shelf_double_mkdir(self):
		print_prog("Test: creating a directory with a directory inside it in one command")
		output1 = subprocess.call(['mkdir','/lfs/dir/subdir'])
		file_exists = os.path.isdir("/lfs/dir/subdir")
		if file_exists:
			os.rmdir('/lfs/dir/subdir')
			os.rmdir('/lfs/dir')
		self.assertTrue(file_exists,'Failed to create directory and/or subdirectory')
	def test_shelf_create_subdir(self):
		print_prog("Test: creating a file in a directory")
		output1 = subprocess.call(['mkdir','/lfs/test_dir'])
		output2 = subprocess.call(['touch','/lfs/test_dir/test_file'])
		file_exists = os.path.isfile('/lfs/test_dir/test_file')
		if file_exists:
			os.remove("/lfs/test_dir/test_file")
			os.rmdir('/lfs/test_dir')
		file_exists_wrong_dir = os.path.isfile('/lfs/test_file')
		if file_exists_wrong_dir:
			os.remove("/lfs/test_file")
			os.rmdir('/lfs/test_dir')
		self.assertFalse(file_exists_wrong_dir, "File was created, but not in correct directory")
		self.assertTrue(file_exists, "File failed to be created in subdirectory, mkdir suspected as cause of failure")
	def test_shelf_mv_subdir(self):
		print_prog("Test: moving a file from /lfs to a subdirectory")
		output1 = subprocess.call(['mkdir','/lfs/test_dir'])
		output2 = subprocess.call(['mv','/lfs/s1','/lfs/test_dir/s1'])
		file_moved = os.path.isfile('/lfs/test_dir/s1')
		subprocess.call(['mv','/lfs/test_dir/s1','/lfs/s1'])
		file_moved2 = os.path.isfile('/lfs/s1')
		self.assertTrue(file_moved and file_moved2, "File failed to be moved to and from subdirectory")
	def test_shelf_rmdir(self):
		print_prog("Test: Remove an empty directory")
		output1 = subprocess.call(['mkdir','/lfs/test_dir'])
		file_exists = os.path.isdir("/lfs/test_dir")
		self.assertTrue(file_exists, "Failed to create directory for rmdir to remove, reported failure or success of rmdir may not be meaningful")
		output2 = subprocess.call(['rmdir','/lfs/test_dir'])
		file_exists = os.path.isdir('/lfs/test_dir')
		self.assertFalse(file_exists, "Failed to remove directory, it may never have been created")
	def test_shelf_ls(self):
		print_prog("Test: ls command")
		output = subprocess.call('ls')
		self.assertTrue(output == 0)
	@unittest.expectedFailure
	def test_shelf_chmod(self):
		print_prog("Test: chmod command")
		output = subprocess.call(['chmod','+x','/lfs/s1'])
		file_can_execute = os.access('/lfs/s1',os.X_OK)
		self.assertTrue(file_can_execute,'File was failed to be seen as executable after chmod, chmod failure expected')

if __name__ == '__main__':
	unittest.main()
