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

import os
import subprocess

# These are common functions that may be used from other scripts


#remove things from system cleanly
def silent_remove(filename):
    try:
        os.remove(filename)
    except OSError:
        pass

#Alternate form of silent remove that truncates to 0 first (Use with TMAS)
def zero_silent_remove(filename):
    subprocess.call(['truncate','-s0G',filename])
    silent_remove(filename)

#function that returns the absolute path to the tm-librarian folder
def get_librarian_path():
    real_path = os.path.dirname(os.path.realpath(__file__))
    librarian_path = real_path[:real_path.rfind('/')]
    return librarian_path
def clear_lfs():
    for f in os.listdir('/lfs'):
        zero_silent_remove('/lfs/' + f)
