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
