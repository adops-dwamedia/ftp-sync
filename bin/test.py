import subprocess
import sys
try:subprocess.check_output("ls adfas", shell=True)
except subprocess.CalledProcessError: sys.exit(1)


