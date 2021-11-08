import os, sys
from helpers.stdCapture import Capture


def toto():
	print("HELLO")


# keep old stds
if 1==1:
	old_stdout = sys.stdout
	old_stderr = sys.stderr
	sys.stdout = Capture(sys.stdout, None, True)
	sys.stderr = Capture(sys.stderr, None, True)
toto()
if 1==1:
	sys.stdout = old_stdout
	sys.stderr = old_stderr