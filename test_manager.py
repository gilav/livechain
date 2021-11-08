#
#
#
import sys, traceback
#from db.databaseUtils import DbInit
from manager.liveManager import LiveManager
import logging

databaseUrl='sqlite:///db-manager.sqlite3'
aName='livechain-manager'


def aCallback(item):
	print(f"aCallback function called with item: {item}")


def test():
	m = LiveManager()
	m.set_callback(aCallback)
	print("test manager running...")




if __name__ == "__main__":
	try:
		#logging.basicConfig(level=logging.DEBUG,
		#					format='%(asctime)s - %(message)s',
		#					datefmt='%Y-%m-%d %H:%M:%S')
		test()
		sys.exit(0)
	except Exception as e:
		exc_type, exc_obj, exc_tb = sys.exc_info()
		print(f"Error {exc_type} {exc_obj}")
		traceback.print_exc(file=sys.stdout)
		sys.exit(2)
