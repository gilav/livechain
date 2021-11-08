import os, sys, traceback
import logging
import myLoggerConfig


class toto():

	def __init__(self):
		self.logger = myLoggerConfig.applyLoggingLevel(self.__class__.__name__, True)

	def start(self):
		self.logger.info("started")

if __name__ == "__main__":
	try:
		toto = toto()
		toto.start()
		sys.exit(0)
	except Exception as e:
		exc_type, exc_obj, exc_tb = sys.exc_info()
		print(f"Error {exc_type} {exc_obj}")
		traceback.print_exc(file=sys.stdout)
		sys.exit(2)
