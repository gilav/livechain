import sys, traceback
import time
from datetime import datetime
from io import StringIO
from typing import Callable, List, Optional, Dict, Union, Tuple
import threading
#
import configuration as configuration
import myLoggerConfig

class Maintenance(threading.Thread):

	def __init__(self, app):
		threading.Thread.__init__(self)
		self.logger = myLoggerConfig.applyLoggingLevel(self.__class__.__name__, True)
		self.app=app
		self.running:bool = False


	def my_start(self):
		self.running=True
		self.start()

	def my_stop(self):
		self.running=True

	def run(self):
		while self.running:
			time.sleep(10)
			for aMission in self.app.get_config().missions:
				inboxPath = self.app.get_config().missionConfig[aMission][configuration.SETTING_TMPSPACE]
				self.clean_tmp_space(aMission, inboxPath)

	#
	#
	#
	def clean_tmp_space(self, aMission, aPath:str, delayTime:float):
		self.logger.debug(f"clean tmp space for mission: {aMission} if older than {delayTime}")

