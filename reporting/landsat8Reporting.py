import time
from typing import Optional
from helpers import dateHelper
import myLoggerConfig
from constants import *

class Landsat8Reporting:

	def __init__(self, m):
		# logger
		self.logger = myLoggerConfig.applyLoggingLevel(self.__class__.__name__, True)
		self.manager = m

	#
	#
	#
	def dailyReport(self, aDate: Optional[str]=dateHelper.dateNow())-> {}:
		self.logger.info(f"dailyReport for date: {aDate}")
		someTime = dateHelper.secsFromDate(aDate)
		bDaySecs = dateHelper.secsBeginDayFromSecs(someTime)
		self.logger.info(f"dailyReport bDaySecs: {bDaySecs}")
		eDaySecs = dateHelper.secsEndDayFromSecs(someTime)
		self.logger.info(f"dailyReport eDaySecs: {eDaySecs}")
		filters=[('at', 'ge', bDaySecs), ('at', 'lt', eDaySecs)]
		return self.manager.getInputs(format=FORMAT_JSON, filters=filters)