#
#
#
import sys, time, traceback
from helpers import dateHelper
from datetime import datetime, timedelta




if __name__ == "__main__":
	try:

		f = dateHelper.nowSecs()
		print(f" nowSecs: {f}")

		f = dateHelper.nowSecs(delta=timedelta(days=-1))
		print(f" nowSecs -1 day: {f}")

		s = dateHelper.dateNow(pattern=dateHelper.DEFAULT_DATE_PATTERN_MSEC)
		print(f" now: {s}")

		someTime=dateHelper.nowSecs()#time.time()
		print(f" time: {someTime}; type: {type(someTime)}")

		s = dateHelper.dateFromSecs(someTime, pattern=dateHelper.DEFAULT_DATE_PATTERN_MSEC)
		print(f" dateFromSec: {s}")

		s2 = dateHelper.secsFromDate(s, pattern=dateHelper.DEFAULT_DATE_PATTERN_MSEC)
		print(f" secsFromDate: {s2}")

		s3 = dateHelper.beginDayFromSecs(s2, pattern=dateHelper.DEFAULT_DATE_PATTERN_MSEC)
		print(f" secsBeginDay: {s3}")

		s3 = dateHelper.endDayFromSecs(s2, pattern=dateHelper.DEFAULT_DATE_PATTERN_MSEC)
		print(f" secsEndDay: {s3}")

		s4 = dateHelper.secsBeginDayFromSecs(someTime)
		print(f" secsBeginDayFromSecs: {s4}")

		s4 = dateHelper.secsEndDayFromSecs(someTime)
		print(f" secsEndDayFromSecs: {s4}")

		sys.exit(0)
	except Exception as e:
		exc_type, exc_obj, exc_tb = sys.exc_info()
		print(f"Error {exc_type} {exc_obj}")
		traceback.print_exc(file=sys.stdout)
		sys.exit(2)
