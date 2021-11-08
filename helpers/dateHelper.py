import time
from datetime import datetime, timedelta
from typing import Optional

DEFAULT_DATE_PATTERN = "%Y-%m-%d %H:%M:%S"
DEFAULT_DATE_PATTERN_MSEC = "%Y-%m-%dT%H:%M:%S.%fZ"


#
# return a float
#
def nowSecs(delta: Optional[timedelta] = None):
	if delta is None:
		return time.time()
	else:
		return (datetime.fromtimestamp(time.time()) + delta).timestamp()

#
# return a float
#
def secsBeginDayFromSecs(t)->float:
	d = datetime.fromtimestamp(t)
	d2 = datetime(d.year, d.month, d.day, 0, 0)
	return d2.timestamp()

#
# return a float
#
def secsEndDayFromSecs(t)->float:
	d = datetime.fromtimestamp(t)
	d2 = datetime(d.year, d.month, d.day, 23, 59, 59)
	return d2.timestamp()

#
# return a date string
#
def beginDayFromSecs(t, pattern=DEFAULT_DATE_PATTERN)->str:
	d = datetime.fromtimestamp(t)
	d2 = datetime(d.year, d.month, d.day, 0, 0)
	return d2.strftime(pattern)

#
# return a date string
#
def endDayFromSecs(t, pattern=DEFAULT_DATE_PATTERN)->str:
	d = datetime.fromtimestamp(t)
	d2 = datetime(d.year, d.month, d.day, 23, 59, 59)
	return d2.strftime(pattern)


#
# return a date string
#
def dateNow(pattern=DEFAULT_DATE_PATTERN)->str:
	d = datetime.fromtimestamp(time.time())
	return d.strftime(pattern)

#
# return a date string from epoch sec
#
def dateFromSecs(t, pattern=DEFAULT_DATE_PATTERN)->str:
	d = datetime.fromtimestamp(t)
	return d.strftime(pattern)

#
# return sec from epoch from a date string
#
def secsFromDate(s, pattern=DEFAULT_DATE_PATTERN)->float:
	d = datetime.strptime(s, pattern)
	return d.timestamp()


#
# return the now date in float seconds
# dateNowSecs('%Y-%m-%dT%H:%M:%SZ')=1444911431.0
#
def dateNowSecs()->float:
	d = datetime.fromtimestamp(time.time())
	return time.mktime(d.timetuple())