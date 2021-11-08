from typing import Callable, List, Optional, Dict, Union, Tuple
import configparser
import datetime
import os, sys, traceback
import logging
import _version

#debug = True
debug = False



config=configparser.ConfigParser()
config.read('loggerConfig.cfg')

#
version='0.1'
#
PREFIX='LOG_LEVEL__'
BASICFORMAT = '%(asctime)s - [%(levelname)s] : %(message)s'
log_level_info = {'logging.DEBUG': logging.DEBUG,
				  'logging.INFO': logging.INFO,
				  'logging.WARNING': logging.WARNING,
				  'logging.ERROR': logging.ERROR,
				  }
if debug:print(f"{config['DEFAULT']['LOG_LEVEL']}")
#
lc_log_format_date_format = '%Y-%m-%d %H:%M:%S,%f'
class MyFormatter(logging.Formatter):
	converter = datetime.datetime.fromtimestamp

	def formatTime(self, record, datefmt=None):
		ct = self.converter(record.created)
		if datefmt:
			s = ct.strftime(datefmt)
		else:
			t = ct.strftime("%Y-%m-%d %H:%M:%S")
			s = "%s,%06d" % (t, record.msecs * 1000)
		return s

#
#stdout_handler = logging.StreamHandler(sys.stdout)

def applyLoggingLevel(name:str, process=False, subName: Optional[str]=''):
	if debug:
		print(f" applyLoggingLevel to logger name: '{name}', subName: '{subName}'")
	configuredLevel=None
	try:
		configuredLevel = config['CLASSES'][f"{PREFIX}{name}"]
		if 1==1 or debug:
			print(f" ## applyLoggingLevel to logger name: '{name}', subName: '{subName}'. found custom level: {configuredLevel}")
	except:
		if 1==1 or debug:
			print(f" ## applyLoggingLevel to logger name: '{name}', subName: '{subName}'. no custom level, use: {config['DEFAULT']['LOG_LEVEL']}")
		configuredLevel = config['DEFAULT']['LOG_LEVEL']
	aLogLevel = log_level_info.get(configuredLevel, logging.ERROR)

	#handlers = []
	lc_log_format = f'{_version.__version__} %(asctime)s - [%(levelname)s] - [%(name)s] - %(message)s'
	lc_process_log_format = f'{_version.__version__} %(asctime)s - [%(levelname)s] - [%(name)s ({os.getpid()})] - %(message)s'
	stdout_handler = logging.StreamHandler(sys.stdout)
	if process:
		stdout_handler.setFormatter(MyFormatter(fmt=lc_process_log_format, datefmt=lc_log_format_date_format))
	else:
		stdout_handler.setFormatter(MyFormatter(fmt=lc_log_format, datefmt=lc_log_format_date_format))
	#handlers.append(stdout_handler)
	aLogger = logging.getLogger(f"{name}{subName}")
	aLogger.propagate=False
	if not aLogger.handlers:
		aLogger.addHandler(stdout_handler)
	aLogger.setLevel(aLogLevel)
	if debug:
		print(f" ######################################## applyLoggingLevel to logger name: {name}. set level: {configuredLevel}")
	return aLogger

def getDefaultLevel():
	return log_level_info.get(config['DEFAULT']['LOG_LEVEL'], logging.ERROR)



#
# test:
#
class aa():
	name='InboxHandler'
	def setLevel(self, l):
		print(f"class aa level set to:{l}")

def test():
	b = aa()
	applyLoggingLevel(b)

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