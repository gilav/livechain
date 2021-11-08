#
#
#
import time
import json
from typing import Callable, List, Optional, Dict, Union, Tuple
from persistqueue import FIFOSQLiteQueue
#
from constants import *
from process.iProcessor import iProcessor
import myLoggerConfig
from aGlobal import aGlobalSemaphore

COMPONENT = 'ItemsQueue'


class ItemsQueue(iProcessor):

	def __init__(self, my_name, apath: str):
		super().__init__()
		self.myMame = my_name
		self.dbPath = apath
		self.logger = myLoggerConfig.applyLoggingLevel(self.__class__.__name__, True)
		self.logger.info(f"self.dbPath:{self.dbPath}")
		self.queue = FIFOSQLiteQueue(path=self.dbPath, multithreading=True)
		self.done = 0

	def put(self, item):
		return self.queue.put(item, block=True)

	def get(self):
		return self.queue.get()

	def size(self):
		return self.queue.size

	def done(self):
		return self.done

	#
	#
	#
	def status(self, format: Optional[str] = FORMAT_TEXT):
		if format == FORMAT_TEXT:
			return f"{self.__class__.__name__} '{self.myMame}' size: {self.queue.size}; done: {self.done}"
		else:
			return json.dumps(
				{'id': COMPONENT,
				 'totals': {
					 'size': self.queue.size,
					 'done': self.done
				 }
				 }
			)

	def getDbPath(self):
		return self.dbPath

	def process(self, **kwargs):
		if 'event' in kwargs:
			e = kwargs['event'] # ItemContext
			rec = {'src_path': e.getSrcPath(), 'at': time.time()}
			self.logger.info(f"process src_path: {e.getSrcPath()}")
			self.queue.put(rec)
			self.done += 1
			#
			aGlobalSemaphore.set({'type': 'status'})
