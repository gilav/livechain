#
#
#
import time
from typing import Callable, List, Optional, Dict, Union, Tuple
from process.iProcessor import iProcessor


class InboxDummyHandler(iProcessor):


	def process(self, **kwargs):
		if 'event' in kwargs:
			e = kwargs['event']
			rec={'src_path': e.src_path, 'at': time.time()}
			print(f"{self.__class__.__name__} process watchdog item; src_path: {e.src_path}")