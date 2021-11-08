#
#
#
import abc
from constants import *
from typing import Optional

class iStatus(metaclass=abc.ABCMeta):

	#
	#
	#
	@abc.abstractmethod
	def status(self, format: Optional[str]=FORMAT_TEXT):
		raise Exception("must define 'status' to use this base class")
