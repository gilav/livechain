#
#
#
import abc

class iProcessor(metaclass=abc.ABCMeta):

	def __init__(self):
		self.name:str=None

	def setName(self, n:str):
		self.name = n

	def getName(self)->str:
		return self.name

	@abc.abstractmethod
	def process(self, **kwargs):
		raise Exception("must define 'process' to use this base class")

