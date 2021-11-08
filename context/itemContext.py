from context.contextDict import ContextDict
from constants import *

class ItemContext(ContextDict):
	""" class for file item """

	def __init__(self, aPath):
		ContextDict.__init__(self)
		#self.src_path = aPath
		self[INPUT_ITEM_SRC_PATH] = aPath

	def getSrcPath(self):
		return self[INPUT_ITEM_SRC_PATH]#self.src_path