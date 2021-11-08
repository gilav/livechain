from context.contextDict import ContextDict
from constants import *

class WebContext(ContextDict):
	""" class for web item """

	def __init__(self, bits):
		ContextDict.__init__(self)
		self[WEB_REQUEST_BITS] = bits

	def getPath(self):
		return self[WEB_REQUEST_BITS].path

	def getQuery(self):
		return self[WEB_REQUEST_BITS].query