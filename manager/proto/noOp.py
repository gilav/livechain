#
#
#
import os,sys,traceback,time
import logging
#
import constants
from process.iProcessor import iProcessor
from api.baseProtocol import Protocol
from api.psProtocol import PsProtocol
from manager.proto.op import Op
import myLoggerConfig

#
#
#
class NoOp(iProcessor, Op):
	logger: logging
	def __init__(self):
		Op.__init__(self)
		self.logger = myLoggerConfig.applyLoggingLevel(self.__class__.__name__, True)

	#
	# Processor interface
	#
	def process(self, **kwargs):
		self.logger.debug(f" process kwargs: {kwargs}")