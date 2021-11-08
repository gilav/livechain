import sys, time, traceback
from api.baseProtocol import Protocol


class PsProtocol(Protocol):
	CALLER='caller'
	TARGET='target'
	HANDLER='handler'
	REF={'version': '0xa001', 'caller': None, 'target': None, 'req': None, 'resp': None, 'handler': None, 'status': None, 'info': None}

	def __init__(self):
		Protocol.__init__(self)
		self.version='0xa001'
		self.caller=None
		self.target=None
		self.handler=None

	def clear(self):
		Protocol.clear(self)
		self.caller=None
		self.target=None
		self.handler=None

	def fromDict(self, aDict:{}):
		if not self.verifyProto(self, aDict):
			#raise Exception(f"invalid protocol: {aDict}")
			raise Exception(f"invalid protocol: {aDict}")
		Protocol.fromDict(self, aDict)
		self.caller=aDict[self.CALLER]
		self.target=aDict[self.TARGET]
		#self.req=aDict[self.REQ]
		##elf.resp=aDict[self.RESP]
		self.handler=aDict[self.HANDLER]
		#self.status=aDict[self.STATUS]
		#self.info=aDict[self.INFO]

	def toDict(self)->{}:
		aDict=Protocol.toDict(self)
		aDict[self.VERSION]=self.version
		aDict[self.CALLER]=self.caller
		aDict[self.TARGET]=self.target
		aDict[self.HANDLER]=self.handler
		return aDict
		"""return {self.VERSION:self.version,
				self.CALLER:self.caller,
				self.TARGET:self.target,
				self.REQ:self.req,
				self.RESP:self.resp,
				self.HANDLER:self.handler,
				self.STATUS:self.status,
				self.INFO:self.info
				}"""