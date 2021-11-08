import sys, time, traceback
from abc import ABC, abstractmethod

class Protocol():
	#
	STATUS_OK="200"
	STATUS_FAIL="201"
	STATUS_INVALID_REQUEST="300"
	#
	VERSION='version'
	REQ='req'
	RESP='resp'
	ACTION='action'
	STATUS='status'
	INFO='info'
	#
	REF={'version': '0xa001', 'caller': None, 'target': None, 'req': None, 'resp': None, 'handler': None, 'status': None, 'info': None}

	def __init__(self):
		self.version='0xa000'
		#self.caller=None # caller id
		#self.target=None # target id
		self.req=None # request
		self.resp=None # response
		#self.handler:None # what handle the call
		self.action=None # what to do
		self.status=None # status
		self.info=None # status

	def __repr__(self)->str:
		return f"{self.toDict()}"

	def clear(self):
		self.req=None # request
		self.resp=None # response
		self.action=None # what to do
		self.status=None # status
		self.info=None # status

	def verifyProto(self, ref, m):
		if self.VERSION not in m:
			return False
		if len(m)!=len(ref.REF):
			return False
		if m[self.VERSION]==ref.version:
			#print("proto version ok")
			return True

	#@abstractmethod
	def fromDict(self, aDict:{}):
		#raise Exception("abstract")
		self.req=aDict[self.REQ]
		self.resp=aDict[self.RESP]
		self.status=aDict[self.STATUS]
		self.info=aDict[self.INFO]

	#def fromDict_impl(self, aDict:{}):
	#	raise Exception("abstract")

	#@abstractmethod
	def toDict(self)->{}:
		#raise Exception("abstract")
		return {
				self.REQ:self.req,
				self.RESP:self.resp,
				self.STATUS:self.status,
				self.INFO:self.info
				}

	#def toDict_impl(self):
	#	raise Exception("abstract")