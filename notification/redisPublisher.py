#
#
#
import os,sys,traceback,time
import redis
import logging
from process.iProcessor import iProcessor
from abc import ABC, abstractmethod
import myLoggerConfig

#
debug=False

class RedisPublisher(ABC, iProcessor):

	def __init__(self, h:str='localhost', p:int=6379):
		self.logger = myLoggerConfig.applyLoggingLevel(self.__class__.__name__, True)
		self.defaultChannel=self.__class__.__name__
		self.host=h
		self.port=p
		self.redis = redis.Redis(self.host, self.port)
		self.logger.info(f"__init__ at address: {self.host}:{self.port}")

	#
	#
	#
	def setDefaultChannel(self, c:str):
		self.defaultChannel=c

	#
	#
	#
	def getChannel(self, c:str)->str:
		return self.defaultChannel

	#
	#
	#
	def publish(self, channel:str, data):
		self.logger.info(f"publishing on channel:{channel}; data:{data}")
		self.redis.publish(channel, data)

	#
	#
	#
	def process(self, **kwargs):
		self.logger.info(f"process; kwargs={kwargs}")
		channel=self.defaultChannel
		if 'channel' in kwargs:
			channel=kwargs['channel']
		if 'mesg' in kwargs:
			self.publish(channel, kwargs['mesg'])


	#
	#
	#
	def close(self):
		self.redis.shutdown()