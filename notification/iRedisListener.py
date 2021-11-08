#
#
#
import redis
import logging
from abc import ABC, abstractmethod



class iRedisListener(ABC):
	channel=None

	#
	#
	#
	@abstractmethod
	def register(self, host:str = 'localhost', post:int=6379, channel:str='iRedisListener'):
		raise Exception("abstract")

	#
	#
	#
	def getChannel(self)->str:
		return self.channel


	#
	#
	#
	def close(self):
		self.redis.shutdown()