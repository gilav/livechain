#
#
#
import redis
import logging
from abc import ABC, abstractmethod



class iRedisPublisher(ABC):
	channel=None

	#
	#
	#
	def publish(self, data):
		print(f" publish on channel:{self.channel}; data:{data}")
		self.redis.publish(self.channels, data)

	#
	#
	#
	#@abstractmethod
	#def register(self, host:str = 'localhost', post:int=6379, channel:str='iRedisPublisher'):
	#	raise Exception("abstract")

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