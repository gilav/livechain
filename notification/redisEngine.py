#
#
#
import os,sys,traceback,time
import threading
import redis
import logging
from typing import Callable
import json
#
import constants
from interfaces.iKillable import iKillable
#from notification.iRedisListener import iRedisListener
from process.iProcessor import iProcessor
from manager.commandHandler import CommandHandler
import os
import myLoggerConfig

DEFAULT_CHARSET='UTF-8'
BROADCAST='live-chain_*'
COMPONENT='live-chain_RedisEngine'
debug=True

#
#
#
class RedisEngine(threading.Thread, iProcessor, iKillable):
	#default_killCommand=f'{COMPONENT}_KILL'
	channel:str
	callbacks:[Callable[..., None]]
	host:str
	port:int
	redis:redis.Redis
	commandHandler:CommandHandler
	app:any

	def __init__(self, app:any, h:str='localhost', p:int=6379):
		self.logger = myLoggerConfig.applyLoggingLevel(self.__class__.__name__, True)
		threading.Thread.__init__(self)
		if id is None:
			raise Exception(f"RedisEngine cannot have None as id")

		# app dependant:
		self.app=app
		self.anUUID = self.app.get_my_id()

		# used to communicate / kill
		# use live_chain COMPONENT name
		#self.mySubscribeId:str = f"{COMPONENT}_{self.anUUID}"
		self.default_killCommand=f'{self.app.COMPONENT}_KILL'
		self.mySubscribeId:str = f"{self.app.COMPONENT}_{self.anUUID}"
		self.myKillCommand:str = f"{self.default_killCommand}_{self.anUUID}"
		self.myKillCommand:str = f"{self.app.COMPONENT}_KILL_{self.anUUID}"
		#
		self.host=h
		self.port=p
		self.redis = redis.Redis(self.host, self.port)
		self.pubsub = self.redis.pubsub()
		self.logger.info(f"__init__ at address: {self.host}:{self.port}")
		self.logger.info(f"generic kill command:{self.default_killCommand}")
		self.logger.info(f"my kill command:{self.myKillCommand}")
		self.logger.info(f"my subscribe id:{self.mySubscribeId}")
		self.logger.info(f"suscribing to channel: '{[COMPONENT, self.mySubscribeId]}'")
		self.pubsub.subscribe([COMPONENT, self.mySubscribeId])
		self.callbacks=[]
		self.commandHandler=CommandHandler(self.app.get_manager())

	#
	#
	#
	def getSubscribeId(self)->str:
		return self.mySubscribeId

	#
	#
	#
	def __repr__(self)->str:
		return f"self.__class__.__name__ name={self.name}; comp={COMPONENT}"

	#
	# Processor interface
	# - send message: args has : constants.REDIS_MESSAGE and possibly constants.REDIS_CHANNEL
	# - command: args has : constants.REDIS_COMMAND
	def process(self, **kwargs):
		if constants.REDIS_MESSAGE in kwargs:
			if constants.REDIS_MESSAGE in kwargs:
				if not constants.REDIS_CHANNEL in kwargs:
					self.logger.debug(f".process mesg: {kwargs[constants.REDIS_MESSAGE]}")
					self.redis.publish(self.channel, kwargs[constants.REDIS_MESSAGE])
				else:
					self.logger.debug(f".process mesg:{ kwargs[constants.REDIS_MESSAGE]} for channel: {kwargs[constants.REDIS_CHANNEL]}")
					self.redis.publish(kwargs[constants.REDIS_CHANNEL], kwargs[constants.REDIS_MESSAGE])
			else:
				self.logger.warning(f".process: no {kwargs[constants.REDIS_MESSAGE]} in kwargs!")
		elif constants.REDIS_COMMAND in kwargs:
			self.logger.debug(f".process command: {kwargs[constants.REDIS_COMMAND]}")

	#
	# iKillable interface
	#
	def getKillCommand(self)->str:
		return self.myKillCommand

	#
	# redis stuffs:
	#
	def close(self):
		self.logger.info(f".close")
		self.redis.shutdown()

	#
	#
	#
	def add_callback(self, c:Callable[..., None]):
		self.callbacks.append(c)

	#
	#
	#
	def remove_callback(self, c:Callable[..., None]):
		self.callbacks.remove(c)

	#
	#
	#
	def process_message(self, mesg): # process my messages
		self.logger.debug(f"process_message; mesg: {mesg}")

		# commands for manager
		payload = mesg['data'].decode('utf8')
		self.logger.debug(f"process_message; payload: {payload}; type: {type(payload)}")
		try:
			aDict = json.loads(mesg['data'])
			self.logger.debug(f"process_message get a dict: {aDict}")

			proto = self.commandHandler.process(payload=aDict)
			self.logger.info(f"process_message response as proto: {proto}")



		except Exception as e:
			self.logger.error(f"process_message error: {e}")
			traceback.print_exc(file=sys.stdout)

		if len(self.callbacks)>0:
			for cb in self.callbacks:
				cb(mesg)

	#
	#
	#
	def run(self):
		if debug:
			self.logger.info("entering subscripsion loop...")
		try:
			for item in self.pubsub.listen():
				if debug:
					self.logger.info("got message")
				if item['type']=='subscribe': # UNUSED HERE
					if debug:
						self.logger.info("received a subscripsion")
				elif item['type']=='message':
					if debug:
						self.logger.info(f"received a message on channel:{item['channel']}")
					if item['channel'].decode(DEFAULT_CHARSET)==self.mySubscribeId:# for me:
						if debug:
							#if item['data'].decode(DEFAULT_CHARSET).startswith(BROADCAST): # broadcast: all live_chain receiver
							#	self.logger.info(f" (pid={os.getpid()}): received a BROADCAST message:{item['data']}")
							#else:
							self.logger.info(f"received a message for myself:{item['data']}")

						if item['data'].decode(DEFAULT_CHARSET).startswith(self.default_killCommand): # kill subscribed components
							if item['data'].decode(DEFAULT_CHARSET) == self.myKillCommand:
								if debug:
									self.logger.info(f"received my killcommand, will terminate...")
								self.pubsub.unsubscribe()
								if debug:
									self.logger.info(f"unsubscribed and finished")
								break
							#else:
							#	if debug:
							#		self.logger.info(f" (pid={os.getpid()}): receive a killcommand: {item['data']}")
						else:
							self.process_message(item)
					else: # not for me message
						if debug:
							self.logger.info(f"receive a message for this COMPONENT type, but not for me: {item['data']}")
				else:
					if debug:
						self.logger.info(f"item is of unused type:'{item['type']}'")
			if debug:
				self.logger.info(f"exiting of subscripsion loop")
		except:
			exc_type, exc_obj, exc_tb = sys.exc_info()
			self.logger.error(f"!! Error: {exc_type} {exc_obj}")
			traceback.print_exc(file=sys.stdout)
		self.pubsub.close()
		if debug:
			self.logger.info(f"!! terminated")