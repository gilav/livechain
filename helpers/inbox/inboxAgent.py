#
# For live chain project
#
# @serco. Lavaux Gilles 2021-09-xx
#
import sys, traceback
import time
from datetime import datetime
import threading
from typing import Optional
from constants import *
from interfaces.iStatus import iStatus
from watchdog.observers import Observer
from watchdog.events import EVENT_TYPE_CREATED, EVENT_TYPE_MOVED, EVENT_TYPE_DELETED, EVENT_TYPE_MODIFIED, EVENT_TYPE_CLOSED
from helpers.inbox.watcher.watchdogHandler import WatchdogHandler
from notification.redisPublisher import RedisPublisher
from workflow.queues.itemsQueue import ItemsQueue

import myLoggerConfig

VERSION="LiveChain component: InboxAgent V:0.1.0"
COMPONENT="live-chain_InboxAgent"


class InboxAgent(threading.Thread, iStatus):

	def __init__(self, my_name: str, aPath:str, itemQueue: ItemsQueue, aPattern:[str]):
		threading.Thread.__init__(self)
		self.myName=my_name
		self.logger = myLoggerConfig.applyLoggingLevel(f"{self.__class__.__name__}/{self.myName}", True, f"/{self.name}")
		self.aPath=aPath
		self.itemQueue=itemQueue
		self.pattern=aPattern
		self.interrestedIn=[EVENT_TYPE_CREATED, EVENT_TYPE_MOVED]
		self.logger.info(VERSION)
		self.event_handler=None
		self.logger.info(f"watching folder: {aPath} with pattern: {self.pattern}")

	#
	#
	#
	def status(self, format: Optional[str]=FORMAT_TEXT):
		if self.handler is not None:
			return self.handler.status(format)
		else:
			return "no handler!"

	#
	#
	#
	def getQueue(self):
		return self.itemQueue

	#
	#
	#
	def run(self):
		pub = RedisPublisher()
		current_time = str(datetime.now())
		pub.publish(COMPONENT, f"{COMPONENT} started at {current_time}")
		self.event_handler = WatchdogHandler(self.myName, self.itemQueue, self.interrestedIn, pub, patterns=self.pattern)
		observer = Observer()
		observer.schedule(self.event_handler, self.aPath, recursive=True)
		observer.start()
		try:
			while True:
				time.sleep(1)
		except KeyboardInterrupt:
			observer.stop()
		observer.join()

if __name__ == "__main__":
	try:
		if len(sys.argv)<3:
			print("syntax: python3 inboxAgent inboxPath pattern")
			sys.exit(1)

		current_time = str(datetime.now())
		aPath = sys.argv[1]
		aPattern = sys.argv[2]
		print(f"will watch folder {aPath} using pattern: {aPattern}\n")
		from helpers.inbox.inboxDummyHandler import InboxDummyHandler
		anHandler = InboxDummyHandler()
		agent = InboxAgent('', aPath, anHandler, sys.argv[2])
		agent.start()
		sys.exit(0)
	except Exception as e:
		exc_type, exc_obj, exc_tb = sys.exc_info()
		print(f"Error {exc_type} {exc_obj}")
		traceback.print_exc(file=sys.stdout)
		sys.exit(2)
