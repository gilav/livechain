#
#
#
import os.path
import json
from watchdog.events import FileSystemEventHandler, FileSystemMovedEvent, FileSystemEvent
from watchdog.events import EVENT_TYPE_MOVED, EVENT_TYPE_DELETED, EVENT_TYPE_CREATED, EVENT_TYPE_MODIFIED, \
	EVENT_TYPE_CLOSED
from typing import Optional
from process.iProcessor import iProcessor
from interfaces.iStatus import iStatus
import helpers.inbox.inboxUtils as inboxUtils
#from context.item import Item
from context.itemContext import ItemContext
from workflow.mover import Mover
from constants import *
import myLoggerConfig
from aGlobal import aGlobalSemaphore

# import logging

COMPONENT = "CrawlerHandlerLandsat8"
debug = True


# debug = False

class CrawlerHandlerLandsat8(iStatus):
	""" """

	def __init__(self,
				 my_name: str,
				 actionHandler: iProcessor,
				 actionHandlerNext: iProcessor,
				 *args, **kwargs):
		self.myName = my_name
		self.logger = myLoggerConfig.applyLoggingLevel(f"{self.__class__.__name__}/{self.myName}", True)
		self.actionHandler = actionHandler
		self.actionHandlerNext = actionHandlerNext
		if 'inPath' in kwargs:
			self.inPath = kwargs['inPath']
		else:
			raise Exception("no inPath in kwargs")

		if 'outPath' in kwargs:
			self.outPath = kwargs['outPath']
		else:
			raise Exception("no outPath in kwargs")

		if 'failPath' in kwargs:
			self.failPath = kwargs['failPath']
		else:
			raise Exception("no failPath in kwargs")

		self.debug = debug
		self.foundCounter = 0
		self.mover = Mover()
		#
		self.defaultChannel = COMPONENT

	#
	#
	#
	def setDefaultChannel(self, c: str):
		self.defaultChannel = c

	#
	# move input folder into validated folder
	# add input item (the .tar file) into queue
	#
	def process(self, **kwargs):
		self.logger.info(f"process kwargs: {kwargs}")
		itemInboxPath = kwargs['event']
		self.logger.info(f"itemInboxPath: {itemInboxPath}")
		aList = inboxUtils.getFilesInDir(itemInboxPath, '.*.tar', True)
		if len(aList) != 1:
			raise Exception("no .tar file found")
		relPath = aList[0][len(self.inPath) + 1:]
		self.logger.info(f"itemInboxPath .tar: {aList[0]}, rel path: {relPath}")

		# trigger status
		aGlobalSemaphore.set({'type': 'status', 'mesg': f"itemInboxPath .tar: {aList[0]}, rel path: {relPath}"}) #f"itemInboxPath .tar: {aList[0]}, rel path: {relPath}")

		# queue item: .tar that will be in the validated folder after the move
		qItem = ItemContext(os.path.join(self.outPath, os.path.join(self.outPath, relPath))) #Item()
		#qItem.src_path = os.path.join(self.outPath, os.path.join(self.outPath, relPath))
		# qItem.src_path=aList[0]
		# move
		item = {}
		item[INPUT_ITEM_SRC_RELATIVE_PATH] = os.path.basename(itemInboxPath)
		self.mover.InputItemMover(item, True, self.inPath, self.outPath, self.failPath)
		self.actionHandler.process(event=qItem)
		self.foundCounter += 1
		if self.actionHandlerNext is not None:
			self.actionHandlerNext.process(mesg=f"{COMPONENT}/{self.myName} new : {kwargs['event']}. {self.counters()}")

	def counters(self) -> str:
		return f"[found:{self.foundCounter}]"

	def status(self, format: Optional[str] = FORMAT_TEXT):
		if format == FORMAT_TEXT:
			return f"[found:{self.foundCounter}]"
		else:
			return json.dumps(
				{'id': COMPONENT,
				 'totals':
					 {'found': self.foundCounter}
				 }
			)
