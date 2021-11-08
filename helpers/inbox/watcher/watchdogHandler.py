#
#
#
import json
from watchdog.events import FileSystemEventHandler, FileSystemMovedEvent, FileSystemEvent, PatternMatchingEventHandler
from watchdog.events import EVENT_TYPE_MOVED, EVENT_TYPE_DELETED, EVENT_TYPE_CREATED, EVENT_TYPE_MODIFIED, \
	EVENT_TYPE_CLOSED
from typing import Optional
from constants import *
from interfaces.iStatus import iStatus
from process.iProcessor import iProcessor
import myLoggerConfig

# import logging

COMPONENT = "WatchdogHandler"
# debug = True
debug = False


class WatchdogHandler(PatternMatchingEventHandler, iStatus):
	""" responds to filesystem selected event types and use the action handler """

	def __init__(self,
				 my_name: str,
				 actionHandler: iProcessor,
				 interrestedIn: [str],
				 actionHandlerNext: iProcessor,
				 *args, **kwargs):
		super(WatchdogHandler, self).__init__(*args, **kwargs)
		self.myName = my_name
		self.logger = myLoggerConfig.applyLoggingLevel(f"{self.__class__.__name__}/{self.myName}", True)
		self.actionHandler = actionHandler
		self.actionHandlerNext = actionHandlerNext
		self.usedEvents = interrestedIn if interrestedIn is not None else []
		self.debug = debug
		self.movedCounter = 0
		self.createdCounter = 0
		self.deletedCounter = 0
		self.modifiedCounter = 0
		self.closedCounter = 0
		#
		self.defaultChannel = COMPONENT

	#
	#
	#
	def setDefaultChannel(self, c: str):
		self.defaultChannel = c

	def on_moved(self, event: FileSystemMovedEvent):
		super().on_moved(event)
		what = 'directory' if event.is_directory else 'file'
		self.logger.info(f"Moved {what}: from {event.src_path} to {event.dest_path}")
		if EVENT_TYPE_MOVED in self.usedEvents:
			what = 'directory' if event.is_directory else 'file'
			self.logger.info(f"use handler for: Moved {what}: from {event.src_path} to {event.dest_path}")
			self.actionHandler.process(event=event)
			self.movedCounter += 1
			if self.actionHandlerNext is not None:
				self.actionHandlerNext.process(
					mesg=f"{COMPONENT}/{self.myName} moved {what}: {event.src_path}. {self.counters()}")
		else:
			if self.debug:
				self.logger.debug(f"not interested in EVENT_TYPE_MOVED event")

	def on_created(self, event: FileSystemEvent):
		super().on_created(event)
		what = 'directory' if event.is_directory else 'file'
		self.logger.info(f"Created {what}: {event.src_path}")
		if EVENT_TYPE_CREATED in self.usedEvents:
			what = 'directory' if event.is_directory else 'file'
			self.logger.info(f"use handler for: Created {what} {event.src_path}")
			self.actionHandler.process(event=event)
			self.createdCounter += 1
			if self.actionHandlerNext is not None:
				self.actionHandlerNext.process(
					mesg=f"{COMPONENT}/{self.myName} new {what}: {event.src_path}. {self.counters()}")
		else:
			if self.debug:
				self.logger.debug(f"not interested in EVENT_TYPE_CREATED event")

	def on_deleted(self, event: FileSystemEvent):
		super().on_deleted(event)
		what = 'directory' if event.is_directory else 'file'
		self.logger.info(f"Deleted {what}: {event.src_path}")
		if EVENT_TYPE_DELETED in self.usedEvents:
			what = 'directory' if event.is_directory else 'file'
			self.logger.info(f"use handler for: Deleted {what} {event.src_path}")
			self.actionHandler.process(event=event)
			self.deletedCounter += 1
			if self.actionHandlerNext is not None:
				self.actionHandlerNext.process(
					mesg=f"{COMPONENT}/{self.myName} deleted {what}: {event.src_path}. {self.counters()}")
		else:
			if self.debug:
				self.logger.debug(f"not interested in EVENT_TYPE_DELETED event")

	def on_modified(self, event: FileSystemEvent):
		super().on_modified(event)
		what = 'directory' if event.is_directory else 'file'
		self.logger.info(f"Modified {what}: {event.src_path}")
		if EVENT_TYPE_MODIFIED in self.usedEvents:
			what = 'directory' if event.is_directory else 'file'
			self.logger.info(f"use handler for: Modified {what} {event.src_path}")
			self.actionHandler.process(event=event)
			self.modifiedCounter += 1
			if self.actionHandlerNext is not None:
				self.actionHandlerNext.process(
					mesg=f"{COMPONENT}/{self.myName} modified {what}: {event.src_path}. {self.counters()}")
		else:
			if self.debug:
				self.logger.debug(f"not interested in EVENT_TYPE_MODIFIED event")

	def on_closed(self, event: FileSystemEvent):
		super().on_modified(event)
		what = 'directory' if event.is_directory else 'file'
		self.logger.info(f"Closed {what}: {event.src_path}")
		if EVENT_TYPE_CLOSED in self.usedEvents:
			what = 'directory' if event.is_directory else 'file'
			self.logger.info(f"use handler for: Closed {what} {event.src_path}")
			self.actionHandler.process(event=event)
			self.closedCounter += 1
			if self.actionHandlerNext is not None:
				self.actionHandlerNext.process(
					mesg=f"{COMPONENT}/{self.myName} closed {what}: {event.src_path}. {self.counters()}")
		else:
			if self.debug:
				self.logger.debug(f"not interested in EVENT_TYPE_CLOSED event")

	def counters(self) -> str:
		return f"[mov:{self.movedCounter} cre:{self.createdCounter} del:{self.deletedCounter} mod:{self.modifiedCounter} clo:{self.closedCounter}]"

	def status(self, format: Optional[str] = FORMAT_TEXT):
		if format == FORMAT_TEXT:
			return f"[mov:{self.movedCounter} cre:{self.createdCounter} del:{self.deletedCounter} mod:{self.modifiedCounter} clo:{self.closedCounter}]"
		else:
			return json.dumps(
				{'id': WatchdogHandler,
				 'totals': {
					 'moved': self.movedCounter,
					 'created': self.createdCounter,
					 'deleted': self.deletedCounter,
					 'modified': self.modifiedCounter,
					 'closed': self.closedCounter,
				 }
				 }
			)
