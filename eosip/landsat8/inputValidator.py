#
# validate input items and if ok move them to validated space
#
import json
import os, sys, traceback, time
from typing import Callable, List, Optional, Dict, Union, Tuple
import shutil
from datetime import datetime
from pathlib import Path
import threading
import re

#
from constants import *
from workflow.queues.itemsQueue import ItemsQueue
import configuration
from configuration import Configuration
from workflow.mover import Mover
import myLoggerConfig
#
from eosip.landsat8.eosipValidator import Landsat8InputValidator

#
COMPONENT = 'InputValidator'

#
REF_BASENAME = 'LC08_L1GT_193024_20210629_20210629_02_RT'

#
debug = False


class InputValidator(threading.Thread):
	running: bool

	def __init__(self, myName: str, pqueu: ItemsQueue, config: Configuration):
		threading.Thread.__init__(self)
		self.myName = myName
		self.logger = myLoggerConfig.applyLoggingLevel(self.__class__.__name__, True)

		#
		self.total = 0
		self.failed = 0
		self.queued = 0
		# self.pending = 0
		self.config = config

		#
		self.pQueue = pqueu  # FIFOSQLiteQueue(path=self.queuePath, multithreading=True)
		self.displayWait: bool = False

		#
		self.mover = Mover()

	#
	# validate the input:
	# - check file still exists (may be removed by operator or other)
	# - get the mission from path
	# - landsat8: use specific validator
	#
	def validate(self, item):
		self.logger.debug(f"validate {item[INPUT_ITEM_SRC_PATH]}")

		if not os.path.exists(item[INPUT_ITEM_SRC_PATH]):
			self.logger.warning(f"validate: {item[INPUT_ITEM_SRC_PATH]} doesn't exists")
			return False, None

		mission, relativePath = self.config.resolveInboxToMission(item[INPUT_ITEM_SRC_PATH])
		self.logger.debug(
			f"###### validate: : {item[INPUT_ITEM_SRC_PATH]} mission: {mission}, relativePath: {relativePath}")
		# os._exit(1)
		if mission is not None:
			# specific validator, return .tar input even if the input was a .jpg (when all pieces are there and ok)
			validator = Landsat8InputValidator()
			validated, inputTar = validator.validateLandsat8Input(item)
			if inputTar != item[INPUT_ITEM_SRC_PATH]:
				item[INPUT_ITEM_SRC_PATH] = inputTar
			item[INPUT_ITEM_SRC_RELATIVE_PATH] = relativePath
			return validated, mission
		return False, None

	def moveFolder(self, anItem: {}, ok: bool, destFolderOk: str, destFolderFail: str):
		if debug:
			# print(f" ###### move item: {anItem[INPUT_ITEM_SRC_PATH]} ok?: {ok} into ok folder:{destFolderOk} or fail folder {destFolderFail}")
			print(
				f"\n\n\n\n ###### move item: {anItem} ok?: {ok} into ok folder:{destFolderOk} or fail folder {destFolderFail}\n\n\n\n")
		try:
			srcbase = self.config.missionConfig[anItem['mission']][configuration.SETTING_INBOX]
			itemFolder = anItem[INPUT_ITEM_SRC_RELATIVE_PATH].split('/')[0]
			if debug:
				print(f"srcbase: {srcbase}; itemFolder: {itemFolder}")
			if ok:
				src = f"{os.path.join(srcbase, itemFolder)}"
				dest = f"{os.path.join(destFolderOk, itemFolder)}"
				if os.path.exists(dest):
					self.handle_dest_folder_exists(dest)
				shutil.move(src, dest)
				print(f" ################ validationg shutil.move {src} to {dest}")
			else:
				src = f"{os.path.join(srcbase, itemFolder)}"
				dest = f"{os.path.join(destFolderFail, itemFolder)}"
				if os.path.exists(dest):
					self.handle_dest_folder_exists(dest)
				shutil.move(src, dest)
				print(f" ################ validationg shutil.move {src} to {dest}")
		except Exception as e:
			self.logger.error(f"Error moving inputs: {e}")
			traceback.print_exc(file=sys.stdout)
			raise e

	#
	#
	#
	def handle_dest_folder_exists(self, dest: str, overWrite: Optional[bool] = False, preserve: Optional[bool] = True):
		# for time being overwrite nothing
		overwrite = False
		preserve = True
		if overWrite:
			pass
		else:
			if preserve:
				os.rename(dest, f"{dest}_{str(time.time()).replace('.', '-')}")

	#
	#
	#
	def one_done(self, item):
		self.total += 1
		# self.pending-=1
		self.displayWait = True
		if debug: self.logger.debug(f"one done: {item[INPUT_ITEM_SRC_PATH]}")
		self.log_summary()

	#
	#
	#
	def one_failed(self, item):
		self.failed += 1
		# self.pending-=1
		self.displayWait = True
		if debug: self.logger.debug(f"one failed: {item[INPUT_ITEM_SRC_PATH]}")
		self.log_summary()

	#
	#
	#
	def log_summary(self):
		# self.logger.info(f"waiting (queued:{self.queued}; pending:{self.pending}; done:{self.total}; failed:{self.failed})...")
		self.logger.info(f"waiting (queued:{self.queued}; done:{self.total}; failed:{self.failed})...")
		self.displayWait = False

	#
	#
	#
	def status(self, format: Optional[str] = FORMAT_TEXT):
		if format == FORMAT_TEXT:
			return f"\n\
				{self.myName}:\n\
					queued:{self.queued}\n\
					done:{self.total}\n\
					failed:{self.failed}"
		else:
			return json.dumps(
				{'id': COMPONENT,
				 'totals': {
					 'queued': self.queued,
					 'done': self.total,
					 'failed': self.failed
				 }
				 }
			)

	#
	#
	#
	def run(self):
		#
		self.running = True
		if debug:
			self.logger.info(f"running...")

		self.displayWait = False
		while self.running:
			if self.pQueue.size() > 0:
				self.queued = self.pQueue.size()
				self.logger.debug(f" #### number of items queued: {self.queued}")
				while self.pQueue.size() > 0:
					item = self.pQueue.get()
					try:
						ok, mission = self.validate(item)
						if ok:
							item['mission'] = mission
							self.logger.debug(f" #### item validated for mission {mission}: {item}")
							self.moveFolder(item, ok,
											self.config.missionConfig[mission][configuration.SETTING_VALIDATED],
											self.config.missionConfig[mission][configuration.SETTING_FAILEDSPACE])
							self.one_done(item)
							self.displayWait = True
						else:
							self.logger.debug(f" #### item not validated: {item}")
							self.one_failed(item)
							self.displayWait = True
					except Exception as e:
						self.logger.error(f"error validating: {item}: {e}")
						traceback.print_exc(file=sys.stdout)

			else:
				self.queued = 0
				if self.displayWait:
					self.log_summary()
				time.sleep(2)

		self.logger.info(f"!! terminated")
