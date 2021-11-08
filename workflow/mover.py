import os, time, sys, traceback
from typing import Callable, List, Optional, Dict, Union, Tuple
import shutil
import myLoggerConfig
import configuration
from constants import *

DEBUG=True

class Mover():

	def __init__(self):
		self.logger = myLoggerConfig.applyLoggingLevel(self.__class__.__name__, True)
		self.debug = DEBUG



	#
	# move item from inbox into validated or failled folders
	#
	def InputItemMover(self, anItem: {}, ok: bool, srcBase: str, destFolderOk: str, destFolderFail: str):
		if self.debug:
			print(f"\n\n\n\n ###### move item: {anItem} ok?: {ok} with srcBase: {srcBase} into ok folder:{destFolderOk} or fail folder {destFolderFail}\n\n\n\n")
		try:
			#srcbase = self.config.missionConfig[anItem['mission']][configuration.SETTING_INBOX]
			itemFolder = anItem[INPUT_ITEM_SRC_RELATIVE_PATH].split('/')[0]
			if self.debug:
				print(f"srcbase: {srcBase}; itemFolder: {itemFolder}")
			if ok:
				src = f"{os.path.join(srcBase, itemFolder)}"
				dest = f"{os.path.join(destFolderOk, itemFolder)}"
				if os.path.exists(dest):
					self.handle_dest_folder_exists(dest)
				shutil.move(src, dest)
				print(f" ################ validationg shutil.move {src} to {dest}")
			else:
				src = f"{os.path.join(srcBase,itemFolder)}"
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
	def handle_dest_folder_exists(self, dest: str, overWrite: Optional[bool]=False, preserve: Optional[bool]=True):
		# for time being overwrite nothing
		overwrite=False
		preserve=True
		if overWrite:
			pass
		else:
			if preserve:
				os.rename(dest, f"{dest}_{str(time.time()).replace('.', '-')}")


	#
	#
	#
	def moveFolder(self, src, dest):
		self.logger(f"moving folder from {src} to {dest}")
		try:
			res = shutil.move(src, dest)
		except Exception as e:
			self.logger.error(f"Error moving folder: {e}")
