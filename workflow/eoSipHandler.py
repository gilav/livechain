import json
import os, sys, traceback, time
import uuid
from typing import Callable, List, Optional, Dict, Union, Tuple
import shutil
from datetime import datetime
from io import StringIO
from pathlib import Path
import multiprocessing as mp
from multiprocessing import Value
import threading
import re

#
from constants import *
import configuration
from interfaces.iKillable import iKillable
from process.iProcessor import iProcessor
#
from workflow.queues.itemsQueue import ItemsQueue
from configuration import Configuration
#
import db.dbLayer as dbLayer
from models.products import SourceProduct, EoSipProduct
#
from wrapper import eoSipConverterWrapper
from wrapper.eoSipConverterWrapper import EoSipConverterWrapper
from helpers.stdCapture import Capture
#
from eosip.landsat8.eosipValidator import Landsat8InputValidator
#
from notification.redisPublisher import RedisPublisher
from workflow.mover import Mover
import myLoggerConfig
from aGlobal import aGlobalSemaphore

#
COMPONENT = 'live-chain_EoSipHandler'

#
DEFAULT_CHARSET = 'UTF-8'
#
MAX_PROCESSES = 10
#
SUPRESS_CONVERTER_STDS = False
#
REF_BASENAME = 'LC08_L1GT_193024_20210629_20210629_02_RT'
currentDir = os.path.dirname(os.path.abspath(eoSipConverterWrapper.__file__))

debug = True
debugMove = False

#
# the pool worker
#
def worker(con):
	# self.logger.info(f"worker({os.getpid()}) working...") # (con:{dir(con)})")
	while True:
		# item = queue.get(True)
		item = con.q.get(True)
		# self.logger.debug(f"worker({os.getpid()}) got {item}")
		# time.sleep(5) # simulate a "long" operation
		try:
			con.c.makeOneEosip(item)
		except:
			exc_type, exc_obj, exc_tb = sys.exc_info()
			print(f"\n ####################### \n ####################### \n ####################### \n ####################### \n UNEXPECTED ERROR: {exc_type} {exc_obj}")
			traceback.print_exc(file=sys.stdout)
			print(f"\n ####################### \n ####################### \n ####################### \n ####################### \n")

# context passed to pool worker
class Context():
	q = None
	c = None


#
#
#
class EoSipHandler(threading.Thread, iProcessor, iKillable):
	running: bool
	pool: mp.Pool
	workerQueue: mp.Queue

	def __init__(self, myName:str, pqueu: ItemsQueue, config: Configuration):
		threading.Thread.__init__(self)
		self.myName= myName
		self.logger = myLoggerConfig.applyLoggingLevel(self.__class__.__name__, True)
		self.pub = RedisPublisher()
		current_time = str(datetime.now())
		self.pub.publish(COMPONENT, f"{COMPONENT} started at {current_time}")
		#
		self.lock = mp.Lock()
		self.inputItemsDone = mp.Manager().list()
		self.itemsProcessing = mp.Manager().list()
		self.total = Value('i', 0)
		self.failed = Value('i', 0)
		self.pending = Value('i', 0)
		# self.invalid:int=0
		self.queued: int = 0
		#
		self.processingDuration = Value('f', 0)
		#
		self.failedList=mp.Manager().list()
		#
		self.config = config
		#
		self.displayWait: bool = False
		self.pQueue = pqueu  # FIFOSQLiteQueue(path=self.queuePath, multithreading=True)
		#
		self.mover = Mover()
		self.logger.info(f"__init__ done")

	#
	#
	#
	def one_done(self, item):
		self.total.value += 1
		self.pending.value -= 1
		self.displayWait = True
		if debug:
			self.logger.debug(f"one done: {item[INPUT_ITEM_SRC_PATH]}")
		self.itemsProcessing.remove(item[INPUT_ITEM_SRC_PATH])
		self.logSummary()

		#
		aGlobalSemaphore.set({'type': 'status'})


	#
	#
	#
	def one_failed(self, item):
		self.failed.value += 1
		self.pending.value -= 1
		self.displayWait = True
		if debug:
			self.logger.debug(f"one failed: {item[INPUT_ITEM_SRC_PATH]}")
		self.itemsProcessing.remove(item[INPUT_ITEM_SRC_PATH])
		self.failedList.append(item)
		self.logSummary()

		#
		aGlobalSemaphore.set({'type': 'status'})

	#
	#
	#
	def logSummary(self):
		self.logger.info(
			f"waiting (queued:{self.queued}; pending:{self.pending.value}; done:{self.total.value}; failed:{self.failed.value})...")
		self.pub.publish(COMPONENT, f"Item(s) queued:{self.queued}; pending:{self.pending.value}; done:{self.total.value}; failed:{self.failed.value}")
		self.displayWait = False

	#
	#
	#
	def status(self, format: Optional[str] = FORMAT_TEXT):
		if format==FORMAT_TEXT:
			avr = ''
			if self.total.value!=0:
				avr = f"\n			average processing duration:{(self.processingDuration.value/self.total.value):.2f}"
			return f"\n\
			{self.myName}:\n\
				queued:{self.queued}\n\
				pending:{self.pending.value}\n\
				done:{self.total.value}\n\
				failed:{self.failed.value}\n\
				processing duration:{(self.processingDuration.value):.2f}\
				{avr}"
		else:
			avr = -1
			if self.total.value!=0:
				avr = self.processingDuration.value/self.total.value
			return json.dumps(
				{'id': COMPONENT,
				 'totals': {
					 'queued': self.queued,
					 'pending':self.pending.value,
					 'done': self.total.value,
					 'failed': self.failed.value,
					 'processing time': self.processingDuration.value,
					 'average processing time': avr
				 }
				 }
			)

	#
	#
	#
	def getProcessed(self) -> str:
		if len(self.itemsProcessing) > 0:
			out = StringIO()
			n=0
			for item in self.itemsProcessing:
				print(f" {n}: {item}", file=out)
				n+=1
			return out.getvalue()
		else:
			return "No item currently processed"

	#
	#
	#
	def getFailed(self) -> str:
		if len(self.failedList) > 0:
			out = StringIO()
			n=0
			for item in self.itemsProcessing:
				print(f" {n}: {item[INPUT_ITEM_SRC_PATH]} prodId: {item[INPUT_ITEM_PRODUCTION_ID]}", file=out)
				n+=1
			return out.getvalue()
		else:
			return []

	#
	#
	#
	def run(self):
		# create multiprocessing queue
		self.workerQueue = mp.Queue()
		# will be passed as arg to pool worker
		con = Context()
		con.q = self.workerQueue
		con.c = self
		# create multiprocessing pool
		# self.pool = mp.Pool(MAX_PROCESSES, worker, (self.workerQueue,))
		self.pool = mp.Pool(MAX_PROCESSES, worker, (con,))
		self.logger.info(f"run: multiprocessing stuff set")

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
					self.logger.debug(f"\n\n\n ############ got an item from the validated queue: {item}\n\n\n")
					ok, mission = self.validate(item)
					if ok:
						item['mission'] = mission
						self.workerQueue.put(item)
						self.logger.debug(f" #### item added to worker queue: {item}")
						self.pending.value += 1
						self.displayWait = True
					else:
						self.logger.debug(f" #### item not validated: {item}")
			else:
				#self.logger.debug(f" ############ validated queue empty")
				self.queued = 0
				if self.displayWait:
					self.logSummary()
				time.sleep(2)

		if debug:
			self.logger.info(f"!! terminated")

	#
	# validate the input: which is the .tar file
	# - get the mission from path
	# - landsat8: use specific validator
	#
	def validate(self, item):
		self.logger.debug(f"validate {item[INPUT_ITEM_SRC_PATH]}")
		mission, relativePath = self.config.resolveValidatedToMission(item[INPUT_ITEM_SRC_PATH])
		self.logger.debug(f"###### validate: {item[INPUT_ITEM_SRC_PATH]} mission: {mission}, relativePath: {relativePath}")
		#os._exit(1)
		if mission is not None:
			# specific validator, return .tar input even if the input was a .jpg (when all pieces are there and ok)
			validator = Landsat8InputValidator()
			validated, inputTar = validator.validateLandsat8Input(item)
			if inputTar != item[INPUT_ITEM_SRC_PATH]:
				item[INPUT_ITEM_SRC_PATH] = inputTar
			item[INPUT_ITEM_SRC_RELATIVE_PATH]=relativePath
			return validated, mission
		return False, None

	#
	# Processor interface: unused
	#
	def process(self, **kwargs):
		if 'eosip' in kwargs:
			self.logger.debug(f" process eosip:{kwargs['eosip']}")
		else:
			self.logger.warning(f" process: no eosip in kwargs!")

	#
	#
	#
	#
	#
	#
	def setInputDone(self, value):
		self.lock.acquire()
		try:
			with self.inputItemsDone.mutex:
				return list(self.base_queue.queue)
		except Exception as e:
			print(f" ERROR getting MemoryQueue values:{str(e)}")
		finally:
			self.lock.release()

	#
	#
	#
	def makeOneEosip(self, item):
		self.logger.info(f" makeOneEosip; item:{item}")
		self.pub.process(mesg=f" makeOneEosip; item:{item[INPUT_ITEM_SRC_PATH]}")
		doneDict = None
		try:
			self.logger.info(f"worker doing item: {item[INPUT_ITEM_SRC_PATH]}")

			if 1==2:
				# already done?
				print(
					f" ########################################### makeOneEosip; self.inputItemsDone:{self.inputItemsDone}")
				if item[INPUT_ITEM_SRC_PATH] in self.inputItemsDone:
					self.logger.info(
						f" @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ makeOneEosip; item:{item[INPUT_ITEM_SRC_PATH]} ALREADY DONE: \n{item}")
					# move input files out of validated inbox
					srcBase = self.config.missionConfig[item['mission']][configuration.SETTING_VALIDATED]
					self.moveInputs(item, srcBase, 0)
					return
				print(
					f" ########################################### makeOneEosip; add in self.inputItemsDone:{item[INPUT_ITEM_SRC_PATH]}")
				self.inputItemsDone.append(item[INPUT_ITEM_SRC_PATH])

			self.itemsProcessing.append(item[INPUT_ITEM_SRC_PATH])
			dbId = self.addInSourceTable(item)

			self.logger.info(f"  !!!!!!!!!!!!!!! ############### DEBUG WRAPPER: worker wrapper STARTS on item:{item[INPUT_ITEM_SRC_PATH]}")
			exitCode, doneFlagFile = self.runWrapper(item, dbId)
			self.logger.info(f"  ############### !!!!!!!!!!!!!!! DEBUG WRAPPER: worker wrapper ENDS on item:{item[INPUT_ITEM_SRC_PATH]} ENDS: exitCode: {exitCode}; doneFlagFile: {doneFlagFile}")

			if not os.path.exists(doneFlagFile):
				raise Exception("wrapper has not created doneFlagFile: {doneFlagFile}")
			with open(doneFlagFile, 'rb') as fd:
				doneDict = json.load(fd)
			self.logger.info(f"worker wrapper exitCode: {exitCode}; doneFlagFile: {doneFlagFile}")
			#
			item['doneFlagFile'] = doneFlagFile
			#
			ok=False
			if exitCode == 0:
				if doneDict[DONE_FLAG_FILE_STATE] == 'SUCCESS':
					ok=True
					self.logger.info(f"====>> item: {item[INPUT_ITEM_SRC_PATH]} done")
					self.pub.publish(COMPONENT, f" makeOneEosip; item:{item[INPUT_ITEM_SRC_PATH]} done")
					self.one_done(item)
				else: # normally impossible case
					print(
						" TODO: control what returns the wrapper when exitCode !=0: wrapper error or converter exit code??")
					#os._exit(1)
					self.logger.error(f"====>> item: {item[INPUT_ITEM_SRC_PATH]} failed, error: {doneDict['error']}")
					self.pub.publish(COMPONENT,
									 f" makeOneEosip; item:{item[INPUT_ITEM_SRC_PATH]} failed, error: {doneDict['error']}")
					self.one_failed(item)
			else:
				self.logger.error(f"====>> item: {item[INPUT_ITEM_SRC_PATH]} failed, error: {doneDict['error']}")
				self.pub.publish(COMPONENT,
								 f" makeOneEosip; item:{item[INPUT_ITEM_SRC_PATH]} failed, error: {doneDict['error']}")
				self.one_failed(item)

			# move the file out of the validated folder
			mission = item['mission']
			self.moveFolder(item, ok, self.config.missionConfig[mission][configuration.SETTING_DONESPACE], self.config.missionConfig[mission][configuration.SETTING_FAILEDSPACE])

			#
			self.updateProductTables(item, dbId, exitCode)
		except Exception as e:
			self.one_failed(item)
			exc_type, exc_obj, exc_tb = sys.exc_info()
			self.logger.error(f"Error {exc_type} {exc_obj}")
			print(f"\n ####################### \n ####################### \n ####################### \n ####################### \n makeOneEosip ERROR: {exc_type} {exc_obj}")
			print(f"\n ####################### \n ####################### \n ####################### \n ####################### \n")
			traceback.print_exc(file=sys.stdout)

	#
	#
	#
	def addInSourceTable(self, item):
		session = dbLayer.Session()
		r = SourceProduct()
		r.filename = os.path.basename(item[INPUT_ITEM_SRC_PATH])
		r.fullpath = item[INPUT_ITEM_SRC_PATH]
		r.at = time.time()
		r.size = os.stat(item[INPUT_ITEM_SRC_PATH]).st_size
		session.add(r)
		session.commit()
		dbId = r.id
		self.logger.debug(f" addInSourceTable; item:{item} has id:{dbId}")
		return dbId

	#
	#
	#
	def addInEosipsTable(self, item, srcDbId, doneDict, session):
		print(f" #### addInEosipsTable: item={item}")
		r = EoSipProduct()
		r.at = time.time()
		r.prodid = item[INPUT_ITEM_PRODUCTION_ID]
		r.fullpath = doneDict['eosip']
		r.filename = os.path.basename(doneDict['eosip'])
		r.size = doneDict['eosipSize']
		r.hashkey = doneDict['eosipMd5']
		session.add(r)
		session.commit()
		dbId = r.id
		self.logger.debug(f" addInEosipsTable; item:{item} has id:{dbId}")
		return dbId

	#
	# update tables:
	# - input table
	# - eosip table in case of success
	#
	def updateProductTables(self, item, srcDbId, exitCode):
		session = dbLayer.Session()
		# update input table
		recs = session.query(SourceProduct).filter(SourceProduct.id == srcDbId).all()
		self.logger.debug(f" updateProductTables; query SourceProduct[id={srcDbId}]: {recs}")
		if len(recs) == 1:
			with open(item['doneFlagFile'], 'rb') as fd:
				doneDict = json.load(fd)
			# both failure and success
			recs[0].eosipdoneat = time.time()
			recs[0].prodid = item[INPUT_ITEM_PRODUCTION_ID]
			if exitCode != 0:
				recs[0].eosipdone = False
				recs[0].comment = doneDict['error']
				session.commit()
			else:
				if 'doneFlagFile' not in item:
					raise Exception("no doneFlagFile in item after conversion ok.")
				self.logger.debug(
					f" updateProductTables; loading doneFlagFile[{srcDbId}] content from file: {item['doneFlagFile']}")
				self.logger.debug(f" updateProductTables; doneDict: {doneDict}")
				recs[0].eosipdone = True
				recs[0].hashkey = doneDict['sourceMd5']
				recs[0].eosipname = os.path.basename(doneDict['eosip'])
				recs[0].eosipsize = doneDict['eosipSize']
				recs[0].eosiphashkey = doneDict['eosipMd5']
				#
				self.processingDuration.value += float(doneDict[DONE_FLAG_FILE_PROCESSING_TIME])
				#
				self.addInEosipsTable(item, srcDbId, doneDict, session)

		else:
			self.logger.error(f" updateProductTables; query SourceProduct[id={srcDbId}] return nothing")

	#
	# doneFlagFile is json
	#
	def runWrapper(self, item, dbId: int):
		self.logger.debug(f" runWrapper; item:{item[INPUT_ITEM_SRC_PATH]}")
		productionId = str(uuid.uuid4())
		ingesterInstance = self.config.missionConfig[item['mission']][configuration.SETTING_CONVERTER]
		converterConfigPath = f"{currentDir}/{self.config.missionConfig[item['mission']][configuration.SETTING_CONVERTER_CONFIG]}"
		doneFlagFile = f"{self.config.missionConfig[item['mission']][configuration.SETTING_TMPSPACE]}/doneFile_{productionId}.json"
		tmpFolder = f"{self.config.missionConfig[item['mission']][configuration.SETTING_TMPSPACE]}/{productionId}"
		logFolder = f"{self.config.missionConfig[item['mission']][configuration.SETTING_TMPSPACE]}/{productionId}/log"
		args = {'-i': str(dbId),
				'-b': productionId,
				'-t': tmpFolder,
				'-o': f"{os.path.join(self.config.missionConfig[item['mission']][configuration.SETTING_OUTBOX], productionId)}",
				'-s': item[INPUT_ITEM_SRC_PATH],
				'--noStds': 'True',
				'--logFolder': logFolder,
				}
		self.logger.debug(f" runWrapper; args:{args}")

		#
		item[INPUT_ITEM_PRODUCTION_ID]=productionId

		now = time.time()
		# keep old stds
		old_stdout = None
		old_stderr = None
		if SUPRESS_CONVERTER_STDS:
			old_stdout = sys.stdout
			old_stderr = sys.stderr
			sys.stdout = Capture(sys.stdout, None, True)
			sys.stderr = Capture(sys.stderr, None, True)
		# self.logger.disabled = True
		# logging.getLogger('root').disabled = True
		w = EoSipConverterWrapper(ingesterInstance, converterConfigPath, doneFlagFile, now)
		exitCode = w.start(args)
		# self.logger.disabled = False
		# logging.getLogger('root').disabled = False
		if SUPRESS_CONVERTER_STDS:
			fd = open(f"{tmpFolder}/stdout.txt", 'w')
			fd.write(sys.stdout.getStdAll())
			fd.flush()
			fd.close()
			fd = open(f"{tmpFolder}/stderr.txt", 'w')
			fd.write(sys.stderr.getStdAll())
			fd.flush()
			fd.close()
			sys.stdout = old_stdout
			sys.stderr = old_stderr

		# doneFlagFile status==SUCCESS/FAILURE:
		# FAILURE:
		"""
		{
		   "orchestrator":"?",
		   "id":"doneFile_2",
		   "at":0.7847530841827393,
		   "time":1633989847.5772767,
		   "processingTime":0.2733008861541748,
		   "exitCode":-1,
		   "state":"FAILURE Exception: product has bad extension, expected .tar:'/home/gilles/shared/converters/live_chain_INBOX/LC08_L1GT_193024_20210629_20210629_02_RT_QB.jpg",
		   "error":"Exception: product has bad extension, expected .tar:'/home/gilles/shared/converters/live_chain_INBOX/LC08_L1GT_193024_20210629_20210629_02_RT_QB.jpg",
		   "source":"/home/gilles/shared/converters/live_chain_INBOX/LC08_L1GT_193024_20210629_20210629_02_RT_QB.jpg"
		}
		
		# SUCCESS:
		{
		   "orchestrator":"?",
		   "id":"doneFile_31",
		   "at":0.9359545707702637,
		   "time":1634035568.7110653,
		   "ctime":"Tue 2021/10/12 12:46:06",
		   "processingTime":0.5973527431488037,
		   "exitCode":0,
		   "state":"SUCCESS",
		   "source":"/home/gilles/shared/converters/live_chain_INBOX/LC08_L1GT_193024_20210629_20210629_02_RT.tar",
		   "sourceSize":32256,
		   "sourceMd5":"486374b3893d1a9c4bb442cc75477fff",
		   "eosip":"/home/gilles/shared/converters/live_chain_OUTBOX/L08_ORCL_OAT_GEO_1P_20210629T100250_20210629T100250_000000_0193_0024_00_v0101.SIP.ZIP",
		   "eosipSize":4541796,
		   "eosipMd5":"6f96529e1cc030da804eec7f9d1205b5"
		}
		"""

		if exitCode != 0:
			print(f"\n ####################### \n ####################### \n ####################### \n ####################### \n wrapper ERROR: {exitCode}")
			print(f"\n ####################### \n ####################### \n ####################### \n ####################### \n")

		return exitCode, doneFlagFile

	#
	# TODO: move this into a mover package
	#
	def moveFolder(self, anItem: {}, ok: bool, destFolderOk: str, destFolderFail: str, overWrite: Optional[bool]=False, preserve: Optional[bool]=True):
		if debugMove:
			print(f"move item: {anItem[INPUT_ITEM_SRC_PATH]} to dest")
		try:
			srcbase = self.config.missionConfig[anItem['mission']][configuration.SETTING_VALIDATED]
			itemFolder = anItem[INPUT_ITEM_SRC_RELATIVE_PATH].split('/')[0]
			if debugMove:
				print(f"srcbase: {srcbase}; itemFolder: {itemFolder}")
			if ok:
				src = f"{os.path.join(srcbase, itemFolder)}"
				dest = f"{os.path.join(destFolderOk, itemFolder)}"
				if os.path.exists(dest):
					self.handle_dest_folder_exists(dest)
				shutil.move(src, dest)
				if debugMove:
					print(f" ################ eoSipHandler shutil.move {src} to {dest}")
			else:
				src = f"{os.path.join(srcbase,itemFolder)}"
				dest = f"{os.path.join(destFolderFail, itemFolder)}"
				if os.path.exists(dest):
					self.handle_dest_folder_exists(dest)
				shutil.move(src, destFolderFail)
				if debugMove:
					print(f" ################ eoSipHandler shutil.move {src} to {dest}")


			"""else:
				srcbase = self.config.missionConfig[anItem['mission']][configuration.SETTING_VALIDATED]
				if debug:
					print(f"srcbase: {srcbase}")
				src = os.path.dirname(anItem[INPUT_ITEM_SRC_PATH])
				relPath = src[len(srcbase):]
				if debug:
					print(f"relPath: {relPath}")
				dest = destFolderFail + relPath
				if os.path.exists(dest):
					self.handle_dest_folder_exists(dest)
				else:
					if debug:
						print(f"will create dest folder fail: {dest}")
					#os._exit(1)
					os.makedirs(dest)
				if debug:
					print(f"move inputs failed from {src} to {dest}")
				for item in anItem['eoProductContent']:
					shutil.move(os.path.join(src, item), os.path.join(dest, item))
				print(f" ############ shutil.move {os.path.join(src, item)} to {os.path.join(dest, item)}")
				"""
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
