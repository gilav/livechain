import os, sys, traceback, inspect
import json
from datetime import datetime
# import logging
from io import StringIO
from typing import Callable, List, Optional, Dict, Union, Tuple
#
import constants
from constants import *
from interfaces.iStatus import iStatus
import configuration as configuration
import helpers.inbox.inboxUtils as inboxChecker
from notification.redisEngine import RedisEngine
from manager.products.dbHelper import getInputProducts, getOutputProducts
from models.products import SourceProduct, EoSipProduct
from reporting.landsat8Reporting import Landsat8Reporting

import myLoggerConfig

#
COMPONENT = "LiveManager"

DEFAULT_DATE_PATTERN = "%Y-%m-%d %H:%M:%S"


def sec_to_date(d, pattern=DEFAULT_DATE_PATTERN):
	d = datetime.fromtimestamp(d)
	return d.strftime(pattern)


#
#
#
class LiveManager(iStatus):
	app: any
	redisEngine: RedisEngine
	host: str
	port: int

	def __init__(self, host: str = 'localhost', port: int = 6379):
		self.host = host
		self.port = port
		self.logger = myLoggerConfig.applyLoggingLevel(self.__class__.__name__, True)
		self.reporting = Landsat8Reporting(self)
		self.logger.info(f"init done")

	def set_app(self, app):
		self.app = app
		self.redisEngine = RedisEngine(self.app, self.host, self.port)
		self.redisEngine.start()
		#
		self.reporting = Landsat8Reporting(self)

	def set_callback(self, c: Callable[..., None]):
		self.redisEngine.set_callback(c)

	#
	# quick status
	#
	def quickStatus(self, format: Optional[str] = constants.FORMAT_TEXT) -> str:
		try:
			if format == constants.FORMAT_TEXT:
				out = StringIO()
				#
				if self.app.getDetectMethod() == DETECT_METHOD_WATCHDOG:
					print("## inputs detection method: inotify\n", file=out)
				else:
					print("## inputs detection method: crawler\n", file=out)

				# inbox queue
				print("## Queues:\n", file=out)
				if self.app.getDetectMethod() == DETECT_METHOD_WATCHDOG:
					print(f" - {self.app.get_inbox_queue().status(format)}", file=out)
				# validated
				print(f" - {self.app.get_validated_status().status(format)}", file=out)

				# worker
				print("\n\n## Workers:\n", file=out)
				if self.app.getDetectMethod() == DETECT_METHOD_WATCHDOG:
					# validator worker
					print(f" - input validator: {self.app.get_input_validator_worker().status()}", file=out)
				# eosip worker
				print(f" - eosip conversion: {self.app.get_eosip_worker().status()}", file=out)
				print(f"    items being processed:\n{self.app.get_eosip_worker().getProcessed()}", file=out)
				print(f"    items failed:\n{self.app.get_eosip_worker().getFailed()}", file=out)

				# web service
				print("\n## Services:\n", file=out)
				print(f" - web service: {self.app.server.status()}", file=out)

				# version
				print(f"\n## Live Chain V: {self.getVersion()}\n", file=out)

				return out.getvalue()
			else:
				return constants.TO_BE_IMPLEMENTED
		except Exception as e:
			out = StringIO()
			print(f"error getting status:\n{traceback.print_exc(file=out)}")
			return out.getvalue()

	#
	#
	#
	def getVersion(self):
		parrent = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
		aPath = os.path.join(os.path.dirname(parrent), '_version.py')
		fd = open(aPath)
		data = fd.read()
		fd.close()
		return data

	#
	# inputs status
	#
	def inputStatus(self, format: Optional[str] = constants.FORMAT_TEXT) -> str:
		try:
			if format == constants.FORMAT_TEXT:
				out = StringIO()
				print(self.get_inboxes_status(format), file=out)
				return out.getvalue()
			else:
				return constants.TO_BE_IMPLEMENTED
		except Exception as e:
			out = StringIO()
			print(f"error getting status:\n{traceback.print_exc(file=out)}")
			return out.getvalue()

	#
	# outputs status
	#
	def outputStatus(self, format: Optional[str] = constants.FORMAT_TEXT) -> str:
		try:
			if format == constants.FORMAT_TEXT:
				out = StringIO()
				print(self.get_outboxes_status(format), file=out)
				return out.getvalue()
			else:
				return constants.TO_BE_IMPLEMENTED
		except Exception as e:
			out = StringIO()
			print(f"error getting status:\n{traceback.print_exc(file=out)}")
			return out.getvalue()

	#
	# all status
	#
	def status(self, format: Optional[str] = constants.FORMAT_TEXT) -> str:
		try:
			if format == constants.FORMAT_TEXT:
				out = StringIO()
				#
				print(f"Live chain status\n\n", file=out)

				# seb service
				print("## Services:\n", file=out)
				print(f" - web service: {self.app.server.status()}", file=out)

				# inbox queue
				print("\n## Queues:\n", file=out)
				print(f" - {self.app.get_inbox_queue().status(format)}", file=out)
				# validated
				print(f" - {self.app.get_validated_status().status(format)}", file=out)

				# worker
				print("\n\n## Workers:\n", file=out)
				# validator worker
				print(f" - input validator: {self.app.get_input_validator_worker().status()}", file=out)
				# eosip worker
				print(f" - eosip conversion: {self.app.get_eosip_worker().status()}", file=out)
				print(f"			items being processed:\n{self.app.get_eosip_worker().getProcessed()}", file=out)
				print(f"			items failed:\n{self.app.get_eosip_worker().getFailed()}", file=out)

				#
				print(f"## Inboxes/Outboxes:\n\n{self.get_inboxes_tatus(format)}", file=out)

				#
				print("\n\n## Products:\n", file=out)
				print(f" - inputs:\n {self.getInputs()}", file=out)
				print(f" - outputs:\n {self.getOutputs()}", file=out)

				# config
				print(f"\n\n## Config:\n\n{self.app.get_config()}", file=out)
				return out.getvalue()
			else:
				allDicts = {}
				for item in self.app.statusProviders.keys():
					print(
						f" @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ - doing status provider {item}: {self.app.statusProviders[item].status(format=FORMAT_JSON)}")
					allDicts[item] = self.app.statusProviders[item].status(format=FORMAT_JSON)
				# return json.dumps(allDicts)
				return json.dumps(allDicts, indent=4)

		except Exception as e:
			out = StringIO()
			print(f"error getting status:\n{traceback.print_exc(file=out)}")
			return out.getvalue()

	#
	#
	#
	def get_queues_status(self, queueName: str, format: Optional[str] = constants.FORMAT_TEXT) -> [str]:
		if format == constants.FORMAT_TEXT:
			return constants.TO_BE_IMPLEMENTED
		else:
			return constants.TO_BE_IMPLEMENTED

	#
	#
	#
	def get_inboxes_status(self, format: Optional[str] = constants.FORMAT_TEXT) -> [str]:
		if format == constants.FORMAT_TEXT:
			out = StringIO()
			for aMission in self.app.get_config().missions:
				aPath = self.app.get_config().missionConfig[aMission][configuration.SETTING_INBOX]
				print(f"{aMission} {inboxChecker.getMissionFolderContent(aMission, aPath, 'Inputs', format)}", file=out)

				aPath = self.app.get_config().missionConfig[aMission][configuration.SETTING_VALIDATED]
				print(f"{aMission} {inboxChecker.getMissionFolderContent(aMission, aPath, 'Validated inputs', format)}",
					  file=out)

				aPath = self.app.get_config().missionConfig[aMission][configuration.SETTING_TMPSPACE]
				print(f"{aMission} {inboxChecker.getMissionFolderContent(aMission, aPath, 'tmp space', format)}",
					  file=out)

			return out.getvalue()
		else:
			return constants.TO_BE_IMPLEMENTED

	#
	#
	#
	def get_outboxes_status(self, format: Optional[str] = constants.FORMAT_TEXT) -> [str]:
		if format == constants.FORMAT_TEXT:
			out = StringIO()
			for aMission in self.app.get_config().missions:
				aPath = self.app.get_config().missionConfig[aMission][configuration.SETTING_OUTBOX]
				print(f"{aMission} {inboxChecker.getMissionFolderContent(aMission, aPath, 'outbox space', format)}",
					  file=out)

				aPath = self.app.get_config().missionConfig[aMission][configuration.SETTING_DONESPACE]
				print(f"{aMission} {inboxChecker.getMissionFolderContent(aMission, aPath, 'done space', format)}",
					  file=out)

				aPath = self.app.get_config().missionConfig[aMission][configuration.SETTING_FAILEDSPACE]
				print(f"{aMission} {inboxChecker.getMissionFolderContent(aMission, aPath, 'failled space', format)}",
					  file=out)

				aPath = self.app.get_config().missionConfig[aMission][configuration.SETTING_TMPSPACE]
				print(f"{aMission} {inboxChecker.getMissionFolderContent(aMission, aPath, 'tmp space', format)}",
					  file=out)

			return out.getvalue()
		else:
			return constants.TO_BE_IMPLEMENTED

	#
	#
	#
	def getWorkersStatus(self, format: Optional[str] = constants.FORMAT_TEXT) -> [str]:
		if format == constants.FORMAT_TEXT:
			return constants.TO_BE_IMPLEMENTED
		else:
			return constants.TO_BE_IMPLEMENTED

	#
	#
	#
	def getInputs(self,
				  #filters: Optional[any] = [],
				  format: Optional[str] = constants.FORMAT_TEXT,
				  **kwargs) -> [str]:
		if format == constants.FORMAT_TEXT:
			# filters=[('filename', 'eq', 'LC08_L1GT_193024_20210629_20210629_02_RT.tar')]
			# filters=[('at', 'lt', '12344433.33234')]
			# recs = getInputProducts([('filename', 'eq', 'LC08_L1GT_193024_20210629_20210629_02_RT.tar')], limit=3, order_by=SourceProduct.at, desc=True)
			recs = getInputProducts(**kwargs) #, limit=5, order_by=SourceProduct.at, desc=True)
			out = StringIO()
			n = 0
			for rec in recs:
				print(f"{n}: {sec_to_date(rec.at)} - {rec.filename}", file=out)
				n += 1
			return len(recs), out.getvalue()
		else:
			# return constants.TO_BE_IMPLEMENTED
			recs = getInputProducts(**kwargs) #, limit=5, order_by=SourceProduct.at, desc=True)
			print(f" %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% getInputs result length: {len(recs)}")
			all = []
			#n=0
			for rec in recs:
				#print(f" rec[{n}]: {rec.as_dict()} type: {type(rec.as_dict())}")
				all.append(rec.as_dict())
				#n+=1
			#print(f" %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% getInputs array: {all}\n %%%%%%%%%%%%%%% type: {type(all)}")
			return len(all), json.dumps(all, indent=4)

	#
	#
	#
	def getOutputs(self,
				   #filters: Optional[any] = [],
				   format: Optional[str] = constants.FORMAT_TEXT,
				   **kwargs) -> [str]:
		if format == constants.FORMAT_TEXT:
			recs = getOutputProducts(**kwargs) #limit=5, order_by=EoSipProduct.at, desc=True)
			out = StringIO()
			n = 0
			for rec in recs:
				print(f"{n}: {sec_to_date(rec.at)} - {rec.filename}", file=out)
				n += 1
			return len(recs), out.getvalue()
		else:
			# return constants.TO_BE_IMPLEMENTED
			recs = getOutputProducts(**kwargs) #, limit=5, order_by=EoSipProduct.at, desc=True)
			all = []
			for rec in recs:
				all.append(rec.as_dict())
			print(f" %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% getOutputs array: {all}\\njson = {json}")
			res = json.dumps(all, indent=4)
			return len(all), res
