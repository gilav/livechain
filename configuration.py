import os, sys, traceback
from io import StringIO
import configparser
import logging
import myLoggerConfig

#
#
# sections name in configuration file
SETTING_Main='Main'
SETTING_Missions='Missions'
SETTING_Persistence='Persistence'
SETTING_Services='Services'
SETTING_Converters='Converters'
SETTING_Maintenance='Maintenance'

# Main
SETTING_CONFIG_NAME='CONFIG_NAME'
SETTING_CONFIG_VERSION='CONFIG_VERSION'

# Missions
SETTING_INBOX='INBOX'
SETTING_OUTBOX='OUTBOX'
SETTING_VALIDATED='VALIDATED'
SETTING_TMPSPACE='TMPSPACE'
SETTING_DONESPACE='DONESPACE'
SETTING_FAILEDSPACE='FAILEDSPACE'
SETTING_INBOX_REGEX='INBOX_REGEX'
SETTING_VALIDATED_REGEX='VALIDATED_REGEX'

# Missions
SETTING_AVAILABLE_MISSIONS='AVAILABLE_MISSIONS'

# persistence
SETTING_MAIN_DB='MAIN_DB'
SETTING_INPUT_QUEUE_DB='INPUT_QUEUE_DB'
SETTING_VALIDATED_QUEUE_DB='VALIDATED_QUEUE_DB'

# services
SETTING_SERVICES_PORT='SERVICE_PORT'
SETTING_SERVICES_INTERFACE='SERVICE_INTERFACE'
SETTING_SERVICES_OS_BASE_PATH='SERVICE_OS_BASE_PATH'

# converters
SETTING_CONVERTER='CONVERTER'
SETTING_CONVERTER_CONFIG='CONVERTER_CONFIG'

# maintenance
SETTING_CLEAN='CLEAN'
SETTING_RETRIGGER='RETRIGGER'

#
#
#
class Configuration():

	def __init__(self, aPath:str):
		self.logger = myLoggerConfig.applyLoggingLevel(self.__class__.__name__, True)
		self.usedConfigFile:str=aPath
		self.missions=[]
		self.missionConfig={}
		#
		self.CONFIG_NAME:str=None
		self.CONFIG_VERSION:str=None
		self.AVAILABLE_MISSIONS:str=None
		self.SETTING_MAIN_DB:str=None
		self.SETTING_MAIN_DB_NAME:str='livechain-main' # fixed
		self.SETTING_INPUT_QUEUE_DB:str=None
		self.SETTING_VALIDATED_QUEUE_DB:str=None
		self.SETTING_SERVICES_PORT:int=-1
		self.SETTING_SERVICES_INTERFACE:str=None
		self.SETTING_SERVICES_OS_BASE_PATH:str=None
		self.SETTING_MAINTENANCE_CLEAN: {}={}
		self.SETTING_MAINTENANCE_RETRIGGER: {}={}



	#
	#
	#
	def readConfig(self):
		if not os.path.exists(self.usedConfigFile):
			raise Exception("configuration file:'%s' doesn't exists" % self.usedConfigFile)

		try:
			self.logger.info(f"{__class__.__name__}.readConfig...")
			self.__config = configparser.RawConfigParser()
			self.__config.optionxform=str
			self.__config.read(self.usedConfigFile)
			#
			self.CONFIG_NAME = self.__config.get(SETTING_Main, SETTING_CONFIG_NAME)
			self.CONFIG_VERSION = self.__config.get(SETTING_Main, SETTING_CONFIG_VERSION)
			#
			self.AVAILABLE_MISSIONS = eval(self.__config.get(SETTING_Missions, SETTING_AVAILABLE_MISSIONS))
			self.readMissionsConfig()
			#
			self.SETTING_MAIN_DB = self.__config.get(SETTING_Persistence, SETTING_MAIN_DB)
			self.SETTING_INPUT_QUEUE_DB = self.__config.get(SETTING_Persistence, SETTING_INPUT_QUEUE_DB)
			self.SETTING_VALIDATED_QUEUE_DB = self.__config.get(SETTING_Persistence, SETTING_VALIDATED_QUEUE_DB)
			#
			self.SETTING_SERVICES_PORT = int(self.__config.get(SETTING_Services, SETTING_SERVICES_PORT))
			self.SETTING_SERVICES_INTERFACE = self.__config.get(SETTING_Services, SETTING_SERVICES_INTERFACE)
			self.SETTING_SERVICES_OS_BASE_PATH = self.__config.get(SETTING_Services, SETTING_SERVICES_OS_BASE_PATH)
			#
			self.SETTING_MAINTENANCE_CLEAN = eval(self.__config.get(SETTING_Maintenance, SETTING_CLEAN))
			self.SETTING_MAINTENANCE_RETRIGGER = eval(self.__config.get(SETTING_Maintenance, SETTING_RETRIGGER))

		except Exception as e:
			print(" Error in reading configuration:")
			exc_type, exc_obj, exc_tb = sys.exc_info()
			traceback.print_exc(file=sys.stdout)
			raise e

	def __repr__(self)->str:
		out = StringIO()
		#print(f"####\nConfiguration {self.CONFIG_NAME} version:{self.CONFIG_VERSION}", file=out)
		if len(self.missions)>0:
			print(f"Known mission(s):", file=out)
			for item in self.missions:
				print(f" - {item}", file=out)
				print(f"    inbox: {self.missionConfig[item][SETTING_INBOX]}", file=out)
				print(f"    validated: {self.missionConfig[item][SETTING_VALIDATED]}", file=out)
				print(f"    outBox: {self.missionConfig[item][SETTING_OUTBOX]}", file=out)
				print(f"    tmpSpace: {self.missionConfig[item][SETTING_TMPSPACE]}", file=out)
				print(f"    doneSpace: {self.missionConfig[item][SETTING_DONESPACE]}", file=out)
				print(f"    failedSpace: {self.missionConfig[item][SETTING_FAILEDSPACE]}", file=out)
				print(f"    inbox regex: {self.missionConfig[item][SETTING_INBOX_REGEX]}", file=out)
				print(f"    validated regex: {self.missionConfig[item][SETTING_VALIDATED_REGEX]}", file=out)
				print(f"\n    converter: {self.missionConfig[item][SETTING_CONVERTER]}", file=out)
				print(f"    converter config: {self.missionConfig[item][SETTING_CONVERTER_CONFIG]}", file=out)
		else:
			print(f"No mission configured", file=out)

		print(f"\nPersistence:", file=out)
		print(f" - Main DB: {self.SETTING_MAIN_DB}", file=out)
		print(f" - INBOX Queue DB: {self.SETTING_INPUT_QUEUE_DB}", file=out)
		print(f" - VALIDATED Queue DB: {self.SETTING_VALIDATED_QUEUE_DB}", file=out)

		print(f"\nMaintenance:", file=out)
		print(f" - Cleanning workspace: {self.SETTING_MAINTENANCE_CLEAN}", file=out)
		print(f" - Retrigger: {self.SETTING_MAINTENANCE_RETRIGGER}", file=out)


		print("####", file=out)
		return out.getvalue()

	#
	#
	#
	def readMissionsConfig(self):
		for item in self.AVAILABLE_MISSIONS:
			if item in self.missions:
				raise Exception(f"Mission {item} already present")
			self.logger.debug(f"readMissionsConfig for:{item}")
			inbox = self.__config.get(SETTING_Missions, f"{SETTING_INBOX}_{item}")
			validated = self.__config.get(SETTING_Missions, f"{SETTING_VALIDATED}_{item}")
			outBox = self.__config.get(SETTING_Missions, f"{SETTING_OUTBOX}_{item}")
			tmpSpace = self.__config.get(SETTING_Missions, f"{SETTING_TMPSPACE}_{item}")
			doneSpace = self.__config.get(SETTING_Missions, f"{SETTING_DONESPACE}_{item}")
			failedSpace = self.__config.get(SETTING_Missions, f"{SETTING_FAILEDSPACE}_{item}")
			inputRegex = self.__config.get(SETTING_Missions, f"{SETTING_INBOX_REGEX}_{item}")
			validatedRegex = self.__config.get(SETTING_Missions, f"{SETTING_VALIDATED_REGEX}_{item}")
			#
			converter = self.__config.get(SETTING_Converters, f"{SETTING_CONVERTER}_{item}")
			converter_config = self.__config.get(SETTING_Converters, f"{SETTING_CONVERTER_CONFIG}_{item}")
			self.missions.append(item)
			self.missionConfig[item]={
				SETTING_INBOX:inbox,
				SETTING_VALIDATED:validated,
				SETTING_OUTBOX:outBox,
				SETTING_TMPSPACE:tmpSpace,
				SETTING_DONESPACE:doneSpace,
				SETTING_FAILEDSPACE:failedSpace,
				SETTING_INBOX_REGEX:eval(inputRegex),
				SETTING_VALIDATED_REGEX:eval(validatedRegex),
				SETTING_CONVERTER:converter,
				SETTING_CONVERTER_CONFIG:converter_config
			}

	#
	#
	#
	def makeFolders(self):
		for mission in self.missions:
			self.logger.debug(f"makeFolders check mission:{mission}")
			for item in [self.missionConfig[mission][SETTING_INBOX],
						 self.missionConfig[mission][SETTING_VALIDATED],
						 self.missionConfig[mission][SETTING_OUTBOX],
						 self.missionConfig[mission][SETTING_TMPSPACE],
						 self.missionConfig[mission][SETTING_DONESPACE],
						 self.missionConfig[mission][SETTING_FAILEDSPACE]
						 ]:
				#if not os.path.exists(os.path.dirname(item)):
				if not os.path.exists(item):
					#self.loggerinfo(f"makeFolders: create folder ({item}): {os.path.dirname(item)}")
					self.logger.debug(f"makeFolders: create folder ({mission}): {item}")
					os.makedirs(item)
				else:
					#self.loggerinfo(f"makeFolders: folder ({item}) already exists: {os.path.dirname(item)}")
					self.logger.debug(f"makeFolders: folder ({mission}) already exists: {item}")

	#
	# from a path, test if it is a mission INBOX
	#
	def resolveInboxToMission(self, aPath):
		for mission in self.missions:
			self.logger.debug(f"resolveInboxToMission check mission:{mission}")
			if os.path.dirname(aPath).startswith(self.missionConfig[mission][SETTING_INBOX]):
				return mission, os.path.dirname(aPath)[len(self.missionConfig[mission][SETTING_INBOX])+1:]
		return None, None

	#
	# from a path, test if it is in a mission VALIDATED subfolder
	# returns: True/False , relative_path
	#
	def resolveValidatedToMission(self, aPath):
		for mission in self.missions:
			self.logger.debug(f"resolveValidatedToMission check mission:{mission}")
			if os.path.dirname(aPath).startswith(self.missionConfig[mission][SETTING_VALIDATED]):
				return mission, os.path.dirname(aPath)[len(self.missionConfig[mission][SETTING_VALIDATED])+1:]
		return None, None