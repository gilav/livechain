#
#
#
import os, sys, traceback
from optparse import OptionParser
from datetime import datetime, timedelta
import uuid
#
from constants import *
import configuration
from configuration import Configuration
from db.databaseUtils import DbInit
from helpers.inbox.inboxAgent import InboxAgent
from helpers.inbox.inboxCrawler import InboxCrawler
from workflow.queues.itemsQueue import ItemsQueue
from workflow.eoSipHandler import EoSipHandler
from workflow.maintenance import Maintenance
from manager.liveManager import LiveManager
from webservice.webservice import SimpleWebService
from aGlobal import aGlobalSemaphore
from helpers import dateHelper
#
from eosip.landsat8.inputValidator import InputValidator
#
import logging
import myLoggerConfig

# import logging

#
DEFAULT_COMPONENT = 'live-chain'

debug = False


class LiveChain:
    """
    the main live chain class
    will:
    - init the database if not already done
    - create a manager (redis connection point)
    - create an inbox agent with persistent queue
    - process the incoming queue items
    """
    COMPONENT = DEFAULT_COMPONENT
    myId: str
    logger: logging
    configurationFileUSed: str
    config: Configuration
    manager: LiveManager
    #
    inboxQueue: ItemsQueue
    inboxAgent: InboxAgent
    inboxCrawler: InboxCrawler
    inputValidator: InputValidator
    #
    validatedQueue: ItemsQueue
    validatedAgent: InboxAgent
    eosipWorker: EoSipHandler
    #
    server: SimpleWebService
    #
    maintenance: Maintenance
    # list of stuff
    validators: {}
    workers: {}
    services: {}

    def __init__(self, config_path: str):
        # logger
        self.logger = myLoggerConfig.applyLoggingLevel(self.__class__.__name__, True)
        #
        self.set_my_id()
        # configuration
        self.configurationFileUSed = config_path
        self.config = Configuration(self.configurationFileUSed)
        self.config.readConfig()
        #
        self.inboxQueue = None
        self.validatedQueue = None
        self.inputValidator = None

        # TODO: make multi mission
        self.inboxAgent = None
        self.inboxCrawler = None

        # status providers
        self.statusProviders = {}

        #
        self.workers = {}
        self.validators = {}
        #
        # self.detectionMethod = DETECT_METHOD_WATCHDOG #DETECT_METHOD_CRAWLING
        self.detectionMethod = DETECT_METHOD_CRAWLING
        self.logger.debug(f"init done")

    #
    #
    #
    def start(self):
        if debug:
            self.logger.info(f"Config used:\n{self.config}")

        #
        self.config.makeFolders()

        # init DB with schema if needed
        DbInit(self.config.SETTING_MAIN_DB_NAME, self.config.SETTING_MAIN_DB)

        # create manager
        self.manager = LiveManager()
        self.manager.set_app(self)

        # instantiate input scanner, ques, validator, workers
        # TODO: multi missions
        for mission in self.config.missions:
            self.logger.info(f"init mission {mission}")

            # queue of validated stuff
            self.validatedQueue = ItemsQueue('validated', self.config.SETTING_VALIDATED_QUEUE_DB)
            self.statusProviders['validatedQueue'] = self.validatedQueue
            # create worker
            aName = f"worker[{len(self.workers)}] converter landsat8"
            self.eosipWorker = EoSipHandler(aName, self.validatedQueue, self.config)
            self.statusProviders['eosipWorker'] = self.eosipWorker
            self.workers[aName] = self.eosipWorker
            self.eosipWorker.start()

            if self.detectionMethod == DETECT_METHOD_WATCHDOG:
                # 1) create inbox queue, agent and validator
                self.inboxQueue = ItemsQueue('inputs', self.config.SETTING_INPUT_QUEUE_DB)
                # create inboxAgent
                self.inboxAgent = InboxAgent('inputs', self.config.missionConfig[mission][configuration.SETTING_INBOX],
                                             self.inboxQueue,
                                             self.config.missionConfig[mission][configuration.SETTING_INBOX_REGEX])
                self.inboxAgent.start()
                self.statusProviders['inputsAgent'] = self.inboxAgent

                # create inbox validator
                aName = f"validator[{len(self.validators)}] inbox landsat8"
                self.inputValidator = InputValidator(aName, self.inboxQueue, self.config)
                self.inputValidator.start()
                self.validators[aName] = self.inputValidator

                # 2)create landsat8 validator, validated queue, agent and worker
                # self.validatedQueue = ItemsQueue('validated', self.config.SETTING_VALIDATED_QUEUE_DB)
                # self.statusProviders['validated']=self.validatedQueue
                # create validatedAgent
                self.validatedAgent = InboxAgent('validated',
                                                 self.config.missionConfig[mission][configuration.SETTING_VALIDATED],
                                                 self.validatedQueue,
                                                 self.config.missionConfig[mission][
                                                     configuration.SETTING_VALIDATED_REGEX])
                self.validatedAgent.start()

            else:  # use folder crawler
                # create crawler
                # TODO: move landsat8 crawlerHandler out of InboxCrawler
                self.inboxCrawler = InboxCrawler('inputs',
                                                 self.config.missionConfig[mission][configuration.SETTING_INBOX],
                                                 self.config.missionConfig[mission][configuration.SETTING_VALIDATED],
                                                 self.config.missionConfig[mission][configuration.SETTING_FAILEDSPACE],
                                                 self.validatedQueue,
                                                 self.config,
                                                 '.*')
                self.inboxCrawler.start()
                self.statusProviders['inputsCrawler'] = self.inboxCrawler

        #
        self.maintenance = Maintenance(self)

        #
        aGlobalSemaphore.setApp(self)
        aGlobalSemaphore.set('starting')

        self.server = SimpleWebService(self)
        self.server.start()
        self.logger.info(f"web server started on http://localhost:{self.config.SETTING_SERVICES_PORT}")

        self.logger.info("started")

    def get_my_id(self):
        return self.myId

    def get_manager(self):
        return self.manager

    def handle_manager_message(self, *an_args, **a_kwargs):
        self.logger.info(f"handle_manager_message: args:{an_args} kwargs:{a_kwargs}")

    def get_manager(self):
        return self.manager

    def get_config(self):
        return self.config

    def getDetectMethod(self):
        return self.detectionMethod

    def get_inbox_queue(self):
        return self.inboxQueue

    def get_validated_status(self):
        return self.validatedQueue

    def get_input_validator_worker(self):
        return self.inputValidator

    def get_eosip_worker(self):
        return self.eosipWorker

    def set_my_id(self) -> str:
        if not os.path.exists('.myID.dat'):
            with open('.myID.dat', 'w') as fd:
                self.myId = str(uuid.uuid4())
                fd.write(self.myId)
                fd.flush()
        else:
            with open('.myID.dat', 'r') as fd:
                self.myId = fd.read()
        self.logger.info(f"myId: {self.myId}")

    #
    # build status using self.statusProviders
    #
    # def buildStatus(self)
    #	allDict = {}
    #	for item in self.statusProviders:

    def do_test(self, **kwargs):
        try:
            # filters=[('filename', 'eq', 'LC08_L1GT_193024_20210629_20210629_01_RT.tar')]
            # filters=[]
            # filters=[('addeddate', 'ge', '2021-10-20T14:00:00.000Z')] #, ('addeddate', 'lt', '2021-10-20T15:00:00.000Z')]
            # return self.manager.status(format=FORMAT_JSON)

            if 'path' in kwargs:
                if kwargs['path'] == '/reporting':

                    wc=kwargs['wc'] if 'wc' in kwargs else {}

                    delta = kwargs['query'] if 'query' in kwargs else None
                    deltaDays = 0
                    if delta is not None and len(delta) > 0:
                        deltaDays = int(delta.split('=')[1])
                    someSecs = dateHelper.nowSecs(delta=timedelta(days=deltaDays))
                    wc['deltaDays']=deltaDays

                    # today:
                    if 1 == 2:
                        # someTime = dateHelper.nowSecs()
                        bDaySecs = dateHelper.secsBeginDayFromSecs(someTime)
                        print(f" bDaySecs: {bDaySecs}")
                        eDaySecs = dateHelper.secsEndDayFromSecs(someTime)
                        print(f" eDaySecs: {eDaySecs}")
                        filters = [('at', 'ge', bDaySecs)]
                        return self.manager.getInputs(format=FORMAT_JSON, filters=filters)

                    bDaySecs = dateHelper.secsBeginDayFromSecs(someSecs)
                    print(f" bDaySecs: {bDaySecs}")
                    eDaySecs = dateHelper.secsEndDayFromSecs(someSecs)
                    print(f" eDaySecs: {eDaySecs}")
                    filters = [('at', 'ge', bDaySecs), ('at', 'lt', eDaySecs)]
                    size, res = self.manager.getInputs(format=FORMAT_JSON, filters=filters)
                    wc['size']=size
                    return res

                else:
                    return f"unhandled : {kwargs['path']}".encode('utf-8')
            else:
                return f"no path in kwargs: {kwargs}".encode('utf-8')

        except Exception as e:
            error = f"{e}\n {traceback.format_exc()}"
            print(error)
            return str(error).encode('utf-8')


if __name__ == "__main__":
    try:
        parser = OptionParser()
        parser.add_option("-c", "--config", dest="configFile", help="path of the configuration file")
        options, args = parser.parse_args(sys.argv)
        if options.configFile is not None:
            if os.path.exists(options.configFile):
                print(f"use configuration file: {options.configFile}")
            else:
                raise Exception("configuration file doesn't exists: {options.configFile}")
        else:
            raise Exception("need at least a configuration file path as argument")
        if debug:
            logging.debug(" configuration readed")

        lc = LiveChain(options.configFile)
        if debug:
            logging.debug(" live_chain created")
        lc.start()
        if debug:
            logging.debug(" live_chain exited")
        sys.exit(0)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(f"Error {exc_type} {exc_obj}")
        traceback.print_exc(file=sys.stdout)
        sys.exit(2)
