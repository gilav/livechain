#
# copyright Serco
# Lavaux Gilles 2018/06
#
# base class for repoting tool:
# - use luigi task info-line or summary/done file to build a report
# - get fields to be included in the html and csv parts from configuraton file
#
#
#
#
# V:0.85
# Code quality: beta
#
# changes:
# - 2018-06-15: first version
#


from abc import ABCMeta, abstractmethod
import os,time, datetime, inspect
from StringIO import StringIO
from datetime import datetime, timedelta
import sys, traceback
import logging
from logging.handlers import RotatingFileHandler
import ConfigParser
import fnmatch
import luigi
import collections


#
from constants import *
from orchestratorStatus import baseFlagFile
from tool_mail import Tool_mail


DEBUG = False


# fixed stuff
csvDelimiter=','
maxErrorMsgDigits=100
#
default_config_file='reporting.cfg'
default_log_folder='reporting_log'
default_log_filename='reporting_tool.log'
#
DEFAULT_DATE_PATTERN = "%Y%m%dT%H%M%S"
REPORT_DATE_PATTERN = "%a %Y/%m/%d %H:%M:%S"
#
default_charset="""<meta charset="utf-8">"""
default_scale="""<meta name="viewport" content="width=device-width, initial-scale=1">"""
default_style = """<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css">"""
#





# read luigi configuration
luigi_config_singleton = luigi.configuration.LuigiConfigParser.instance()

#
# get summary folder
#
processSummaryFolder = luigi_config_singleton.get(
    "summary",
    "path",
    default='dummy'
)
if processSummaryFolder=='dummy':
    raise Exception("no summary/path setting defined in luigi configuration file!")


#
# get report folder
#
reportingFolder = luigi_config_singleton.get(
    "reporting",
    "path",
    default='dummy'
)
if reportingFolder == 'dummy':
    raise Exception("no reporting/path setting defined in luigi configuration file!")



# get username
default_username='username_not_set'
try:
    default_username = os.environ["USER"]
except:
    pass

# get hostname
default_hostname='hostname_not_set'
try:
    default_hostname = os.environ["HOSTNAME"]
except:
    pass




#
# base class for reporting tool
#
class BaseReportingTool():

    #
    #
    #
    def __init__(self):
        #
        self.toolMail = Tool_mail()
        #
        self.processSummaryFolder=processSummaryFolder
        self.reportingFolder=reportingFolder
        self.default_hostname=default_hostname
        self.default_username = default_username
        #
        self.debug = DEBUG
        self.setLogger()
        self.usedConfigFile=None
        #
        self.userName=None
        self.hostName=None
        #
        self.batchId = None
        self.luigiTask=None
        self.name_task_class=None
        self.luigiOk=None
        self.luigiException = None
        #
        self.processInfoFilesFolder = None
        #
        self.tableFields=[]
        self.CSV_header=[]
        self.CSV_mapping=collections.OrderedDict()

        #
        self.html_style_file = None
        # pieces to be filled
        self.done_csvSIO=None
        self.done_already_csvSIO = None
        self.html_doneSIO = None
        self.html_already_doneSIO = None
        self.html_bodySIO = None
        #
        self.there_is_alreadyDone=False
        self.there_is_done = False

        #
        self.tasksTotal=0
        self.processToBeDone = 0
        self.processTotal = 0
        self.processOk = 0
        self.processFailed = 0
        self.processAlreadyExecuted = 0
        #
        self.num_ProcessProducts = 0
        self.num_PrepareInputs = 0
        self.num_Run_Processing = 0
        self.jsonFiles=[]
        self.num_ProcessProductsSummary = 0
        self.num_PrepareInputsSummary = 0
        self.num_Run_ProcessingSummary = 0
        #
        self.biggestAtSummary = 0
        self.smallestAtSummary = 0
        self.biggesttAt = 0
        self.smallestAt = 0

        #
        self.errorMap=None

        self.duration = 0
        self.durationPerTask = 0
        self.batchComplete=False;
        #
        self.numOfSummaryFiles=0




    #
    # set logger
    #
    def setLogger(self):
        # logger
        self.LOG_FOLDER = default_log_folder
        self.logger = logging.getLogger(self.__class__.__name__)
        # self.logger.setLevel(logging.DEBUG)
        self.logger.setLevel(logging.NOTSET)
        basicFormat = '%(asctime)s - [%(levelname)s] : %(message)s'
        self.formatter = logging.Formatter(basicFormat)
        #
        self.file_handler = RotatingFileHandler(default_log_filename, '', 1000000, 1)
        self.file_handler.setLevel(logging.DEBUG)
        self.file_handler.setFormatter(self.formatter)
        self.logger.addHandler(self.file_handler)
        #
        steam_handler = logging.StreamHandler()
        steam_handler.setLevel(logging.DEBUG)
        steam_handler.setFormatter(self.formatter)
        self.logger.addHandler(steam_handler)



    #
    # read the configuration
    #
    def readConfig(self, path=None):
        if not os.path.exists(path):
            raise Exception("configuration file:'%s' doesn't exists" % path)

        self.usedConfigFile = path
        self.logger.info(" reading configuration '%s'" % path)
        self.__config = ConfigParser.RawConfigParser(allow_no_value=True)
        self.__config.optionxform = str
        self.__config.read(path)
        #
        self.CONFIG_NAME = self.__config.get(SETTING_section_min, SETTING_CONFIG_NAME)
        self.CONFIG_VERSION = self.__config.get(SETTING_section_min, SETTING_CONFIG_VERSION)

        #configure mail tool
        self.toolMail.setConfig(self.__config)

        try:
            self.userName = self.__config.get(SETTING_section_defaults, SETTING_USER_NAME)
            print("  set userName:"+self.userName)
        except:
            pass

        try:
            self.hostName = self.__config.get(SETTING_section_defaults, SETTING_HOSTNAME)
            print("  set hostName:"+self.hostName)
        except:
            pass

        try:
            self.html_style_file = self.__config.get(SETTING_section_formatting, SETTING_HTML_STYLE)
            print("  set style from file:"+self.html_style)
            try:
                cssPath = "%s/%s" % (self.currentDir, self.html_style_file)
                fd = open(cssPath, 'r')
                self.html_style = fd.read()
                fd.close()
            except:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                print("  Error getting .css file: %s %s" % (exc_type, exc_obj))
                traceback.print_exc(file=sys.stdout)
        except:
            pass

        try:
            #self.CSV_header = self.__config.get(SETTING_section_tableFields, SETTING_TABLE_CSV_HEADER)
            tmp=self.__config.get(SETTING_section_tableFields, SETTING_TABLE_CSV_MAPPING)
            if self.debug:
                print(" @@@@@ RAW SETTING_TABLE_CSV_MAPPING:%s" % tmp)
            self.CSV_mapping = collections.OrderedDict()
            toks = tmp[1:-1].split(',')
            for tok in toks: # like 'Conversion_Date':None
                key=tok.split(':')[0].strip()
                value = tok.split(':')[1].strip()
                self.CSV_mapping[key[1:-1]]=eval(value)
            if self.debug:
                print(" @@@@@ OK SETTING_TABLE_CSV_MAPPING:%s" % self.CSV_mapping)

        except:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print("  Error getting .css file: %s %s" % (exc_type, exc_obj))
            traceback.print_exc(file=sys.stdout)
            raise e


    #
    #  entry point when running from luigi orchestrator
    #
    def baseluigiCheck(self, aLuigiTask, anException):
        self.logger.info(" will use luigi task'%s' and exception:%s" % (aLuigiTask, anException))
        self.luigiTask = aLuigiTask
        if anException is None:
            self.luigiOk=True
        else:
            self.luigiException = anException
        self.batchId = aLuigiTask.batchId

        # summary files
        aPath = "%s/%s" % (self.processSummaryFolder, self.batchId)
        if not os.path.exists(aPath):
            print(" Error: summary folder does not exists for batch '%s': %s" % (self.batchId, aPath))
            raise Exception("summary folder does not exists for batch '%s': %s" % (self.batchId, aPath))
        self.logger.info(" will use batch '%s' summary files at path:'%s'" % (self.batchId, aPath))
        self.processInfoFilesFolder=aPath

        self.now = dateNow(REPORT_DATE_PATTERN)
        self.name_task_class = "%s from '%s'" % (aLuigiTask.task_family, self.batchId)

        #
        self.html_body = StringIO()
        self.html_doneSIO = StringIO()
        self.html_already_doneSIO = StringIO()
        self.done_csvSIO_totals = StringIO()
        self.done_csvSIO = StringIO()
        self.done_already_csvSIO = StringIO()



    #
    # retrive info from the json summary/done files
    #
    def getInfoFiles(self):
        num_PrepareInputs=0
        num_ProcessProducts=0
        num_Run_Processing=0
        self.jsonFiles = fnmatch.filter(os.listdir(self.processInfoFilesFolder), '*.json')
        if len(self.jsonFiles)==0:
            print(" Error: no summary found at path:%s" % self.processInfoFilesFolder)
            raise Exception(" no summary found at path:%s" % self.processInfoFilesFolder)
        self.numOfSummaryFiles = len(self.jsonFiles)
        print("  number of json summary files:%s" % (self.numOfSummaryFiles))

        print("  number of json summary files:%s" % (self.numOfSummaryFiles))
        aPath = "%s/PrepareInputs_0.summary.json" % (self.processInfoFilesFolder)
        if os.path.exists(aPath):
            num_PrepareInputs+=1
        aPath = "%s/ProcessProducts_0.summary.json" % (self.processInfoFilesFolder)
        if os.path.exists(aPath):
            num_ProcessProducts+=1
        aPath = "%s/Run_Processing_0.summary.json" % (self.processInfoFilesFolder)
        if os.path.exists(aPath):
            num_Run_Processing+=1

        if num_PrepareInputs + num_ProcessProducts + num_Run_Processing == 3:
            print("  this batch is completed")
            self.batchComplete=True
        else:
            print("  this batch is NOT completed, miss %s task(s)" % (3 - (num_PrepareInputs + num_ProcessProducts + num_Run_Processing)))


    #
    # build the csv header line
    #
    def buildCsvHeader(self):
        n=0
        res = StringIO()
        for header in self.CSV_mapping:
            if self.debug:
                print(" ######## buildCsvHeader field[%s]:%s" % (n, header))
            res.write(header)
            res.write(csvDelimiter)
            n+=1
        return res.getvalue()

    #
    # build the csv row lines
    #
    def buildCsvRows(self, aBaseFile, resolver):
        n=0
        res = StringIO()
        for header in self.CSV_mapping:
            mapping = self.CSV_mapping[header]
            if self.debug:
                print(" ######## buildCsv field[%s]:%s; mapping=%s" % (n, header, mapping))
            if aBaseFile.hasKey(mapping):
                res.write(aBaseFile.getValue(mapping))
            else:
                res.write(resolver[header])
            res.write(csvDelimiter)
            n+=1
        res.write('\n')
        return res.getvalue()


#
# return the now date string
# dateNow('%Y-%m-%dT%H:%M:%SZ')=2015-10-15 14:08:45Z
#
def dateNow(pattern=DEFAULT_DATE_PATTERN):
    d = datetime.fromtimestamp(time.time())
    return d.strftime(pattern)


#
# get a file ctime
#
def getFileCtime(aPath, pattern=DEFAULT_DATE_PATTERN):
    ctime = os.path.getctime(aPath)
    dt = datetime.fromtimestamp(ctime)
    return dt.strftime(pattern)


#
# get a value from a dict, or N/A
#
def getDictValue(adict, key):
    if key in adict:
        return True, adict[key]
    else:
        return False, 'N/A'


#
# add a value to a dict[<String>,<list of String>]
#
def addToMapList(aDict, aName, aValue):
    aList = None
    if aDict.has_key(aName):
        aList = aDict[aName]
    else:
        aList = []
        aDict[aName] = aList
    try:
        aList.index(aValue)
    except:
        aList.append(aValue)