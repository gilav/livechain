#
# copyright Serco
# Lavaux Gilles 2017/10
#
# Wrapper for the EoSip converter. 
# Helps to use the converter from within Luigi scenarios
#
# The wrapper is responsible of:
# - calling the converter
# - create in any case a 'doneFlagFile' with valid content. It will be used by the scenario to get success/failure info for the conversion (and reporting info)
#
# Requirements:
#  - PYTHON_PATH has to point to two libraries: the eoSip_converter package + the needed_sip_spec_version package.
#
# syntax: EoSipConverterWrapper.py EoSipConverterInstanceName path_to_converter_configuration path_to_done_file_flag startTime [any eoSip_converter parameter pair][...]
#
#
# V:0.91
# Code quality: RC_2
#
# changes:
#  - 20121-10-200: modif for live_chain: support --noStds + --logFolder args
#  - 2018-06-15: simplify it by using xxxFlagFile classes
#
from typing import Optional
import io
import os, time, inspect
import sys
import traceback
from io import StringIO
from datetime import datetime, timedelta
import configparser

#
import wrapper.constants as constants
from wrapper.orchestratorStatus import eoSipConversionFlagFile, baseFlagFile


#
VERSION='  Lavaux Gilles/Serco 2018. V:0.99'






#
# TEST AND DEBUG:
#
DEBUG = False
#DEBUG = True

# in test mode processing with id==test_failIndex will be flagged as failled
TEST_MODE = False
TEST_FAIL_INDEX = 2


# constants
BAD_CONVERION_LOG_NAME = 'bad_conversion_1.log'
ERROR_TB_DELIM = 'Traceback (most recent call last):'
NO_SYSITEM_INFO='no sysItem info!'
#


# default date patterns used in reporting
DEFAULT_DATE_PATTERN = "%Y%m%dT%H%M%S"
REPORT_DATE_PATTERN = "%a %Y/%m/%d %H:%M:%S"

# wrapper configuration file
WRAPPERS_CONFIGURATION="wrappers.cfg"

# default log folder path: user ~/log
# is changed if l--ogFolder arg is used
DEFAULT_LOG_FOLDER='./log'

# this code path
homeDir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))


# will contain the inported sysItem library after the environment(PATH, PYTHONPATH) is set up by the wrapper based on his configuration and converter used
sysItem=None




#
# import that can be done only after path/environment is well set
#
#
def delayedImports():
    global sysItem
    from sysim import sysItem as sysItem
    if DEBUG:
        print(" !!!!!!!!!! DIR sysItem:%s" % sysItem)


#
# get a file ctime
#
def getFileCtime(aPath, pattern=DEFAULT_DATE_PATTERN):
    ctime = os.path.getctime(aPath)
    dt = datetime.fromtimestamp(ctime)
    return dt.strftime(pattern)




#
# class
#
class EoSipConverterWrapper():
    converterInstance = None
    converterWorkPath = None
    doneFlagPath = None
    debug = DEBUG

    #
    #
    #
    def __init__(self, converterName, converterConfigPath, doneFlagPath, startTime, noStds: Optional[bool]=False):
        if self.debug:
            print("EoSipConverterWrapper init")
            print(VERSION)
        self.converterName=converterName
        self.converterInstance=None
        self.converterConfigPath = converterConfigPath
        self.doneFlagPath=doneFlagPath
        self.startTime=startTime
        self.orchestrator = '?'
        #
        self.noStds=False
        self.logFolder=None

        if not os.path.exists(self.converterConfigPath):
            raise Exception("configuration file does not exists:%s" % self.converterConfigPath)

        if self.debug:
            print(f" #### EoSipConverterWrapper converter config: {self.converterConfigPath}")

        # set needed PYTONPATH and other env variables
        self.setConverterEnvironment(self.converterConfigPath)

        # import
        delayedImports()
        if self.debug:
            print(" delayedImports; sysItem:%s" % sysItem)

        # create converter
        # import ingester_xxx
        if self.debug:
            print(" will import package:'%s'" % self.converterName)
        module = __import__(self.converterName)
        if self.debug:
            print(" converter package imported:%s" % module)

        # instantiate class
        if self.debug:
            print(" will instantiate class:'%s'" % self.converterName)
        class_ = getattr(module, self.converterName)
        if self.debug:
            print(" converter class:%s" % class_)
        self.converterInstance = class_()
        if self.debug:
            print(" got converter instance:%s" % self.converterInstance)


    #
    #
    #
    def restoreOsEnv(self):
        sys.path = self.origSystemPath
        os.environ = self.origOsEnviron


    #
    # set environment variables/path bases on configuration file used
    #
    def setConverterEnvironment(self, converterConfigurationPath):
        try:
            config = configparser.RawConfigParser()
            config.optionxform = str
            wrapperConfig = "%s/%s" % (homeDir, WRAPPERS_CONFIGURATION)
            if not os.path.exists(wrapperConfig):
                raise Exception("wrappers configuration file:'%s' doesn't exists" % wrapperConfig)
            if not self.noStds:
                print(f"wrapper use configuration: {wrapperConfig}")
            config.read(wrapperConfig)

            if self.debug:
                print("\n\nsys.path=%s\nos.environ=%s" % (sys.path, os.environ))
            self.origSystemPath=sys.path
            self.origOsEnviron=os.environ

            #
            n = 0
            found = False
            for each_section in config.sections():
                if self.debug:
                    print(f" # doing wrapper section{n}:'{each_section}' VS REF:'{converterConfigurationPath}'")
                if each_section == converterConfigurationPath:
                    if self.debug:
                        print(" #  wrapper section[%s]:%s MATCH" % (n, each_section))
                    found = True
                    adict = dict(config.items(each_section))
                    for name in adict:
                        if self.debug:
                            print("  # os.path/os.environ setting:%s = %s" % (name, adict[name]))
                        if name.startswith('my_'):
                            if name == 'my_PYTHONPATH':
                                toks = adict[name].split(':')
                                i = 0
                                for item in toks:
                                    if self.debug:
                                        print("   # sys.path.append[%s] with:%s" % (i, item))
                                    sys.path.append(item)
                                    i += 1
                            elif name == 'my_PATH':
                                os.environ['PATH'] = adict[name] + ":" + os.environ['PATH']
                                if self.debug:
                                    print("   # extend PATH with:%s" % (adict[name]))
                            else:
                                if self.debug:
                                    print("   # set os.environ[%s]=%s" % (name[3:], adict[name]))
                                os.environ[name[3:]] = adict[name]
                        else:
                            if self.debug:
                                print("   # os.environ[%s]=%s" % (name, adict[name]))
                            os.environ[name] = adict[name]
                    break
                n += 1

            if not found:
                raise Exception("wrapper configuration has no section for '%s'" % converterConfigurationPath)

            if self.debug:
                print("\n\nsys.path after=%s\nos.environ after=%s" % (sys.path, os.environ))
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print("Error: %s %s" % (exc_type, exc_obj))
            traceback.print_exc(file=sys.stdout)
            raise e



    # the known type of info we can retrieve from converter logs
    knwonProcessingInfo = ['error', 'eosip', 'sysitems', 'ctime']

    #
    # return a processing info, readed from various logs
    #
    def getProcessingInfo(self, params, type, converterWorkFolder, summaryFile, context=None):
        if self.debug:
            print("  getProcessingInfo for type:'%s'; converterWorkFolder:'%s'; summaryFile:'%s'; context='%s'" % (type, converterWorkFolder, summaryFile, context))
        strBuffer = StringIO()
        try:
            #
            # get source and destination
            #
            if type=='eosip':
                eosip=''
                fd=open(summaryFile, 'r')
                data=fd.read()
                fd.close()
                #
                pos = data.find('eosip[0]:')
                if pos > 0:
                    pos2 = data.find('\n', pos)
                    eosip=data[pos+len('eosip[0]:'):pos2]
                #
                src=''
                pos = data.find('done[0]:')
                if pos > 0:
                    pos2 = data.find('\n', pos)
                    src=data[pos+len('done[0]:'):pos2].split('|')[0]
                return src, eosip
            #
            # get source and destination
            #
            elif type == 'sysitems':
                eosip = ''
                aPath=f"{DEFAULT_LOG_FOLDER}/sysImgs/%s.img" % context
                if os.path.exists(aPath):
                    fd=open(aPath, 'r')
                    data=fd.read()
                    fd.close()
                    if self.debug:
                        print("\n\n\nSYSITEM:\n%s\n\n" % data)
                    lines=data.split('\n')
                    srcItem = sysItem.SysItem()
                    srcItem.fromString(lines[4])
                    eoSipItem = sysItem.SysItem()
                    eoSipItem.fromString(lines[5])
                    hashSrc = srcItem.getHash()
                    hashEoSip = eoSipItem.getHash()
                    sizeSrc = srcItem.getSize()
                    sizeEoSip = eoSipItem.getSize()
                    dateCtime = getFileCtime(aPath, REPORT_DATE_PATTERN)
                    if self.debug:
                        print("   hashSrc:%s; sizeSrc:%s; hashEoSip:%s; sizeEosip:%s; ctime:%s" % (hashSrc, sizeSrc, hashEoSip, sizeEoSip, dateCtime))
                    return hashSrc, sizeSrc, hashEoSip, sizeEoSip, dateCtime
                else:
                    print("\n\n\nError: no SYSITEM file at path:'%s'\n\n\n" % aPath)
                    return NO_SYSITEM_INFO, NO_SYSITEM_INFO, NO_SYSITEM_INFO, NO_SYSITEM_INFO, NO_SYSITEM_INFO

            #
            # get last line of error file
            #
            elif type=='error':
                aPath = "%s/%s" % (converterWorkFolder, BAD_CONVERION_LOG_NAME)
                fd=open(aPath, 'r')
                data=fd.read()
                fd.close()
                #
                if self.debug:
                    print("\nreaded:%s\n" % data)
                #
                lastLine=''
                done=False
                while not done and len(lastLine.strip())==0:
                    pos = data.rfind('\n')
                    if self.debug:
                        print(" pos=%s" % pos)
                    if pos >=0:
                        lastLine=data[pos:].strip()
                        if self.debug:
                            print(" lastline='%s' " % lastLine)
                        data=data[0:pos-1]
                    else:
                        done=True
                strBuffer.write(lastLine)
            #
            #
            #
            else:
                print(" !! WARNING: unknown conversion result info of type:%s" % type)
                return "unknown conversion result info of type:%s" % type

            #
            print("  getProcessingInfo for:%s returns:'%s'" % (type, strBuffer.getvalue()))
            return strBuffer.getvalue()
        except:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print("Error getting processing info '%s': %s %s" % (type, exc_type, exc_obj))
            traceback.print_exc(file=sys.stdout)
            return "Error getting processing info '%s': %s %s" % (type, exc_type, exc_obj)





    #
    # build the doneFlagFile
    #
    # params:
    # 'params': argument used to launch process
    # 'exitCode': process exit code
    # 'fatalMesg': fatal error if any
    # 'aDict': key/value pair coming from scenario
    #
    # NEW: make also in json format
    #
    def buildDoneFile(self, params, exitCode, fatalMesg, aDict={}):
        if self.debug:
            print(" Wrapper will build done file ...")

        # use flagFile class
        aFlagFile=eoSipConversionFlagFile.EoSipConversionFlagFile(self.doneFlagPath)
        aFlagFile.fillFromWrapper(params=params, exitCode=exitCode, fatalMesg=fatalMesg, aDict=aDict)
        #flagFilectime = os.path.getctime(self.doneFlagPath)

        # set orchestrator and id
        tmp=os.path.basename(self.doneFlagPath)
        if self.debug:
            print(" done flag file basename:%s" % tmp)
        if tmp.endswith('.json'):
            tmp=tmp[0:-len('.json')]
        self.id = tmp
        if self.debug:
            print("  id from done flag file name:%s" % id)

        aFlagFile.setPair(baseFlagFile.PROCESS_id, self.id)
        aFlagFile.setPair(baseFlagFile.PROCESS_orchestrator, self.orchestrator)
        aFlagFile.setPair(baseFlagFile.PROCESS_exitCode, exitCode)

        # duration if any
        if baseFlagFile.PROCESS_processingTime in aDict:
            aFlagFile.setPair(baseFlagFile.PROCESS_processingTime, aDict[baseFlagFile.PROCESS_processingTime])

        if fatalMesg is not None:
            aFlagFile.setPair(baseFlagFile.PROCESS_fatalError, fatalMesg)
        else:
            src = 'NA'
            if '-s' in params:
                src = params['-s']

            #
            # completely fail: no work folder
            #
            if not os.path.exists(self.converterWorkPath):
                t = time.time()
                aFlagFile.setPair(baseFlagFile.PROCESS_error, "No conversion working folder found: that's very bad...")
                aFlagFile.setPair(baseFlagFile.PROCESS_source, src)
                aFlagFile.setPair(baseFlagFile.PROCESS_at, (t - float(self.startTime)))
                aFlagFile.setPair(baseFlagFile.PROCESS_time, t)

            else:
                converterWorkFolder = None
                files = os.listdir(self.converterWorkPath)
                n = 0
                for item in files:
                    if self.debug:
                        print(" folder %s in %s: %s" % (n, self.converterWorkPath, item))
                    if item.startswith('batch_') and item.endswith('_workfolder_0'):
                        converterWorkFolder = "%s/%s" % (self.converterWorkPath, item)
                        break
                    n += 1
                if converterWorkFolder is None:
                    raise Exception("can not find converter batch folder inside:%s" % self.converterWorkPath)

                #
                # get info from log folder
                # substitute in basename of converterWorkFolder: '_workfolder_0' with: xxx
                # like: 'batch_irs1c1d_luigi_0_log.txt'
                #
                logBaseName = os.path.basename(converterWorkFolder)
                if self.logFolder is None:
                    summaryFile = f"./log/%s" % logBaseName.replace('_workfolder_0', '_log.txt')
                else:
                    summaryFile = f"{self.logFolder}/%s" % logBaseName.replace('_workfolder_0', '_log.txt')
                if self.debug:
                    print("  conversion summary file:%s" % summaryFile)

                #
                if exitCode != 0:
                    # retrieve error
                    error = self.getProcessingInfo(params, 'error', converterWorkFolder, summaryFile)
                    if self.debug:
                        print("  doneFileFlag error content added: %s=%s" % ('error', error))
                    t = time.time()
                    aFlagFile.setPair(baseFlagFile.PROCESS_error, error)
                    aFlagFile.setPair(baseFlagFile.PROCESS_source, src)
                    aFlagFile.setPair(baseFlagFile.PROCESS_at, (t - float(self.startTime)))
                    aFlagFile.setPair(baseFlagFile.PROCESS_time, t)
                    aFlagFile.setPair(baseFlagFile.PROCESS_state, "FAILURE %s" % error)

                    if self.debug:
                        print("  doneFileFlag done for failure case")
                else:
                    aFlagFile.setPair(baseFlagFile.PROCESS_state, 'SUCCESS')
                    #
                    # retrieve info
                    # source and destination paths
                    src, eosip = self.getProcessingInfo(params, 'eosip', converterWorkFolder, summaryFile)
                    aFlagFile.setPair(baseFlagFile.PROCESS_source, src)
                    if self.debug:
                        print("  doneFileFlag content added: %s=%s" % ('source', src))
                    aFlagFile.setPair(baseFlagFile.PROCESS_eosip, eosip)
                    if self.debug:
                        print("  doneFileFlag content added: %s=%s" % ('eosip', eosip))
                    #
                    srcMd5, srcSize, destMd5, destSize, ctime = self.getProcessingInfo(params, 'sysitems',
                                                                                       converterWorkFolder,
                                                                                       summaryFile,
                                                                                       os.path.basename(eosip))
                    if self.debug:
                        print("  doneFileFlag content added: %s=%s" % ('srcMd5', srcMd5))
                        print("  doneFileFlag content added: %s=%s" % ('sourceSize', srcSize))
                        print("  doneFileFlag content added: %s=%s" % ('eosipMd5', destMd5))
                        print("  doneFileFlag content added: %s=%s" % ('eosipSize', destSize))
                        print("  doneFileFlag content added: %s=%s" % ('ctime', ctime))
                    t = time.time()

                    aFlagFile.setPair(baseFlagFile.PROCESS_sourceMd5, srcMd5)
                    aFlagFile.setPair(baseFlagFile.PROCESS_sourceSize, srcSize)
                    aFlagFile.setPair(baseFlagFile.PROCESS_eosipMd5, destMd5)
                    aFlagFile.setPair(baseFlagFile.PROCESS_eosipSize, destSize)
                    aFlagFile.setPair(baseFlagFile.PROCESS_ctime, ctime)
                    aFlagFile.setPair(baseFlagFile.PROCESS_at, (t - float(self.startTime)))
                    aFlagFile.setPair(baseFlagFile.PROCESS_time, t)


                    if self.debug:
                        print("  doneFileFlag done for success case")

        # save flag file
        aFlagFile.save()

        if not self.noStds:
            print("  doneFileFlag json file created: %s" % (self.doneFlagPath))




    #
    # launch the converter
	# create the done_flag_file that will be used by Luigi task as output
    # it will contains a summary of the conversion containing pair values (':' separated), that can be used by Luigi task to build report:
    # - exitcode:a code
    # - error: a string
    # - eosip_path: a path
    # - sysitem_x: a sysitem line (usually one line for parent product and one for eosip)
    #
    #
    def start(self, args):
        if self.debug:
            print("EoSipConverterWrapper.start()")

        if self.debug:
            print(" start args:%s" % args)

        # require the -t (temporary folder path) parameter
        # to be able to get conversion info after completion
        self.converterWorkPath=None
        if '-t' in args:
            self.converterWorkPath=args['-t']
        else:
            errorMesg = "FATAL: no -t argument given"
            print(" will exit because of " + errorMesg)
            self.buildDoneFile(None, -255, errorMesg, {})
            return  -255
        #

        # remove wrapper args
        if constants.ORCHESTRATOR_PARAM in args:
            self.orchestrator = args[constants.ORCHESTRATOR_PARAM]
            del args[constants.ORCHESTRATOR_PARAM]

        # start converter
        params=[]

        # check if --noStds is in args (will be passes to converter). Needed here to supress output
        if '--noStds' in args:
            if args['--noStds'] == 'True':
                print(f" #### wrapper: --noStds is True")
                self.noStds=True

        # check if --logFolder is in args (will be passes to converter). Needed here to be able to build the summary path (getProcessingInfo method)
        if '--logFolder' in args:
            print(f" #### wrapper: --logFolder is: {args['--logFolder']}")
            self.logFolder = args['--logFolder']
            #self.logFolder = f"{os.path.join(args['--logFolder'], self.converterWorkPath)}"


        params.append("-c")
        params.append(self.converterConfigPath)
        #
        for item in args.keys():
            params.append(item)
            params.append(args[item])

        # report test:
        #self.buildDoneFile(params, 0, None)
        #os._exit(-1)

        if not self.noStds:
            print(" starting converter with params:%s" % params)

        #
        id=-1
        if '-i' in args:
            id = int(args['-i'])
            if DEBUG:
                print("  id='%s'" % id)

        aDict={}
        started=time.time()
        aDict['started']=started

        # TEST: fail one conversion at index x
        if TEST_MODE:
            index = TEST_FAIL_INDEX
            print("!!!!!!!\n!!!!!!!\n WRAPPER RUN IN TEST MODE: IT WILL FAIL ON ITEM AT INDEX: %s\n!!!!!!!\n!!!!!!!\n" % index)
            if id==index:
                exitCode = -55
            else:
                exitCode = self.converterInstance.starts(params)
        else:
            exitCode = self.converterInstance.starts(params)
        if self.debug:
            print("  exitCode=%s" % exitCode)
        t = time.time()
        aDict['stoped'] = t
        aDict[baseFlagFile.PROCESS_processingTime] = t-started


        if not self.noStds:
            print("\n\n Wrapper says: converter exit code is: %s" % exitCode)
        try:
            self.buildDoneFile(args, exitCode, None, aDict)
        except:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(" write doneFlagFile error:%s %s" % (exc_type, exc_obj))
            traceback.print_exc(file=sys.stdout)
            exitCode = -99

        try:
            self.restoreOsEnv()
            if self.debug:
                print("\n\nsys.path restored=%s\nos.environ restored=%s" % (sys.path, os.environ))
        except:
            print(" ERROR !!!")
        return exitCode


#
# parse parameters needed by eoSip_converter
# at this time: just a few
#
knwonParamsName=['-i', '-l', '-b', '-o', '-t', '-s', '-m', '--noStds', '--logFolder', constants.ORCHESTRATOR_PARAM]


#
# check parameters
#
def parseParams(args):
    aDict={}
    n = len(args)
    if n==0:
        return
    if n%2 !=0:
        i=0
        for item in args:
            if DEBUG:
                print(" param[%s]:%s" % (i, item))
            i+=1
        raise Exception("additional parameters have to be in pair: num params givem=%s" % n)
    for n in range(int(n/2)):

        if DEBUG:
            print(" - do params: %s %s" % (args[n*2], args[n*2+1]))

        if args[n*2] in knwonParamsName:
            aDict[args[n*2]]=args[n*2+1]
        else:
            raise Exception("unknown param in anyParams list:%s" % args[n*2])
    return aDict
        

#
# main
#
if __name__ == "__main__":
    try:
        out = StringIO()
        out2 = StringIO()
        noStds = False
        error=False

        if len(sys.argv) >= 3:
            converterInstance = sys.argv[1]
            converterConfigPath = sys.argv[2]
            print(" EoSipConverterWrapper converter used: %s" % sys.argv[1], file=out)
            if len(sys.argv) > 4:
                doneFlagFile = sys.argv[3]
                startTime = sys.argv[4]
                anyParam = sys.argv[5:]
                aDict = parseParams(anyParam)
                # add wrapper args if not already set
                if not constants.ORCHESTRATOR_PARAM in aDict:
                    aDict[constants.ORCHESTRATOR_PARAM]='by operator'
                print(" EoSip converter:%s uses configuration:%s; doneFlagFile:%s; startTime:%s and anyParam:%s" % (converterInstance, converterConfigPath, doneFlagFile, startTime, anyParam), file=out2)
                aDict = parseParams(anyParam)
                if '--noStds' in aDict:
                    noStds=True

            if DEBUG:
                i=0
                print("sys.argv:", file=out)
                for item in sys.argv:
                    print(" sys.argv[%s]:%s" % (i, item), file=out)
                    i+=1

            if doneFlagFile.startswith('-'):
                error=True
                raise Exception("invalid param doneFlagFile:%s" % doneFlagFile)
            if not noStds:
                print(f"{out.getvalue()}{out2.getvalue()}")
            EoSipConverterWrapper(converterInstance, converterConfigPath, doneFlagFile, startTime, noStds).start(aDict)
        else:
            error=True
            print("syntax: EoSipConverterWrapper.py EoSipConverterInstanceName path_to_converter_configuration path_to_done_file_flag startTime_or_0 [any eosip suppoted parameter]\n", file=out)

        if error:
            print(f"==> EoSipConverterWrapper exits with error")
        else:
            if not noStds:
                print(f"==>EoSipConverterWrapper exits ok")
    except:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(f" EoSipConverterWrapper error: {exc_type} {exc_obj}")
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

