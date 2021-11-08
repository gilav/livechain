#
# copyright Serco
# Lavaux Gilles 2017/10
#
# base class for done flag file ( or summary file) used by orchestrator scenario
#
# Requirements:
#  -
#

#
#
# V:0.10
# Code quality: alpha
#
#

from abc import ABCMeta, abstractmethod
import os, sys, time, datetime, inspect
import json


# json variable name for process status
PROCESS_orchestrator='orchestrator'
PROCESS_id='id'
PROCESS_uuid='uuid'
PROCESS_processingTime='processingTime'
PROCESS_exitCode='exitCode'
PROCESS_error='error'
PROCESS_state='state'
PROCESS_source='source'
PROCESS_sourceSize='sourceSize'
PROCESS_sourceMd5='sourceMd5'
PROCESS_eosip='eosip'
PROCESS_eosipSize='eosipSize'
PROCESS_eosipMd5='eosipMd5'
PROCESS_ctime='ctime'
PROCESS_at='at'
PROCESS_time='time'
PROCESS_fatalError='fatalError'
PROCESS_orchestrator='orchestrator'
PROCESS_toBeDone='toBeDone'

# minimum list of keys:
MINIMUM_KEYS_LIST=[PROCESS_toBeDone, PROCESS_orchestrator, PROCESS_uuid, PROCESS_id, PROCESS_at, PROCESS_time, PROCESS_ctime, PROCESS_processingTime, PROCESS_exitCode, PROCESS_state, PROCESS_error, PROCESS_fatalError]


DEBUG = False

CAN_OVERWRITE_VALUE=True

#
#
#
class BaseFlagFile():
    # file path
    path = None
    # minumum key list
    baseKeysList=[]
    # the dictionnary
    keyDict={}
    # extra keys
    extraKeyList=[]

    def __init__(self, aPath):
        self.debug = DEBUG
        self.path = aPath
        self.baseKeysList = MINIMUM_KEYS_LIST
        self.extraKeyList = []
        for item in self.baseKeysList:
            self.keyDict[item]=None


    def setPath(self, aPath):
        self.path = aPath

    def load(self):
        if not os.path.exists(self.path):
            raise Exception(" BaseFlagFile does not exists:%s" % self.path)
        fd=open(self.path, 'r')
        self.keyDict =  json.loads(fd.read())
        fd.close()
        if self.debug:
            print("  loaded %s key from BaseFlagFile at path:%s" % (len(self.keyDict), self.path))


    #
    # save all not None pair
    #
    def save(self):
        a, b = os.path.split(self.path)
        if not os.path.exists(a):
            os.makedirs(a)
            if self.debug:
                print("  FlagFile parent dir created:%s" % a)

        if not os.path.exists(self.path):
            if self.debug:print(" create FlagFile at path:%s" % self.path)
        filledDict={}
        for item in self.keyDict:
            if self.keyDict[item] is not None:
                filledDict[item]=self.keyDict[item]

        if self.debug:
            print(" ### filledDict:%s" % filledDict)

        fd = open(self.path, 'w')
        fd.write(json.dumps(filledDict))
        fd.flush()
        fd.close()
        if self.debug:
            print("  saved %s key to FlagFile at path:%s" % (len(self.keyDict), self.path))


    def setPair(self, key, value):
        if self.debug:
            print("@@ setPair; extraKeyList=%s"  % self.extraKeyList)

        if not key in self.baseKeysList and not key in self.extraKeyList:
            raise Exception("unknown key:%s" % key)

        if self.debug:
            print("  setPair; key=%s; value=%s" % (key, value))

        if key in self.baseKeysList and self.keyDict[key] is not None:
            if CAN_OVERWRITE_VALUE:
                print("  warning: overwriting key %s base value %s with:%s" % (key, self.keyDict[key], value))
            else:
                raise Exception("refuse to overwrite base value: key %s value %s with:%s" % (key, self.keyDict[key], value))
        elif key in self.extraKeyList and self.keyDict[key] is not None:
            if CAN_OVERWRITE_VALUE:
                print("  warning: overwriting key %s extra value %s with:%s" % (key, self.keyDict[key], value))
            else:
                raise Exception("refuse to overwrite extra value: key %s value %s with:%s" % (key, self.keyDict[key], value))
        self.keyDict[key]=value

    def getValue(self, key):
        if not key in self.keyDict:
            raise Exception(" key doesn't exists:'%s'"% key)
        return self.keyDict[key]

    def hasKey(self, key):
        return key in self.keyDict

    def knowsKey(self, key):
        return key in self.extraKeyList or key in self.keyDict

    def setKnownValue(self, key, value):
        self.keyDict[key] = value


    #
    #
    #
    #@abstractmethod
    def fillFromWrapper(self, **kwargs):
        #raise Exception("abstractmethod")
        pass


    def getInfo(self, full=False):
        return "BaseFlagFile at path:%s\n keyDict=%s" % (self.path, self.keyDict)