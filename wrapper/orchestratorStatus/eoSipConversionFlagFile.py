#
# copyright Serco
# Lavaux Gilles 2017/10
#
# class for done flag file ( or summary file) used by orchestrator scenario doing EoSip conversion
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

from . import baseFlagFile


# added to MINIMUM_KEYS_LIST
KEYS_LIST=[baseFlagFile.PROCESS_orchestrator, baseFlagFile.PROCESS_source, baseFlagFile.PROCESS_sourceSize, baseFlagFile.PROCESS_sourceMd5,
           baseFlagFile.PROCESS_eosip, baseFlagFile.PROCESS_eosipSize, baseFlagFile.PROCESS_eosipMd5, baseFlagFile.PROCESS_ctime]


class EoSipConversionFlagFile(baseFlagFile.BaseFlagFile):

    def __init__(self, aPath):
        baseFlagFile.BaseFlagFile.__init__(self, aPath)
        self.extraKeyList=KEYS_LIST
        for item in self.extraKeyList:
            self.keyDict[item]=None

