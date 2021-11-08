#
# copyright Serco
# Lavaux Gilles 2017/10
#
# class for done flag file ( or summary file) used by orchestrator scenario doing product categorization
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

import baseFlagFile

CATEGORIZATION_TYPE='Type'


# added to MINIMUM_KEYS_LIST
KEYS_LIST=[baseFlagFile.PROCESS_orchestrator, baseFlagFile.PROCESS_source, CATEGORIZATION_TYPE]


class ProductCategorizationFlagFile(baseFlagFile.BaseFlagFile):

    def __init__(self, aPath):
        baseFlagFile.BaseFlagFile.__init__(self, aPath)
        self.extraKeyList=KEYS_LIST
        for item in self.extraKeyList:
            self.keyDict[item]=None

