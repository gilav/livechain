#
# copyright Serco
# Lavaux Gilles 2017/10
#
# class for done flag file ( or summary file) used by orchestrator scenario doing system image
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


# "path|type|size|perm|hash|ctime"
SYSITEM_PATH='path'
SYSITEM_TYPE='type'
SYSITEM_SIZE='size'
SYSITEM_PERM='perm'
SYSITEM_HASH='hash'
SYSITEM_ctime='ctime'

# added to MINIMUM_KEYS_LIST
KEYS_LIST=[SYSITEM_PATH, baseFlagFile.PROCESS_source, SYSITEM_TYPE, SYSITEM_SIZE, SYSITEM_PERM, SYSITEM_HASH, SYSITEM_ctime]


class SystemImageFlagFile(baseFlagFile.BaseFlagFile):

    def __init__(self, aPath):
        baseFlagFile.BaseFlagFile.__init__(self, aPath)
        self.extraKeyList=KEYS_LIST
        for item in self.extraKeyList:
            self.keyDict[item]=None

