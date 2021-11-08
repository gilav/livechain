import os, sys, traceback
import json
from datetime import datetime, timedelta
from sysim import sysItem

#
NO_SYSITEM_INFO='no sysItem info!'
DEFAULT_DATE_PATTERN = "%Y%m%dT%H%M%S"
REPORT_DATE_PATTERN = "%a %Y/%m/%d %H:%M:%S"
#

#
"""
## doneFile example:
## conversion OK:
exitCode:0
source:/home/gilles/shared/converters/live_chain/inbox/LC08_L1GT_193024_20210629_20210629_02_RT.tar
eosip:/home/gilles/shared/converters/live_chain/outbox/L08_ORCL_OAT_GEO_1P_20210629T100250_20210629T100250_000000_0193_0024_00_v0101.SIP.ZIP
sourceMd5:486374b3893d1a9c4bb442cc75477fff
sourceSize:32256
eosipMd5:6fef457f3398079879fdfb5640ba526a
eosipSize:4541796
ctime:Mon 2021/10/11 16:33:54
at:0.9244787693023682
time:1633962835.916126

## conversion failed:
exitCode:-1
AttributeError: 'XmlHelper' object has no attribute 'createNode
source:/home/gilles/shared/converters/live_chain/inbox/LC08_L1GT_193024_20210629_20210629_02_RT.tar
at:0.9448635578155518
time:1633449126.140751
"""
#
exitCode='exitCode'
error='error'
source='source'
eosip='eosip'
sourceMd5='sourceMd5'
sourceSize='sourceSize'
eosipMd5='eosipMd5'
eosipSize='eosipSize'
ctime='ctime'
at='at'
time='time'


#
debug = False


#
# get a file ctime
#
def getFileCtime(aPath, pattern=DEFAULT_DATE_PATTERN):
	ctime = os.path.getctime(aPath)
	dt = datetime.fromtimestamp(ctime)
	return dt.strftime(pattern)


def doneFileToDict(aPath):
	if not os.path.exists(aPath):
		raise Exception(f"no doneFile at payh: {aPath}")
	fd=open(aPath, 'r')
	lines=fd.readlines()
	fd.close()
	if debug:
		print("SYSITEM:\n%s" % lines)
	exitCode=lines[0]
	if len(lines)==5:
		error=lines[1]
		srcItem=lines[2]
	else:
		srcItem = sysItem.SysItem()
		srcItem.fromString(lines[4])
		eoSipItem = sysItem.SysItem()
		eoSipItem.fromString(lines[5])
		hashSrc = srcItem.getHash()
		hashEoSip = eoSipItem.getHash()
		sizeSrc = srcItem.getSize()
		sizeEoSip = eoSipItem.getSize()
		dateCtime = getFileCtime(aPath, REPORT_DATE_PATTERN)
		aDict = {
			exitCode:'exitCode',
			source:'source',
			eosip:'eosip',
			sourceMd5:'sourceMd5',
			sourceSize:'sourceSize',
			eosipMd5:'eosipMd5',
			eosipSize:'eosipSize',
			ctime:'ctime',
			at:'at',
			time:'time',
		}

