#
#
#
import os, sys, time, traceback
from pathlib import Path
import re
#import logging
import myLoggerConfig


#
REF_BASENAME='LC08_L1GT_193024_20210629_20210629_02_RT'

#debug=False
debug=True

class Landsat8InputValidator():

	def __init__(self):
		self.logger = myLoggerConfig.applyLoggingLevel(self.__class__.__name__, True)

	#
	#
	#
	def validateLandsat8Input(self, anItem):
		toks = os.path.basename(anItem['src_path']).split('_')
		inputPath = anItem['src_path']
		validatedPath = inputPath
		basename = Path(inputPath).stem #[0:pos]
		if basename.endswith('_QB'):
			basename=basename[0:-3]
		elif basename.endswith('_TIR'):
			basename=basename[0:-4]

		ext = Path(inputPath).suffix

		if len(REF_BASENAME)!= len(basename):
			self.logger.error(f"{self.__class__.__name__} (pid={os.getpid()}) validateLandsat8; basename: {basename}; ext: {ext}; invalid length. ==> Not Validated")
			return False, None

		neededExt=None
		validated=False


		if ext=='.tar':
			neededExt='.jpg'
		else:
			neededExt='.tar'
		self.logger.debug(f"{self.__class__.__name__} (pid={os.getpid()}) validateLandsat8; basename: {basename}; ext: {ext}; neededExt: {neededExt}")
		if toks[1][1]=='1':

			# need .tar + 3 jpg
			jpegFiles = [f for f in os.listdir(os.path.dirname(inputPath)) if re.match(r'.*\.jpg', f)]
			tarFiles = [f for f in os.listdir(os.path.dirname(inputPath)) if re.match(r'.*\.tar', f)]
			self.logger.debug(f"{self.__class__.__name__} (pid={os.getpid()}) validateLandsat8 0; num jpegFiles: {len(jpegFiles)}; num tarFiles: {len(tarFiles)}")
			if len(tarFiles)==0:
				self.logger.debug(f"{self.__class__.__name__} (pid={os.getpid()}) validateLandsat8 0; no .tar found. ==> Not Validated")
				return False, None
			if len(jpegFiles)<3:
				self.logger.debug(f"{self.__class__.__name__} (pid={os.getpid()}) validateLandsat8 0; less than 3 .jpg found. ==> Not Validated")
				return False, None

			n=0
			ok=False
			eoProductContent=[]
			for item in jpegFiles:
				if debug:
					print(f"test jpg: {item}")
					print(f"test 1: {(Path(item).stem+'.jpg')} VS {(basename+'_TIR.jpg')}")
					print(f"test 2: {(Path(item).stem+'.jpg')} VS {(basename+'_QB.jpg')}")
					print(f"test 3: {(Path(item).stem+'.jpg')} VS {(basename+'.jpg')}")
				if (Path(item).stem+'.jpg')==(basename+'_TIR.jpg'):
					if debug:print("_TIR.jpg found")
					eoProductContent.append(basename+'_TIR.jpg')
					n+=1
				elif (Path(item).stem+'.jpg')==(basename+'_QB.jpg'):
					if debug:print("_QB.jpg found")
					eoProductContent.append(basename+'_QB.jpg')
					n+=1
				elif (Path(item).stem+'.jpg')==(basename+'.jpg'):
					if debug:print(".jpg found")
					eoProductContent.append(basename+'.jpg')
					n+=1

			if n!=3:
				self.logger.debug(f"{self.__class__.__name__} (pid={os.getpid()}) validateLandsat8; not ok: not 3 browses found but: {n}")
			else:
				self.logger.debug(f"{self.__class__.__name__} (pid={os.getpid()}) validateLandsat8; ok: 3 browses found")
				ok=True
			if ok:
				n=0
				for item in tarFiles:
					if (Path(item).stem+'.tar')!=(basename+'.tar'):
						ok=False
						self.logger.debug(f"{self.__class__.__name__} (pid={os.getpid()}) validateLandsat8; not matching tar: {(Path(item).stem+'.tar')}")
					else:
						eoProductContent.append(Path(item).stem+'.tar')
						n+=1
						self.logger.debug(f"{self.__class__.__name__} (pid={os.getpid()}) validateLandsat8; matching tar: {(Path(item).stem+'.tar')}")
				if n==1:
					self.logger.info(f"{self.__class__.__name__} (pid={os.getpid()}) validateLandsat8; ok: 1 matching .tar found")

			validated=ok
			if ext=='.jpg':
				validatedPath=os.path.join(os.path.dirname(inputPath), tarFiles[0])
			self.logger.info(f"@@@@@  {self.__class__.__name__} (pid={os.getpid()}) validateLandsat8 ; ==>  validated input: {validated}; input tar: {validatedPath}")

			if validated:
				anItem['eoProductContent']=eoProductContent
		return validated, validatedPath

