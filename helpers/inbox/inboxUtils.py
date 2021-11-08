import os, sys, traceback
import logging
import re
from os import listdir
from os.path import isfile,isdir, join
from io import StringIO
import json
from typing import Callable, List, Optional, Dict, Union, Tuple
#
import constants

debug=False
#debug=True

def getMissionFolderContent(aMission:str, aPath:str, aLabel:str, format: Optional[str]=constants.FORMAT_TEXT)->str:
	if format==constants.FORMAT_TEXT:
		out2 = StringIO()
		# folders
		folderList = getFoldersInDir(aPath)
		nf=0
		for item in folderList:
			print(f"   - folder {nf}: {item}", file=out2)
			nf+=1
		# files
		fileList = getFilesInDir(aPath)
		n=0
		for item in fileList:
			print(f"   - file {n}: {item}", file=out2)
			n+=1
		#
		out = StringIO()
		print(f"{aLabel} at {aPath}\n Content: {nf} folder(s); {n} file(s)", file=out)
		print(out2.getvalue(), file=out)
		return out.getvalue()
	else:
		return json.dumps({})

def getFilesInDir(apath, pattern:Optional[str]=None, fullPath: Optional[bool]=False)->[]:
	file_list =  [f for f in listdir(apath) if isfile(join(apath, f))]
	if pattern is not None:
		if debug:
			print("## use pattern")
		file_list = list(filter(lambda x: re.match(pattern, x), file_list))
	if fullPath:
		if debug:
			print("## use fullPath")
		file_list2=[]
		for item in file_list:
			file_list2.append(join(apath, item))
		file_list=file_list2
	if debug:
		n=0
		for item in file_list:
			print(f"  - file {n}: {item}")
			n+=1
	return file_list

def getFoldersInDir(apath, pattern:Optional[str]=None, fullPath: Optional[bool]=False)->[]:
	folder_list =  [f for f in listdir(apath) if isdir(join(apath, f))]
	if pattern is not None:
		if debug:
			print("## use pattern")
		folder_list = list(filter(lambda x: re.match(pattern, x), folder_list))
	if fullPath:
		if debug:
			print("## use fullPath")
		folder_list2=[]
		for item in folder_list:
			folder_list2.append(join(apath, item))
		folder_list=folder_list2
	if debug:
		n=0
		for item in folder_list:
			print(f"  - folder {n}: {item}")
			n+=1
	return folder_list

def getAllFoldersInDir(aPath, pattern:Optional[str]=None, fullPath: Optional[bool]=False)->[]:
	directory_list = []
	for root, dirs, files in os.walk(aPath, topdown=False):
		for name in dirs:
			if fullPath:
				directory_list.append(os.path.join(root, name))
			else:
				directory_list.append(name)
	if debug:
		n=0
		for item in directory_list:
			print(f"  - folder {n}: {item}")
			n+=1
	return directory_list
