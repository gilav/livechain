import multiprocessing
import time
import threading
import multiprocessing as mp
from constants import *

DEBUG = False

class Singleton(type):
	_instances = {}

	def __call__(cls, *args, **kwargs):
		if cls not in cls._instances:
			cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
		return cls._instances[cls]


class Semaphore(threading.Thread, metaclass=Singleton):

	def __init__(self, *args, **kwargs):
		threading.Thread.__init__(self)
		self.debug = DEBUG
		# use a list as semaphore
		self.aList = mp.Manager().list()
		self.counter: int = 0
		self.running = False
		self.app = None

	#
	#
	#
	def run(self):
		self.running = True
		self.waitForChange()

	#
	#
	#
	def waitForChange(self):
		print(f" #@@@@#@@@@#@@@@#@@@@# Semaphore: running")
		while self.running:
			if self.debug:
				print(f" #@@@@#@@@@#@@@@#@@@@# Semaphore: waiting for change")
			if len(self.aList)==0:
				time.sleep(1)
			else:
				#self.event.wait()
				v = self.aList.pop(0)
				self.counter+=1
				if type(v) is dict:
					print(f" #@@@@#@@@@#@@@@#@@@@# Semaphore: some change; dict v: {v} ; counter: {self.counter}")
					if 'type' in v:
						if v['type'] == 'status':
							if self.app is not None:
								allStatus = self.app.manager.status(format=FORMAT_JSON)
								print(f" #@@@@#@@@@#@@@@#@@@@# Semaphore: allStatus: \n{allStatus}")
						else:
							print(f" #@@@@#@@@@#@@@@#@@@@# Semaphore:  unknown dict type: {v['type']}")
				elif type(v) is str:
					print(f" #@@@@#@@@@#@@@@#@@@@# Semaphore: some change; str v: '{v}'; counter: {self.counter}")
				else:
					print(f" #@@@@#@@@@#@@@@#@@@@# Semaphore: some change; v: '{v}'; type: {type(v)}; counter: {self.counter}")
		print(f" #@@@@#@@@@#@@@@#@@@@# Semaphore: stopping")

	#
	#
	#
	def set(self, v):
		print(f" #@@@@#@@@@#@@@@#@@@@# Semaphore: set event; 'v: {v}; counter: {self.counter}")
		self.aList.append(v)

	#
	#
	#
	def setApp(self, app):
		self.app=app