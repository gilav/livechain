from io import StringIO

#
# class that capture/filter stdout or stderr
#
class Capture(StringIO):
	callback=None
	silent=False
	matchingString=[]
	matchingLines=None
	mesg=None
	allStd=None

	def __init__(self, stdout, cb=None, silent=False):
		self.__stdout = stdout
		self.callback=cb
		self.silent=silent
		StringIO.__init__(self)
		self.matchingLines=[]
		self.mesg=''
		self.allStd=StringIO()


	def getRealStd(self):
		return self.__stdout


	def write(self, s):
		StringIO.write(self, s)
		self.allStd.write(s)
		#
		match = self.matchFilter(s)
		#
		if match:
			if self.callback is not None:
				self.callback.write(s)
			else:
				if not self.silent:
					self.__stdout.write(s)

	def setFilter(self, f):
		try:
			self.matchingString.index(f)
		except:
			self.matchingString.append(f)
			self.mesg="%sadded filter:%s\n" % (self.mesg, f)

	def matchFilter(self, mess):
		for item in self.matchingString:
			if mess.find(item)>=0:
				return True
				#self.matchingLines.append(mess)
				#self.__stdout.write(mess)

	def getMatchingLines(self):
		res=''
		for item in self.matchingLines:
			res="%s%s\n" % (res, item)
		return res

	def read(self):
		self.seek(0)
		self.__stdout.write(StringIO.read(self))

	def getMesg(self):
		return self.mesg

	def getStdAll(self):
		return self.allStd.getvalue()