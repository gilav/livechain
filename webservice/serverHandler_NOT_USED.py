import json
import logging
import os
import signal
import threading
from io import StringIO
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler, SimpleHTTPRequestHandler
import shutil
import myLoggerConfig
from contextlib import contextmanager

JSON=0
HTML=1

DEBUG=True

@contextmanager
def file_chunks(filename, chunk_size):
	f = open(filename, 'rb')
	try:
		def gen():
			b = f.read(chunk_size)
			while b:
				yield b
				b = f.read(chunk_size)
		yield gen()
	finally:
		f.close()


class ServerHandler(SimpleHTTPRequestHandler):
	def __init__(self, *args, **kwargs):
		self.logger = myLoggerConfig.applyLoggingLevel(self.__class__.__name__, True)
		self.app = None
		self.debug = DEBUG
		SimpleHTTPRequestHandler.__init__(self, *args, **kwargs)

	#
	#
	#
	def translate_path(self, path):
		print(f" #### translate_path; path: {path}; server.base_path: {self.server.base_path}")
		path = SimpleHTTPRequestHandler.translate_path(self, path)
		relpath = os.path.relpath(path, os.getcwd())
		fullpath = os.path.join(self.server.base_path, relpath)
		print(f" #### translate_path; fullpath: {fullpath}; relpath: {relpath}")
		return fullpath

	def do_HEAD(self):
		if self.debug:
			print(" ## HEAD: headers:\n%s" % self.headers)
		SimpleHTTPRequestHandler.do_HEAD(self)

	def do_GET(self):
		self.logger.info(f"GET request; path={self.path}")

		translatedPath = self.translate_path(self.path)
		self.logger.info(f"GET request; translatedPath={translatedPath}")
		if os.path.exists(translatedPath) and os.path.isfile(translatedPath):
			self.sendFileReply(translatedPath)
		else:
			format=JSON
			try:
				if self.path == '/status':
					sep='\n'
					#body = f"GET self.path:{self.path}" #self.app.get_status().replace(sep, '<br>')  # data_structure.data_status()
					body=f"\n{self.app.manager.status().replace(sep, '<br>')}"
					answer=f'<html><head><meta charset="utf-8"><title>test</title></head><body>{body}</body></html>'.encode('utf-8')
					format=HTML
				else:
					answer = f'<html><head><meta charset="utf-8"><title>test</title></head><body>GET self.path:{self.path}</body></html>'.encode('utf-8')
					format=HTML
			except Exception as exp:
				self.logger.error(f"{exp.args}")
				answer = None

			self.send_answer(answer, 404, format)


	def do_POST(self):
		self.logger.info("POST request")
		if self.path == '/acknowledge':
			acknowledge_data = self.rfile.read(int(self.headers['Content-Length'])).decode("utf-8")
			data = json.loads(acknowledge_data)
			answer=f'<html><head><meta charset="utf-8"><title>test</title></head><body>{data}</body></html>'.encode('utf-8')
			#format=HTML
		else:
			answer = None
		self.send_answer(answer, 503)

	def log_message(self, format, *args):
		print(f"log_message; format: {format}; args: {args}")

	def sendFileReply(self, aPath):
		self.logger.info(f"sendFileReply; aPath={aPath}")
		aSize = os.stat(aPath).st_size
		self.send_response(200)
		self.send_header('Content-type','text/html')
		self.send_header('Content-length', aSize)
		self.end_headers()
		with file_chunks(aPath, 1024) as chunks:
			for chunk in chunks:
				# process the chunk
				self.wfile.write(chunk)

	def send_answer(self, answer, error_code, format=JSON):
		if answer:
			self.send_response(200)
			self.end_headers()
			if format==JSON:
				self.wfile.write(json.dumps(answer, default=json_serial).encode('utf-8'))
			else:
				self.wfile.write(answer)
		else:
			self.send_response(error_code)
			self.end_headers()

def json_serial(obj):
	"""JSON serializer for objects not serializable by default json code"""
	if isinstance(obj, (datetime)):
		return obj.isoformat()
	raise TypeError("Type %s not serializable" % type(obj))