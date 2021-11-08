import json
import logging
import os
import signal
import sys
import threading
import traceback
from io import StringIO
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler, SimpleHTTPRequestHandler
from jinja2 import Markup
from jinja2.exceptions import TemplateNotFound
from webservice.template import TemplateEngine
from webservice.view import views
import mimetypes
from urllib.parse import urlparse

#
from context.webContext import WebContext
import myLoggerConfig



# IF PYTHON 2 NEEDS TO BE SUPPORTED
# if sys.version_info[0] == 3:
#     from http.server import HTTPServer, BaseHTTPRequestHandler
# else:
#     from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
# from UniOProducer.producer import AbstractProducer

JSON=0
HTML=1

DEBUG=False


from contextlib import contextmanager

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

#
#
#
class AbstractWebService(threading.Thread):
    def __init__(self):
        self.logger = myLoggerConfig.applyLoggingLevel(self.__class__.__name__, True)
        self.stop = False
        self.debug = DEBUG
        threading.Thread.__init__(self)

    def run(self):
        self.run_forever()

    def kill_server(self):
        self.stop_server()
        self.stop = True

    def run_forever(self):
        if self.debug:
            print(f"AbstractWebService run_forever starts; pid: {os.getpid()}")
        while not self.stop:
            self.handle_request()
        self.logger.info("WebServer Stopped")
        if self.debug:
            print(f"AbstractWebService run_forever ends; pid: {os.getpid()}")
        os.kill(os.getpid(), signal.SIGINT)

    def stop_server(self):
        raise NotImplementedError

    # abstract
    def handle_request(self):
        raise NotImplementedError


class MyHTTPServer(HTTPServer):
    """The main server, you pass in base_path which is the path you want to serve requests from"""
    def __init__(self, base_path, server_address, requestHandlerClass):
        self.base_path = base_path
        HTTPServer.__init__(self, server_address, requestHandlerClass)

#
# app is Live_Chain
#
class SimpleWebService(AbstractWebService):
    def __init__(self, app):
        self.logger = myLoggerConfig.applyLoggingLevel(self.__class__.__name__, True)
        self.interface=app.config.SETTING_SERVICES_INTERFACE
        self.port=app.config.SETTING_SERVICES_PORT
        self.web_path=app.config.SETTING_SERVICES_OS_BASE_PATH
        self.templateEngine = TemplateEngine(f"{os.path.join(self.web_path, 'templates')}")
        self.httpd = MyHTTPServer(self.web_path, (self.interface, self.port), self.makeHandlerWithAppStructure(app, self.templateEngine))
        self.httpd.timeout = 2
        AbstractWebService.__init__(self)

    def status(self):
        out = StringIO()
        print(f"{self.__class__.__name__} listening on {self.interface}:{self.port}, serving OS path: {self.web_path}", file=out)

        return out.getvalue()

    def stop_server(self):
        self.logger.info("socket will be closed")
        if self.debug:
            print(f"SimpleWebService stop_server; socket will be closed")
        self.httpd.socket.close()
        self.httpd.server_close()
        if self.debug:
            print(f"SimpleWebService stop_server; server stopped")

    # abstract
    def handle_request(self):
        if self.debug:
            print(f"SimpleWebService handle_request")
        return self.httpd.handle_request()

    #
    # make the request handler
    #
    def makeHandlerWithAppStructure(self, app, templateEngine):
        class ServerHandler(SimpleHTTPRequestHandler):
            def __init__(self, *args, **kwargs):
                self.logger = myLoggerConfig.applyLoggingLevel(self.__class__.__name__, True)
                self.app = app
                self.templateEngine = templateEngine
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
                format=HTML
                try:
                    translatedPath = self.translate_path(self.path)
                    self.logger.info(f"GET request; translatedPath={translatedPath}")

                    # bits.scheme, bits.netloc,
                    # bits.path, bits.params, bits.query, bits.fragment,
                    # bits.username, bits.password, bits.hostname, bits.port
                    bits = urlparse(self.path)
                    self.logger.info(f" #### GET request; bits={bits}")

                    if os.path.exists(translatedPath) and os.path.isfile(translatedPath):
                        self.sendFileReply(translatedPath)
                    else:
                        answer=None
                        wc:{} = WebContext(bits)
                        try:
                            if bits.path == '/status':
                                sep='\n'
                                #body=f"{self.app.manager.status().replace(sep, '<br>')}"
                                #answer=f'<html><head><meta charset="utf-8"><title>test</title></head><body>{body}</body></html>'.encode('utf-8')
                                body=Markup(self.app.manager.quickStatus())
                                template = self.templateEngine.getTemplate('status.html')
                                answer = views.statusView(template, self, self.app, body=body).encode("utf-8")
                                if self.debug:
                                    print(f" @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ answer type: {type(answer)}")
                            elif bits.path == '/input':
                                body=Markup(self.app.manager.inputStatus())
                                template = self.templateEngine.getTemplate('input.html')
                                answer = views.statusView(template, self, self.app, body=body).encode("utf-8")
                            elif bits.path == '/output':
                                body=Markup(self.app.manager.outputStatus())
                                template = self.templateEngine.getTemplate('output.html')
                                answer = views.statusView(template, self, self.app, body=body).encode("utf-8")
                            elif bits.path == '/config':
                                body=Markup(self.app.get_config())
                                template = self.templateEngine.getTemplate('config.html')
                                answer = views.statusView(template, self, self.app, body=body).encode("utf-8")
                            elif bits.path == '/reporting':
                                #body=Markup(self.app.do_test(path=bits.path, query=bits.query, wc=wc))
                                body=Markup(self.app.do_test(path=bits.path, query=bits.query, wc=wc))

                                size=wc['size'] if 'size' in wc else 0


                                info=f"Daily report(s) of today (size: {size})"
                                if len(bits.query) > 0:
                                    deltaDays = bits.query.split('=')[1]
                                    info=f"Daily report(s) of day: {deltaDays} (size: {size})"

                                template = self.templateEngine.getTemplate('reporting.html')
                                answer = views.statusView(template, self, self.app, body=body, info=info).encode("utf-8")
                            elif bits.path == '/test':
                                body=Markup(self.app.do_test())
                                template = self.templateEngine.getTemplate('test.html')
                                answer = views.statusView(template, self, self.app, body=body).encode("utf-8")
                            elif bits.path == '/':
                                # send index.html, use template
                                template = self.templateEngine.getTemplate('index.html')
                                answer = views.indexView(template, self, self.app).encode("utf-8")
                                if self.debug:
                                    print(f" @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ answer type: {type(answer)}")
                        except TemplateNotFound as te:
                            print(f" @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ Exception as e 0: {str(te)}")
                            traceback.print_exc(file=sys.stdout)
                            answer = f"template not found: {str(te)}"
                            self.logger.error(answer)
                        except Exception as e:
                            print(f" @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ Exception as e 1: {str(e)}")
                            traceback.print_exc(file=sys.stdout)
                            answer = str(e)
                            self.logger.error(answer)

                        self.send_answer(answer, 404, format)
                except Exception as e:
                    print(f" @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ Exception as e 2: {e}")
                    traceback.print_exc(file=sys.stdout)
                    self.logger.error(f"{e}")
                    self.send_answer(e, 404, format)


            def do_POST(self):
                self.logger.info("POST request")
                if self.path == '/test':
                    acknowledge_data = self.rfile.read(int(self.headers['Content-Length'])).decode("utf-8")
                    data = json.loads(acknowledge_data)
                    answer=f'<html><head><meta charset="utf-8"><title>test</title></head><body>{data}</body></html>'.encode('utf-8')
                else:
                    answer = None
                self.send_answer(answer, 503)

            def log_message(self, format, *args):
                print(f"log_message; format: {format}; args: {args}")

            #
            # send back a file
            #
            def sendFileReply(self, aPath):
                mimeTypes = mimetypes.MimeTypes().guess_type(os.path.basename(aPath))
                print(f" ############ mimes type: {mimeTypes}")
                self.logger.info(f"sendFileReply; aPath={aPath}")
                aSize = os.stat(aPath).st_size
                self.send_response(200)
                self.send_header('Content-type', mimeTypes[0])
                self.send_header('Content-length', aSize)
                self.end_headers()
                with file_chunks(aPath, 1024) as chunks:
                    for chunk in chunks:
                        # process the chunk
                        self.wfile.write(chunk)

            #
            # TODO: change
            #  send an html or json reply
            #
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

        return ServerHandler


## TODO remove
def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))
