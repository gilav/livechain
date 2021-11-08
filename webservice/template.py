import json
import logging
import os
from io import StringIO
import jinja2
from datetime import datetime
import myLoggerConfig


class TemplateEngine():

	def __init__(self, templatePath):
		print(f" @@@@@@@@@@@@@@@@ TemplateEngine.__init__ templatePath: {templatePath}")
		self.templateLoader = jinja2.FileSystemLoader(searchpath=templatePath)
		self.templateEnv = jinja2.Environment(loader=self.templateLoader, autoescape=True)

	def getTemplate(self, templateName:str):
		print(f" @@@@@ apply template on file: {templateName}")
		template = self.templateEnv.get_template(templateName)
		return template
		#outputText = template.render()  # this is where to put args to the template renderer
		#return templateName





