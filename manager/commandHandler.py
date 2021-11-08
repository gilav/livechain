#
#
#
import os, sys, traceback, time
#
import constants
from process.iProcessor import iProcessor
from api.baseProtocol import Protocol
from api.psProtocol import PsProtocol
import myLoggerConfig

#
proto_ref = {'version': '0xa001', 'caller': None, 'target': None, 'req': None, 'resp': None, 'handler': None,
			 'status': None, 'info': None}
# exists:
COMMAND_Op = "Op"
command_NoOp = "NoOp"
COMMAND_EchoOp = "EchoOp"
COMMAND_InputsOp = "InputsOp"
COMMAND_EosipOp = "EosipOp"
# TODO:
COMMAND_ReportOp = "ReportOp"
COMMAND_RedoOp = "RedoOp"
#
KNOWN_COMMANDS = [COMMAND_Op, command_NoOp, COMMAND_EchoOp, COMMAND_InputsOp, COMMAND_EosipOp]


#
#
#
class CommandHandler(iProcessor):

	def __init__(self, manager):
		self.logger = myLoggerConfig.applyLoggingLevel(self.__class__.__name__, True)
		self.manager = manager

	#
	# Processor interface
	# - args has PsProtocol
	def process(self, **kwargs):
		if not constants.PAYLOAD in kwargs:
			raise Exception(f"no {constants.PAYLOAD}")
		self.logger.info(
			f" ##### process protocol: {kwargs[constants.PAYLOAD]}; type: {type(kwargs[constants.PAYLOAD])}")
		return self.do_payload(kwargs[constants.PAYLOAD])

	#
	# get dict, return proto
	#
	def do_payload(self, payload):
		psp = None
		try:
			psp = PsProtocol()
			psp.fromDict(payload)
			self.logger.debug(f" ##### do_payload; input is proto: {psp}")
			# print(f"######### op: {psp.req[constants.API_OP]}; type: {type(psp.req[constants.API_OP])}")

			## TODO: instanciate class from op name
			if psp.req[constants.API_OP] in KNOWN_COMMANDS:
				if psp.req[constants.API_OP] == command_NoOp:  # do nothing
					pass
				elif psp.req[constants.API_OP] == COMMAND_EchoOp:  # reply with 'echo: xxx'
					psp.resp = f"echo: {psp.req[constants.API_DATA]}"
					psp.status = psp.STATUS_OK
					psp.handler = self.__class__.__name__
					psp.target = psp.caller
				elif psp.req[constants.API_OP] == COMMAND_InputsOp:  # on inputs operations
					# test: get inputs
					psp.resp = self.manager.getInputs()
					psp.status = psp.STATUS_OK
					psp.handler = self.__class__.__name__
					psp.target = psp.caller
				elif psp.req[constants.API_OP] == COMMAND_EosipOp:  # on eosip operations
					pass
			else:
				psp.resp = f"unknown operation: {psp.req}. Has to be in: {KNOWN_COMMANDS}"
				psp.status = psp.STATUS_FAIL
		except Exception as e:
			self.logger.error(f" ##### ##### do_payload; is not proto; error: {e}")
			if psp is None:
				psp = PsProtocol()
				psp.fromDict(proto_ref)
				psp.req = payload
				psp.info = str(e)
			else:
				self.logger.info(f" ##### ##### TODO #### do_payload; error but has proto: {psp}")
		self.logger.debug(f" ##### ##### do_payload; returns proto: {psp}")
		return psp
