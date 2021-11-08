import os, sys, traceback
import logging
from api.baseProtocol import Protocol
from api.psProtocol import PsProtocol

#
aProt_ok = {'version': '0xa001', 'caller': None, 'target': None, 'req': None, 'resp': None, 'handler': None, 'status': None, 'info': None}
aProt_bad = {'version': '0xa003', 'caller': None, 'target': None, 'req': None, 'resp': None, 'handler': None, 'status': None, 'info': None}
aProt_bad2 = {'version': '0xa001', 'target': None, 'req': None, 'resp': None, 'handler': None, 'status': None, 'info': None}

# for redis publish cli:
redis_0= '{"version": "0xa001", "caller": None, "target": None, "req": None, "resp": None, "handler": None, "status": None, "info": None}'



aProt_COMMAND_ECHO = '{"version": "0xa001", "caller": "None", "target": "None", "req": "None", "resp": "None", "handler": "None", "status": "None", "info": "None"}'

def test():
	psp = PsProtocol()
	print(f"psp: {psp}")
	aDict = psp.toDict()
	print(f"psp to dict: {aDict}")

	print("\n")
	psp.fromDict(aProt_ok)

	print("\n")
	psp.fromDict(aProt_bad)

	print("\n")
	psp.fromDict(aProt_bad2)


if __name__ == "__main__":
	try:
		logging.basicConfig(level=logging.DEBUG,
							format='%(asctime)s - %(message)s',
							datefmt='%Y-%m-%d %H:%M:%S')
		test()
		sys.exit(0)
	except Exception as e:
		exc_type, exc_obj, exc_tb = sys.exc_info()
		print(f"Error {exc_type} {exc_obj}")
		traceback.print_exc(file=sys.stdout)
		sys.exit(2)
