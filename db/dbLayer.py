from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.util import has_identity
import sqlalchemy as sa
import myLoggerConfig


"""
Valid SQLite URL forms are:
 sqlite:///:memory: (or, sqlite://)
 sqlite:///relative/path/to/file.db
 sqlite:////absolute/path/to/file.db
 """

debug = False

engine = None # create_engine('postgresql://usr:pass@localhost:5432/dbName')
name = None
Session = None #sessionmaker(bind=engine)
if debug:print("creating Base")
Base = declarative_base()


#
#
#
def init(aName, anUrl):
	if debug:print(" dbLayer.init")
	global name, engine, Session
	engine = create_engine(anUrl)
	Session = sessionmaker(bind=engine)
	name = aName

#
#
#
def createDbSchema():
	if debug:print(" dblayer.createDbSchema: engine=%s" % (engine))
	# - generate database schema for all known
	Base.metadata.create_all(engine)
	if debug:print(" dblayer.createDbSchema; schema created")

#
#
#
def databaseIsEmpty():
	table_names = sa.inspect(engine).get_table_names()
	if debug:print(" dblayer.databaseIsEmpty check; table_names=%s" % table_names)
	is_empty = table_names == []
	return is_empty

#
# test if model exists in db
#
def modelExistsInDb(aModel):
	if debug:print(" @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ modelExistsInDb on:%s" % aModel)
	if not has_identity(aModel):
		if debug:print(" @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@  @@##@@ model has no identity!!")
	else:
		if debug:print(" @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@  @@##@@ model has identity!!")
	try:
		totalInDb(aModel)
		if debug:print(" @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@  @@##@@ model exists in db yes!")
	except Exception as e:
		# FATAL for now
		print(" @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@  @@##@@ modelExistsInDb Exception: %s" % e)
		raise e

#
# total of model record in db
#
def totalInDb(aModel):
	if debug:print(" @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ totalInDb")
	try:
		aSession = Session()
		total = len(aSession.query(aModel).all())
		if debug:print(" @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@  @@##@@ totalInDb:%s" % total)
		return total
	except Exception as e:
		# FATAL for now
		print(" @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@  @@##@@ totalInDb Exception: %s" % e)
		raise e

#
#
#
def info():
	return "dblayer: name:%s; engine=%s; Session=%s; Base=%s" % (name, engine, Session, Base)