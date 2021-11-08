#
# sqlalchemy base class implementation
#
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.util import has_identity
import sqlalchemy as sa

import os

engine = None # create_engine('postgresql://usr:pass@localhost:5432/sqlalchemy')
name = None
Session = None #sessionmaker(bind=engine)
print("creating Base")
Base = declarative_base()


#
#
#
def init(aName=None, anUrl=None):
    print(" sqlAlchemylayer.init")
    global name, engine, Session
    engine = create_engine(anUrl)
    Session = sessionmaker(bind=engine)
    name = aName


#
#
#
def createDbSchema():
    print(" sqlAlchemylayer.createDbSchema: engine=%s" % (engine))
    # - generate database schema for all known
    Base.metadata.create_all(engine)
    print(" sqlAlchemylayer.createDbSchema; schema created")

#
#
#
def databaseIsEmpty():
    table_names = sa.inspect(engine).get_table_names()
    print(" databaseIsEmpty check; table_names=%s" % table_names)
    is_empty = table_names == []
    return is_empty

#
# test if model exists in db
#
def modelExistsInDb(aModel):
    print(" @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ modelExistsInDb on:%s" % aModel)
    if not has_identity(aModel):
        print(" @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@  @@##@@ model has no identity!!")
    else:
        print(" @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@  @@##@@ model has identity!!")
    try:
        totalInDb(aModel)
        print(" @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@  @@##@@ model exists in db yes!")
    except Exception as e:
        # FATAL for now
        print(" @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@  @@##@@ modelExistsInDb Exception: %s" % e)
        raise e


#
# total of model record in db
#
def totalInDb(aModel):
    print(" @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ totalInDb")
    try:
        aSession = Session()
        total = len(aSession.query(aModel).all())
        print(" @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@  @@##@@ totalInDb:%s" % total)
        return total
    except Exception as e:
        # FATAL for now
        print(" @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@  @@##@@ totalInDb Exception: %s" % e)
        raise e

#
#
#
def info():
    return "sqlalchemyLayer: name:%s; engine=%s; Session=%s; Base=%s" % (name, engine, Session, Base)



