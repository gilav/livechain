#from  toto import a, init
#import tutu

#print("hello:%s" % init())

#print("hello:%s" % init())

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.util import has_identity
import sqlalchemy as sa
from sqlalchemy import Column, String, Text, Integer, BigInteger, Boolean, Date, DateTime, Table, ForeignKey
import datetime
#

import db.dbLayer as dbLayer

anUrl='sqlite:///db.sqlite3'
aName='test-db'

"""
engine = None # create_engine('postgresql://usr:pass@localhost:5432/sqlalchemy')
name = None
Session = None #sessionmaker(bind=engine)
print("creating Base")
Base = declarative_base()


#
#
#
class Manager(dbLayer.Base):
	__tablename__ = 'manager'

	id = Column(Integer, primary_key=True)
	type = Column(String(40), unique=False, nullable=False)
	description  = Column(String(1024), unique=False, nullable=True)
	at = Column(DateTime, default=datetime.datetime.utcnow)"""

#dbLayer.init(aName, anUrl)

from models.management import Manager

"""
print(" sqlAlchemylayer.init")
engine = create_engine(anUrl)
Session = sessionmaker(bind=engine)
name = aName
"""

dbLayer.init(aName, anUrl)

dbLayer.createDbSchema()

"""
print(" sqlAlchemylayer.createDbSchema: engine=%s" % (dbLayer.engine))
# - generate database schema for all known
dbLayer.Base.metadata.create_all(dbLayer.engine)
print(" sqlAlchemylayer.createDbSchema; schema created")
"""