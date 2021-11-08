#
#
#
#
from sqlalchemy import Column, String, Text, Integer, BigInteger, Boolean, Date, DateTime, Table, ForeignKey, Float
from sqlalchemy.orm import relationship
#
import datetime
#
from db.dbLayer import Base

#
# input table containing basic info on incoming products
#
class SourceProduct(Base):
	__tablename__ = 'inputs'
	id = Column(Integer, primary_key=True)
	at = Column(Float)
	fullpath = Column(String(512))
	filename = Column(String(128))
	size = Column(BigInteger())
	hashkey = Column(String(40))
	addeddate = Column(DateTime, default=datetime.datetime.utcnow)
	comment = Column(String(1024))
	eosipdone = Column(Boolean(False))
	prodid = Column(String(36))
	eosipname = Column(String(120))
	eosipsize = Column(BigInteger())
	eosiphashkey = Column(String(40))
	eosipdoneat = Column(Float)
	valid = Column(Boolean(False))
	selected = Column(Boolean(False))

	def as_dict(self):
		return {c.name: str(getattr(self, c.name)) for c in self.__table__.columns}

#
# containing EoSip products
#
class EoSipProduct(Base):
	__tablename__ = 'eosips'
	id = Column(Integer, primary_key=True)
	at = Column(Float)
	prodid = Column(String(36))
	fullpath = Column(String(512))
	filename = Column(String(128))
	size = Column(BigInteger())
	hashkey = Column(String(40))
	addeddate = Column(DateTime, default=datetime.datetime.utcnow)
	comment = Column(String(1024))
	valid = Column(Boolean(False))
	selected = Column(Boolean(False))

	def as_dict(self):
		return {c.name: str(getattr(self, c.name)) for c in self.__table__.columns}