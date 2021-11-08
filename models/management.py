#
#
#
#
from sqlalchemy import Column, String, Text, Integer, BigInteger, Boolean, Date, DateTime, Table, ForeignKey
from sqlalchemy.orm import relationship
#
import datetime
#
from db.dbLayer import Base

#
#
#
class OperationActions(Base):
	__tablename__ = 'operationactions'

	id = Column(Integer, primary_key=True)
	type_id = Column(Integer, ForeignKey('operationtypes.id'))
	type = relationship("OperationTypes", back_populates="parent")
	description  = Column(String(1024), unique=False, nullable=True)
	at = Column(DateTime, default=datetime.datetime.utcnow)

#
#
#
class OperationTypes(Base):
	__tablename__ = 'operationtypes'
	id = Column(Integer, primary_key=True)
	type = Column(String(40), unique=True, nullable=False)
	description  = Column(String(1024), unique=False, nullable=True)
	parent = relationship("OperationActions", back_populates="type")