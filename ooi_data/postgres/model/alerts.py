# coding: utf-8
from sqlalchemy import (Column, Integer, String, Text, BigInteger, LargeBinary, Float, Boolean)

from .base import Base

metadata = Base.metadata

class AlertAlarm(Base):
    __tablename__ = 'alert_alarms'

    eventid = Column(BigInteger, primary_key=True)
    acknowledgetime = Column(BigInteger)
    acknowledged = Column(Boolean, nullable=False)
    acknowledgedby = Column(String(64))
    associatedid = Column(Integer)
    deployment = Column(Integer)
    eventcount = Column(Integer, nullable=False)
    id = Column(LargeBinary)
    maxmessagelen = Column(Integer, nullable=False)
    message = Column(Text, nullable=False)
    method = Column(String(32))
    node = Column(String(8))
    omscomponent = Column(String(32))
    omseventid = Column(String(64))
    omsfttimestamp = Column(Float(53))
    omsgroup = Column(String(32))
    omsplatformclass = Column(String(32))
    omsplatformid = Column(String(32))
    sensor = Column(String(32))
    severity = Column(Integer, nullable=False)
    storetime = Column(BigInteger, nullable=False)
    subsite = Column(String(32))
    time = Column(Float(53))
    type = Column(Integer, nullable=False)


class AlertFilter(Base):
    __tablename__ = 'alert_filter'

    eventid = Column(Integer, primary_key=True)
    description = Column(String(255), nullable=False)
    severity = Column(Integer, nullable=False)
    filter = Column(Integer, nullable=False)
    highval = Column(Float(53))
    lowval = Column(Float(53), nullable=False)
    enabled = Column(Boolean, nullable=False)
    eventreceiptdelta = Column(BigInteger)
    pdid = Column(String(255))
    node = Column(String(16))
    sensor = Column(String(16))
    subsite = Column(String(16))
    stream = Column(String(255))