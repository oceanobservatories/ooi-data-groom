# coding: utf-8
from sqlalchemy import (Boolean, Column, DateTime, Integer,
                        String)
from sqlalchemy import CheckConstraint

from .base import MetadataBase

metadata = MetadataBase.metadata


class Annotation(MetadataBase):
    __tablename__ = 'annotation'
    __qcflag_values = ['NOT_OPERATIONAL', 'NOT_AVAILABLE', 'PENDING_INGEST',
                       'NOT_EVALUATED', 'SUSPECT', 'FAIL', 'PASS']

    id = Column(Integer, primary_key=True)
    annotation = Column(String(255), nullable=False)
    begindt = Column(DateTime, nullable=False)
    enddt = Column(DateTime, nullable=False)
    exclusionflag = Column(Boolean, nullable=False)
    method = Column(String(255))
    node = Column(String(255))
    sensor = Column(String(255))
    stream = Column(String(255))
    subsite = Column(String(255), nullable=False)
    source = Column(String(255))
    qcflag = Column(String(64))

    __table_args__ = (
        CheckConstraint(qcflag.in_(__qcflag_values)),
    )
