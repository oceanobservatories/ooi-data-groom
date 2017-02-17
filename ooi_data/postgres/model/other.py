# coding: utf-8
from sqlalchemy import (BigInteger, Boolean, Column, DateTime, Float, Index, Integer,
                        String, Text, UniqueConstraint)

from .base import MetadataBase

metadata = MetadataBase.metadata


class IngestHistory(MetadataBase):
    __tablename__ = 'ingest_history'
    __table_args__ = (
        Index('refdes_idx', 'subsite', 'node', 'sensor'),
    )

    id = Column(Integer, primary_key=True)
    deployment = Column(Integer)
    dequeuetime = Column(BigInteger)
    enqueuetime = Column(BigInteger)
    filename = Column(String(255))
    method = Column(String(255))
    particlecount = Column(Integer)
    node = Column(String(16))
    sensor = Column(String(16))
    subsite = Column(String(16))
    timestamp = Column(BigInteger, nullable=False)


class Keyword(MetadataBase):
    __tablename__ = 'keywords'

    keyword = Column(String(255), primary_key=True)
    mapping = Column(Text)


class Ooiuser(MetadataBase):
    __tablename__ = 'ooiuser'

    userkey = Column(Integer, primary_key=True)
    streamenginelogging = Column(Boolean, nullable=False)
    emailaddress = Column(String(128))
    username = Column(String(64), nullable=False, unique=True)


class QcParameter(MetadataBase):
    __tablename__ = 'qc_parameter'

    qcpid = Column(Integer, primary_key=True)
    parameter = Column(String(255))
    qcid = Column(String(255))
    node = Column(String(16))
    sensor = Column(String(16))
    subsite = Column(String(16))
    streamparameter = Column(String(255))
    value = Column(String(255))
    valuetype = Column(String(255))


class Refdestranslate(MetadataBase):
    __tablename__ = 'refdestranslate'
    __table_args__ = (
        UniqueConstraint('subsite', 'node', 'sensor', 'deploymentnumber'),
    )

    eventid = Column(Integer, primary_key=True)
    deploymentnumber = Column(Integer, nullable=False)
    node = Column(String(8), nullable=False)
    nominalreferencedesignator = Column(String(255), nullable=False)
    sensor = Column(String(32), nullable=False)
    subsite = Column(String(32), nullable=False)


class Remoteresource(MetadataBase):
    __tablename__ = 'remoteresource'

    remoteresourceid = Column(Integer, primary_key=True)
    datasource = Column(String(255))
    lastmodifiedtimestamp = Column(DateTime)
    label = Column(String(255))
    resourcenumber = Column(String(255))
    url = Column(String(255))


class SensorSubscription(MetadataBase):
    __tablename__ = 'sensor_subscription'

    subscriptionid = Column(Integer, primary_key=True)
    email = Column(String(255), nullable=False)
    enabled = Column(Boolean, nullable=False)
    format = Column(String(255), nullable=False)
    interval = Column(Integer, nullable=False)
    laststatus = Column(Integer)
    laststatustext = Column(Text)
    lastrun = Column(BigInteger)
    method = Column(String(255), nullable=False)
    parameters = Column(String(255))
    node = Column(String(16))
    sensor = Column(String(16))
    subsite = Column(String(16))
    stream = Column(String(255), nullable=False)
    username = Column(String(255), nullable=False)


class StreamEngineAsyncRequest(MetadataBase):
    __tablename__ = 'stream_engine_async_request'

    request_uuid = Column(String(255), primary_key=True)
    recipient_email = Column(String(255))
    request_host = Column(String(255))
    is_finished = Column(Boolean)
    method = Column(String(255))
    mime_type = Column(String(255))
    reference_designator = Column(String(255))
    request_time = Column(String(255))
    request_user = Column(String(255))
    stream = Column(String(255))


class StreamEngineHeuristic(MetadataBase):
    __tablename__ = 'stream_engine_heuristics'

    stream = Column(String(255), primary_key=True, nullable=False)
    type = Column(String(255), primary_key=True, nullable=False)
    value = Column(Integer, nullable=False)


class StreamEngineJob(MetadataBase):
    __tablename__ = 'stream_engine_jobs'

    job_uuid = Column(String(255), primary_key=True)
    request_body = Column(Text)
    job_time = Column(DateTime)
    parent_uuid = Column(String(255))
    request_path = Column(String(255))
    request_time = Column(DateTime)
    status = Column(String(255))
    weight = Column(Integer)
    output_url = Column(String(255))


class Vocab(MetadataBase):
    __tablename__ = 'vocab'
    __table_args__ = (
        UniqueConstraint('subsite', 'node', 'sensor'),
    )

    vocabid = Column(Integer, primary_key=True)
    instrument = Column(String(255), nullable=False)
    manufacturer = Column(String(128))
    maxdepth = Column(Float(53))
    mindepth = Column(Float(53))
    model = Column(String(128))
    node = Column(String(16))
    sensor = Column(String(16))
    subsite = Column(String(16), nullable=False)
    tocl1 = Column(String(255), nullable=False)
    tocl2 = Column(String(255), nullable=False)
    tocl3 = Column(String(255), nullable=False)
