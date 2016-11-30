# coding: utf-8
from sqlalchemy import (BigInteger, Boolean, Column, DateTime, Float, ForeignKey, Index, Integer,
                        String, Text, UniqueConstraint)
from sqlalchemy.orm import relationship
from sqlalchemy.sql.sqltypes import NullType

from .base import Base

metadata = Base.metadata


class Annotation(Base):
    __tablename__ = 'annotation'

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


class ClusterTask(Base):
    __tablename__ = 'cluster_task'

    details = Column(String(256), primary_key=True, nullable=False)
    name = Column(String(256), primary_key=True, nullable=False)
    extrainfo = Column(String(256))
    lastexecution = Column(BigInteger)
    running = Column(Boolean)


class CommonObsSpatial(Base):
    __tablename__ = 'common_obs_spatial'

    gid = Column(String(32), primary_key=True, index=True)
    aerodromeflag = Column(String(1))
    catalogtype = Column(Integer, nullable=False)
    country = Column(String(32))
    elevation = Column(Integer)
    icao = Column(String(16), index=True)
    the_geom = Column(NullType)
    name = Column(String(255))
    pressurelevel = Column(String(16))
    rbsnindicator = Column(String(4))
    state = Column(String(32))
    stationid = Column(String(16), nullable=False)
    upperairelevation = Column(Integer)
    upperairgeom = Column(NullType)
    wmoindex = Column(Integer)
    wmoregion = Column(Integer)


class IngestHistory(Base):
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


class Keyword(Base):
    __tablename__ = 'keywords'

    keyword = Column(String(255), primary_key=True)
    mapping = Column(Text)


class Level(Base):
    __tablename__ = 'level'
    __table_args__ = (
        UniqueConstraint('masterlevel_name', 'levelonevalue', 'leveltwovalue'),
    )

    id = Column(BigInteger, primary_key=True)
    levelonevalue = Column(Float(53), nullable=False)
    leveltwovalue = Column(Float(53), nullable=False)
    masterlevel_name = Column(ForeignKey(u'level_master.name'), nullable=False)

    level_master = relationship(u'LevelMaster')


class LevelMaster(Base):
    __tablename__ = 'level_master'

    name = Column(String(255), primary_key=True)
    description = Column(String(255))
    type = Column(String(255))
    unit = Column(String(255))


class Ooiuser(Base):
    __tablename__ = 'ooiuser'

    userkey = Column(Integer, primary_key=True)
    streamenginelogging = Column(Boolean, nullable=False)
    emailaddress = Column(String(128))
    username = Column(String(64), nullable=False, unique=True)


class PluginInfo(Base):
    __tablename__ = 'plugin_info'

    name = Column(String(255), primary_key=True)
    database = Column(String(255))
    initialized = Column(Boolean)
    tablename = Column(String(255))


class QcParameter(Base):
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


class Refdestranslate(Base):
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


class Remoteresource(Base):
    __tablename__ = 'remoteresource'

    remoteresourceid = Column(Integer, primary_key=True)
    datasource = Column(String(255))
    lastmodifiedtimestamp = Column(DateTime)
    label = Column(String(255))
    resourcenumber = Column(String(255))
    url = Column(String(255))


class SensorSubscription(Base):
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


class StreamEngineAsyncRequest(Base):
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


class StreamEngineHeuristic(Base):
    __tablename__ = 'stream_engine_heuristics'

    stream = Column(String(255), primary_key=True, nullable=False)
    type = Column(String(255), primary_key=True, nullable=False)
    value = Column(Integer, nullable=False)


class StreamEngineJob(Base):
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


class Vocab(Base):
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


