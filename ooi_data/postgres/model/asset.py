# coding: utf-8
from geoalchemy2 import Geometry
from sqlalchemy import (BigInteger, Boolean, Column, DateTime, Float, ForeignKey, Integer,
                        LargeBinary, String, Table, Text, UniqueConstraint)
from sqlalchemy.orm import relationship
from sqlalchemy.sql.sqltypes import NullType

from .base import Base

metadata = Base.metadata


class Ingestinfo(Base):
    __tablename__ = 'ingestinfo'

    id = Column(Integer, primary_key=True)
    ingestmask = Column(String(255))
    ingestmethod = Column(String(32))
    ingestpath = Column(String(255))
    ingestqueue = Column(String(255))
    eventid = Column(ForeignKey(u'xdeployment.eventid'))

    xdeployment = relationship(u'Xdeployment')


class Xasset(Base):
    __tablename__ = 'xasset'

    assetid = Column(Integer, primary_key=True)
    datasource = Column(String(255))
    lastmodifiedtimestamp = Column(DateTime)
    assettype = Column(String(24))
    deliverydate = Column(BigInteger)
    deliveryordernumber = Column(String(128))
    depthrating = Column(Float(53))
    description = Column(String(1024))
    editphase = Column(String(32), nullable=False)
    firmwareversion = Column(String(128))
    institutionpropertynumber = Column(String(128))
    institutionpurchaseordernumber = Column(String(128))
    depth = Column(Float(53))
    latitude = Column(Float(53))
    location = Column(Geometry)
    longitude = Column(Float(53))
    orbitradius = Column(Float(53))
    manufacturer = Column(String(128))
    mobile = Column(Boolean)
    modelnumber = Column(String(128))
    name = Column(String(255))
    notes = Column(Text)
    ooipartnumber = Column(String(128))
    ooipropertynumber = Column(String(128))
    ooiserialnumber = Column(String(128))
    owner = Column(String(128))
    height = Column(Float(53))
    length = Column(Float(53))
    weight = Column(Float(53))
    width = Column(Float(53))
    powerrequirements = Column(Float(53))
    purchasedate = Column(BigInteger)
    purchaseprice = Column(Float(53))
    serialnumber = Column(String(128))
    shelflifeexpirationdate = Column(BigInteger)
    softwareversion = Column(String(128))
    uid = Column(String(128), unique=True)

    remoteresources = relationship(u'Xremoteresource', secondary='xasset_xremoteresource')
    # instrument = relationship(u'Xdeployment', secondary='xinstrument', foreign_keys=['Xasset.assetid'])
    events = relationship(u'Xevent', secondary='xasset_xevents')
    # node = relationship(u'Xdeployment', secondary='xnode')
    # mooring = relationship(u'Xdeployment', secondary='xmooring')


class Xarray(Xasset):
    __tablename__ = 'xarray'

    assetid = Column(ForeignKey(u'xasset.assetid'), primary_key=True)


t_xasset_xevents = Table(
    'xasset_xevents', metadata,
    Column('assetid', ForeignKey(u'xasset.assetid'), primary_key=True, nullable=False),
    Column('eventid', ForeignKey(u'xevent.eventid'), primary_key=True, nullable=False, unique=True)
)


t_xasset_xremoteresource = Table(
    'xasset_xremoteresource', metadata,
    Column('assetid', ForeignKey(u'xasset.assetid'), nullable=False),
    Column('remoteresourceid', ForeignKey(u'xremoteresource.remoteresourceid'), nullable=False, unique=True)
)


class Xcalibration(Base):
    __tablename__ = 'xcalibration'

    calid = Column(Integer, primary_key=True)
    name = Column(String(255))
    assetid = Column(ForeignKey(u'xinstrument.assetid'))

    xinstrument = relationship(u'Xinstrument')


class Xevent(Base):
    __tablename__ = 'xevent'

    eventid = Column(Integer, primary_key=True)
    datasource = Column(String(255))
    lastmodifiedtimestamp = Column(DateTime)
    eventname = Column(String(255), nullable=False)
    eventstarttime = Column(BigInteger)
    eventstoptime = Column(BigInteger)
    eventtype = Column(String(255), nullable=False)
    notes = Column(Text)
    assetuid = Column(String(128))


class Xassetstatusevent(Xevent):
    __tablename__ = 'xassetstatusevent'

    location = Column(String(256))
    reason = Column(String(256))
    severity = Column(Integer)
    status = Column(String(64))
    eventid = Column(ForeignKey(u'xevent.eventid'), primary_key=True)


class Xstorage(Xevent):
    __tablename__ = 'xstorage'

    buildingname = Column(String(64))
    performedby = Column(String(256))
    physicallocation = Column(String(64))
    roomidentification = Column(String(64))
    shelfidentification = Column(String(64))
    eventid = Column(ForeignKey(u'xevent.eventid'), primary_key=True)


class Xatvendor(Xevent):
    __tablename__ = 'xatvendor'

    authorizationforpayment = Column(String(256))
    authorizationnumber = Column(String(256))
    invoicenumber = Column(String(128))
    reason = Column(String(256))
    receivedfromvendorby = Column(String(128))
    senttovendorby = Column(String(128))
    vendoridentification = Column(String(256))
    vendorpointofcontact = Column(String(128))
    eventid = Column(ForeignKey(u'xevent.eventid'), primary_key=True)


class Xlocationevent(Xevent):
    __tablename__ = 'xlocationevent'

    depth = Column(Float(53))
    latitude = Column(Float(53))
    location = Column(NullType)
    longitude = Column(Float(53))
    orbitradius = Column(Float(53))
    eventid = Column(ForeignKey(u'xevent.eventid'), primary_key=True)


class Xretirement(Xevent):
    __tablename__ = 'xretirement'

    disposition = Column(String(256))
    reason = Column(String(256))
    retiredby = Column(String(256))
    eventid = Column(ForeignKey(u'xevent.eventid'), primary_key=True)


class Xacquisition(Xevent):
    __tablename__ = 'xacquisition'

    authorizationforpayment = Column(String(256))
    authorizationnumber = Column(String(256))
    invoicenumber = Column(String(128))
    purchaseprice = Column(Float(53))
    purchasedby = Column(String(256))
    receivedfromvendorby = Column(String(128))
    vendoridentification = Column(String(256))
    vendorpointofcontact = Column(String(128))
    eventid = Column(ForeignKey(u'xevent.eventid'), primary_key=True)


class Xcruiseinfo(Xevent):
    __tablename__ = 'xcruiseinfo'

    cruiseidentifier = Column(String(64), nullable=False)
    editphase = Column(String(32), nullable=False)
    shipname = Column(String(64), nullable=False)
    uniquecruiseidentifier = Column(String(64), nullable=False, unique=True)
    eventid = Column(ForeignKey(u'xevent.eventid'), primary_key=True)


class Xintegrationevent(Xevent):
    __tablename__ = 'xintegrationevents'

    deploymentnumber = Column(Integer, nullable=False)
    integratedby = Column(String(256))
    node = Column(String(16))
    sensor = Column(String(16))
    subsite = Column(String(16))
    versionnumber = Column(Integer, nullable=False)
    eventid = Column(ForeignKey(u'xevent.eventid'), primary_key=True)


class Xcalibrationdatum(Xevent):
    __tablename__ = 'xcalibrationdata'

    cardinality = Column(Integer)
    comments = Column(String(256))
    dimensions = Column(LargeBinary)
    values = Column(LargeBinary)
    eventid = Column(ForeignKey(u'xevent.eventid'), primary_key=True)
    calid = Column(ForeignKey(u'xcalibration.calid'))

    xcalibration = relationship(u'Xcalibration')


class Xdeployment(Xevent):
    __tablename__ = 'xdeployment'
    __table_args__ = (
        UniqueConstraint('subsite', 'node', 'sensor', 'deploymentnumber', 'versionnumber'),
    )

    deployedby = Column(String(128))
    deploymentnumber = Column(Integer, nullable=False)
    editphase = Column(String(32), nullable=False)
    inductiveid = Column(Integer)
    depth = Column(Float(53))
    latitude = Column(Float(53))
    # location = Column(NullType)
    longitude = Column(Float(53))
    orbitradius = Column(Float(53))
    recoveredby = Column(String(128))
    node = Column(String(16))
    sensor = Column(String(16))
    subsite = Column(String(16))
    versionnumber = Column(Integer, nullable=False)
    eventid = Column(ForeignKey(u'xevent.eventid'), primary_key=True)
    deploycruiseid = Column(ForeignKey(u'xcruiseinfo.eventid'))
    massetid = Column(ForeignKey(u'xmooring.assetid'))
    nassetid = Column(ForeignKey(u'xnode.assetid'))
    recovercruiseid = Column(ForeignKey(u'xcruiseinfo.eventid'))
    sassetid = Column(ForeignKey(u'xinstrument.assetid'))

    xcruiseinfo = relationship(u'Xcruiseinfo', primaryjoin='Xdeployment.deploycruiseid == Xcruiseinfo.eventid')
    xmooring = relationship(u'Xmooring', primaryjoin='Xdeployment.massetid == Xmooring.assetid')
    xnode = relationship(u'Xnode', primaryjoin='Xdeployment.nassetid == Xnode.assetid')
    xcruiseinfo1 = relationship(u'Xcruiseinfo', primaryjoin='Xdeployment.recovercruiseid == Xcruiseinfo.eventid')
    xinstrument = relationship(u'Xinstrument', primaryjoin='Xdeployment.sassetid == Xinstrument.assetid')


class Xinstrument(Base):
    __tablename__ = 'xinstrument'
    assetid = Column('assetid', ForeignKey(u'xasset.assetid'), primary_key=True)
    eventid = Column('eventid', ForeignKey(u'xdeployment.eventid'))
    asset = relationship(Xasset)


class Xmooring(Base):
    __tablename__ = 'xmooring'
    assetid = Column('assetid', ForeignKey(u'xasset.assetid'), primary_key=True)
    eventid = Column('eventid', ForeignKey(u'xdeployment.eventid'))


class Xnode(Base):
    __tablename__ = 'xnode'
    assetid = Column('assetid', ForeignKey(u'xasset.assetid'), primary_key=True)
    eventid = Column('eventid', ForeignKey(u'xdeployment.eventid'))


class Xremoteresource(Base):
    __tablename__ = 'xremoteresource'

    remoteresourceid = Column(Integer, primary_key=True)
    datasource = Column(String(255))
    lastmodifiedtimestamp = Column(DateTime)
    keywords = Column(String(1024))
    label = Column(String(255))
    resourcenumber = Column(String(255))
    status = Column(String(32))
    url = Column(String(255))
