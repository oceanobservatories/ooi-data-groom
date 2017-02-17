# coding: utf-8
from geoalchemy2 import Geometry
from sqlalchemy import (BigInteger, Boolean, Column, DateTime, Float, ForeignKey, Integer,
                        LargeBinary, String, Table, Text, UniqueConstraint)
from sqlalchemy import CheckConstraint
from sqlalchemy.orm import relationship

from .base import MetadataBase

metadata = MetadataBase.metadata


class Xevent(MetadataBase):
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


class Xasset(MetadataBase):
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


t_xasset_xremoteresource = Table(
    'xasset_xremoteresource', metadata,
    Column('assetid', ForeignKey(u'xasset.assetid'), nullable=False),
    Column('remoteresourceid', ForeignKey(u'xremoteresource.remoteresourceid'), nullable=False, unique=True)
)


class Xacquisition(Xevent):
    __tablename__ = 'xacquisition'
    eventid = Column(ForeignKey(u'xevent.eventid'), primary_key=True)
    authorizationforpayment = Column(String(256))
    authorizationnumber = Column(String(256))
    invoicenumber = Column(String(128))
    purchaseprice = Column(Float(53))
    purchasedby = Column(String(256))
    receivedfromvendorby = Column(String(128))
    vendoridentification = Column(String(256))
    vendorpointofcontact = Column(String(128))


class Xarray(Xasset):
    __tablename__ = 'xarray'
    assetid = Column(ForeignKey(u'xasset.assetid'), primary_key=True)
    commissiondate = Column(DateTime)
    decommissiondate = Column(DateTime)


class Xcalibration(MetadataBase):
    __tablename__ = 'xcalibration'
    calid = Column(Integer, primary_key=True)
    name = Column(String(255))
    assetid = Column(ForeignKey(u'xinstrument.assetid'))
    instrument = relationship(u'Xinstrument')


class Xassetstatusevent(Xevent):
    __tablename__ = 'xassetstatusevent'
    __status_values = ['FAILED', 'NOT_TRACKED', 'OPERATIONAL', 'REMOVED']
    eventid = Column(ForeignKey(u'xevent.eventid'), primary_key=True)
    location = Column(String(256))
    reason = Column(String(256))
    severity = Column(Integer)
    status = Column(String(64))
    method = Column(String(64))

    __table_args__ = (
        CheckConstraint(status.in_(__status_values)),
    )


class Xstorage(Xevent):
    __tablename__ = 'xstorage'
    eventid = Column(ForeignKey(u'xevent.eventid'), primary_key=True)
    buildingname = Column(String(64))
    performedby = Column(String(256))
    physicallocation = Column(String(64))
    roomidentification = Column(String(64))
    shelfidentification = Column(String(64))


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
    eventid = Column(ForeignKey(u'xevent.eventid'), primary_key=True)
    depth = Column(Float(53))
    latitude = Column(Float(53))
    location = Column(Geometry)
    longitude = Column(Float(53))
    orbitradius = Column(Float(53))


class Xretirement(Xevent):
    __tablename__ = 'xretirement'
    eventid = Column(ForeignKey(u'xevent.eventid'), primary_key=True)
    disposition = Column(String(256))
    reason = Column(String(256))
    retiredby = Column(String(256))


class Xcruiseinfo(Xevent):
    __tablename__ = 'xcruiseinfo'
    eventid = Column(ForeignKey(u'xevent.eventid'), primary_key=True)
    editphase = Column(String(32), nullable=False)
    shipname = Column(String(64), nullable=False)
    uniquecruiseidentifier = Column(String(64), nullable=False, unique=True)


class Xintegrationevent(Xevent):
    __tablename__ = 'xintegrationevents'
    eventid = Column(ForeignKey(u'xevent.eventid'), primary_key=True)
    deploymentnumber = Column(Integer, nullable=False)
    integratedby = Column(String(256))
    node = Column(String(16))
    sensor = Column(String(16))
    subsite = Column(String(16))
    versionnumber = Column(Integer, nullable=False)


class Xcalibrationdatum(Xevent):
    __tablename__ = 'xcalibrationdata'
    eventid = Column(ForeignKey(u'xevent.eventid'), primary_key=True)
    cardinality = Column(Integer)
    comments = Column(String(256))
    dimensions = Column(LargeBinary)
    values = Column(LargeBinary)
    calid = Column(ForeignKey(u'xcalibration.calid'))
    xcalibration = relationship(u'Xcalibration')


class Xdeployment(Xevent):
    __tablename__ = 'xdeployment'
    __table_args__ = (
        UniqueConstraint('subsite', 'node', 'sensor', 'deploymentnumber', 'versionnumber'),
    )
    eventid = Column(ForeignKey(u'xevent.eventid'), primary_key=True)
    deployedby = Column(String(128))
    deploymentnumber = Column(Integer, nullable=False)
    editphase = Column(String(32), nullable=False)
    inductiveid = Column(Integer)
    depth = Column(Float(53))
    latitude = Column(Float(53))
    location = Column(Geometry)
    longitude = Column(Float(53))
    orbitradius = Column(Float(53))
    waterdepth = Column(Float(53))
    recoveredby = Column(String(128))
    node = Column(String(16))
    sensor = Column(String(16))
    subsite = Column(String(16))
    versionnumber = Column(Integer, nullable=False)
    deploycruiseid = Column(ForeignKey(u'xcruiseinfo.eventid'))
    massetid = Column(ForeignKey(u'xmooring.assetid'))
    nassetid = Column(ForeignKey(u'xnode.assetid'))
    recovercruiseid = Column(ForeignKey(u'xcruiseinfo.eventid'))
    sassetid = Column(ForeignKey(u'xinstrument.assetid'))

    deploy_cruise = relationship(u'Xcruiseinfo', primaryjoin='Xdeployment.deploycruiseid == Xcruiseinfo.eventid')
    mooring_asset = relationship(u'Xmooring', primaryjoin='Xdeployment.massetid == Xmooring.assetid')
    node_asset = relationship(u'Xnode', primaryjoin='Xdeployment.nassetid == Xnode.assetid')
    recover_cruise = relationship(u'Xcruiseinfo', primaryjoin='Xdeployment.recovercruiseid == Xcruiseinfo.eventid')
    instrument_asset = relationship(u'Xinstrument', primaryjoin='Xdeployment.sassetid == Xinstrument.assetid')


class Xinstrument(MetadataBase):
    __tablename__ = 'xinstrument'
    assetid = Column('assetid', ForeignKey(u'xasset.assetid'), primary_key=True)
    eventid = Column('eventid', ForeignKey(u'xdeployment.eventid'))
    asset = relationship(Xasset)


class Xmooring(MetadataBase):
    __tablename__ = 'xmooring'
    assetid = Column('assetid', ForeignKey(u'xasset.assetid'), primary_key=True)
    eventid = Column('eventid', ForeignKey(u'xdeployment.eventid'))
    asset = relationship(Xasset)


class Xnode(MetadataBase):
    __tablename__ = 'xnode'
    assetid = Column('assetid', ForeignKey(u'xasset.assetid'), primary_key=True)
    eventid = Column('eventid', ForeignKey(u'xdeployment.eventid'))
    asset = relationship(Xasset)


class Xremoteresource(MetadataBase):
    __tablename__ = 'xremoteresource'
    remoteresourceid = Column(Integer, primary_key=True)
    datasource = Column(String(255))
    lastmodifiedtimestamp = Column(DateTime)
    keywords = Column(String(1024))
    label = Column(String(255))
    resourcenumber = Column(String(255))
    status = Column(String(32))
    url = Column(String(255))
