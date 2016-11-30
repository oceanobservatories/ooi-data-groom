# coding: utf-8
from sqlalchemy import (Column, DateTime, Float, ForeignKey, Integer,
                        LargeBinary, String, Table)
from sqlalchemy.orm import relationship
from sqlalchemy.sql.sqltypes import NullType

from .base import Base

metadata = Base.metadata


class Asset(Base):
    __tablename__ = 'asset'

    assetid = Column(Integer, primary_key=True)
    datasource = Column(String(255))
    lastmodifiedtimestamp = Column(DateTime)
    description = Column(String(255))
    instrumentclass = Column(String(64))
    owner = Column(String(255))
    type = Column(String(64))
    manufacturer = Column(String(255))
    modelnumber = Column(String(64))
    serialnumber = Column(String(64))
    depthratingm = Column(Float)
    heightcm = Column(Float)
    lengthcm = Column(Float)
    requiredpowerw = Column(Float)
    weightoz = Column(Float)
    widthcm = Column(Float)
    deliverydate = Column(DateTime)
    purchasecost = Column(Float)
    purchasedate = Column(DateTime)
    deliveryorder_remoteresourceid = Column(ForeignKey(u'remoteresource.remoteresourceid'))
    purchaseorder_remoteresourceid = Column(ForeignKey(u'remoteresource.remoteresourceid'))

    remoteresource = relationship(u'Remoteresource', primaryjoin='Asset.deliveryorder_remoteresourceid == Remoteresource.remoteresourceid')
    remoteresource1 = relationship(u'Remoteresource', primaryjoin='Asset.purchaseorder_remoteresourceid == Remoteresource.remoteresourceid')
    remoteresource2 = relationship(u'Remoteresource', secondary='asset_remoteresource')


class AssetEvent(Base):
    __tablename__ = 'asset_event'

    eventid = Column(Integer, primary_key=True)
    datasource = Column(String(255))
    lastmodifiedtimestamp = Column(DateTime)
    enddate = Column(DateTime)
    eventdescription = Column(String(255))
    eventtype = Column(Integer)
    recordedby = Column(String(255))
    startdate = Column(DateTime, nullable=False)
    asset_assetid = Column(ForeignKey(u'asset.assetid'), nullable=False)

    asset = relationship(u'Asset')
    attachment = relationship(u'Attachment', secondary='asset_event_attachment')
    asset1 = relationship(u'Asset', secondary='asset_event_integration')
    remoteresource = relationship(u'Remoteresource', secondary='asset_event_remoteresource')


class AssetEventTest(AssetEvent):
    __tablename__ = 'asset_event_test'

    plannumber = Column(String(64))
    eventid = Column(ForeignKey(u'asset_event.eventid'), primary_key=True)


class AssetEventTag(AssetEvent):
    __tablename__ = 'asset_event_tag'

    organization = Column(String(255))
    tag = Column(String(255))
    eventid = Column(ForeignKey(u'asset_event.eventid'), primary_key=True)


class AssetEventLocation(AssetEvent):
    __tablename__ = 'asset_event_location'

    locationlonlat = Column(NullType)
    locationname = Column(String(255))
    locationtype = Column(String(255))
    eventid = Column(ForeignKey(u'asset_event.eventid'), primary_key=True)

    remoteresource = relationship(u'Remoteresource', secondary='asset_event_location_remoteresource')


class AssetEventStorageEntrance(AssetEventLocation):
    __tablename__ = 'asset_event_storage_entrance'

    eventid = Column(ForeignKey(u'asset_event_location.eventid'), primary_key=True)


class AssetEventDeployment(AssetEventLocation):
    __tablename__ = 'asset_event_deployment'

    cruisenumber = Column(String(64))
    deploymentnumber = Column(Integer)
    deploymentshipname = Column(String(255))
    mooring = Column(String(64))
    recoverycruisenumber = Column(String(64))
    recoverylocationlonlat = Column(LargeBinary)
    recoveryshipname = Column(String(255))
    node = Column(String(16))
    sensor = Column(String(16))
    subsite = Column(String(16))
    eventid = Column(ForeignKey(u'asset_event_location.eventid'), primary_key=True)
    cruiseplandocument_remoteresourceid = Column(ForeignKey(u'remoteresource.remoteresourceid'))
    recoveryplandocument_remoteresourceid = Column(ForeignKey(u'remoteresource.remoteresourceid'))

    remoteresource = relationship(u'Remoteresource', primaryjoin='AssetEventDeployment.cruiseplandocument_remoteresourceid == Remoteresource.remoteresourceid')
    remoteresource1 = relationship(u'Remoteresource', primaryjoin='AssetEventDeployment.recoveryplandocument_remoteresourceid == Remoteresource.remoteresourceid')


class AssetEventManufacturerReturn(AssetEventLocation):
    __tablename__ = 'asset_event_manufacturer_return'

    authorizationnumber = Column(String(64))
    eventid = Column(ForeignKey(u'asset_event_location.eventid'), primary_key=True)


class AssetEventOperability(AssetEvent):
    __tablename__ = 'asset_event_operability'

    reason = Column(String(255))
    state = Column(String(32))
    eventid = Column(ForeignKey(u'asset_event.eventid'), primary_key=True)


class AssetEventRetirement(AssetEvent):
    __tablename__ = 'asset_event_retirement'

    disposition = Column(String(32))
    eventid = Column(ForeignKey(u'asset_event.eventid'), primary_key=True)


class Calibrationevent(AssetEvent):
    __tablename__ = 'calibrationevent'

    eventid = Column(ForeignKey(u'asset_event.eventid'), primary_key=True)


class AssetEventInoperability(AssetEvent):
    __tablename__ = 'asset_event_inoperability'

    status = Column(String(32))
    eventid = Column(ForeignKey(u'asset_event.eventid'), primary_key=True)


t_asset_event_attachment = Table(
    'asset_event_attachment', metadata,
    Column('asset_event_eventid', ForeignKey(u'asset_event.eventid'), primary_key=True, nullable=False),
    Column('attachments_attachmentid', ForeignKey(u'attachment.attachmentid'), primary_key=True, nullable=False)
)


t_asset_event_deployment_url = Table(
    'asset_event_deployment_url', metadata,
    Column('deploymentevent_eventid', ForeignKey(u'asset_event_deployment.eventid'), nullable=False),
    Column('doc_url', String(255))
)


t_asset_event_integration = Table(
    'asset_event_integration', metadata,
    Column('eventid', ForeignKey(u'asset_event.eventid'), primary_key=True),
    Column('integratedinto_assetid', ForeignKey(u'asset.assetid'), nullable=False)
)


t_asset_event_location_remoteresource = Table(
    'asset_event_location_remoteresource', metadata,
    Column('asset_event_location_eventid', ForeignKey(u'asset_event_location.eventid'), primary_key=True, nullable=False),
    Column('locationchangenotes_remoteresourceid', ForeignKey(u'remoteresource.remoteresourceid'), primary_key=True, nullable=False)
)


t_asset_event_note = Table(
    'asset_event_note', metadata,
    Column('assetevent_eventid', ForeignKey(u'asset_event.eventid'), nullable=False),
    Column('note', String(4096))
)


t_asset_event_remoteresource = Table(
    'asset_event_remoteresource', metadata,
    Column('asset_event_eventid', ForeignKey(u'asset_event.eventid'), primary_key=True, nullable=False),
    Column('remotedocuments_remoteresourceid', ForeignKey(u'remoteresource.remoteresourceid'), primary_key=True, nullable=False)
)


t_asset_event_test_procedure = Table(
    'asset_event_test_procedure', metadata,
    Column('testevent_eventid', ForeignKey(u'asset_event_test.eventid'), nullable=False),
    Column('number', String(16))
)


t_asset_remoteresource = Table(
    'asset_remoteresource', metadata,
    Column('asset_assetid', ForeignKey(u'asset.assetid'), primary_key=True, nullable=False),
    Column('remotedocuments_remoteresourceid', ForeignKey(u'remoteresource.remoteresourceid'), primary_key=True, nullable=False)
)


class AssetattachmentMetadatum(Base):
    __tablename__ = 'assetattachment_metadata'

    assetattachment_attachmentid = Column(ForeignKey(u'attachment.attachmentid'), primary_key=True, nullable=False)
    key = Column(String(255), primary_key=True, nullable=False)
    type = Column(String(255), primary_key=True, nullable=False)
    value = Column(String(255), primary_key=True, nullable=False)

    attachment = relationship(u'Attachment')


class AsseteventMetadatum(Base):
    __tablename__ = 'assetevent_metadata'

    assetevent_eventid = Column(ForeignKey(u'asset_event.eventid'), primary_key=True, nullable=False)
    key = Column(String(255), primary_key=True, nullable=False)
    type = Column(String(255), primary_key=True, nullable=False)
    value = Column(String(255), primary_key=True, nullable=False)

    asset_event = relationship(u'AssetEvent')


class AssetrecordMetadatum(Base):
    __tablename__ = 'assetrecord_metadata'

    assetrecord_assetid = Column(ForeignKey(u'asset.assetid'), primary_key=True, nullable=False)
    key = Column(String(255), primary_key=True, nullable=False)
    type = Column(String(255), primary_key=True, nullable=False)
    value = Column(String(255), primary_key=True, nullable=False)

    asset = relationship(u'Asset')


class Attachment(Base):
    __tablename__ = 'attachment'

    attachmentid = Column(Integer, primary_key=True)
    datasource = Column(String(255))
    lastmodifiedtimestamp = Column(DateTime)
    data = Column(LargeBinary)


class CalibrationeventCalibrationcoefficient(Base):
    __tablename__ = 'calibrationevent_calibrationcoefficient'

    calibrationevent_eventid = Column(ForeignKey(u'calibrationevent.eventid'), primary_key=True, nullable=False)
    cardinality = Column(Integer)
    dimensions = Column(LargeBinary)
    name = Column(String(255), primary_key=True, nullable=False)
    values = Column(LargeBinary)

    calibrationevent = relationship(u'Calibrationevent')