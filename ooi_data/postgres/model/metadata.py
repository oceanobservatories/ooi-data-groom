# coding: utf-8
from sqlalchemy import (BigInteger, Column, Float, Integer, String,
                        UniqueConstraint, DateTime, ForeignKey, Sequence)
from sqlalchemy.orm import relationship

from .base import MetadataBase

metadata = MetadataBase.metadata


class PartitionMetadatum(MetadataBase):
    __tablename__ = 'partition_metadata'
    __table_args__ = (
        UniqueConstraint('subsite', 'node', 'sensor', 'method', 'stream', 'bin', 'store'),
    )
    id = Column(Integer, Sequence('partition_metadata_seq'), primary_key=True)
    subsite = Column(String(16), nullable=False)
    node = Column(String(16), nullable=False)
    sensor = Column(String(16), nullable=False)
    method = Column(String(255), nullable=False)
    stream = Column(String(255), nullable=False)
    store = Column(String(255), nullable=False)
    bin = Column(BigInteger, nullable=False)
    count = Column(BigInteger, nullable=False)
    first = Column(Float, nullable=False)
    last = Column(Float, nullable=False)
    modified = Column(DateTime)

    def __repr__(self):
        return str({'id': self.id, 'bin': self.bin, 'count': self.count, 'first': self.first, 'last': self.last,
                    'method': self.method, 'node': self.node, 'sensor': self.sensor, 'subsite': self.subsite,
                    'store': self.store, 'stream': self.stream, 'modified': self.modified})


class StreamMetadatum(MetadataBase):
    __tablename__ = 'stream_metadata'
    __table_args__ = (
        UniqueConstraint('subsite', 'node', 'sensor', 'method', 'stream'),
    )

    id = Column(Integer, Sequence('stream_metadata_seq'), primary_key=True)
    count = Column(BigInteger, nullable=False)
    first = Column(Float, nullable=False)
    last = Column(Float, nullable=False)
    method = Column(String(255), nullable=False)
    node = Column(String(16), nullable=False)
    sensor = Column(String(16), nullable=False)
    subsite = Column(String(16), nullable=False)
    stream = Column(String(255), nullable=False)


class ProcessedMetadatum(MetadataBase):
    __tablename__ = 'processed_metadata'
    __table_args__ = (
        UniqueConstraint('processor_name', 'partition_id'),
    )
    id = Column(Integer, primary_key=True)
    processor_name = Column(String, nullable=False)
    processed_time = Column(DateTime, nullable=False)
    partition_id = Column(Integer, ForeignKey('partition_metadata.id', ondelete='CASCADE'))
    partition = relationship(PartitionMetadatum)


triggers = """
ALTER TABLE partition_metadata ADD COLUMN modified TIMESTAMP;

UPDATE partition_metadata pm1
SET modified = (
    SELECT TIMESTAMP '1900-1-1' + last * INTERVAL '1 second'
    FROM partition_metadata pm2
    WHERE pm1.id = pm2.id)
WHERE modified is null
AND first > 2208988800 AND last > 2208988800
AND first < 2208988800 + EXTRACT(EPOCH FROM NOW())
AND last < 2208988800 + EXTRACT(EPOCH FROM NOW());

UPDATE partition_metadata pm1
SET modified = TIMESTAMP '1970-1-1'
WHERE modified is null;

CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.modified = now();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_partition_metadata_modtime BEFORE UPDATE ON partition_metadata
FOR EACH ROW WHEN (new.modified = old.modified OR old.modified is NULL) EXECUTE PROCEDURE update_modified_column();

CREATE TRIGGER insert_partition_metadata_modtime BEFORE INSERT ON partition_metadata
FOR EACH ROW EXECUTE PROCEDURE update_modified_column();
"""