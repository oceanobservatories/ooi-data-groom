# coding: utf-8
from sqlalchemy import (BigInteger, Column, Float, Integer, String,
                        UniqueConstraint, DateTime, ForeignKey, Sequence, func)
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy.orm import relationship

from .base import Base

metadata = Base.metadata


class PartitionMetadatum(Base):
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


# ALTER TABLE partition_metadata ADD COLUMN modified TIMESTAMP;
# UPDATE partition_metadata SET modified='1970-1-1'::timestamp;
# CREATE OR REPLACE FUNCTION update_modified_column()
# RETURNS TRIGGER AS $$
# BEGIN
#     NEW.modified = now();
#     RETURN NEW;
# END;
# $$ language 'plpgsql';
# create trigger update_partition_metadata_modtime BEFORE UPDATE ON partition_metadata
# FOR EACH ROW WHEN (new.modified = old.modified OR old.modified is NULL) EXECUTE PROCEDURE update_modified_column();
# create trigger update_partition_metadata_modtime BEFORE INSERT ON partition_metadata
# FOR EACH ROW EXECUTE PROCEDURE update_modified_column();


class StreamMetadatum(Base):
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


class ProcessedMetadatum(Base):
    __tablename__ = 'processed_metadata'
    __table_args__ = (
        UniqueConstraint('processor_name', 'partition_id'),
    )
    id = Column(Integer, primary_key=True)
    processor_name = Column(String, nullable=False)
    processed_time = Column(DateTime, nullable=False)
    partition_id = Column(Integer, ForeignKey('partition_metadata.id', ondelete='CASCADE'))
    partition = relationship(PartitionMetadatum)
