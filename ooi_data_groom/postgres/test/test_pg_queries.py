import datetime
import unittest

from ooi_data.postgres.model import MetadataBase, PartitionMetadatum, ProcessedMetadatum
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database, drop_database

from ooi_data_groom.postgres.queries import find_modified_bins_by_jobname

connection_url = 'postgresql://postgres@localhost:5432/unittest'


class OoiDataUnitTest(unittest.TestCase):
    engine = None
    Session = None

    @classmethod
    def setUpClass(cls):
        cls.engine = engine = create_engine(connection_url, echo=True)
        if not database_exists(engine.url):
            create_database(engine.url, template='template_postgis')
        cls.Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        MetadataBase.metadata.create_all(bind=cls.engine)
        cls.insert_metadata_records()

    @classmethod
    def tearDownClass(cls):
        if database_exists(cls.engine.url):
            drop_database(cls.engine.url)
            pass

    @classmethod
    def insert_metadata_records(cls):
        subsite = node = sensor = method = store = 'test'
        stream = 'botpt_nano_sample'
        modified = datetime.datetime(2000, 1, 2)
        session = cls.Session()
        binsize_seconds = 3* 60 * 60
        starting_bin = 3682368000
        end = starting_bin + binsize_seconds * 20
        for bin_number in xrange(starting_bin, end, binsize_seconds):
            count = binsize_seconds * 20
            last = bin_number + binsize_seconds - 1.0/20
            pmd = PartitionMetadatum(first=bin_number, last=last, bin=bin_number, store=store,
                                     subsite=subsite, node=node, sensor=sensor, method=method,
                                     stream=stream, modified=modified, count=count)
            session.add(pmd)
        session.commit()

    def test_find_modified_bins_since(self):
        session = self.Session()
        subsite = node = sensor = method = 'test'
        stream = 'botpt_nano_sample'
        jobname = 'test_find_modified_bins_since'
        bins = find_modified_bins_by_jobname(session, jobname, subsite=subsite, node=node,
                                             sensor=sensor, method=method, stream=stream).all()
        self.assertEqual(len(bins), 20)

        now = datetime.datetime.utcnow()
        for bin in bins:
            pmd = ProcessedMetadatum(processor_name=jobname, processed_time=now, partition_id=bin.id)
            session.add(pmd)
        session.commit()

        bins = find_modified_bins_by_jobname(session, jobname, subsite=subsite, node=node,
                                             sensor=sensor, method=method, stream=stream).all()
        self.assertEqual(len(bins), 0)