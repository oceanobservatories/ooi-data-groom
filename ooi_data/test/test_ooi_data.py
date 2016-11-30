import unittest

import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database, drop_database

from ooi_data.postgres.data import find_modified_bins_by_jobname
from ooi_data.postgres.model import Base, Parameter, Stream, PartitionMetadatum, ProcessedMetadatum
from ooi_data.preload_database.load_preload import read_csv_data, update_db

connection_url = 'postgresql://postgres@localhost:5432/unittest'


class OoiDataUnitTest(unittest.TestCase):
    engine = None
    Session = None

    @classmethod
    def setUpClass(cls):
        cls.engine = engine = create_engine(connection_url, echo=True)
        if not database_exists(engine.url):
            create_database(engine.url)
        cls.Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        Base.metadata.create_all(bind=cls.engine)
        read_csv_data()
        update_db(cls.Session())
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
        botpt = session.query(Stream).get(87)
        binsize_seconds = botpt.binsize_minutes * 60
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

    def test_fetch_parameter(self):
        session = self.Session()
        p908 = session.query(Parameter).get(908)
        self.assertEqual(p908.id, 908)
        self.assertEqual(p908.name, 'seawater_temperature')
        self.assertEqual(p908.parameter_function_id, 35)
        self.assertEqual(p908.parameter_function_map, {u'a1': u'CC_a1', u'a0': u'CC_a0', u'a3': u'CC_a3',
                                                       u'a2': u'CC_a2', u't0': u'PD193'})

    def test_fetch_stream(self):
        session = self.Session()
        botpt_nano_sample = session.query(Stream).filter(Stream.name == 'botpt_nano_sample').first()
        self.assertEqual(botpt_nano_sample.id, 87)
        self.assertEqual(botpt_nano_sample.time_parameter, 7)
        self.assertEqual(botpt_nano_sample.stream_content, 'NANO Sensor Data Products')
        self.assertEqual(botpt_nano_sample.stream_type, 'Science')

    def test_stream_dependencies(self):
        session = self.Session()
        botpt_nano_sample = session.query(Stream).filter(Stream.name == 'botpt_nano_sample').first()
        botpt_nano_sample_15s = session.query(Stream).filter(Stream.name == 'botpt_nano_sample_15s').first()
        self.assertEqual(botpt_nano_sample_15s.id, 741)
        self.assertEqual(botpt_nano_sample_15s.source_streams, [botpt_nano_sample])

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