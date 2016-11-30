import os
import unittest
import uuid

import xarray as xr
from cassandra.cluster import Cluster

from ..session import SessionManager
from ..data import insert_dataframe, fetch_bin


DELETE_KEYSPACE = 'drop keyspace %s'
CREATE_KEYSPACE = "create keyspace %s with replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
USE_KEYSPACE = 'use %s'
CREATE_TABLE = """
CREATE TABLE botpt_nano_sample (
    subsite text,
    node text,
    sensor text,
    bin bigint,
    method text,
    time double,
    deployment int,
    id uuid,
    driver_timestamp double,
    ingestion_timestamp double,
    internal_timestamp double,
    port_timestamp double,
    preferred_timestamp text,
    quality_flag text,
    provenance uuid,
    bottom_pressure double,
    date_time_string text,
    press_trans_temp double,
    sensor_id text,
    time_sync_flag text,
    PRIMARY KEY((subsite, node, sensor, bin), method, time, deployment, id)
);
"""


HERE = os.path.dirname(__file__)


class CassandraDataUnitTest(unittest.TestCase):
    @staticmethod
    def create_keyspace(keyspace):
        cluster = Cluster(['127.0.0.1'])
        session = cluster.connect()
        session.execute(DELETE_KEYSPACE % keyspace)
        session.execute(CREATE_KEYSPACE % keyspace)
        session.execute(USE_KEYSPACE % keyspace)
        session.execute(CREATE_TABLE)
        cluster.shutdown()

    @classmethod
    def setUpClass(cls):
        keyspace = 'unittest'
        cls.create_keyspace(keyspace)
        SessionManager.init(['127.0.0.1'], keyspace)

    def test_01_insert_dataframe(self):
        stream = 'botpt_nano_sample'
        ds = xr.open_dataset(os.path.join(HERE, 'botpt_nano_sample_bin_3682368000.nc'), decode_times=False)
        dataframe = ds.to_dataframe()

        import numpy as np
        size = dataframe.time.size
        dataframe['date_time_string'] = ['test'] * size
        dataframe['driver_timestamp'] = dataframe.time.copy()
        dataframe['ingestion_timestamp'] = dataframe.time.copy()
        dataframe['internal_timestamp'] = dataframe.time.copy()
        dataframe['port_timestamp'] = dataframe.time.copy()
        dataframe['preferred_timestamp'] = ['internal_timestamp'] * size
        dataframe['press_trans_temp'] = np.ones(size)
        dataframe['provenance'] = [uuid.uuid4()] * size
        dataframe['quality_flag'] = ['OK'] * size
        dataframe['sensor_id'] = ['NANO'] * size
        dataframe['time_sync_flag'] = ['P'] * size

        subsite = node = sensor = method = 'test'
        binsize = 3600*3
        insert_dataframe(subsite, node, sensor, method, stream, 0, binsize, dataframe)

        count = list(SessionManager.execute_lazy('select count(*) from %s' % stream))[0][0]
        self.assertEqual(count, dataframe.time.size)

    def test_02_fetch_bin(self):
        df = fetch_bin('botpt_nano_sample', ['time', 'bottom_pressure'], 'test', 'test', 'test', 'test', 3682368000)
        print df
        assert False
