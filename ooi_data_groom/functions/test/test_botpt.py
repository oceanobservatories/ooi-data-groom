import datetime
import os
import time
import unittest

import xarray as xr

from functions.botpt import make_15s, make_24h, make_15s_optimized

TEST_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(TEST_DIR, 'data')
FNAME = 'RS03CCAL-MJ03F-05-BOTPTA301-streamed-botpt_nano_sample_20160909T000000-20160916T003108.850000.nc'
NTP_DELTA = (datetime.datetime(1970, 1, 1) - datetime.datetime(1900, 1, 1)).total_seconds()


def get_botpt():
    filename = os.path.join(DATA_DIR, 'botpt_nano_sample_bin_3682368000.nc')
    ds = xr.open_dataset(filename, decode_times=False, decode_cf=False)
    ds.attrs = {}
    return ds[['time', 'bottom_pressure']]


class BotptUnitTest(unittest.TestCase):
    @staticmethod
    def get_dataset(filename):
        filename = os.path.join(DATA_DIR, filename)
        return xr.open_dataset(filename, decode_times=False, decode_cf=False)

    @classmethod
    def setUpClass(cls):
        cls.native_dataset = cls.get_dataset('botpt_nano_sample_bin_3682368000.nc')
        cls.fifteen_dataset = cls.get_dataset('botpt_nano_sample_15s_bin_3682368000.nc')

    def test_botpt_15s(self):
        raw_size = self.native_dataset.time.size
        expected_size = raw_size / 300 + 1
        df = make_15s(self.native_dataset)
        self.assertEqual(df.botsflu_time15s.size, expected_size)

    def test_botpt_24h(self):
        expected_size = 1
        df = make_24h(self.fifteen_dataset)
        self.assertEqual(df.botsflu_time24h.size, expected_size)

    def test_calculation_time_15s(self):
        start = time.time()
        make_15s(self.native_dataset)
        elapsed = time.time() - start
        self.assertLessEqual(elapsed, 2)

    def test_calculation_time_15s_optimized(self):
        start = time.time()
        make_15s_optimized(self.native_dataset)
        elapsed = time.time() - start
        self.assertLessEqual(elapsed, 1)
