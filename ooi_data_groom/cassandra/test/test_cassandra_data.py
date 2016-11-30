import unittest
from functools import partial

import itertools
import pandas as pd
from mock import MagicMock

from ..data import get_bin_number, fetch_bin
from ..session import SessionManager


class CassandraDataUnitTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pass

    def setUp(self):
        SessionManager.prepare = MagicMock(return_value=None)
        SessionManager.execute = MagicMock(return_value=[{'col1': [1, 2, 3]} for _ in range(20)])

    def test_bin_number(self):
        self.assertEqual(get_bin_number(1000, 10), 1000)
        self.assertEqual(get_bin_number(1001, 10), 1000)
        self.assertEqual(get_bin_number(1002, 10), 1000)
        self.assertEqual(get_bin_number(1009, 10), 1000)
        self.assertEqual(get_bin_number(1010, 10), 1010)

    def test_fetch(self):
        for dataframe in fetch_bin('stream', ['col1'], 'subsite', 'node', 'sensor', 'method', 1):
            self.assertIn('col1', dataframe)
            self.assertIsInstance(dataframe, pd.DataFrame)

        expected = 'select col1 from stream where subsite=? and node=? and sensor=? and method=? and bin=?'
        SessionManager.prepare.assert_called_once_with(expected)

        expected = [None, ('subsite', 'node', 'sensor', 'method', 1)]
        SessionManager.execute.assert_called_once_with(*expected)

    def test_fetch_end(self):
        vals = list(fetch_bin('stream', ['col1'], 'subsite', 'node', 'sensor', 'method', 1, count=-10))
        self.assertNotEqual(len(vals), 0)

        expected = 'select col1 from stream where subsite=? and node=? and sensor=? and method=? ' \
                   'and bin=? order by method DESC limit 10'
        SessionManager.prepare.assert_called_once_with(expected)

        expected = [None, ('subsite', 'node', 'sensor', 'method', 1)]
        SessionManager.execute.assert_called_once_with(*expected)

    def test_fetch_begin(self):
        vals = list(fetch_bin('stream', ['col1'], 'subsite', 'node', 'sensor', 'method', 1, count=10))
        self.assertNotEqual(len(vals), 0)

        expected = 'select col1 from stream where subsite=? and node=? and sensor=? and method=? ' \
                   'and bin=? order by method ASC limit 10'
        SessionManager.prepare.assert_called_once_with(expected)

        expected = [None, ('subsite', 'node', 'sensor', 'method', 1)]
        SessionManager.execute.assert_called_once_with(*expected)

    def get_botpt(self):
        cols = ['time', 'bottom_pressure']
        f = partial(fetch_bin, 'botpt_nano_sample', cols, 'RS03CCAL', 'MJ03F', '05-BOTPTA301', 'streamed')
        previous_bin = f(3682400400, count=-18000)
        this_bin = f(3682411200)
        next_bin = f(3682422000, count=6000)

        return pd.concat(itertools.chain(previous_bin, this_bin, next_bin)).sort_values('time')