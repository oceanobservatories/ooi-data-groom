import datetime
import logging
import uuid

import numpy as np
import pandas as pd
from cassandra.concurrent import execute_concurrent_with_args

from session import SessionManager


NTP_EPOCH = datetime.datetime(1900, 1, 1)
UNIX_EPOCH = datetime.datetime(1970, 1, 1)
NTP_OFFSET = (UNIX_EPOCH - NTP_EPOCH).total_seconds()


log = logging.getLogger(__name__)


def get_bin_number(timestamp, binsize):
    return int(timestamp / binsize) * binsize


def fetch_bin(subsite, node, sensor, method, stream, bin_number, cols, min_time=None, max_time=None):
    """
    Fetch the specified bin from the database
    """
    log.info('fetch_bin(%s, %s, %s, %s, %s, %s, %s, %s, %s)',
             subsite, node, sensor, method, stream, bin_number, cols, min_time, max_time)
    s = 'select %s from %s where subsite=? and node=? and sensor=? and method=? and bin=?' % (','.join(cols),
                                                                                              stream)

    args = [subsite, node, sensor, method, bin_number]
    if min_time is not None:
        min_time = (min_time - NTP_EPOCH).total_seconds()
        s += ' and time >= ?'
        args.append(min_time)
    if max_time is not None:
        max_time = (max_time - NTP_EPOCH).total_seconds()
        s += ' and time < ?'
        args.append(max_time)

    ps = SessionManager.prepare(s)
    result = SessionManager.execute_numpy(ps, args)
    df = pd.concat((pd.DataFrame(r, columns=cols) for r in result)).sort_values('time')
    df.index = np.arange(len(df))
    return df


def insert_dataframe(subsite, node, sensor, method, stream, deployment, binsize, dataframe):
    """
    Insert the provided dataframe into cassandra, calculating bin based on binsize
    """
    log.info('insert_dataframe(%s, %s, %s, %s, %s, %s, %s, <DATAFRAME>)',
             subsite, node, sensor,method, stream, deployment, binsize)

    metadata_cols = SessionManager.get_query_columns(stream)
    data_cols = [col for col in dataframe.columns if col in metadata_cols]
    fixed_cols = ['subsite', 'node', 'sensor', 'method', 'deployment']
    fixed_values = "'%s', '%s', '%s', '%s', %d" % (subsite, node, sensor, method, deployment)
    variable_cols = ['bin'] + data_cols

    statement = "INSERT INTO %s (%s, %s) VALUES (%s, %s)" % (
        stream, ','.join(fixed_cols), ','.join(variable_cols), fixed_values, ','.join(('?' for _ in variable_cols)))
    ps = SessionManager.prepare(statement)

    # add bin number to dataframe
    dataframe['bin'] = [get_bin_number(t, binsize) for t in dataframe.time.values]

    def values_generator(df_group):
        for index, row in df_group.iterrows():
            vals = []
            for col in variable_cols:
                val = row[col]
                if isinstance(val, np.ndarray):
                    val = val.tolist()
                vals.append(val)
            yield vals

    inserted = {}
    for bin_number, group in dataframe.groupby('bin'):
        first = group.time.min()
        last = group.time.max()
        count = group.time.size
        log.info('Inserting into %s bin %d first: %.2f last: %.2f count: %d', stream, bin_number, first, last, count)
        results = execute_concurrent_with_args(SessionManager.session(), ps, values_generator(group), concurrency=200)
        success_mask = [success for success, _ in results]
        if not all(success_mask):
            log.error('Unable to insert all records, failed records: %r', group[np.logical_not(success_mask)])
            first = group.time[success_mask].min()
            last = group.time[success_mask].max()
            count = group.time[success_mask].size

        inserted[bin_number] = {'first': first, 'last': last, 'count': count}

    return inserted


def delete_dataframe(dataframe, metadata_record):
    """
    Delete all rows exactly matching the deployment number, time and id from the matching rows in this dataframe
    """
    log.info('delete_dataframe(<DATAFRAME>, %s)', metadata_record)
    query = 'delete from %s where subsite=? and node=? and sensor=? ' \
            'and bin=? and method=? and time=? and deployment=? and id=?' % metadata_record.stream
    query = SessionManager.prepare(query)

    def values_generator(df):
        for index, row in df.iterrows():
            args = (metadata_record.subsite, metadata_record.node, metadata_record.sensor,
                    metadata_record.bin, metadata_record.method, row.time, row.deployment, row.id)
            yield args

    sess = SessionManager.session()
    results = execute_concurrent_with_args(sess, query, values_generator(dataframe), concurrency=200)
    return sum((success for success, _ in results if success))