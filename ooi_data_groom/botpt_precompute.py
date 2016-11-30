import json
import logging
import uuid
from operator import attrgetter

import ion_functions
import numpy as np
import pandas as pd
from .cassandra.data import fetch_bin, insert_dataframe, delete_dataframe
from .cassandra.session import SessionManager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ooi_data_groom.cassandra.provenance import insert_l0_provenance, fetch_l0_provenance, ProvTuple
from ooi_data_groom.functions.botpt import make_15s
from ooi_data_groom.pg_queries import (find_bins_by_time, update_partition, update_stream_metadata,
                                       set_partition, recreate_stream_metadata, record_processing_metadata,
                                       find_modified_bins_by_jobname, find_previous_bin, find_next_bin)

connection_url = 'postgresql://awips@localhost:5432/metadata'
engine = create_engine(connection_url, echo=False)
Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)

log = logging.getLogger('botpt_precompute')
log.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s: %(levelname)s: %(message)s')

ch = logging.StreamHandler()
ch.setFormatter(formatter)

# Create a file handler and set level to info.
log_filename = 'botpt_precompute.log'
fh = logging.FileHandler(log_filename)
fh.setFormatter(formatter)

# Add fh to the logger.
log.addHandler(fh)
log.addHandler(ch)


pd.set_option('display.width', 160)
ION_VERSION = getattr(ion_functions, '__version__', 'unversioned')
L0_STREAM = 'botpt_nano_sample'
JOB_NAME = 'botpt_precompute'
OVERLAP_SECONDS = 60 * 20  # 20 minutes


def find_bins(session):
    bins_to_process = set()

    log.info('Finding modified bins for job: %s', JOB_NAME)
    for metadata_record in find_modified_bins_by_jobname(session, JOB_NAME, stream=L0_STREAM):
        # If a botpt bin has been modified both it and the surrounding bins must be recalculated
        bins_to_process.add(metadata_record)
        next_record = find_next_bin(session, metadata_record, max_elapsed_seconds=OVERLAP_SECONDS)
        if next_record is not None:
            bins_to_process.add(next_record)
        prev_record = find_previous_bin(session, metadata_record, max_elapsed_seconds=OVERLAP_SECONDS)
        if prev_record is not None:
            bins_to_process.add(prev_record)
    return sorted(bins_to_process, key=attrgetter('sensor', 'bin'))


def trim_data_to_bin(dataframe, metadata_record):
    index = (dataframe.botsflu_time15s >= metadata_record.bin) & (dataframe.botsflu_time15s < metadata_record.last)
    trimmed_dataframe = dataframe[index]
    log.debug('trimming dataframe to bin bounds - starting size: %d final_size: %d',
              dataframe.botsflu_time15s.size, trimmed_dataframe.botsflu_time15s.size)
    return trimmed_dataframe


def process_bin(session, metadata_record):
    log.info('Processing bin: %r', metadata_record)
    cols = ['time', 'bottom_pressure', 'provenance']
    previous_bin = find_previous_bin(session, metadata_record, max_elapsed_seconds=OVERLAP_SECONDS)
    next_bin = find_next_bin(session, metadata_record, max_elapsed_seconds=OVERLAP_SECONDS)

    dataframe = fetch_bin(metadata_record.subsite, metadata_record.node, metadata_record.sensor,
                          metadata_record.method, metadata_record.stream, metadata_record.bin, cols)
    prov_values = set(dataframe.provenance.values)
    prov_values.discard(None)
    provenance = fetch_l0_provenance(metadata_record, prov_values, 0)

    dataframes = []

    if previous_bin is not None:
        previous_dataframe = fetch_bin(previous_bin.subsite, previous_bin.node, previous_bin.sensor,
                                       previous_bin.method, previous_bin.stream, previous_bin.bin, cols,
                                       min_time=metadata_record.first-OVERLAP_SECONDS)
        dataframes.append(previous_dataframe)

    dataframes.append(dataframe)

    if next_bin is not None:
        next_dataframe = fetch_bin(next_bin.subsite, next_bin.node, next_bin.sensor,
                                   next_bin.method, next_bin.stream, next_bin.bin, cols,
                                   max_time=metadata_record.last+OVERLAP_SECONDS)
        dataframes.append(next_dataframe)

    dataframe = pd.concat(dataframes).sort_values('time')
    return trim_data_to_bin(make_15s(dataframe), metadata_record), provenance


def generate_provenance(metadata_record, provenance):
    prov_id = uuid.uuid4()
    file_name = json.dumps(provenance)

    parser_name = 'precomputed using ion_functions.data.prs_functions'
    parser_version = 'precomputed using ion_functions version %s' % ION_VERSION
    provenance = ProvTuple(subsite=metadata_record.subsite,
                           node=metadata_record.node,
                           sensor=metadata_record.sensor,
                           method=metadata_record.method,
                           deployment=0,
                           id=prov_id,
                           filename=file_name,
                           parsername=parser_name,
                           parserversion=parser_version)
    return provenance


def insert_precomputed_botpt_15s_data(session, dataframe, metadata_record, computed_provenance):
    """
    Delete any previously precomputed data matching this dataframe
    Insert new computed_provenance
    Insert new precomputed data
    :param session:
    :param dataframe:
    :param computed_provenance:
    :return:
    """
    cols = ['time', 'deployment', 'id']
    store = 'cass'
    precomputed_stream = 'botpt_nano_15s_precomputed'
    precomputed_binsize = 86400

    insert_l0_provenance(computed_provenance)
    # assign provenance values to each data point
    dataframe['provenance'] = [computed_provenance.id for _ in dataframe.time.values]

    first = dataframe.botsflu_time15s.min()
    last = dataframe.botsflu_time15s.max()

    bins = find_bins_by_time(session, metadata_record.subsite, metadata_record.node, metadata_record.sensor,
                             metadata_record.method, precomputed_stream, 'cass', first, last)

    delete_count = 0
    for each in bins:
        # fetch ALL existing data from this bin
        current_data = fetch_bin(each.subsite, each.node, each.sensor, each.method, each.stream,
                                 each.bin, cols=cols)
        # subset data where we plan on replacing data
        delete_mask = (current_data.time >= first) & (current_data.time <= last)
        to_delete = current_data[delete_mask]
        remaining = current_data[np.logical_not(delete_mask)]

        size = to_delete.time.size
        if size:
            # delete the existing data
            delete_dataframe(to_delete, each)
            # find new bin bounds and size
            post_delete_count = remaining.time.size
            post_delete_first = remaining.time.min()
            post_delete_last = remaining.time.max()

            # update the partition metadata
            set_partition(session, each.subsite, each.node, each.sensor, each.method, each.stream, each.store,
                          each.bin, post_delete_first, post_delete_last, post_delete_count)
            delete_count += size

    # recreate the stream metadata from the aggregate partitions (if we deleted anything)
    if delete_count:
        recreate_stream_metadata(session, metadata_record.subsite, metadata_record.node,
                                 metadata_record.sensor, metadata_record.method, precomputed_stream)

    # insert our new data
    results = insert_dataframe(metadata_record.subsite, metadata_record.node, metadata_record.sensor,
                               metadata_record.method, precomputed_stream, 0, precomputed_binsize, dataframe)

    # update the metadata records
    for bin_number in results:
        first = results[bin_number]['first']
        last = results[bin_number]['last']
        count = results[bin_number]['count']
        update_partition(session, metadata_record.subsite, metadata_record.node, metadata_record.sensor,
                         metadata_record.method, precomputed_stream, store, bin_number, first, last, count)
        update_stream_metadata(session, metadata_record.subsite, metadata_record.node, metadata_record.sensor,
                               metadata_record.method, precomputed_stream, first, last, count)

    record_processing_metadata(session, metadata_record.id, JOB_NAME)


def main():
    log.info('Creating database sessions')
    SessionManager.init(['localhost'], 'ooi')
    session = Session()

    for metadata_record in find_bins(session):
        try:
            result, provenance = process_bin(session, metadata_record)
            computed_provenance = generate_provenance(metadata_record, provenance)
            insert_precomputed_botpt_15s_data(session, result, metadata_record, computed_provenance)
        except KeyboardInterrupt:
            break
        except StandardError:
            log.exception('')


if __name__ == '__main__':
    main()