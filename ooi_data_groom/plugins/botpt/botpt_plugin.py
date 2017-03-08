import json
import logging
import uuid
from operator import attrgetter

import datetime
import ion_functions
import numpy as np
import pandas as pd
from ooi_data.postgres.model import Stream

from ooi_data_groom.cassandra.data import fetch_bin, insert_dataframe, delete_dataframe
from ooi_data_groom.cassandra.provenance import insert_l0_provenance, fetch_l0_provenance, ProvTuple
from ooi_data_groom.plugins.botpt.common import make_15s, make_24h
from ooi_data_groom.plugins.manager import PluginProvider
from ooi_data_groom.postgres.queries import (find_bins_by_time, update_partition, update_stream_metadata,
                                             set_partition, recreate_stream_metadata, record_processing_metadata,
                                             find_modified_bins_by_jobname, find_previous_bin, find_next_bin)


log = logging.getLogger(__name__)


ION_VERSION = getattr(ion_functions, '__version__', 'unversioned')
NTP_EPOCH = datetime.datetime(1900, 1, 1)
UNIX_EPOCH = datetime.datetime(1970, 1, 1)
NTP_OFFSET = (UNIX_EPOCH - NTP_EPOCH).total_seconds()


class BotptPrecompute(object):
    def __init__(self, session, job_name, base_stream, output_stream, overlap_seconds, source_time, output_time, cols):
        self.session = session
        self.overlap_seconds = overlap_seconds
        self.job_name = job_name
        self.base_stream = base_stream
        self.output_stream = output_stream
        self.source_time = source_time
        self.output_time = output_time
        self.query_cols = cols

    def execute(self):
        log.info('Starting job: %s', self.job_name)
        for metadata_record in self.find_bins():
            try:
                result, provenance = self.process_bin(metadata_record)
                computed_provenance = self.generate_provenance(metadata_record, provenance)
                self.insert_precomputed_botpt_data(result, metadata_record, computed_provenance)
            except KeyboardInterrupt:
                break
            except StandardError:
                log.exception('')

    def find_bins(self):
        bins_to_process = set()

        log.info('Finding modified bins for job: %s', self.job_name)
        for metadata_record in find_modified_bins_by_jobname(self.session, self.job_name, stream=self.base_stream):
            # If a botpt bin has been modified both it and the surrounding bins must be recalculated
            bins_to_process.add(metadata_record)
            next_record = find_next_bin(self.session, metadata_record, max_elapsed_seconds=self.overlap_seconds)
            if next_record is not None:
                bins_to_process.add(next_record)
            prev_record = find_previous_bin(self.session, metadata_record, max_elapsed_seconds=self.overlap_seconds)
            if prev_record is not None:
                bins_to_process.add(prev_record)
        return sorted(bins_to_process, key=attrgetter('sensor', 'bin'))

    def process_bin(self, metadata_record):
        log.info('BOTPT precompute processing bin: %r', metadata_record)
        previous_bin = find_previous_bin(self.session, metadata_record, max_elapsed_seconds=self.overlap_seconds)
        next_bin = find_next_bin(self.session, metadata_record, max_elapsed_seconds=self.overlap_seconds)

        dataframe = fetch_bin(metadata_record.subsite, metadata_record.node, metadata_record.sensor,
                              metadata_record.method, metadata_record.stream, metadata_record.bin, self.query_cols)
        prov_values = set(dataframe.provenance.values)
        prov_values.discard(None)
        provenance = fetch_l0_provenance(metadata_record, prov_values, 0)

        dataframes = []

        if previous_bin is not None:
            min_time = metadata_record.first - datetime.timedelta(seconds=self.overlap_seconds)
            previous_dataframe = fetch_bin(previous_bin.subsite, previous_bin.node, previous_bin.sensor,
                                           previous_bin.method, previous_bin.stream, previous_bin.bin, self.query_cols,
                                           min_time=min_time)
            dataframes.append(previous_dataframe)

        dataframes.append(dataframe)

        if next_bin is not None:
            next_dataframe = fetch_bin(next_bin.subsite, next_bin.node, next_bin.sensor,
                                       next_bin.method, next_bin.stream, next_bin.bin, self.query_cols,
                                       max_time=metadata_record.last+datetime.timedelta(seconds=self.overlap_seconds))
            dataframes.append(next_dataframe)

        dataframe = pd.concat(dataframes).sort_values(self.source_time)
        return self.trim_data_to_bin(self.precompute(dataframe), metadata_record), provenance

    def trim_data_to_bin(self, dataframe, metadata_record):
        last = (metadata_record.last - NTP_EPOCH).total_seconds()
        index = (dataframe[self.output_time] >= metadata_record.bin) & (dataframe[self.output_time] < last)
        trimmed_dataframe = dataframe[index]
        log.debug('trimming dataframe to bin bounds - starting size: %d final_size: %d',
                  dataframe[self.output_time].size, trimmed_dataframe[self.output_time].size)
        return trimmed_dataframe

    def generate_provenance(self, metadata_record, provenance):
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

    def insert_precomputed_botpt_data(self, dataframe, metadata_record, computed_provenance):
        """
        Delete any previously precomputed data matching this dataframe
        Insert new computed_provenance
        Insert new precomputed data
        :param dataframe:
        :param metadata_record:
        :param computed_provenance:
        :return:
        """
        cols = ['time', 'deployment', 'id']
        store = 'cass'
        precomputed_binsize = self.session.query(Stream).filter(
            Stream.name == self.output_stream).first().binsize_minutes * 60

        insert_l0_provenance(computed_provenance)
        # assign provenance values to each data point
        dataframe['provenance'] = [computed_provenance.id for _ in dataframe.time.values]

        first_ntp = dataframe[self.output_time].min()
        last_ntp = dataframe[self.output_time].max()
        first = datetime.datetime.utcfromtimestamp(first_ntp - NTP_OFFSET)
        last = datetime.datetime.utcfromtimestamp(last_ntp - NTP_OFFSET)

        bins = find_bins_by_time(self.session, metadata_record.subsite, metadata_record.node, metadata_record.sensor,
                                 metadata_record.method, self.output_stream, 'cass', first, last)

        delete_count = 0
        for each in bins:
            # fetch ALL existing data from this bin
            current_data = fetch_bin(each.subsite, each.node, each.sensor, each.method, each.stream,
                                     each.bin, cols=cols)
            # subset data where we plan on replacing data
            delete_mask = (current_data.time >= first_ntp) & (current_data.time <= last_ntp)
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
                set_partition(self.session, each.subsite, each.node, each.sensor, each.method, each.stream, each.store,
                              each.bin, post_delete_first, post_delete_last, post_delete_count)
                delete_count += size

        # recreate the stream metadata from the aggregate partitions (if we deleted anything)
        if delete_count:
            recreate_stream_metadata(self.session, metadata_record.subsite, metadata_record.node,
                                     metadata_record.sensor, metadata_record.method, self.output_stream)

        # insert our new data
        results = insert_dataframe(metadata_record.subsite, metadata_record.node, metadata_record.sensor,
                                   metadata_record.method, self.output_stream, 0, precomputed_binsize, dataframe)

        # update the metadata records
        for bin_number in results:
            first = results[bin_number]['first']
            last = results[bin_number]['last']
            first = datetime.datetime.utcfromtimestamp(first - NTP_OFFSET)
            last = datetime.datetime.utcfromtimestamp(last - NTP_OFFSET)

            count = results[bin_number]['count']
            update_partition(self.session, metadata_record.subsite, metadata_record.node, metadata_record.sensor,
                             metadata_record.method, self.output_stream, store, bin_number, first, last, count)
            update_stream_metadata(self.session, metadata_record.subsite, metadata_record.node, metadata_record.sensor,
                                   metadata_record.method, self.output_stream, first, last, count)

        record_processing_metadata(self.session, metadata_record.id, self.job_name)

    @staticmethod
    def precompute(dataframe):
        raise NotImplemented


class BotptPrecompute15s(BotptPrecompute):
    def __init__(self, session):
        base_stream = 'botpt_nano_sample'
        output_stream = 'botpt_nano_sample_15sec'
        job_name = 'botpt_precompute_15'
        # The algorithm which computes the backwards-looking ten minute mean depth rate
        # requires the previous twenty minutes of data to produce the first value.
        overlap_seconds = 60 * 20  # 20 minutes
        source_time = 'time'
        output_time = 'botsflu_time15s'
        cols = ['time', 'bottom_pressure', 'provenance']

        super(BotptPrecompute15s, self).__init__(session, job_name, base_stream, output_stream,
                                                 overlap_seconds, source_time, output_time, cols)

    @staticmethod
    def precompute(dataframe):
        return make_15s(dataframe)


class BotptPrecompute24h(BotptPrecompute):
    def __init__(self, session):
        base_stream = 'botpt_nano_sample_15sec'
        output_stream = 'botpt_nano_sample_24hour'
        job_name = 'botpt_precompute_24'
        # The algorithm which computes the backwards-looking 8 week mean depth rate
        # requires the previous 8 weeks of data to produce the first value.
        overlap_seconds = 86400 * 7 * 8
        source_time = 'time'
        output_time = 'botsflu_time24h'
        cols = ['time', 'botsflu_meanpres', 'provenance']

        super(BotptPrecompute24h, self).__init__(session, job_name, base_stream, output_stream,
                                                 overlap_seconds, source_time, output_time, cols)

    @staticmethod
    def precompute(dataframe):
        return make_24h(dataframe)


class BotptPlugin(PluginProvider):
    plugin_name = 'botpt_precompute'

    def execute(self, sessionmaker):
        log.info('Executing plugin: %s', self.plugin_name)
        session = sessionmaker()

        BotptPrecompute15s(session).execute()
        BotptPrecompute24h(session).execute()
