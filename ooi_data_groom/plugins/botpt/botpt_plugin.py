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
    def __init__(self, session, job_name, base_stream, output_stream, overlap_seconds, source_time, output_time,
                 cols, output_stream_derived_cols, subsite=None, node=None, sensor=None, method=None):
        self.session = session
        self.overlap_seconds = overlap_seconds
        self.job_name = job_name
        self.base_stream = base_stream
        self.output_stream = output_stream
        self.source_time = source_time
        self.output_time = output_time
        self.query_cols = cols
        self.output_stream_derived_cols = output_stream_derived_cols
        self.subsite = subsite
        self.node = node
        self.sensor = sensor
        self.method = method

    def execute(self):
        log.info('Starting job: %s', self.job_name)
        for metadata_record in self.find_bins():
            success = False
            try:
                result, provenance = self.process_bin(metadata_record)
                if result.size:
                    computed_provenance = self.generate_provenance(metadata_record, provenance)
                    self.insert_precomputed_botpt_data(result, metadata_record, computed_provenance)
                    success = True
                else:
                    log.info('No precompute records generated from base_stream bin: %r', metadata_record)
            except KeyboardInterrupt:
                break
            except StandardError:
                log.exception('')

            # Always record the processing of the partition_metadata record
            # regardless of whether derived stream data was generated to
            # prevent infinite reprocessing with the same result in subsequent runs
            record_processing_metadata(self.session, metadata_record.id, self.job_name, success)

    def find_bins(self):
        bins_to_process = set()

        log.info('Finding modified bins for job: %s', self.job_name)
        for metadata_record in find_modified_bins_by_jobname(self.session, self.job_name,
                                                             subsite=self.subsite,
                                                             node=self.node,
                                                             sensor=self.sensor,
                                                             method=self.method,
                                                             stream=self.base_stream):
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
                                       max_time=metadata_record.last + datetime.timedelta(seconds=self.overlap_seconds))
            dataframes.append(next_dataframe)

        dataframe = pd.concat(dataframes).sort_values(self.source_time)
        if not dataframe.time.size:
            log.warning('No data found for bin: %r', metadata_record)
            return pd.DataFrame(), provenance
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
        # columns in common to both the 15sec and 24hour streams
        cols = ['time', 'deployment', 'id']
        cols.extend([i for i in self.output_stream_derived_cols if i not in cols])

        store = 'cass'
        precomputed_binsize = self.session.query(Stream).filter(
            Stream.name == self.output_stream).first().binsize_minutes * 60

        insert_l0_provenance(computed_provenance)
        # assign provenance values to each data point
        dataframe['provenance'] = [computed_provenance.id for _ in dataframe.time.values]

        first_ntp = dataframe[self.output_time].min()
        last_ntp = dataframe[self.output_time].max()

        orig_dataframe_size = dataframe[self.output_time].size

        log.info("new dataframe before data comparison: size (%d), min (%d), max(%d)",
                 orig_dataframe_size, first_ntp, last_ntp)

        # convert times to python datetime
        first = datetime.datetime.utcfromtimestamp(first_ntp - NTP_OFFSET)
        last = datetime.datetime.utcfromtimestamp(last_ntp - NTP_OFFSET)

        bins = find_bins_by_time(self.session, metadata_record.subsite, metadata_record.node, metadata_record.sensor,
                                 metadata_record.method, self.output_stream, 'cass', first, last)

        # initialize the new dataframe exclude mask to exclude nothing (eg., insert all records)
        dataframe_exclude_mask = np.zeros_like(dataframe.time, dtype=bool)

        delete_count = 0
        for each in bins:
            # fetch ALL existing data from this bin
            current_data = fetch_bin(each.subsite, each.node, each.sensor, each.method, each.stream,
                                     each.bin, cols=cols)
            # subset data where we plan on replacing data
            delete_mask = (current_data.time >= first_ntp) & (current_data.time <= last_ntp)
            to_delete = current_data[delete_mask]
            remaining = current_data[np.logical_not(delete_mask)]

            delete_size = to_delete.time.size
            if delete_size:
                first_to_delete_ntp = to_delete.time.min()
                last_to_delete_ntp = to_delete.time.max()

                # subset of new data (dataframe) that we will compare to existing data (to_delete)
                # to see if it has changed and needs to be deleted and reinserted
                dataframe_compare_mask = (dataframe.time >= first_to_delete_ntp)\
                                         & (dataframe.time <= last_to_delete_ntp)
                compare_dataframe = dataframe[dataframe_compare_mask]
                compare_size = len(compare_dataframe)

                # compare the old and new data in the time range where they intersect
                # only if they have the same shape in that time range
                if compare_size == delete_size:
                    log.info("dataframe to_delete before data comparison: size (%d), min (%d), max(%d)",
                             delete_size, first_to_delete_ntp, last_to_delete_ntp)

                    # mask to hold the results of the comparison of new and existing data
                    comparison_result_mask = np.ones_like(to_delete.time, dtype=bool)

                    # compare the values in each column using using numpy.isclose() with the default absolute (1e-08)
                    # and relative (1e-05) tolerances which seem appropriate for the actual data in these streams
                    for col in self.output_stream_derived_cols:
                        comparison_result_mask = comparison_result_mask\
                                               & np.isclose(to_delete[col], compare_dataframe[col], equal_nan=True)

                    # index of all of the non-equal rows
                    unequal_row_index = np.nonzero(np.logical_not(comparison_result_mask))[0]
                    if unequal_row_index.size:
                        # get the index of the first non-equal row
                        first_unequal_row_index = unequal_row_index[0]
                        # get the time of the first non-equal row. records in the existing and new dataframes
                        # before this time are equal and do not need to be deleted and reinserted. These
                        # rows of existing data can be simply be retained as they are.
                        first_unequal_date = to_delete.time.iloc[first_unequal_row_index]
                        log.info("current and new dataframe unequal_row_index size (%d), "
                                 "first_unequal_row_index (%d), first_unequal_date (%d)",
                                 unequal_row_index.size, first_unequal_row_index, first_unequal_date)

                        # regenerate the dataframe exclude mask to exclude retained rows so that they are not
                        # inserted again, which would create duplicate records
                        dataframe_exclude_mask |= (dataframe.time >= to_delete.time.iloc[0])\
                                               & (dataframe.time < first_unequal_date)

                        # regenerate the delete mask to exclude retained records since we will not reinsert them
                        # from the new dataframe
                        delete_mask = (current_data.time >= first_unequal_date) & (current_data.time <= last_ntp)
                    else:
                        log.info("current and new dataframes are identical in intersecting region, "
                                 "excluding from delete and reinsert")
                        # regenerate the dataframe exclude mask to exclude retained records
                        dataframe_exclude_mask |= dataframe_compare_mask

                        # regenerate the delete mask to exclude retained records
                        delete_mask = np.logical_not(np.ones_like(delete_mask, dtype=bool))

                    to_delete = current_data[delete_mask]
                    remaining = current_data[np.logical_not(delete_mask)]

                    log.info("dataframe to_delete after data comparison: size (%d), size diff (%d)",
                             len(to_delete), delete_size-len(to_delete))

                    delete_size = len(to_delete)
                else:
                    log.info("dataframe to_delete size (%d) and dataframe to compare size (%d) "
                             "are different, deleting and reinserting entire intersecting region",
                             delete_size, compare_size)

            # If there is still something left to delete
            if delete_size:
                log.info("Deleting current data from %d to %d", to_delete.time.min(), to_delete.time.max())
                # delete the existing data
                delete_dataframe(to_delete, each)
                # find new bin bounds and size
                post_delete_count = remaining.time.size
                post_delete_first = None
                post_delete_last = None

                # We can only set the min and max times if there are records remaining. If there is none,
                # we don't need to set these since the partition_metadata will be deleted anyway
                if post_delete_count:
                    post_delete_first = remaining.time.min()
                    post_delete_last = remaining.time.max()

                    # convert times to python datetime
                    post_delete_first = datetime.datetime.utcfromtimestamp(post_delete_first - NTP_OFFSET)
                    post_delete_last = datetime.datetime.utcfromtimestamp(post_delete_last - NTP_OFFSET)

                # update the partition metadata
                set_partition(self.session, each.subsite, each.node, each.sensor, each.method, each.stream, each.store,
                              each.bin, post_delete_first, post_delete_last, post_delete_count)
                delete_count += delete_size

        # recreate the stream metadata from the aggregate partitions (if we deleted anything)
        if delete_count:
            recreate_stream_metadata(self.session, metadata_record.subsite, metadata_record.node,
                                     metadata_record.sensor, metadata_record.method, self.output_stream)

        dataframe = dataframe[np.logical_not(dataframe_exclude_mask)]

        final_dataframe_size = len(dataframe)
        log.info("new dataframe after data comparison: size (%d), size diff (%d)",
                 final_dataframe_size, orig_dataframe_size-final_dataframe_size)
        if final_dataframe_size:
            log.info("new dataframe after data comparison: min (%d) max(%d)",
                     dataframe.time.min(), dataframe.time.max())

        # insert our new data
        results = insert_dataframe(metadata_record.subsite, metadata_record.node, metadata_record.sensor,
                                   metadata_record.method, self.output_stream, 0, precomputed_binsize, dataframe)

        # update the metadata records
        for bin_number in results:
            first = results[bin_number]['first']
            last = results[bin_number]['last']

            # convert times to python datetime
            first = datetime.datetime.utcfromtimestamp(first - NTP_OFFSET)
            last = datetime.datetime.utcfromtimestamp(last - NTP_OFFSET)

            count = results[bin_number]['count']
            update_partition(self.session, metadata_record.subsite, metadata_record.node, metadata_record.sensor,
                             metadata_record.method, self.output_stream, store, bin_number, first, last, count)
            update_stream_metadata(self.session, metadata_record.subsite, metadata_record.node, metadata_record.sensor,
                                   metadata_record.method, self.output_stream, first, last, count)


    @staticmethod
    def precompute(dataframe):
        raise NotImplemented


class BotptPrecompute15s(BotptPrecompute):
    def __init__(self, session, **kwargs):
        base_stream = 'botpt_nano_sample'
        output_stream = 'botpt_nano_sample_15sec'
        job_name = 'botpt_precompute_15'
        # The algorithm which computes the backwards-looking ten minute mean depth rate
        # requires the previous twenty minutes of data to produce the first value.
        overlap_seconds = 60 * 20  # 20 minutes
        source_time = 'time'
        output_time = 'botsflu_time15s'
        cols = ['time', 'bottom_pressure', 'provenance']
        output_stream_derived_cols = ['time', 'botsflu_10minrate', 'botsflu_5minrate',
                                      'botsflu_meandepth', 'botsflu_meanpres', 'botsflu_predtide']

        super(BotptPrecompute15s, self).__init__(session, job_name, base_stream, output_stream,
                                                 overlap_seconds, source_time, output_time,
                                                 cols, output_stream_derived_cols, **kwargs)

    @staticmethod
    def precompute(dataframe):
        return make_15s(dataframe)


class BotptPrecompute24h(BotptPrecompute):
    def __init__(self, session, **kwargs):
        base_stream = 'botpt_nano_sample_15sec'
        output_stream = 'botpt_nano_sample_24hour'
        job_name = 'botpt_precompute_24'
        # The algorithm which computes the backwards-looking 8 week mean depth rate
        # requires the previous 8 weeks of data to produce the first value.
        overlap_seconds = 86400 * 7 * 8
        source_time = 'time'
        output_time = 'botsflu_time24h'
        cols = ['time', 'botsflu_meanpres', 'provenance']
        output_stream_derived_cols = ['time', 'botsflu_daydepth',
                                      'botsflu_4wkrate', 'botsflu_8wkrate']

        super(BotptPrecompute24h, self).__init__(session, job_name, base_stream, output_stream,
                                                 overlap_seconds, source_time, output_time,
                                                 cols, output_stream_derived_cols, **kwargs)

    @staticmethod
    def precompute(dataframe):
        return make_24h(dataframe)


class BotptPlugin(PluginProvider):
    plugin_name = 'botpt_precompute'

    def execute(self, sessionmaker, **kwargs):
        log.info('Executing plugin: %s', self.plugin_name)
        session = sessionmaker()
        try:
            BotptPrecompute15s(session, **kwargs).execute()
            BotptPrecompute24h(session, **kwargs).execute()
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

