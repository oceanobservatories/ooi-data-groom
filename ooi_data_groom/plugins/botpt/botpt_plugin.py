import json
import logging
import uuid
from operator import attrgetter
from collections import namedtuple

import datetime
import ion_functions
import numpy as np
import pandas as pd
from ooi_data.postgres.model import Stream

from ooi_data_groom.cassandra.data import fetch_bin, insert_dataframe, delete_dataframe, get_bin_number
from ooi_data_groom.cassandra.provenance import insert_l0_provenance, fetch_l0_provenance, ProvTuple
from ooi_data_groom.plugins.botpt.common import make_15s, make_24h, ntp_to_datetime, DataNotFoundException
from ooi_data_groom.plugins.manager import PluginProvider
from ooi_data_groom.postgres.queries import (find_bins_by_time, update_partition, update_stream_metadata,
                                             set_partition, recreate_stream_metadata, record_processing_metadata,
                                             find_modified_bins_by_jobname, find_previous_bin, find_next_bin)

log = logging.getLogger(__name__)

ION_VERSION = getattr(ion_functions, '__version__', 'unversioned')
NTP_EPOCH = datetime.datetime(1900, 1, 1)
UNIX_EPOCH = datetime.datetime(1970, 1, 1)
NTP_OFFSET = (UNIX_EPOCH - NTP_EPOCH).total_seconds()

# The stream with predicted tide data
PRED_TIDE_STREAM = 'instrument_predicted_tide'
# Number of seconds to pad the tide data on either side on either side of the source stream dataset time range
# to ensure that we cover the requested time range. Tide data is available in 15 second increments
PRED_TIDE_TIME_RANGE_PAD = 15 * 4


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
            except DataNotFoundException as e:
                log.exception(e)
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

        log.info('Finding modified bins for %s-%s-%s for job: %s' %
                 (self.subsite, self.node, self.sensor, self.job_name))
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
            log.info("previous bin: %d" % previous_bin.bin)
            min_time = metadata_record.first - datetime.timedelta(seconds=self.overlap_seconds)
            previous_dataframe = fetch_bin(previous_bin.subsite, previous_bin.node, previous_bin.sensor,
                                           previous_bin.method, previous_bin.stream, previous_bin.bin, self.query_cols,
                                           min_time=min_time)
            dataframes.append(previous_dataframe)

        dataframes.append(dataframe)

        if next_bin is not None:
            log.info("next bin: %d" % next_bin.bin)
            next_dataframe = fetch_bin(next_bin.subsite, next_bin.node, next_bin.sensor,
                                       next_bin.method, next_bin.stream, next_bin.bin, self.query_cols,
                                       max_time=metadata_record.last + datetime.timedelta(seconds=self.overlap_seconds))
            dataframes.append(next_dataframe)

        dataframe = pd.concat(dataframes).sort_values(self.source_time)
        dataframe.index = np.arange(len(dataframe))
        if not dataframe.time.size:
            log.warning('No data found for bin: %r', metadata_record)
            return pd.DataFrame(), provenance
        dataframe = self.prune_duplicate_times(dataframe)
        return self.trim_data_to_bin(self.precompute(dataframe, metadata_record), metadata_record), provenance

    def prune_duplicate_times(self, dataframe):
        orig_len = dataframe.time.size
        mask = np.diff(np.insert(dataframe.time.values, 0, 0.0)) != 0
        if not mask.all():
            dataframe = dataframe[mask]
            dataframe.index = np.arange(len(dataframe))
        new_len = dataframe.time.size
        log.info("Number of duplicate times removed from source data: %d" % (orig_len - new_len))
        return dataframe

    def trim_data_to_bin(self, dataframe, metadata_record):
        last = (metadata_record.last - NTP_EPOCH).total_seconds()
        index = (dataframe[self.output_time] >= metadata_record.bin) & (dataframe[self.output_time] < last)
        trimmed_dataframe = dataframe[index]
        trimmed_dataframe.index = np.arange(len(trimmed_dataframe))
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
        Delete any previously precomputed data matching this dataframe that has since changed
        Insert new computed_provenance
        Insert new precomputed data
        :param dataframe:
        :param metadata_record:
        :param computed_provenance:
        :return:
        """
        log.info("insert_precomputed_botpt_data (metadata_record, computed_provenance): (%s, %s)" %
                 (metadata_record, computed_provenance))

        # columns in common to both the 15sec and 24hour streams
        cols = ['time', 'deployment', 'id', 'provenance']
        cols.extend([i for i in self.output_stream_derived_cols if i not in cols])

        store = 'cass'
        precomputed_binsize = self.session.query(Stream).filter(
            Stream.name == self.output_stream).first().binsize_minutes * 60

        dataframe['id'] = [uuid.uuid4() for _ in dataframe.time.values]

        first_ntp = dataframe[self.output_time].min()
        last_ntp = dataframe[self.output_time].max()

        orig_dataframe_size = dataframe[self.output_time].size

        # convert times to python datetime
        first = datetime.datetime.utcfromtimestamp(first_ntp - NTP_OFFSET)
        last = datetime.datetime.utcfromtimestamp(last_ntp - NTP_OFFSET)

        log.info("new dataframe before data comparison: size (%d), min (%f, %s), max(%f, %s)",
                 orig_dataframe_size, first_ntp, str(first), last_ntp, str(last))

        # get the existing bins in the precomputed stream (self.output_stream)
        # look back bin_duration seconds to get the previous bin just to get the previous provenance
        lookback_first = first - datetime.timedelta(seconds=precomputed_binsize)
        existing_bins = find_bins_by_time(self.session, metadata_record.subsite, metadata_record.node,
                                          metadata_record.sensor, metadata_record.method, self.output_stream, 'cass',
                                          lookback_first, last)

        # initialize the new dataframe exclude mask to exclude nothing (eg., insert all records)
        dataframe_exclude_mask = np.zeros_like(dataframe.time, dtype=bool)

        bin_update_count = {}
        unchanged_count = 0
        provenance_last_unchanged_record = None
        existing_bin_no = -1
        for each in existing_bins:
            existing_bin_no += 1

            # fetch ALL existing data from this bin
            existing_dataframe = fetch_bin(each.subsite, each.node, each.sensor, each.method, each.stream,
                                     each.bin, cols=cols)
            existing_dataframe = self.prune_duplicate_times(existing_dataframe)

            existing_first_ntp = existing_dataframe.time.min()
            existing_last_ntp = existing_dataframe.time.max()

            log.info("full existing dataframe: bin (%d), size (%d), min (%f), max(%f)" %
                     (each.bin, existing_dataframe.time.size, existing_first_ntp, existing_last_ntp))

            # If the existing data frame is a previous bin (not this bin), just get the provenance
            df_bin_number = get_bin_number(dataframe.time.values[0], precomputed_binsize)
            if existing_last_ntp < df_bin_number:
                idx_last_unchanged_existing = existing_dataframe.index[-1]
                provenance_last_unchanged_record = \
                    (existing_dataframe.loc[idx_last_unchanged_existing, 'time'],
                     existing_dataframe.loc[idx_last_unchanged_existing, 'provenance'])
                log.info("previous existing bin last %f is less than df bin %d. "
                         "using previous bin no %d to get provenance_last_unchanged_record: %s" %
                         (existing_last_ntp, df_bin_number, existing_bin_no, str(provenance_last_unchanged_record)))
                continue

            log.info("comparing existing bin no: %d" % existing_bin_no)

            intersecting_values, a_ind, b_ind = np.intersect1d(dataframe.time.values,
                                                               existing_dataframe.time.values, return_indices=True)

            working_dataframe = dataframe.iloc[a_ind]
            working_existing_dataframe = existing_dataframe.iloc[b_ind]

            existing_first_ntp = working_existing_dataframe.time.min()
            existing_last_ntp = working_existing_dataframe.time.max()

            log.info("comparing dataframe region: size (%d), min (%f), max(%f)",
                     len(intersecting_values), existing_first_ntp, existing_last_ntp)

            # mask to hold the results of the comparison of new and existing data
            comparison_result_mask = np.ones_like(working_dataframe.time, dtype=bool)

            # compare the values in each column using numpy.isclose() with the default absolute (1e-08)
            # and relative (1e-05) tolerances which seem appropriate for the actual data in these streams
            for col in self.output_stream_derived_cols:
                comparison_result_mask = comparison_result_mask\
                                        & np.isclose(working_dataframe[col],
                                                     working_existing_dataframe[col], equal_nan=True)

            # Exclude dataframe rows that are equal to existing rows from further processing
            exclude_indexes = a_ind[comparison_result_mask]
            dataframe_exclude_mask[exclude_indexes] = True
            unchanged_count += len(exclude_indexes)

            # Update records in the dataframe from the existing dataframe where the comparison mask is false
            dataframe_update_to_indexes = a_ind[np.logical_not(comparison_result_mask)]
            existing_dataframe_update_from_indexes = b_ind[np.logical_not(comparison_result_mask)]
            # update the id's so the primary keys match and inserts will be handled as updates
            update_ids = existing_dataframe.loc[existing_dataframe_update_from_indexes, 'id']
            dataframe.loc[dataframe_update_to_indexes, 'id'] = update_ids.values

            bin_update_count[each.bin] = bin_update_count.setdefault(each.bin, 0) + len(dataframe_update_to_indexes)

            log.info("comparison results for bin (%d): updates (%d), unchanged (%d)" %
                     (each.bin, len(dataframe_update_to_indexes), len(exclude_indexes)))

            # get the provenance of the last unchanged existing record.
            # the index of the last unchanged existing record will be
            # one less than the index of the first changed record
            idx_last_unchanged_existing = existing_dataframe.index[-1]

            log.info("initializing idx_last_unchanged_existing last idx in existing_data_frame: %d" %
                     idx_last_unchanged_existing)

            if len(existing_dataframe_update_from_indexes) > 0:
                idx_first_changed_existing = existing_dataframe_update_from_indexes[0]
                log.info("idx_first_changed_existing: %d" % idx_first_changed_existing)
                if idx_first_changed_existing > 0:
                    idx_last_unchanged_existing = idx_first_changed_existing - 1
                    log.info("idx_last_unchanged_existing: %d" % idx_last_unchanged_existing)

            if provenance_last_unchanged_record is None or \
               provenance_last_unchanged_record[0] < existing_dataframe.loc[idx_last_unchanged_existing, 'time']:
                provenance_last_unchanged_record = \
                    (existing_dataframe.loc[idx_last_unchanged_existing, 'time'],
                     existing_dataframe.loc[idx_last_unchanged_existing, 'provenance'])

        # Upsert only the records that have not been excluded.
        # This includes new records as well as existing records that have changed
        dataframe = dataframe[np.logical_not(dataframe_exclude_mask)]

        final_dataframe_size = len(dataframe)
        update_count = sum(bin_update_count.values())
        log.info("dataframe after comparison: original size (%d), new size (%d), "
                 "update_count (%d), insert count (%d), unchanged_count (%d)",
                 orig_dataframe_size, final_dataframe_size,
                 update_count, final_dataframe_size-update_count, unchanged_count)
        if final_dataframe_size:
            log.info("new dataframe after data comparison: min(%f) max(%f)",
                     dataframe.time.min(), dataframe.time.max())
        else:
            log.info("data did not change in dataframe, not inserting/updating data or metadata, returning ...")
            return

        # check the previous computed provenance to see if this computed provenance
        # used the same set of data files and can be reused
        previous_computed_provenance = None
        if provenance_last_unchanged_record:
            log.info("provenance_last_unchanged_record: %s" % str(provenance_last_unchanged_record))
            stream_key = namedtuple('stream_key', ['subsite', 'node', 'sensor', 'method', 'stream']) \
                (metadata_record.subsite, metadata_record.node, metadata_record.sensor,
                 metadata_record.method, self.output_stream)
            prev_prov_dict = fetch_l0_provenance(stream_key, [provenance_last_unchanged_record[1]], 0)
            previous_provenance_dict = prev_prov_dict.get(str(provenance_last_unchanged_record[1]), {})
            if previous_provenance_dict.get('filename'):
                filename = previous_provenance_dict.get('filename')
                previous_computed_provenance = json.loads(filename)

            if previous_computed_provenance:
                log.info("previous_computed_provenance: %s" % str(previous_computed_provenance))
            else:
                log.error("could not find previous_computed_provenance for stream_key %s, "
                          "provenance_last_unchanged_record (time, provenance) %s"
                          % (str(stream_key), str(provenance_last_unchanged_record)))

        if previous_computed_provenance and \
                previous_computed_provenance == json.loads(computed_provenance.filename):
            log.info("reusing previous_computed_provenance as it is the same as computed_provenance")
            computed_provenance_id = provenance_last_unchanged_record[1]
        else:
            log.info("inserting new computed_provenance, %s" % str(computed_provenance))
            insert_l0_provenance(computed_provenance)
            computed_provenance_id = computed_provenance.id

        # assign provenance values to each data point
        dataframe['provenance'] = [computed_provenance_id for _ in dataframe.time.values]

        # insert our new data (inserts plus updates)
        results = insert_dataframe(metadata_record.subsite, metadata_record.node, metadata_record.sensor,
                                   metadata_record.method, self.output_stream, 0, precomputed_binsize, dataframe)

        # update the metadata records
        for bin_number in results:
            first = results[bin_number]['first']
            last = results[bin_number]['last']

            # convert times to python datetime
            first = datetime.datetime.utcfromtimestamp(first - NTP_OFFSET)
            last = datetime.datetime.utcfromtimestamp(last - NTP_OFFSET)

            count = max(results[bin_number]['count'] - bin_update_count.get(bin_number, 0), 0)
            update_partition(self.session, metadata_record.subsite, metadata_record.node, metadata_record.sensor,
                             metadata_record.method, self.output_stream, store, bin_number, first, last, count)
            update_stream_metadata(self.session, metadata_record.subsite, metadata_record.node, metadata_record.sensor,
                                   metadata_record.method, self.output_stream, first, last, count)

    def get_predicted_tide(self, min_time, max_time, metadata_record):
        min_dt = datetime.datetime.utcfromtimestamp(min_time - NTP_OFFSET)
        max_dt = datetime.datetime.utcfromtimestamp(max_time - NTP_OFFSET)
        arg_str = '%s, %s, %s, %s, %s, %s, %s (%f), %s (%f)' % \
                  (metadata_record.subsite, metadata_record.node, metadata_record.sensor,
                   metadata_record.method, PRED_TIDE_STREAM, metadata_record.store,
                   min_dt, min_time, max_dt, max_time)
        log.info('calling fetch_bins_by_time(%s)', arg_str)
        partition_metadata_list = find_bins_by_time(self.session,
                                                    metadata_record.subsite,
                                                    metadata_record.node,
                                                    metadata_record.sensor,
                                                    metadata_record.method,
                                                    PRED_TIDE_STREAM,
                                                    metadata_record.store,
                                                    min_dt, max_dt)
        if not partition_metadata_list:
            raise DataNotFoundException("partition_metadata not found for %s" % arg_str)
        dataframes = []
        for pm in partition_metadata_list:
            df = fetch_bin(pm.subsite, pm.node, pm.sensor, pm.method, pm.stream, pm.bin, ('time', 'predicted_tide'),
                                           min_time=min_dt, max_time=max_dt)
            if not df.size:
                log.warning('No %s data found for bin: %r', PRED_TIDE_STREAM, pm)
                continue
            dataframes.append(df)
        if not dataframes:
            raise DataNotFoundException("Cassandra data not found for %s" % arg_str)
        dataframe = pd.concat(dataframes).sort_values(self.source_time)
        return dataframe

    @staticmethod
    def precompute(dataframe, metadata_record):
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

    def precompute(self, dataframe, metadata_record):
        # Pull the tide data for a time range slightly larger the source dataframe
        pred_tide_dataframe = self.get_predicted_tide(dataframe.time.min() - PRED_TIDE_TIME_RANGE_PAD,
                                                      dataframe.time.max() + PRED_TIDE_TIME_RANGE_PAD,
                                                      metadata_record)
        try:
            return make_15s(dataframe, pred_tide_dataframe)
        except ValueError as e:
            log.error("ValueError caught from make_15s: " 
                      "dataframe-%s(min: %s (%f), max: %s (%f)), dataframe-%s(min: %s (%f), max: %s (%f))",
                      metadata_record.stream,
                      ntp_to_datetime(dataframe.time.min()), dataframe.time.min(),
                      ntp_to_datetime(dataframe.time.max()), dataframe.time.max(),
                      PRED_TIDE_STREAM,
                      ntp_to_datetime(pred_tide_dataframe.time.min()), pred_tide_dataframe.time.min(),
                      ntp_to_datetime(pred_tide_dataframe.time.max()), pred_tide_dataframe.time.max())
            raise


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
        cols = ['time', 'botsflu_meandepth', 'provenance']
        output_stream_derived_cols = ['time', 'botsflu_daydepth',
                                      'botsflu_4wkrate', 'botsflu_8wkrate']

        super(BotptPrecompute24h, self).__init__(session, job_name, base_stream, output_stream,
                                                 overlap_seconds, source_time, output_time,
                                                 cols, output_stream_derived_cols, **kwargs)

    def precompute(self, dataframe, metadata_record):
        try:
            return make_24h(dataframe)
        except ValueError as e:
            log.error("ValueError caught from make_24h: " 
                      "dataframe-%s(min: %s (%f), max: %s (%f))",
                      metadata_record.stream,
                      ntp_to_datetime(dataframe.time.min()), dataframe.time.min(),
                      ntp_to_datetime(dataframe.time.max()), dataframe.time.max())
            raise


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

