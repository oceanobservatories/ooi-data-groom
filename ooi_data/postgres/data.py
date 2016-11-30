import datetime
from sqlalchemy import and_, func, or_
from sqlalchemy.orm import aliased

from .model import PartitionMetadatum, ProcessedMetadatum, StreamMetadatum


def find_modified_bins_by_jobname(session, job_name, subsite=None, node=None, sensor=None,
                                  method=None, stream=None):

    filter_constraints = []
    if subsite:
        filter_constraints.append(PartitionMetadatum.subsite == subsite)
    if node:
        filter_constraints.append(PartitionMetadatum.node == node)
    if sensor:
        filter_constraints.append(PartitionMetadatum.sensor == sensor)
    if method:
        filter_constraints.append(PartitionMetadatum.method == method)
    if stream:
        filter_constraints.append(PartitionMetadatum.stream == stream)

    query = session.query(PartitionMetadatum)

    if job_name:
        subquery = session.query(ProcessedMetadatum).filter(ProcessedMetadatum.processor_name == job_name).subquery()
        alias = aliased(ProcessedMetadatum, subquery)
        query = query.outerjoin(alias)
        filter_constraints.append(or_(PartitionMetadatum.modified > alias.processed_time,
                                      alias.processed_time.is_(None)))

    if filter_constraints:
        query = query.filter(and_(*filter_constraints))
    query = query.order_by(PartitionMetadatum.bin)

    return query


def find_bins_by_time(session, subsite, node, sensor, method, stream, store, min_time, max_time):
    query = session.query(PartitionMetadatum)
    query = query.filter(
        PartitionMetadatum.subsite == subsite,
        PartitionMetadatum.node == node,
        PartitionMetadatum.sensor == sensor,
        PartitionMetadatum.method == method,
        PartitionMetadatum.stream == stream,
        PartitionMetadatum.store == store,
        PartitionMetadatum.first < max_time,
        PartitionMetadatum.last > min_time
    )
    return query


def find_previous_bin(session, metadata_record, max_elapsed_seconds=None):
    query = session.query(PartitionMetadatum)
    query = query.filter(
        PartitionMetadatum.subsite == metadata_record.subsite,
        PartitionMetadatum.node == metadata_record.node,
        PartitionMetadatum.sensor == metadata_record.sensor,
        PartitionMetadatum.method == metadata_record.method,
        PartitionMetadatum.stream == metadata_record.stream,
        PartitionMetadatum.bin < metadata_record.bin
    )
    query = query.order_by(PartitionMetadatum.bin.desc())
    previous_record = query.first()

    if max_elapsed_seconds is None or previous_record is None:
        return previous_record

    if metadata_record.first - previous_record.last < max_elapsed_seconds:
        return previous_record


def find_next_bin(session, metadata_record, max_elapsed_seconds=None):
    query = session.query(PartitionMetadatum)
    query = query.filter(
        PartitionMetadatum.subsite == metadata_record.subsite,
        PartitionMetadatum.node == metadata_record.node,
        PartitionMetadatum.sensor == metadata_record.sensor,
        PartitionMetadatum.method == metadata_record.method,
        PartitionMetadatum.stream == metadata_record.stream,
        PartitionMetadatum.bin > metadata_record.bin
    )
    query = query.order_by(PartitionMetadatum.bin.asc())
    next_record = query.first()

    if max_elapsed_seconds is None or next_record is None:
        return next_record

    if next_record.first - metadata_record.last < max_elapsed_seconds:
        return next_record


def find_last_job_time(session, name):
    result, = session.query(func.min(ProcessedMetadatum.processed_time)).filter(
        ProcessedMetadatum.processor_name == name).first()
    if result is None:
        return datetime.datetime.utcfromtimestamp(0)
    return result


def get_bin(session, subsite, node, sensor, method, stream, store, bin_number, for_update=False):
    query = session.query(PartitionMetadatum)
    if for_update:
        query = query.with_for_update(nowait=True, of=PartitionMetadatum)
    query = query.filter(
        PartitionMetadatum.subsite == subsite,
        PartitionMetadatum.node == node,
        PartitionMetadatum.sensor == sensor,
        PartitionMetadatum.method == method,
        PartitionMetadatum.stream == stream,
        PartitionMetadatum.store == store,
        PartitionMetadatum.bin == bin_number
    )
    return query.first()


def get_stream(session, subsite, node, sensor, method, stream, for_update=False):
    query = session.query(StreamMetadatum)
    if for_update:
        query = query.with_for_update(nowait=True, of=StreamMetadatum)
    query = query.filter(
        StreamMetadatum.subsite == subsite,
        StreamMetadatum.node == node,
        StreamMetadatum.sensor == sensor,
        StreamMetadatum.method == method,
        StreamMetadatum.stream == stream,
    )
    return query.first()


def update_partition(session, subsite, node, sensor, method, stream, store, bin_number, first, last, count):
    with session.begin_nested():
        pm = get_bin(session, subsite, node, sensor, method, stream, store, bin_number, for_update=True)
        if pm is None:
            pm = PartitionMetadatum(subsite=subsite, node=node, sensor=sensor, method=method, stream=stream,
                                    store=store, bin=bin_number, first=first, last=last, count=count)
            session.add(pm)

        else:
            pm.first = min(first, pm.first)
            pm.last = max(last, pm.last)
            pm.count += count

    session.commit()


def set_partition(session, subsite, node, sensor, method, stream, store, bin_number, first, last, count):
    with session.begin_nested():
        pm = get_bin(session, subsite, node, sensor, method, stream, store, bin_number, for_update=True)
        if pm is None and count != 0:
            pm = PartitionMetadatum(subsite=subsite, node=node, sensor=sensor, method=method, stream=stream,
                                    store=store, bin=bin_number, first=first, last=last, count=count)
            session.add(pm)

        else:
            if count == 0:
                if pm:
                    session.delete(pm)
            else:
                pm.first = first
                pm.last = last
                pm.count = count

    session.commit()


def recreate_stream_metadata(session, subsite, node, sensor, method, stream):
    with session.begin_nested():
        sm = get_stream(session, subsite, node, sensor, method, stream, for_update=True)
        if sm is None:
            sm = StreamMetadatum(subsite=subsite, node=node, sensor=sensor, method=method, stream=stream)
            session.add(sm)

        query = session.query(func.min(PartitionMetadatum.first),
                              func.max(PartitionMetadatum.last),
                              func.sum(PartitionMetadatum.count))
        query = query.filter(PartitionMetadatum.subsite == subsite,
                             PartitionMetadatum.node == node,
                             PartitionMetadatum.sensor == sensor,
                             PartitionMetadatum.method == method,
                             PartitionMetadatum.stream == stream)
        result = query.first()

        if result is None:
            session.delete(sm)
        else:
            first, last, count = result
            sm.first = first
            sm.last = last
            sm.count = count

    session.commit()


def update_stream_metadata(session, subsite, node, sensor, method, stream, first, last, count):
    with session.begin_nested():
        sm = get_stream(session, subsite, node, sensor, method, stream, for_update=True)
        if sm is None:
            sm = StreamMetadatum(subsite=subsite, node=node, sensor=sensor, method=method, stream=stream,
                                 first=first, last=last, count=count)
            session.add(sm)

        else:
            sm.first = min(first, sm.first)
            sm.last = max(last, sm.last)
            sm.count += count

    session.commit()


def get_processing_metadata(session, record_id, job_name):
    return session.query(ProcessedMetadatum).filter(
        ProcessedMetadatum.processor_name == job_name,
        ProcessedMetadatum.partition_id == record_id
    ).first()


def record_processing_metadata(session, record_id, job_name):
    with session.begin_nested():
        processed = get_processing_metadata(session, record_id, job_name)

        if processed is None:
            processed = ProcessedMetadatum(processor_name=job_name,
                                           partition_id=record_id,
                                           processed_time=func.now())
            session.add(processed)

        else:
            processed.processed_time = func.now()

    session.commit()
