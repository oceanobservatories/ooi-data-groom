import logging
import uuid
from collections import namedtuple

from cassandra.concurrent import execute_concurrent_with_args

from .session import SessionManager

logging.getLogger('cassandra').setLevel(logging.WARNING)
log = logging.getLogger(__name__)

ProvTuple = namedtuple('provenance_tuple', ['subsite', 'sensor', 'node', 'method', 'deployment', 'id',
                                            'filename', 'parsername', 'parserversion'])

L0_DATASET = 'select * from dataset_l0_provenance where subsite=? and node=? and ' \
             'sensor=? and method=? and deployment=? and id=?'


def fetch_l0_provenance(stream_key, provenance_values, deployment):
    """
    Fetch the l0_provenance entry for the passed information.
    All of the necessary information should be stored as a tuple in the
    provenance metadata store.
    """
    if stream_key.method.startswith('streamed'):
        deployment = 0

    prov_ids = []
    for each in set(provenance_values):
        if isinstance(each, uuid.UUID):
            prov_ids.append(each)
        else:
            try:
                prov_ids.append(uuid.UUID(each))
            except ValueError:
                pass

    provenance_arguments = [
        (stream_key.subsite, stream_key.node, stream_key.sensor,
         stream_key.method, deployment, prov_id) for prov_id in prov_ids]

    query = SessionManager.prepare(L0_DATASET)
    results = execute_concurrent_with_args(SessionManager.session(), query, provenance_arguments)
    results = [list(rows) for success, rows in results if success]
    records = [ProvTuple(*rows[0]) for rows in results if len(rows) > 0]

    if len(provenance_arguments) != len(records):
        log.warn("Could not find %d provenance entries", len(provenance_arguments) - len(records))

    prov_dict = {
        str(row.id): {'filename': row.filename,
                      'parsername': row.parsername,
                      'parserversion': row.parserversion}
        for row in records}
    return prov_dict


def insert_l0_provenance(provenance_record):
    """
    Insert the specified provenance record into cassandra
    """
    fields = ','.join(ProvTuple._fields)
    placeholders = ','.join(('?' for _ in ProvTuple._fields))
    query = 'insert into dataset_l0_provenance (%s) values (%s)' % (fields, placeholders)
    query = SessionManager.prepare(query)
    SessionManager.execute_lazy(query, provenance_record)
