from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.protocol import NumpyProtocolHandler, LazyProtocolHandler
from cassandra.query import _clean_column_name, tuple_factory, named_tuple_factory


class SessionManager(object):
    _prepared_statement_cache = {}

    @staticmethod
    def init(contact_points, keyspace, **kwargs):
        consistency = ConsistencyLevel.LOCAL_ONE
        cluster = Cluster(contact_points, **kwargs)
        SessionManager._setup(cluster, keyspace, consistency)

    @classmethod
    def _setup(cls, cluster, keyspace, consistency_level):
        cls.cluster = cluster
        cls._prepared_statement_cache = {}

        cls.__session = cls.cluster.connect(keyspace)

        if consistency_level is not None:
            cls.__session.default_consistency_level = consistency_level

    @classmethod
    def prepare(cls, statement):
        if statement not in cls._prepared_statement_cache:
            cls._prepared_statement_cache[statement] = cls.__session.prepare(statement)
        return cls._prepared_statement_cache[statement]

    @classmethod
    def get_query_columns(cls, table):
        # grab the column names from our metadata
        cols = cls.cluster.metadata.keyspaces[cls.__session.keyspace].tables[table].columns.keys()
        cols = map(_clean_column_name, cols)
        unneeded = ['subsite', 'node', 'sensor', 'method']
        cols = [c for c in cols if c not in unneeded]
        return cols

    @classmethod
    def execute_lazy(cls, *args, **kwargs):
        return cls.__session.execute(*args, **kwargs)

    @classmethod
    def execute_numpy(cls, *args, **kwargs):
        try:
            cls.__session.row_factory = tuple_factory
            cls.__session.client_protocol_handler = NumpyProtocolHandler
            return cls.__session.execute(*args, **kwargs)
        finally:
            cls.__session.row_factory = named_tuple_factory
            cls.__session.client_protocol_handler = LazyProtocolHandler

    @classmethod
    def execute(cls, *args, **kwargs):
        return cls.execute_lazy(*args, **kwargs)

    @classmethod
    def session(cls):
        return cls.__session

    @classmethod
    def pool(cls):
        return cls.__pool
