import os
import logging
import logging.config

import sys
import yaml

from apscheduler.schedulers.blocking import BlockingScheduler
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .cassandra.session import SessionManager


log = logging.getLogger(__name__)
here = os.path.dirname(__file__)


def setup_logging(default_path='logging.yml', default_level=logging.INFO, env_key='LOG_CFG'):
    """
    Setup logging configuration
    """
    path = os.path.join(here, default_path)
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


def main():
    setup_logging()
    from .plugins import PluginProvider

    if len(sys.argv) < 2:
        log.error('Exiting! No configuration file provided.')
        sys.exit(1)

    config = yaml.load(open(sys.argv[1]))

    postgres = config.get('postgres', {})
    cassandra = config.get('cassandra', {})
    plugin_config = config.get('plugin', {})

    pg_url = postgres.get('url')
    contacts = cassandra.get('contacts')
    keyspace = cassandra.get('keyspace', 'ooi')
    protocol_version = cassandra.get('protocol_version', 3)
    if isinstance(contacts, basestring):
        contacts = [contacts]
    plugin_name = plugin_config.get('name')
    plugin_schedule = plugin_config.get('schedule')
    plugin_kwargs = plugin_config.get('kwargs', {})

    if not all((pg_url, contacts, plugin_name, plugin_schedule)):
        log.error('Exiting! Invalid or incomplete configuration file.')
        sys.exit(1)

    log.info('Creating database sessions')

    engine = create_engine(pg_url, echo=False)
    Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    SessionManager.init(contacts, keyspace, protocol_version=protocol_version)

    scheduler = BlockingScheduler()
    plugin = PluginProvider.plugin_map.get(plugin_name)()

    scheduler.add_job(plugin.execute, args=(Session,), kwargs=plugin_kwargs, **plugin_schedule)
    scheduler.start()


if __name__ == '__main__':
    main()