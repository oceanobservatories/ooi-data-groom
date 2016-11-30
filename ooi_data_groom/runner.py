import os
import logging
import logging.config

import yaml

from apscheduler.schedulers.blocking import BlockingScheduler
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .cassandra.session import SessionManager
from .plugins import PluginProvider


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
    log.info('Creating database sessions')

    connection_url = 'postgresql://awips@localhost:5432/metadata'
    engine = create_engine(connection_url, echo=False)
    # from ooi_data.postgres.model import MetadataBase
    # MetadataBase.metadata.create_all(bind=engine)
    Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    SessionManager.init(['localhost'], 'ooi')

    scheduler = BlockingScheduler()

    plugin = PluginProvider.plugin_map.get('botpt_nano_fifteen')()

    scheduler.add_job(plugin.execute, trigger='cron', args=(Session,), minute=0)
    scheduler.start()


if __name__ == '__main__':
    main()