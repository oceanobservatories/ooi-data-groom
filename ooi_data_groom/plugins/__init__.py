import os
from keyword import iskeyword
from os.path import dirname
from .manager import PluginProvider

basedir = dirname(__file__)

__all__ = [PluginProvider]


for name in os.listdir(basedir):
    if os.path.isdir(os.path.join(basedir, name)):
        module = name
        if not module.startswith('.') and not iskeyword(module):
            try:
                __import__('%s.%s' % (__name__, module))
            except StandardError:
                import logging
                logger = logging.getLogger(__name__)
                logger.warning('Ignoring exception while loading the %r plug-in.', module)
