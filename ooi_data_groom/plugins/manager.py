
class PluginMount(type):
    def __init__(cls, name, bases, attrs):
        if not hasattr(cls, 'plugin_map'):
            cls.plugin_map = {}
        else:
            if hasattr(cls, 'plugin_name'):
                cls.plugin_map[cls.plugin_name] = cls


class PluginProvider(object):
    __metaclass__ = PluginMount

