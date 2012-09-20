import inspect
from redis_shard.shard import RedisShardAPI

class RedisShardPlugin(object):
    name = 'redis'

    def __init__(self, config, keyword='rdb'):
      self.config = config
      self.keyword = keyword
      self.redisdb = None

    def setup(self,app):
        for other in app.plugins:
            if not isinstance(other,RedisShardPlugin): continue
            if other.keyword == self.keyword:
                raise PluginError("Found another redis shard plugin with "\
                        "conflicting settings (non-unique keyword).")
        if self.redisdb is None:
            self.redisdb = RedisShardAPI(self.config)

    def apply(self,callback,context):
        conf = context['config'].get('redis_shard') or {}
        args = inspect.getargspec(context['callback'])[0]
        keyword = conf.get('keyword',self.keyword)
        if keyword not in args:
            return callback

        def wrapper(*args,**kwargs):
            kwargs[self.keyword] = self.redisdb
            rv = callback(*args, **kwargs)
            return rv
        return wrapper

Plugin = RedisShardPlugin
