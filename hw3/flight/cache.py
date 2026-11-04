import logging
import os
from threading import Lock
from uuid import uuid4

from redis import RedisError
from redis.backoff import NoBackoff
from redis.retry import Retry
from redis.sentinel import Sentinel


class FlightCache:
    def __init__(self):
        host, port = os.environ['REDIS_SENTINEL'].rsplit(':', 1)
        sentinel = Sentinel([(host, int(port))], socket_timeout=0.3, socket_connect_timeout=0.3)
        self.redis = sentinel.master_for('flights', socket_timeout=0.3, socket_connect_timeout=0.3, retry=Retry(NoBackoff(), 0))
        self.ttl = int(os.environ.get('CACHE_TTL_SECONDS', '300'))
        if not 300 <= self.ttl <= 600:
            raise ValueError('CACHE_TTL_SECONDS должен быть от 300 до 600')
        self.namespace = uuid4().hex
        self.generation = 0
        self.lock = Lock()

    def get(self, key, message_type, load):
        with self.lock:
            generation = self.generation
            full_key = f'{self.namespace}:{generation}:{key}'
            try:
                cached = self.redis.get(full_key)
                if cached is not None:
                    logging.info('cache_hit key=%s', full_key)
                    return message_type.FromString(cached)
            except RedisError:
                logging.warning('cache_unavailable')
        logging.info('cache_miss key=%s', full_key)
        value = load()
        with self.lock:
            if self.generation == generation:
                try:
                    self.redis.set(full_key, value.SerializeToString(), ex=self.ttl)
                except RedisError:
                    logging.warning('cache_unavailable')
        return value

    def invalidate(self):
        with self.lock:
            self.generation += 1
            try:
                keys = list(self.redis.scan_iter(match=f'{self.namespace}:*'))
                if keys:
                    self.redis.unlink(*keys)
            except RedisError:
                logging.warning('cache_unavailable')
            logging.info('cache_invalidated generation=%s', self.generation)
