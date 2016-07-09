import time
import logging

from gcd.etc import Bundle
from gcd.work import new_queue, dequeue, Task, Thread


NA = object()

logger = logging.getLogger()


class Miss(Exception):
    pass


class Cache:

    def __init__(self, tts=None, ttl=None, cache=None):
        self._default_tts = tts or float('inf')
        self._default_ttl = ttl or float('inf')
        self._cache = {} if cache is None else cache
        Task(ttl, self.clean_up).start()

    def __getitem__(self, key):
        now = time.time()
        entry = self._cache.get(key)
        if not entry or (now - entry.wtime > self._tts(key, entry.val)):
            entry = Bundle(wtime=now, rtime=now, val=self._get(key))
            self._cache[key] = entry
        else:
            entry.rtime = now
        if entry.val is NA:
            raise KeyError(key)
        else:
            return entry.val

    def clean_up(self):
        now = time.time()
        self._cache = {key: entry for key, entry in self._cache.items()
                       if now - entry.rtime <= self._ttl(key, entry.val)}

    def _get(self, key):
        raise NotImplementedError

    def _tts(self, key, val):
        return self._default_tts

    def _ttl(self, key, val):
        return self._default_ttl


class AsynCache(Cache):

    def __init__(self, tts=None, ttl=None, cache=None, hwm=None):
        super().__init__(tts, ttl, cache)
        self._queue = new_queue(hwm)
        Thread(self._process_queue, daemon=True).start()

    def _get(self, key):
        self._queue.put(key)
        # Inform current stale/missing value while waiting for the fresh one to
        # be async loaded from cache.
        entry = self._cache.get(key)
        if entry:
            return entry.val
        else:
            raise Miss(key)

    def _process_queue(self):
        while True:
            keys = set(dequeue(self._queue, 1))
            try:
                vals = dict(self._get_batch(keys))
            except Exception:
                logger.exception('Error getting cache batch')
                continue
            now = time.time()
            for key in keys:
                entry = Bundle(wtime=now, rtime=now, val=vals.get(key, NA))
                self._cache[key] = entry

    def _get_batch(self, keys):  # Returns (key, val), (key, val), ...
        raise NotImplementedError
