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
        self._default_tts = tts or float("inf")
        self._default_ttl = ttl or float("inf")
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

    def items(self):
        return ((k, e.val) for k, e in self._cache.items() if e.val is not NA)

    def clean_up(self):
        now = time.time()
        self._cache = {
            key: entry
            for key, entry in self._cache.items()
            if now - entry.rtime <= self._ttl(key, entry.val)
        }

    def _get(self, key):
        raise NotImplementedError

    def _tts(self, key, val):
        return self._default_tts

    def _ttl(self, key, val):
        return self._default_ttl


class AsynCache(Cache):
    def __init__(self, tts=None, ttl=None, cache=None, hwm=None, preload=False):
        super().__init__(tts, ttl, cache)
        self._queue = new_queue(hwm)
        if preload:
            self._load_batch(None)
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
            self._load_batch(set(dequeue(self._queue, 1)))

    def _load_batch(self, keys):
        try:
            batch = dict(self._get_batch(keys))
        except Exception:
            logger.exception("Error getting cache batch")
        else:
            now = time.time()
            for key in keys or batch.keys():
                entry = Bundle(wtime=now, rtime=now, val=batch.get(key, NA))
                self._cache[key] = entry

    def _get_batch(self, keys):  # Returns (key, val), (key, val), ...
        raise NotImplementedError
