import time
import json
import logging
import traceback
import socket
import multiprocessing as mp

from collections import defaultdict
from contextlib import contextmanager

from gcd.work import Batcher
from gcd.store import PgStore, execute
from gcd.chronos import as_memory


class Statistics:

    def __init__(self, memory=1):
        self.memory = as_memory(memory)
        self.n = self._sum = self._sqsum = 0
        self.min = float('inf')
        self.max = -float('inf')
        self._max_time = 0

    def add(self, x, x_time=None):
        if x_time is None:
            x_time = time.time()
        delta = x_time - self._max_time
        if delta >= 0:
            m, w = self.memory ** delta, 1
            self._max_time = x_time
        else:
            m, w = 1, self.memory ** -delta
        x *= w
        self.n = m * self.n + w
        self._sum = m * self._sum + x
        self._sqsum = m * self._sqsum + x * x
        self.min = min(m * self.min, x)
        self.max = max(m * self.max, x)
        return self

    @property
    def mean(self):
        if self.n > 0:
            return self._sum / self.n

    @property
    def stdev(self):
        if self.n > 1:
            sqmean = self.mean ** 2
            return ((self._sqsum - self.n * sqmean) / (self.n - 1)) ** 0.5

    def as_dict(self):
        return {a: getattr(self, a)
                for a in ('n', 'mean', 'stdev', 'min', 'max')}


class Monitor(defaultdict):

    def __init__(self, **info_base):
        super().__init__(int)
        self._info_base = info_base

    def stats(self, *names, memory=1):
        stats = self.get(names)
        if stats is None:
            stats = self[names] = Statistics(memory)
        return stats

    @contextmanager
    def timeit(self, *names, memory=1):
        t0 = time.perf_counter()
        try:
            yield
        finally:
            t1 = time.perf_counter()
            self.stats(*names, memory=memory).add(t1 - t0)

    def info(self):
        info = self._info_base.copy()
        for keys, value in self.items():
            if isinstance(value, Statistics):
                value = value.as_dict()
            sub_info = info
            for key in keys[:-1]:
                sub_info = sub_info.setdefault(key, {})
            sub_info[keys[-1]] = value
        return info


class DictFormatter(logging.Formatter):

    def __init__(self, attrs=None):
        super().__init__()
        if attrs is None:
            attrs = 'name', 'levelname', 'created', 'context'
        assert 'asctime' not in attrs
        self._attrs = attrs

    def format(self, record):
        log = {a: getattr(record, a)
               for a in self._attrs if hasattr(record, a)}
        if isinstance(record.msg, dict):
            log.update(record.msg)
        else:
            log['message'] = record.getMessage()
        if record.exc_info:
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
            log['exc_info'] = record.exc_text
        if record.stack_info:
            log['stack_info'] = self.formatStack(record.stack_info)
        return log


class JsonFormatter(DictFormatter):

    def __init__(self, attrs=None, *args, **kwargs):
        super().__init__(attrs)
        self._dumps = lambda log: json.dumps(log, *args, **kwargs)

    def format(self, record):
        return self._dumps(super().format(record))


class ContextFilter(logging.Filter):

    @staticmethod
    def install(*args, **kwargs):
        ContextFilter.instance = ContextFilter(*args, **kwargs)
        return ContextFilter.instance

    @staticmethod
    def info(**kwargs):
        if ContextFilter.instance:
            ContextFilter.instance.info.update(kwargs)

    instance = None

    def __init__(self, host=False, process=True, **info):
        if host:
            info['host'] = socket.gethostname()
        if process:
            p = mp.current_process()
            info['process'] = p.name, p.pid
        self.info = info

    def filter(self, record):
        record.context = self.info
        return True


class StoreHandler(logging.Handler):

    def __init__(self, formatter=None, store=None, period=5):
        logging.Handler.__init__(self)
        store = store or PgJsonLogStore()
        if not isinstance(formatter, logging.Formatter):
            formatter = JsonFormatter(formatter)
        self.setFormatter(formatter)
        self._batcher = Batcher(period, store.add).start()

    def emit(self, record):
        try:
            self._batcher.add(self.format(record))
        except:  # Avoid reentering or aborting: just a heads up in stderr.
            traceback.print_exc()


class PgJsonLogStore(PgStore):

    def __init__(self, conn_or_pool=None, table='logs', create=True):
        self._table = table
        super().__init__(conn_or_pool, create)

    def add(self, logs):
        with self.transaction():
            execute("""
                    INSERT INTO %s (log)
                    SELECT cast(v.log AS jsonb) FROM (%%s) AS v (log)
                    """ % self._table, ((l,) for l in logs), values=True)

    def _create(self):
        with self.transaction():
            execute('CREATE TABLE IF NOT EXISTS %s (log jsonb)' % self._table)
