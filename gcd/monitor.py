import json
import logging
import traceback
import socket
import multiprocessing as mp

from collections import defaultdict
from contextlib import contextmanager
from time import perf_counter, time as time_

from gcd.work import Batcher
from gcd.store import PgStore, execute
from gcd.chronos import as_memory


def forget(memory, max_time, weight, time):
    time = time or time_()
    max_time = max_time or time
    delta = time - max_time
    if delta >= 0:
        a, b = memory ** delta, weight
        max_time = time
    else:
        a, b = 1, weight * memory ** -delta
    return a, b, max_time


class Forgetter:

    def __init__(self, memory):
        self.memory = as_memory(memory)
        self.max_time = None

    def forget(self, weight=1, time=None):
        self.a, self.b, self.max_time = forget(
            self.memory, self.max_time, weight, time)


class Statistics:

    def __init__(self, memory=1, full=False):
        self._shared_forgetter = isinstance(memory, Forgetter)
        if self._shared_forgetter:
            self.forgetter = memory
        else:
            self.forgetter = Forgetter(memory) if memory != 1 else None
        self.n = self.mean = 0
        if full:
            self._sqdelta = 0
            self.min = float('inf')
            self.max = -float('inf')
        self._full = full

    def add(self, x, weight=1, time=None):
        forgetter = self.forgetter
        if forgetter:
            if not self._shared_forgetter:
                forgetter.forget(weight, time)
            a, b = forgetter.a, forgetter.b
        else:
            a, b = 1, weight
        # https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
        # #Online_algorithm (Welford)
        self.n = a * self.n + b
        delta = x - self.mean
        self.mean += b * delta / self.n
        if self._full:
            delta2 = x - self.mean
            self._sqdelta = a * self._sqdelta + b * delta * delta2
            self.min = min(a * self.min, b * x)
            self.max = max(a * self.max, b * x)
        return self

    @property
    def sum(self):
        return self.mean * self.n

    @property
    def stdev(self):
        if self.n > 0:
            return (self._sqdelta / self.n) ** 0.5

    def as_dict(self):
        full_attrs = ('stdev', 'min', 'max') if self._full else ()
        return {a: getattr(self, a) for a in ('n', 'mean') + full_attrs}


class Monitor(defaultdict):

    def __init__(self, **info_base):
        super().__init__(int)
        self._info_base = info_base

    def stats(self, *names, memory=1, full=False):
        stats = self.get(names)
        if stats is None:
            stats = self[names] = Statistics(memory, full)
        return stats

    @contextmanager
    def timeit(self, *names, memory=1, full=True):
        t0 = perf_counter()
        try:
            yield
        finally:
            t1 = perf_counter()
            self.stats(*names, memory=memory, full=full).add(t1 - t0)

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
        store = store or JsonLogStore()
        if not isinstance(formatter, logging.Formatter):
            formatter = JsonFormatter(formatter)
        self.setFormatter(formatter)
        self._batcher = Batcher(store.add, period=period).start()

    def emit(self, record):
        try:
            self._batcher.put(self.format(record))
        except:  # Avoid reentering or aborting: just a heads up in stderr.
            traceback.print_exc()


class JsonLogStore(PgStore):

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
        execute("""
                CREATE TABLE IF NOT EXISTS %s (log jsonb);
                CREATE INDEX IF NOT EXISTS %s_created_index
                ON %s ((to_timestamp((log->>'created')::double precision)));
                CREATE INDEX IF NOT EXISTS %s_name_levelname_created_index
                ON %s ((log->>'name'),
                       (log->>'levelname'),
                       (to_timestamp((log->>'created')::double precision)));
                """ % ((self._table,) * 5))
