import time
import json
import logging
import datetime
import traceback

from collections import defaultdict
from contextlib import contextmanager

from gcd.etc import chunks
from gcd.work import Batcher, Task
from gcd.store import Store, execute
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
        self._sqsum = m * self._sqsum + x*x
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


class Monitor(defaultdict, Task):

    def __init__(self, log_period, log_fun, **base_info):
        defaultdict.__init__(self, int)
        self._log_fun = log_fun
        self._log_handlers = []
        self._base_info = base_info
        Task.__init__(self, log_period, self._log)

    def stats(self, *names, memory=1):
        stats = self.get(names)
        if stats is None:
            stats = self[names] = Statistics(memory)
        return stats

    @contextmanager
    def timeit(self, *names, memory=1):
        t0 = time.clock()
        try:
            yield
        finally:
            t1 = time.clock()
            self.stats(*names, memory).add(t1 - t0)

    def on_log(self, handler):
        self._log_handlers.append(handler)

    def _log(self):
        for handler in self._log_handlers:
            handler(self)
        info = self._base_info.copy()
        for keys, value in self.items():
            if isinstance(value, Statistics):
                value = {a: getattr(value, a)
                         for a in ('n', 'mean', 'stdev', 'min', 'max')}
            sub_info = info
            for key in keys[:-1]:
                sub_info = sub_info.setdefault(key, {})
            sub_info[keys[-1]] = value
        self._log_fun(info)
        self.clear()


class JsonFormatter(logging.Formatter, json.JSONEncoder):

    def __init__(self, attrs=('name', 'levelname', 'created')):
        logging.Formatter.__init__(self)
        self._attrs = attrs

    def format(self, record):
        log = {a: getattr(record, a) for a in self._attrs}
        if isinstance(record.msg, dict):
            log.update(record.msg)
        else:
            log['message'] = record.getMessage()
        if record.exc_info:
            log['exc_info'] = self.formatException(record.exc_info)
        return json.dumps(log, cls=self._Encoder)

    class _Encoder(json.JSONEncoder):

        def default(self, obj):
            if isinstance(obj, (datetime.datetime, datetime.date)):
                return obj.timestamp()
            else:
                return super().default(obj)


class StoreHandler(logging.Handler):

    def __init__(self, store, period=5):
        logging.Handler.__init__(self)
        self._batcher = Batcher(period, store.add).start()

    def emit(self, record):
        try:
            self._batcher.add(self.format(record))
        except:  # Avoid reentering or aborting: just a heads up in stderr.
            traceback.print_exc()


class PgLogStore(Store):

    def __init__(self, conn_or_pool=None, table='logs'):
        super().__init__(conn_or_pool)
        self._table = table

    def add(self, logs):
        for chunk in chunks(logs, 1000):
            with self.transaction():
                execute("""
                        INSERT INTO %s (log)
                        SELECT cast(v.log AS jsonb) FROM (%%s) AS v (log)
                        """ % self._table, ((l,) for l in chunk), values=True)

    def create(self, drop=False):
        with self.transaction():
            execute("""
                    %sDROP TABLE IF EXISTS %s;
                    CREATE TABLE %s (log jsonb);
                    """ % ('' if drop else '--', self._table, self._table))
        return self
