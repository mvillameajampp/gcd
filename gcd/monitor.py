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
from gcd.chronos import day


class MovingStatistics:

    def __init__(self, memory=1, period=day):
        self.n = 0
        self.sum = 0
        self.sqsum = 0
        self.min = float('inf')
        self.max = -float('inf')
        self._memory = memory ** (1 / period)
        self._last = time.time()

    def add(self, x):
        now = time.time()
        mu = self._memory ** (now - self._last)
        self._last = now
        self.n += mu * 1
        self.sum = mu * self.sum + x
        self.sqsum = mu * self.sqsum + x*x
        self.min = min(mu * self.min, x)
        self.max = max(mu * self.max, x)
        return self

    @property
    def mean(self):
        if self.n > 0:
            return self.sum / self.n

    @property
    def stdev(self):
        if self.n > 1:
            sqmean = self.mean ** 2
            return ((self.sqsum - self.n * sqmean) / (self.n - 1)) ** 0.5

    def as_dict(self):
        return dict(n=self.n, mean=self.mean, stdev=self.stdev,
                    min=self.min, max=self.max)

    def __repr__(self):
        return repr(self.as_dict())


class SimpleEventLog(defaultdict, Task):

    def __init__(self, log_period, log_fun, **log_base):
        defaultdict.__init__(self, int)
        Task.__init__(self, log_period, self._log)
        self._log_fun = log_fun
        self._log_base = log_base

    def stat(self, *names_and_val, **kwargs):
        names = names_and_val[:-1]
        val = names_and_val[-1]
        stats = self.get(names)
        if stats is None:
            stats = self[names] = MovingStatistics(**kwargs)
        stats.add(val)

    @contextmanager
    def timeit(self, *names, **kwargs):
        t0 = time.clock()
        yield
        t1 = time.clock()
        self.stat(*names, t1 - t0, **kwargs)

    def _log(self):
        log = self._log_base.copy()
        for keys, value in self.items():
            if isinstance(value, MovingStatistics):
                value = value.as_dict()
            sub_log = log
            for key in keys[:-1]:
                sub_log = sub_log.setdefault(key, {})
            sub_log[keys[-1]] = value
        self._log_fun(log)
        self.clear()


class JsonDateTimeEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        else:
            return json.JSONEncoder.default(self, obj)


class JsonFormatter(logging.Formatter, json.JSONEncoder):

    def __init__(self, attrs=[], encoder=JsonDateTimeEncoder):
        logging.Formatter.__init__(self)
        self._attrs = attrs
        self._encoder = JsonDateTimeEncoder

    def format(self, record):
        log = {}
        if isinstance(record.msg, dict):
            log.update(record.msg)
        else:
            log['message'] = record.getMessage()
        if record.exc_info:
            log['exc_info'] = self.formatException(record.exc_info)
        for attr in self._attrs:
            if attr == 'asctime':
                val = self.formatTime(record)
            else:
                val = getattr(record, attr, None)
            log[attr] = val
        return json.dumps(log, cls=JsonDateTimeEncoder)


class StoreHandler(logging.Handler):

    def __init__(self, store):
        logging.Handler.__init__(self)
        self._batcher = Batcher(5, store.add).start()

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
