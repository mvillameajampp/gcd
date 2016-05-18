import os
import time

from datetime import datetime


ms = millisecond = milliseconds = 1 / 1000
second = seconds = 1000 * milliseconds
minute = minutes = 60 * seconds
hour = hours = 60 * minutes
day = days = 24 * hours
week = weeks = 7 * days
month = months = 4 * weeks
year = years = 12 * months


def gm_parse(string, format=None):
    if format:
        formats = format,
    else:
        formats = ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S',
                   '%Y-%m-%dZ%H:%M:%S.%f', '%Y-%m-%dZ%H:%M:%S',
                   '%Y-%m-%d', '%H:%M:%S.%f', '%H:%M:%S')
    for format in formats:
        try:
            return datetime.strptime(string, format).timestamp()
        except ValueError:
            if format is formats[-1]:
                raise


def gm_format(seconds, format='%Y-%m-%d %H:%M:%S.%f'):
    return datetime.fromtimestamp(seconds).strftime(format)


def set_timezone(timezone=None):
    if timezone:
        os.environ['TZ'] = timezone
        time.tzset()
    elif 'TZ' in os.environ:
        del os.environ['TZ']


def as_timer(period_or_timer):
    if isinstance(period_or_timer, Timer):
        return period_or_timer
    else:
        return Timer(period_or_timer)


class Timer:

    def __init__(self, period, start_at=None, align=False):
        assert not (start_at and align)
        self.period = period
        if start_at:
            self._next_time = start_at
        else:
            now = time.time()
            last_time = (int(now / period) * period) if align else now
            self._next_time = last_time + period

    @property
    def is_time(self):
        now = time.time()
        if now >= self._next_time:
            while now >= self._next_time:
                self._next_time += self.period
            return True
        else:
            return False

    def wait(self):
        while not self.is_time:
            time.sleep(max(0, self._next_time - time.time()))


class LeakyBucket:

    def __init__(self, freq):
        self._creation_time = time.time()
        self._period = 1 / freq
        self._used = 0

    def wait(self):
        now = time.time()
        leaked = int((now - self._creation_time) / self._period)
        if self._used > leaked:
            next_leak_time = self._creation_time + (leaked + 1) * self._period
            time.sleep(next_leak_time - now)
        self._used += 1
