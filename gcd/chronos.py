import os
import time
import calendar


ms = millisecond = milliseconds = 1 / 1000
second = seconds = 1000 * milliseconds
minute = minutes = 60 * seconds
hour = hours = 60 * minutes
day = days = 24 * hours
week = weeks = 7 * days
month = months = 4 * weeks
year = years = 12 * months


def parse(datetime):
    formats = ('%Y-%m-%d %H:%M:%S.%N', '%Y-%m-%dZ%H:%M:%S.%N',
               '%Y-%m-%d %H:%M:%S', '%Y-%m-%dZ%H:%M:%S',
               '%Y-%m-%d', '%H:%M:%S', '%H:%M:%S.%N')
    for format in formats:
        try:
            return calendar.timegm(time.strptime(datetime, format))
        except ValueError:
            if format is formats[-1]:
                raise


def set_timezone(timezone=None):
    if timezone:
        os.environ['TZ'] = timezone
        time.tzset()
    elif 'TZ' in os.environ:
        del os.environ['TZ']


class Timer:

    def __init__(self, period, start_now=False, align=False):
        assert not (start_now and align)
        self.period = period
        now = time.time()
        last_time = (int(now / period) * period) if align else now
        self._next_time = last_time + (0 if start_now else period)

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
