import os
import time

from datetime import datetime, timedelta


def utc(*args, **kwargs):
    if args and type(args[0]) is str:
        iso = args[0].replace("T", " ").rstrip("Z")
        formats = (
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
            "%H:%M:%S.%f",
            "%H:%M:%S",
        )
        for format in formats:
            try:
                return strptime(iso, format)
            except ValueError:
                if format is formats[-1]:
                    raise
    else:
        return datetime(*args, **kwargs).timestamp()


def iso(ts):
    return strftime(ts, "%Y-%m-%d %H:%M:%S.%f")


def strptime(str, format):
    return datetime.strptime(str, format).timestamp()


def strftime(ts, format):
    return datetime.fromtimestamp(ts).strftime(format)


def span(*args, **kwargs):
    return timedelta(*args, **kwargs).total_seconds()


def trunc(ts, span):
    return (ts // span) * span


def set_timezone(timezone=None):
    if timezone:
        os.environ["TZ"] = timezone
    elif "TZ" in os.environ:
        del os.environ["TZ"]
    time.tzset()


def as_datetime(ts):
    return datetime.fromtimestamp(ts)


def as_memory(memory):
    try:
        memory, period = memory
    except TypeError:
        memory, period = memory, 1
    assert 0 < memory <= 1
    return memory ** (1 / period)


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
    def __init__(self, freq, capacity):
        self._period = 1 / freq
        self._capacity = capacity
        self._used = 0
        self._last_leak = time.time()

    def use(self):
        if self._used == self._capacity:
            leaked = int((time.time() - self._last_leak) / self._period)
            if leaked == 0:
                return False
            self._last_leak += leaked * self._period
            self._used = max(0, self._used - leaked)
        self._used += 1
        return True

    def wait(self, space=1):
        assert space <= self._capacity
        to_leak = space - (self._capacity - self._used)
        if to_leak > 0:
            now = time.time()
            until = to_leak * self._period + self._last_leak
            if until > now:
                time.sleep(until - now)
            self._last_leak = until
            self._used = max(0, self._used - to_leak)
