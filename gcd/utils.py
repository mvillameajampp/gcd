import os
import operator
import logging
import json
import datetime
import subprocess

from itertools import islice, chain
from functools import reduce
from contextlib import contextmanager

from gcd.work import sh


def product(iterable, start=1):
    return reduce(operator.mul, iterable, start)


def chunks(iterable, size):
    iterator = iter(iterable)
    while True:
        chunk = islice(iterator, size)
        try:
            yield chain((next(chunk),), chunk)
        except StopIteration:
            return


def snippet(text, length):
    if len(text) <= length:
        return text
    else:
        return text[:length-3] + '...'


def as_many(obj, as_type=None):
    if not isinstance(obj, (list, tuple, set)):
        obj = (obj,)
    if as_type and as_type is not type(obj):
        obj = as_type(obj)
    return obj


def as_file(file_or_path, *args, **kwargs):
    if type(file_or_path) is str:
        return open(file_or_path, *args, **kwargs)
    else:
        return file_or_path


def load_pyconfig(file_or_path, config=None):
    config = config or Config()
    config.__dict__['Config'] = Config
    exec(as_file(file_or_path).read(), config.__dict__)
    return config


def dmenu(choices=[], *args):
    try:
        cmd = 'dmenu ' + ' '.join(args)
        choices = '\n'.join(choices)
        return sh(cmd, choices).strip()
    except subprocess.CalledProcessError as error:
        if error.stderr:
            raise
        return None


@contextmanager
def cwd(path):
    prev = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(prev)


class BundleMixin:

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __repr__(self):
        return repr(self.__dict__)


class Bundle(BundleMixin):

    def __init__(self, **kwargs):
        self.__dict__ = kwargs


Config = Bundle


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


class PositionalAttribute:

    def install(attrs, scope, vals_attr):
        for index, attr in enumerate(attrs):
            scope[attr] = PositionalAttribute(index, vals_attr)

    def __init__(self, index, vals_attr):
        self.index = index
        self.vals_attr = vals_attr

    def __get__(self, obj, type=None):
        return getattr(obj, self.vals_attr)[self.index]

    def __set__(self, obj, val):
        getattr(obj, self.vals_attr)[self.index] = val
