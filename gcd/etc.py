import operator
import logging

from itertools import islice, chain
from functools import reduce, lru_cache
from contextlib import contextmanager


logger = logging.getLogger(__name__)


def identity(x):
    return x


def nop(*args, **kwargs):
    pass


def attrsetter(name):
    return lambda obj, value: setattr(obj, name, value)


def sign(x):
    return -1 if x < 0 else 1


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


@contextmanager
def as_file(file_or_path, *args, **kwargs):
    if type(file_or_path) is str:
        file = open(file_or_path, *args, **kwargs)
    else:
        file = file_or_path
    try:
        yield file
    finally:
        if type(file_or_path) is str:
            file.close()


def load_pyconfig(file_or_path, config=None):
    config = config or Config()
    config.__dict__['Config'] = Config
    with as_file(file_or_path) as cfg_file:
        exec(cfg_file.read(), config.__dict__)
    return config


def retry_on(errors, attempts=3):
    def decorator(fun):
        def wrapper(*args, **kwargs):
            for i in range(attempts):
                try:
                    return fun(*args, **kwargs)
                except errors:
                    logger.exception('Retrying %s, %s/%s attempts' %
                                     (fun.__name__, i + 1, attempts))
        return wrapper
    errors = as_many(errors, tuple)
    return decorator


@lru_cache(maxsize=100)
def template(file_or_path_or_str, **kwargs):
    import jinja2
    environment = jinja2.Environment(
        line_statement_prefix=kwargs.pop('line_statement_prefix', '%'),
        trim_blocks=kwargs.pop('trim_blocks', True),
        lstrip_blocks=kwargs.pop('lstrip_blocks', True),
        **kwargs)
    try:
        with as_file(file_or_path_or_str) as tmpl_file:
            return environment.from_string(tmpl_file.read())
    except FileNotFoundError:
        return environment.from_string(file_or_path_or_str)


class Bundled:

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __repr__(self):
        return repr(self.__dict__)


class Bundle(Bundled):

    def __init__(self, dict=None, **kwargs):
        if dict is not None:
            self.__dict__ = dict
        self.__dict__.update(kwargs)


class Config(Bundle):
    pass


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
