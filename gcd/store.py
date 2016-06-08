import logging
import threading
import random
import re
import zlib
import time
import pickle
import psycopg2
import json as json_

from unittest import TestCase
from operator import attrgetter
from psycopg2.pool import ThreadedConnectionPool

from gcd.etc import identity, attrsetter, snippet
from gcd.nix import sh

logger = logging.getLogger(__name__)


def execute(sql, args=(), cursor=None, values=False):
    return _execute('execute', sql, args, cursor, values)


def executemany(sql, args, cursor=None):
    return _execute('executemany', sql, args, cursor, False)


class Transaction:

    pool = None

    _local = threading.local()

    def active():
        return getattr(Transaction._local, 'active', None)

    def __init__(self, conn_or_pool=None):
        conn_or_pool = conn_or_pool or Transaction.pool
        self._pool = self._conn = None
        if hasattr(conn_or_pool, 'cursor'):
            self._conn = conn_or_pool
        else:
            self._pool = conn_or_pool

    def __enter__(self):
        active = Transaction.active()
        if active:
            return active
        Transaction._local.active = self
        if self._pool:
            self._conn = self._pool.acquire()
        self._cursors = []
        return self

    def cursor(self, *args, **kwargs):
        cursor = self._conn.cursor(*args, **kwargs)
        self._cursors.append(cursor)
        return cursor

    def __exit__(self, type_, value, traceback):
        active = Transaction.active()
        if active != self:
            return
        try:
            for cursor in self._cursors:
                try:
                    if not getattr(cursor, 'withhold', False):
                        cursor.close()
                except Exception:
                    pass  # Might have been legitimately closed by the user.
            if type_ is None:
                self._conn.commit()
            else:
                logger.error('Transaction rollback',
                             exc_info=(type_, value, traceback))
                self._conn.rollback()
        finally:
            Transaction._local.active = None
            if self._pool:
                self._pool.release(self._conn)
                self._conn = None


class Store:

    def __init__(self, conn_or_pool=None, create=True):
        self._conn_or_pool = conn_or_pool
        if create:
            with self.transaction():
                self._creation_lock()
                self._create()

    def transaction(self):
        return Transaction(self._conn_or_pool)

    def _create(self):
        raise NotImplementedError


class PgStore(Store):

    def _creation_lock(self):
        execute('SELECT pg_advisory_xact_lock(0)')


class PgConnectionPool:

    def __init__(self, *args, min_conns=1, keep_conns=10, max_conns=10,
                 **kwargs):
        self._pool = ThreadedConnectionPool(
            min_conns, max_conns, *args, **kwargs)
        self._keep_conns = keep_conns

    def acquire(self):
        pool = self._pool
        conn = pool.getconn()
        pool.minconn = min(self._keep_conns, len(pool._used))
        return conn

    def release(self, conn):
        self._pool.putconn(conn)

    def close(self):
        if hasattr(self, '_pool'):
            self._pool.closeall()

    __del__ = close


class PgFlattener:

    def __init__(self, obj_type, json=False, gzip=False):
        assert not (json and gzip)
        self.obj_type = obj_type
        self.col_type = 'jsonb' if json else 'bytea'

        if json:
            pair = json_.dumps, identity
        elif not gzip:
            pair = pickle.dumps, pickle.loads
        else:
            pair = (lambda obj: zlib.compress(pickle.dumps(obj)),
                    lambda col: pickle.loads(zlib.decompress(col)))
        self._dumps, self._loads = pair

        if obj_type is None:
            pair = identity, None
        elif hasattr(obj_type, '__getstate__'):
            pair = obj_type.__getstate__, obj_type.__setstate__
        elif hasattr(obj_type, '__dict__'):
            pair = attrgetter('__dict__'), attrsetter('__dict__')
        self._get_state, self._set_state = pair

    def flatten(self, obj):
        return self._dumps(self._get_state(obj))

    def unflatten(self, col):
        state = self._loads(col)
        if self.obj_type:
            obj = self.obj_type.__new__(self.obj_type)
            self._set_state(obj, state)
            return obj
        else:
            return state


class PgTestCase(TestCase):

    db = 'test'

    def setUp(self):
        sh('dropdb --if-exists %s &> /dev/null' % self.db)
        sh('createdb %s' % self.db)

    def tearDown(self):
        # Try to kill it in bg because some conns might still be open.
        sh('dropdb %s &' % self.db)

    def connect(self, **kwargs):
        return psycopg2.connect(dbname=self.db, **kwargs)

    def pool(self, **kwargs):
        return PgConnectionPool(dbname=self.db, **kwargs)


def _execute(attr, sql, args, cursor, values):
    if cursor is None:
        cursor = Transaction.active().cursor()
    fun = getattr(cursor, attr)
    if values:
        sql, args = _values(sql, args)
    if logger.isEnabledFor(logging.DEBUG):
        _debugged(fun, sql, args)
    else:
        fun(sql, args)
    return cursor


def _values(sql, args):  # args can be any iterable.
    args_iter = iter(args)
    arg = next(args_iter)
    args = list(arg)
    args.extend(v for a in args_iter for v in a)
    value_sql = '(' + ','.join(['%s'] * len(arg)) + ')'
    values_sql = 'VALUES ' + ','.join([value_sql] * (len(args) // len(arg)))
    sql %= values_sql
    return sql, args


def _debugged(fun, sql, args):
    query_id = random.randint(0, 10000)
    log_sql = snippet(re.sub(r'[\n\t ]+', ' ', sql[:500]).strip(), 100)
    log_args = snippet(str(args[:20]), 100)
    logger.debug(dict(query=query_id, sql=log_sql, args=log_args))
    try:
        t0 = time.perf_counter()
        fun(sql, args)
        t1 = time.perf_counter()
        logger.debug(dict(query=query_id, time=t1 - t0))
    except:
        logger.exception(dict(query=query_id))
