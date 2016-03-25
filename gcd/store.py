import logging
import random
import re
import time

from itertools import chain
from gcd.utils import snippet
from psycopg2.pool import ThreadedConnectionPool


logger = logging.getLogger(__name__)


def execute(tx_or_cursor, sql, args=(), *, values=False):
    return _execute(tx_or_cursor, 'execute', sql, args, values)


def executemany(tx_or_cursor, sql, args):
    return _execute(tx_or_cursor, 'executemany', sql, args)


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


class Transaction:

    def __init__(self, conn_or_pool):
        self._pool = self._conn = None
        if hasattr(conn_or_pool, 'cursor'):
            self._conn = conn_or_pool
        else:
            self._pool = conn_or_pool

    def __enter__(self):
        if self._pool:
            self._conn = self._pool.acquire()
        self._cursors = []
        return self

    def cursor(self, *args, **kwargs):
        cursor = self._conn.cursor(*args, **kwargs)
        self._cursors.append(cursor)
        return cursor

    def __exit__(self, type_, value, traceback):
        try:
            for cursor in self._cursors:
                try:
                    if not getattr(cursor, 'withhold', False):
                        cursor.close()
                except Exception:
                    logger.exception('Error closing cursor')
            if type_ is None:
                self._conn.commit()
            else:
                logger.error('Transaction rollback',
                             exc_info=(type_, value, traceback))
                self._conn.rollback()
        finally:
            if self._pool:
                self._pool.release(self._conn)
                self._conn = None


class Store:

    def __init__(self, conn_or_pool):
        self._conn_or_pool = conn_or_pool

    def transaction(self):
        return Transaction(self._conn_or_pool)


def _execute(tx_or_cursor, attr, sql, args, values=False):
    if isinstance(tx_or_cursor, Transaction):
        cursor = tx_or_cursor.cursor()
    else:
        cursor = tx_or_cursor
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
    args_iter = chain((arg,), args_iter)
    args = tuple(v for a in args_iter for v in a)
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
        start_time = time.time()
        fun(sql, args)
        logger.debug(dict(query=query_id, time=time.time() - start_time))
    except:
        logger.exception(dict(query=query_id))
