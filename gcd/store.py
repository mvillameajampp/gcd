import logging
import random
import re
import time
import json
import psycopg2
import select
import threading as mt

from unittest import TestCase
from psycopg2.pool import ThreadedConnectionPool

from gcd.etc import snippet, Bundle
from gcd.nix import sh


logger = logging.getLogger(__name__)


def execute(sql, args=(), cursor=None, values=False, named=False):
    return _execute("execute", sql, args, cursor, values, named)


def executemany(sql, args, cursor=None, named=False):
    return _execute("executemany", sql, args, cursor, False, named)


def named(cursor, rows=None):
    if rows is None:
        rows = iter(cursor.fetchone, None)
    names = [d[0] for d in cursor.description]
    for row in rows:
        yield Bundle(zip(names, row))


class Transaction:

    pool = None

    _local = mt.local()

    @staticmethod
    def active():
        return getattr(Transaction._local, "active", None)

    def __init__(self, conn_or_pool=None):
        conn_or_pool = conn_or_pool or Transaction.pool
        self._pool = self._conn = None
        if hasattr(conn_or_pool, "cursor"):
            self._conn = conn_or_pool
        else:
            self._pool = conn_or_pool

    def __enter__(self):
        active = Transaction.active()
        if active:
            return active
        if self._pool:
            self._conn = self._pool.acquire()
        self._cursors = []
        Transaction._local.active = self
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
                    if not getattr(cursor, "withhold", False):
                        cursor.close()
                except Exception:
                    pass  # Might have been legitimately closed by the user.
            if type_ is None:
                self._conn.commit()
            else:
                logger.error("Transaction rollback", exc_info=(type_, value, traceback))
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

    def _creation_lock(self):
        raise NotImplementedError


class PgStore(Store):
    def _creation_lock(self):
        execute("SELECT pg_advisory_xact_lock(0)")


class PgConnectionPool:
    def __init__(self, *args, min_conns=1, keep_conns=10, max_conns=10, **kwargs):
        self._pool = ThreadedConnectionPool(min_conns, max_conns, *args, **kwargs)
        self._keep_conns = keep_conns

    def acquire(self):
        pool = self._pool
        conn = pool.getconn()
        pool.minconn = min(self._keep_conns, len(pool._used))
        return conn

    def release(self, conn):
        self._pool.putconn(conn)

    def close(self):
        if hasattr(self, "_pool"):
            self._pool.closeall()

    __del__ = close


class PgTestCase(TestCase):

    db = "test"

    def setUp(self):
        sh(("{ dropdb --if-exists %s > /dev/null 2>&1 ; } || true", self.db))
        sh(("createdb %s", self.db))
        self._to_close = []

    def tearDown(self):
        for conn_or_pool in self._to_close:
            conn_or_pool.close()
        sh(("{ dropdb %s > /dev/null 2>&1 ; } || true", self.db))

    def connect(self, **kwargs):
        conn = psycopg2.connect(dbname=self.db, **kwargs)
        self._to_close.append(conn)
        return conn

    def pool(self, **kwargs):
        pool = PgConnectionPool(dbname=self.db, **kwargs)
        self._to_close.append(pool)
        return pool


class PrestoError(Exception):
    pass


def query_presto_cli(query, command="presto-cli", **kwargs):
    if query and query[-1] != ";":
        query += ";"
    kwargs.update(file="/dev/stdin", output_format="JSON")
    args = ("--%s %s" % (k.replace("_", "-"), v) for k, v in kwargs.items())
    proc = sh("exec %s %s |&" % (command, " ".join(args)), query)
    try:
        while True:
            row = proc.stdout.readline()
            if not row:
                return
            yield json.loads(row)
    finally:
        return_code = proc.poll()
        if return_code == 0:
            return
        try:
            stderr = select.select([proc.stderr], [], [], 0.5)[0][0]
            error_message = stderr.readline().rstrip("\n")
        except IndexError:
            error_message = "Finished or aborted with code %s" % return_code
        if return_code is None:
            try:
                proc.stdout.close()
                proc.terminate()
                proc.wait()
            except Exception:
                logger.exception("Failed to terminate %s process", command)
        if error_message:
            raise PrestoError(error_message)


def _execute(attr, sql, args, cursor, values, named_):
    if cursor is None:
        cursor = Transaction.active().cursor()
    fun = getattr(cursor, attr)
    if values:
        sql, args = _values(sql, args)
    if logger.isEnabledFor(logging.DEBUG):
        _debugged(fun, sql, args)
    else:
        fun(sql, args)
    return named(cursor) if named_ else cursor


def _values(sql, args):  # args can be any iterable.
    args_iter = iter(args)
    arg = next(args_iter)
    args = list(arg)
    args.extend(v for a in args_iter for v in a)
    value_sql = "(" + ",".join(["%s"] * len(arg)) + ")"
    values_sql = "VALUES " + ",".join([value_sql] * (len(args) // len(arg)))
    sql %= values_sql
    return sql, args


def _debugged(fun, sql, args):
    query_id = random.randint(0, 10000)
    log_sql = snippet(re.sub(r"[\n\t ]+", " ", sql[:500]).strip(), 100)
    log_args = snippet(
        str((list(args.items()) if isinstance(args, dict) else args)[:20]), 100
    )
    logger.debug(dict(query=query_id, sql=log_sql, args=log_args))
    try:
        t0 = time.perf_counter()
        fun(sql, args)
        t1 = time.perf_counter()
        logger.debug(dict(query=query_id, time=t1 - t0))
    except Exception:
        logger.exception(dict(query=query_id))
