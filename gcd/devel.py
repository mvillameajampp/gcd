from __future__ import print_function

import socket
import os
import sys
import pdb
import pprint
import threading as mt

from inspect import currentframe, getframeinfo
from contextlib import contextmanager
from pprint import PrettyPrinter

from gcd.nix import flock, sh

try:
    from importlib import reload
except ImportError:  # Python 2.
    reload = reload


__all__ = ['reload', 'echo', 'lecho', 'pecho', 'trace', 'brk', 'rbrk', 'fbrk',
           'fixrl']


def echo(*args, **kwargs):
    file = kwargs.pop('file', sys.stderr)
    print(*args, file=file, flush=True, **kwargs)


def lecho(*args, **kwargs):
    info = getframeinfo(currentframe().f_back)
    path = os.path.relpath(info.filename)
    echo('[%s:%s]' % (path, info.lineno), *args, **kwargs)


def pecho(obj, classes=None, file=sys.stderr, *args, **kwargs):
    with patched_pprint(classes):
        pprint.pprint(obj, stream=file, *args, **kwargs)
        file.flush()


def trace(fun):
    def prefix(n, m=0):
        return '| ' * n + ' ' * m
    def pformat(prefix, obj):
        with patched_pprint():
            text = pprint.pformat(obj, compact=True, width=cols - len(prefix))
            return text.replace('\n', '\n' + prefix)
    def traced(*args, **kwargs):
        level = getattr(trace._local, 'level', 0)
        col = min(level, 10)
        name = fun.__name__
        echo('%s%s%s' % (
            prefix(col), name,
            pformat(prefix(col + 1, len(name) - 2), (args, kwargs))))
        try:
            trace._local.level = level + 1
            res = fun(*args, **kwargs)
            return res
        except Exception as err:
            res = err
            raise
        finally:
            echo('%s`> %s' % (prefix(col), pformat(prefix(col, 3), res)))
            trace._local.level -= 1
    cols = int(sh('stty size|').split()[1])
    return traced


trace._local = mt.local()


brk = pdb.set_trace


def rbrk(port=4000, host='localhost'):
    rdb = RemotePdb((host, port))
    rdb.set_trace(frame=sys._getframe().f_back)


def fbrk():
    fdb = ForkablePdb()
    fdb.set_trace(frame=sys._getframe().f_back)


def fixrl():
    # Workaround until 3.5.2: fix readline notion of current terminal width.
    # https://bugs.python.org/issue23735
    import ctypes
    ctypes.cdll['libreadline.so'].rl_resize_terminal()
    print('Readline fixed.')


@contextmanager
def patched_pprint(classes=None):
    def simplify(obj):
        if (obj.__class__.__repr__ not in PrettyPrinter._dispatch and
                (not classes or obj.__class__ in classes) and
                hasattr(obj, '__dict__')):
            obj = (obj.__class__.__qualname__, vars(obj))
        return obj

    def new_safe_repr(obj, *args, **kwargs):
        return old_safe_repr(simplify(obj), *args, **kwargs)
    old_safe_repr, pprint._safe_repr = pprint._safe_repr, new_safe_repr

    def new_format(self, obj, *args, **kwargs):
        return old_format(self, simplify(obj), *args, **kwargs)
    old_format, PrettyPrinter._format = PrettyPrinter._format, new_format

    yield

    pprint._safe_repr, PrettyPrinter._format = old_safe_repr, old_format


class RemotePdb(pdb.Pdb):

    def __init__(self, address):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
        sock.bind(address)
        with flock('/tmp/rdb_lock'):
            echo('>> rdb listening at %s:%s...' % sock.getsockname())
            sock.listen(1)
            conn, address = sock.accept()
            echo(' connection accepted.\n')
        self.cfile = conn.makefile('rw')
        pdb.Pdb.__init__(self, stdin=self.cfile, stdout=self.cfile)


class ForkablePdb(pdb.Pdb):

    pid = None

    def __init__(self):
        pdb.Pdb.__init__(self, nosigint=True)

    def interaction(self, frame, traceback):
        with flock('/tmp/fdb_lock'):
            if ForkablePdb.pid != os.getpid():
                sys.stdin = os.fdopen(0)
                ForkablePdb.pid = os.getpid()
            pdb.Pdb.interaction(self, frame, traceback)

    def _cmdloop(self):
        self.cmdloop()
