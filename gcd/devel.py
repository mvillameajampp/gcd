from __future__ import print_function

import socket
import os
import sys
import pdb
import pprint

from inspect import currentframe, getframeinfo
from contextlib import contextmanager
from pprint import PrettyPrinter

from gcd.work import flock

try:
    import builtins
except ImportError:  # Python 2.
    import __builtin__ as builtins

try:
    from importlib import reload
except ImportError:  # Python 2.
    reload = reload


__all__ = ['reload', 'echo', 'lecho', 'pecho', 'brk', 'rbrk', 'fbrk']


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


brk = pdb.set_trace


def rbrk(port=4000, host='localhost'):
    rdb = RemotePdb((host, port))
    rdb.set_trace(frame=sys._getframe().f_back)


def fbrk():
    fdb = ForkablePdb()
    fdb.set_trace(frame=sys._getframe().f_back)


def install_builtins():
    for attr in __all__:
        setattr(builtins, attr, globals()[attr])


@contextmanager
def patched_pprint(classes):
    def simplify(obj):
        if (obj.__class__.__repr__ not in PrettyPrinter._dispatch and
                (not classes or obj.__class__ in classes) and
                hasattr(obj, '__dict__')):
            obj = obj.__dict__
        return obj

    def safe_repr(obj, *args, **kwargs):
        return _safe_repr(simplify(obj), *args, **kwargs)
    _safe_repr, pprint._safe_repr = pprint._safe_repr, safe_repr

    def format(self, obj, *args, **kwargs):
        return _format(self, simplify(obj), *args, **kwargs)
    _format, PrettyPrinter._format = PrettyPrinter._format, format

    yield

    pprint._safe_repr, PrettyPrinter._format = _safe_repr, _format


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
