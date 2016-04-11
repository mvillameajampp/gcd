import os
import sys
import subprocess
import fcntl
import argparse

from contextlib import contextmanager


path = os.path
env = os.environ
exit = sys.exit


def sh(cmd, input=None):
    if not isinstance(cmd, str):
        cmd = cmd[0] % tuple(sh.quote(arg) for arg in cmd[1:])
    if input is not None and not isinstance(input, str):
        input = '\n'.join(input)
    stdin = None if input is None else subprocess.PIPE
    stdout = stderr = None if cmd.rstrip().endswith('&') else subprocess.PIPE
    proc = subprocess.Popen(cmd, shell=True, universal_newlines=True,
                            stdin=stdin, stdout=stdout, stderr=stderr)
    if stdin or stdout:
        output, error = proc.communicate(input)
    if stdout:
        if proc.returncode != 0 or error:
            raise sh.Error(proc.returncode, cmd, output, error)
        else:
            return output.rstrip('\n')
sh.quote = lambda text: "'%s'" % text.replace("'", "'\\''")
sh.Error = subprocess.CalledProcessError


def expand(expr):
    return sh('echo %s' % expr)


def cat(path):
    with(open(path)) as file:
        return file.read().strip('\n')


def arg_parser(*args, **kwargs):
    parser = argparse.ArgumentParser(*args, **kwargs)
    return parser.add_argument, parser.parse_args


@contextmanager
def flock(path):
    with open(path, 'w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


@contextmanager
def cwd(path):
    prev = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(prev)
