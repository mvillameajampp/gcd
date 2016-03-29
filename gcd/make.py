import os
import sys
import shelve
import textwrap
import argparse

from os import path
from itertools import chain

from gcd.etc import as_many
from gcd.work import sh as _sh, cwd


def rule(fun):
    def wrapper(*args, **kwargs):
        code = fun.__code__.co_code
        gen = fun(*args, **kwargs)
        paths = next(gen)
        if type(paths) is str:
            inputs, output = [], paths
        else:
            *inputs, output = paths
        mtimes = {i: path.getmtime(i)
                  for i in chain.from_iterable(map(as_many, inputs))}
        with shelve.open(_memo) as memo:
            if path.exists(output) and output in memo:
                mtimes[output] = path.getmtime(output)
                if memo[output] == (code, args, kwargs, mtimes):
                    return
            try:
                next(gen)
            except StopIteration:
                pass
            mtimes[output] = path.getmtime(output)
            memo[output] = code, args, kwargs, mtimes
    return wrapper
_memo = '.%s.memo' % path.splitext(path.basename(sys.argv[0]))[0]


def command(fun, name=None):
    if name:
        fun.__name__ = name
    _commands.append(fun)
_commands = []


def run(commands=None, argv=sys.argv):
    global args
    with cwd(path.dirname(argv[0]) or '.'):
        subparsers = _parser.add_subparsers(dest='_cmd')
        subparsers.required = True
        for cmd in commands or _commands:
            subparser = subparsers.add_parser(cmd.__name__, help=cmd.__doc__)
            gen = cmd(subparser.add_argument)
            subparser.set_defaults(_gen=gen)
            next(gen)
        args = _parser.parse_args(argv[1:])
        try:
            next(args._gen)
        except StopIteration:
            pass
_parser = argparse.ArgumentParser()
arg = _parser.add_argument
arg('--quiet', '-q', action='store_true', help='Omit messages.')


def echo(msg):
    if args.quiet:
        return
    try:
        width = int(_sh('stty size').split()[1])
    except:
        width = 80
    lines = textwrap.wrap(msg, width=width)
    bar = '=' * max(len(l) for l in lines)
    msg = '\n'.join(lines)
    print('\n\033[1m%s\n%s\n%s\n\033[0m' % (bar, msg, bar), flush=True)


def sh(cmd, exit=True):
    echo(cmd)
    code = os.system(cmd)
    if exit and code != 0:
        sys.exit(1)
    return code


def cmdclass(install=None, clean=None):
    from setuptools.command.install import install as install_
    from distutils.command.clean import clean as clean_
    cmdclass = {}
    for name, cls, cmd in (('install', install_, install),
                           ('clean', clean_, clean)):
        if cmd:
            class _(cls):
                def run(self, cmd=cmd):
                    if os.system(cmd) == 0:
                        super().run()
            cmdclass[name] = _
    return cmdclass


# ------------------------------ Rules ----------------------------------------


@rule
def preprocess(pre, post, context={}, markers=['$', '$', '@', '`', '`']):
    yield pre, post
    import jinja2
    environment = jinja2.Environment(
        block_start_string=markers[0],
        block_end_string=markers[1],
        line_statement_prefix=markers[2],
        variable_start_string=markers[3],
        variable_end_string=markers[4],
        trim_blocks=True,
        lstrip_blocks=True)
    with open(pre) as pre_file:
        template = environment.from_string(pre_file.read())
    with open(post, 'w') as post_file:
        post_file.write(template.render(context))


@rule
def ccompile(source, output='%(base)s.%(ext)s', includes=[], libraries=[],
             include_dirs=[], library_dirs=[], cpp=False, debug=False,
             shared=True, clang=False, capi=False):
    def add_options(flag, options):
        return (' ' + ' '.join(flag + o for o in options)) if options else ''
    output_base = path.splitext(source)[0]
    output_ext = '.so' if shared else '.o'
    output %= {'base': output_base, 'ext': output_ext}
    yield source, includes, output
    if capi:
        base = path.dirname(path.dirname(path.abspath(sys.executable)))
        python = 'python' + sys.version[:3]
        include_dirs.append(path.join(base, 'include', python + sys.abiflags))
        library_dirs.append(path.join(base, 'lib', python))
        libraries.append(python + sys.abiflags)
    if cpp:
        cmd = ('clang++' if clang else 'g++') + ' --std=c++14'
    else:
        cmd = ('clang' if clang else 'gcc') + ' --std=c11 --ms-extensions'
    cmd += ' -Werror'
    cmd += ' -g' if debug else ' -O3'
    cmd += ' -fPIC --shared' if shared else ''
    cmd += add_options('-I', include_dirs)
    cmd += add_options('-L', library_dirs)
    cmd += add_options('-l', libraries)
    sh('%s -o %s %s' % (cmd, output, source))


@rule
def cythonize(source, debug=False, annotate=False):
    csource = path.splitext(source)[0] + '.c'
    yield source, csource
    cmd = 'cython'
    cmd += ' --gdb' if debug else ''
    cmd += ' -a' if annotate else ''
    sh('%s %s' % (cmd, source))


# ------------------------------ Commands -------------------------------------


def pylint(omit=[]):
    def lint(_):
        'Run flake8 linter'
        yield
        sh("shopt -s globstar; flake8 --exclude '%s' **/*.py" %
           ','.join(as_many(omit)))
    return lint


def pytest(coverage_packages, coverage_omit=[]):
    def test(arg):
        'Run unit tests.'

        arg('--pattern', '-p', help='Only run tests matching pattern.')
        arg('--integration', '-i', action='store_true',
            help='Also run integration tests (itest_*.py pattern).')
        arg('--coverage', '-c', action='store_true',
            help='Show coverage report after testing.')
        yield

        def cmd(pattern, append=False):
            if args.coverage:
                prefix = "coverage run %s--source='%s' --omit='%s'" % (
                    '-a ' if append else '',
                    ','.join(as_many(coverage_packages)),
                    ','.join(as_many(coverage_omit)))
            else:
                prefix = 'python'
            return "%s -m unittest discover -v -t . -s tests -p '%s'" % (
                prefix, pattern)

        pattern = args.pattern if args.pattern else 'test_*.py'
        code = sh(cmd(pattern), exit=False)
        if args.integration and not args.pattern:
            code += sh(cmd('itest_*.py', True), exit=False)
        if args.coverage:
            sh('coverage report')
        if code != 0:
            sys.exit(1)

    return test


def build(builder, modules):
    def build(arg):
        'Build binary extensions.'
        arg('--debug', '-d', action='store_true',
            help='Compile with debug information.')
        arg('--module', '-m', choices=modules,
            help='Build only the specified module.')
        yield
        builder([args.module] if args.module else modules, args.debug)
    return builder


def clean(*paths):
    def clean(args):
        'Clean generated files.'
        yield
        for path in paths:  # noqa
            os.remove(path)
    return clean
