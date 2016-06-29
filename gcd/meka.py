import os
import sys
import shelve
import textwrap

from itertools import chain

from gcd.etc import as_many, template
from gcd.nix import sh as _sh, sh_quote, cmd, as_cmd, path, argv


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


def meka(chdir=True):
    if chdir:
        os.chdir(path.dirname(argv[0]))
    cmd.arg('--quiet', '-q', action='store_true', help='Suppress messages.')


def echo(msg):
    if cmd.args.quiet:
        return
    try:
        width = int(_sh('stty size|').split()[1])
    except:
        width = 80
    lines = textwrap.wrap(msg, width=width)
    bar = '~' * max(len(l) for l in lines)
    msg = '\n'.join(lines)
    print('\n\033[1m%s\n%s\n%s\n\033[0m' % (bar, msg, bar), flush=True)


def sh(cmd, input=None):
    cmd = as_cmd(cmd)
    echo(cmd)
    return _sh(cmd, input)


def cmdclass(build=None, clean=None):
    from setuptools.command.install import install
    from distutils.command.clean import clean as clean_
    cmdclass = {}
    for cls, cmd in (install, build), (clean_, clean):  # noqa
        if cmd:
            class _(cls):
                def run(self, cmd=cmd):
                    _sh(cmd)
                    super().run()
            cmdclass[cls.__name__] = _
    return cmdclass


# ------------------------------ Rules ----------------------------------------


@rule
def render(tmpl, output, context={}, **kwargs):
    yield tmpl, output
    with open(output, 'w') as out_file:
        out_file.write(template(tmpl, **kwargs).render(context))


@rule
def ccompile(source, output='%(base)s.%(ext)s', incs=[], libs=[], inc_dirs=[],
             lib_dirs=[], cpp=False, debug=False, shared=True, clang=False,
             capi=False):
    def add_options(flag, options):
        return (' ' + ' '.join(flag + o for o in options)) if options else ''
    output_base = path.splitext(source)[0]
    output_ext = '.so' if shared else '.o'
    output %= {'base': output_base, 'ext': output_ext}
    yield source, incs, output
    if capi:
        base = path.dirname(path.dirname(path.abspath(sys.executable)))
        python = 'python' + sys.version[:3]
        inc_dirs.append(path.join(base, 'include', python + sys.abiflags))
        lib_dirs.append(path.join(base, 'lib', python))
        libs.append(python + sys.abiflags)
    if cpp:
        cmd = ('clang++' if clang else 'g++') + ' --std=c++14'
    else:
        cmd = ('clang' if clang else 'gcc') + ' --std=c11 --ms-extensions'
    cmd += ' -Werror'
    cmd += ' -g' if debug else ' -O3'
    cmd += ' -fPIC --shared' if shared else ''
    cmd += add_options('-I', inc_dirs)
    cmd += add_options('-L', lib_dirs)
    cmd += add_options('-l', libs)
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


def gen(tmpl, **kwargs):
    def gen():
        for name, default in kwargs.items():
            cmd.arg('--' + name, default=default)
        cmd.arg('--output', '-o', default='/dev/stdout')
        yield
        context = {n: getattr(cmd.args, n) for n in kwargs}
        render(tmpl, cmd.args.output, context)
    gen.__name__ = 'gen' + path.splitext(path.basename(tmpl))[0]
    gen.__doc__ = 'Generate output from %s jinja2 template.' % tmpl
    return gen


def pylint(omit=[]):
    def lint():
        'Run flake8 linter'
        yield
        sh("shopt -s globstar; flake8 -v --exclude '%s' --ignore E306 "
           "**/*.py || true" % ','.join(as_many(omit)))
    return lint


def pytest(cov_pkgs, cov_omit=[]):
    def test():
        'Run unit tests.'

        cmd.arg('--pattern', '-p', help='Only run tests matching pattern.')
        cmd.arg('--integration', '-i', action='store_true',
                help='Also run integration tests (itest_*.py pattern).')
        cmd.arg('--coverage', '-c', action='store_true',
                help='Show coverage report after testing.')
        yield

        def sh_test(pattern, append=False):
            if args.coverage:
                prefix = "coverage run %s--source='%s' --omit='%s'" % (
                    '-a ' if append else '',
                    ','.join(as_many(cov_pkgs)),
                    ','.join(as_many(cov_omit)))
            else:
                prefix = 'python'
            sh("%s -m unittest discover -v -t . -s tests -p %s || true" %
               (prefix, sh_quote(pattern)))

        args = cmd.args
        pattern = args.pattern if args.pattern else 'test_*.py'
        code = sh_test(pattern)
        if args.integration and not args.pattern:
            code = code or sh_test('itest_*.py', True)
        if args.coverage:
            sh('coverage report')
        if code != 0:
            sys.exit(1)

    return test


def build(builder, modules):
    def build():
        'Build binary extensions.'
        cmd.arg('--debug', '-d', action='store_true',
                help='Compile with debug information.')
        cmd.arg('--module', '-m', choices=modules,
                help='Build only the specified module.')
        yield
        builder([cmd.args.module] if cmd.args.module else modules,
                cmd.args.debug)
    return build


def clean(*paths):
    def clean():
        'Clean generated files.'
        yield
        for path in paths:  # noqa
            try:
                os.remove(path)
            except FileNotFoundError:
                pass
    return clean
