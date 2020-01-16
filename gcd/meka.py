import os
import re
import sys
import shelve
import textwrap

from distutils.core import Extension
from distutils.command.build_ext import build_ext as build_ext_

from gcd.etc import template
from gcd.nix import sh as _sh, cmd, as_cmd, path, argv


def rule(fun):
    def wrapper(*args, **kwargs):
        code = fun.__code__.co_code
        gen = fun(*args, **kwargs)
        inputs, outputs = next(gen)
        mtimes = {i: path.getmtime(i) for i in inputs}
        mtimes.update((o, path.getmtime(o)) for o in outputs if path.exists(o))
        key = "@@".join(sorted(outputs))
        with shelve.open(_memo) as memo:
            if memo.get(key) == (code, args, kwargs, mtimes):
                return
            try:
                next(gen)
            except StopIteration:
                pass
            mtimes.update((o, path.getmtime(o)) for o in outputs)
            memo[key] = code, args, kwargs, mtimes

    return wrapper


_memo = ".%s.memo" % path.splitext(path.basename(sys.argv[0]))[0]


def meka(chdir=True):
    if chdir:
        os.chdir(path.dirname(argv[0]))
    cmd.arg("--quiet", "-q", action="store_true", help="Suppress messages.")


def echo(msg, *args, **kwargs):
    if cmd.args.quiet:
        return
    width = 0
    try:
        width = int(_sh("stty size|").split()[1])
    except Exception:  # Not in tty.
        pass
    if width <= 0:  # Inside ansible width == -1, why?
        width = 80
    lines = textwrap.wrap(msg, width=width)
    bar = "~" * max(len(l) for l in lines)
    msg = "\n".join(lines)
    print(
        "\n\033[1m%s\n%s\n%s\n\033[0m" % (bar, msg, bar),
        flush=True,
        file=sys.stderr,
        *args,
        **kwargs
    )


def sh(cmd, input=None):
    cmd = as_cmd(cmd)
    echo(cmd)
    return _sh(cmd, input)


# ------------------------------ Rules ----------------------------------------


@rule
def render(tmpl, output, context={}, **kwargs):
    yield [tmpl], [output]
    with open(output, "w") as out_file:
        out_file.write(template(tmpl, **kwargs).render(context))


@rule
def ccompile(
    source,
    output="%(base)s.%(ext)s",
    incs=[],
    libs=[],
    inc_dirs=[],
    lib_dirs=[],
    cpp=False,
    debug=False,
    shared=True,
    clang=False,
    capi=False,
):
    def add_options(flag, options):
        return (" " + " ".join(flag + o for o in options)) if options else ""

    output_base = path.splitext(source)[0]
    output_ext = ".so" if shared else ".o"
    output %= {"base": output_base, "ext": output_ext}
    yield [source, incs], [output]
    if capi:
        base = path.dirname(path.dirname(path.abspath(sys.executable)))
        python = "python" + sys.version[:3]
        inc_dirs.append(path.join(base, "include", python + sys.abiflags))
        lib_dirs.append(path.join(base, "lib", python))
        libs.append(python + sys.abiflags)
    if cpp:
        cmd = ("clang++" if clang else "g++") + " --std=c++14"
    else:
        cmd = ("clang" if clang else "gcc") + " --std=c11 --ms-extensions"
    cmd += " -Werror"
    cmd += " -g" if debug else " -O3"
    cmd += " -fPIC --shared" if shared else ""
    cmd += add_options("-I", inc_dirs)
    cmd += add_options("-L", lib_dirs)
    cmd += add_options("-l", libs)
    sh("%s -o %s %s" % (cmd, output, source))


# ------------------------------ Commands -------------------------------------


def gen(tmpl, **kwargs):
    def gen():
        for name, default in kwargs.items():
            cmd.arg("--" + name, default=default)
        cmd.arg("--output", "-o", default="/dev/stdout")
        yield
        context = {n: getattr(cmd.args, n) for n in kwargs}
        render(tmpl, cmd.args.output, context)

    gen.__name__ = "gen" + path.splitext(path.basename(tmpl))[0]
    gen.__doc__ = "Generate output from %s jinja2 template." % tmpl
    return gen


def build(builder, modules):
    def build():
        """Build binary extensions."""
        cmd.arg(
            "--debug", "-d", action="store_true", help="Compile with debug information."
        )
        cmd.arg(
            "--module", "-m", choices=modules, help="Build only the specified module."
        )
        yield
        builder([cmd.args.module] if cmd.args.module else modules, cmd.args.debug)

    return build


def clean(*paths):
    def clean():
        """Clean generated files."""
        yield
        for path in paths:  # noqa
            try:
                os.remove(path)
            except FileNotFoundError:
                pass

    return clean


# ------------------------------ Disutils -------------------------------------


class CExtension(Extension):

    pass


class build_ext(build_ext_):
    def get_export_symbols(self, ext):
        if isinstance(ext, CExtension):
            return ext.export_symbols
        else:
            return super().get_export_symbols(ext)

    def get_ext_fullpath(self, ext_name):
        path = super().get_ext_fullpath(ext_name)
        ext = next(e for e in self.extensions if e.name == ext_name)
        if isinstance(ext, CExtension):
            path = re.sub(r"/[^/]*$", ".so", path)
        return path
