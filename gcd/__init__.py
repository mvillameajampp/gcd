import os

from gcd.devel import install_builtins


if os.environ.get('PYDEV'):
    install_builtins()

del os, install_builtins
