import os

if os.environ.get('PYDEV'):
    from gcd.devel import install_builtins
    install_builtins()
    del install_builtins

del os
