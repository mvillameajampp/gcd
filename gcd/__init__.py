import os

if 'DAATDEV' in os.environ:
    from gcd.devel import install_builtins
    install_builtins()
