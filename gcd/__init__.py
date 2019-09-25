import os

if os.environ.get('PYDEV'):
    from gcd.devel import install_builtins
    install_builtins()
    del install_builtins
    if os.environ.get('PYDEV') == 'cy':
        import pyximport
        pyximport.install(reload_support=True)
        del pyimport


with open(os.path.join(os.path.dirname(__file__), "VERSION"), 'r') as vf:
   __version__ = vf.read().strip()


del os
