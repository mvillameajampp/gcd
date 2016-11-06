from setuptools import setup

from gcd.meka import cmd, pylint, pytest, meka


cmd.sub(pylint())

cmd.sub(pytest(cov_pkgs='gcd'))

setup(
    name='gcd',
    version='1.0',
    packages=['gcd'],
    package_data={'gcd': ['resources/*']},
    scripts=['scripts/docdoc', 'scripts/wacky'],
    cmdclass=cmd.cmdclass
)
