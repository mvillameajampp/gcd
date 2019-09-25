import os
from setuptools import setup


with open(os.path.join(os.path.dirname(__file__), "gcd/VERSION"), 'r') as vf:
    version = vf.read().strip()


setup(
    name='gcd',
    version=version,
    packages=['gcd'],
    package_data={'gcd': ['resources/*']},
    scripts=['scripts/docdoc', 'scripts/wacky']
)
