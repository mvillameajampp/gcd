from setuptools import setup

setup(
    name='gcd',
    version='1.0',
    packages=['gcd'],
    package_data={'gcd': ['res/*']},
    scripts=['bin/liftup', 'bin/wacky']
)
