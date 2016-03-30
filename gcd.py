#!/usr/bin/env python

from gcd.make import command, pylint, pytest, run

command(pylint())

command(pytest(coverage_packages='gcd'))

run()
