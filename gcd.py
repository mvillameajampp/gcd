#!/usr/bin/env python

from gcd.make import cmd, pylint, pytest, make

cmd.sub(pylint())

cmd.sub(pytest(cov_pkgs='gcd'))

cmd.run(make)
