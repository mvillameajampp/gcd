==========
Change Log
==========

All notable changes to this project will be documented here.

Sort subsections like so: Added, Bugfixes, Improvements, Technical tasks.
Group anything an end user shouldn't care deeply about into technical
tasks, even if they're technically bugs. Only include as "bugfixes"
bugs with user-visible outcomes.

When major components get significant changes worthy of mention, they
can be described in a Major section.

v1.2.0 - Unreleased
===================

Changed
-------

* The file setup.py and removed the requirements.txt file

Bugfixes
--------

* Avoid representation errors in chronos.trunc

Technical Tasks
---------------

* Improve the .gitignore to ignore more common things
* Ignore flake8 rule D202 because it conflicts with black formatting

v1.1.2 - 2019-11-05
===================

Bugfixes
--------

* Copy version file when GCD is being installed

v1.1.1 - 2019-11-05
===================

Bugfixes
--------

* Make sure that the requirements files are on the built package


v1.1.0 - 2019-11-01
===================

Added
-----

* Created the documentation
* Changelog file
