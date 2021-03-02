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

v3.0.1 - Unreleased
===================

- PREDEX-471_: Migration to new CI

v3.0.0 - 2020-06-24
===================

Changed
-------

- Removed etc.PicklableFunction
- Removed work.sorted

v2.1.0 - 2020-04-30
===================

Changed
-------

- PX-362_ Support marshalling of default arguments

v2.0.0 - 2020-01-28
===================

Changed
-------

- PX-249_ Remove flattener and vacuumer from gcd.store
- PX-67_ Remove gcd.cache (the only user was the sampler)

Added
-----

- PX-250_ Implement presto query using CLI
- PX-270_
  - New helper class for (limited) serializable functions
  - New `stop` method for stopping ongoing tasks
  - Prefetch presto results in local file

Fixed
-----

- PX-302_ Fix default max limit in etc.clip


v1.2.0 - 2020-01-28
===================

Changed
-------

- The file setup.py and removed the requirements.txt file

Bugfixes
--------

- Avoid representation errors in chronos.trunc

Technical Tasks
---------------

- Improve the .gitignore to ignore more common things
- Ignore flake8 rule D202 because it conflicts with black formatting

v1.1.2 - 2019-11-05
===================

Bugfixes
--------

- Copy version file when GCD is being installed

v1.1.1 - 2019-11-05
===================

Bugfixes
--------

- Make sure that the requirements files are on the built package


v1.1.0 - 2019-11-01
===================

Added
-----

- Created the documentation
- Changelog file


.. _PX-67: https://jampphq.atlassian.net/browse/PX-67
.. _PX-249: https://jampphq.atlassian.net/browse/PX-249
.. _PX-250: https://jampphq.atlassian.net/browse/PX-250
.. _PX-270: https://jampphq.atlassian.net/browse/PX-270
.. _PX-302: https://jampphq.atlassian.net/browse/PX-302
.. _PX-362: https://jampphq.atlassian.net/browse/PX-362
.. _PREDEX-471: https://jampphq.atlassian.net/browse/PREDEX-471
