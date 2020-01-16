============
Contributing
============

To contribute changes to the GCD library:

* Try to do a Pull Request without doing a fork if possible.

  If you work at Jampp, you should have permissions. If that isn't
  the case, talk with the DevOps team

* When doing a pull request make sure:

  * to update the version file.
  * Run the tests locally to make sure that no tests fail
  * Add new tests

* Make sure to format the code and that there is no pyling error.

  There is a `pre-commit <https://pre-commit.com/>`__ configuration files,
  so you should:

  .. code-block:: bash

        pip install pre-commit
        pre-commit install

* Check the documentation

  .. code-block:: bash

        pip install -r requirements-doc.txt
        make docs-doc8
        make docs-spelling

* Create a pull request

* When merging the pull request use ``Squash and merge`` option to reduce the
  changes history
