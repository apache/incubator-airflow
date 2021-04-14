 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

.. contents:: :local:

Static Code Checks
==================

The static code checks in Airflow are used to verify that the code meets certain quality standards.
All the static code checks can be run through pre-commit hooks.

Some of the static checks in pre-commits require Breeze Docker images to be installed locally.
The pre-commit hooks perform all the necessary installation when you run them
for the first time. See the table below to identify which pre-commit checks require the Breeze Docker images.

Sometimes your image is outdated and needs to be rebuilt because some dependencies have been changed.
In such cases, the Docker-based pre-commit will inform you that you should rebuild the image.

You can also run some static code checks via `Breeze <BREEZE.rst#aout-airflow-breeze>`_ environment
using available bash scripts.

Pre-commit Hooks
----------------

Pre-commit hooks help speed up your local development cycle and place less burden on the CI infrastructure.
Consider installing the pre-commit hooks as a necessary prerequisite.


This table lists pre-commit hooks used by Airflow and indicates which hooks
require Breeze Docker images to be installed locally:

=================================== ================================================================ ============
**Hooks**                            **Description**                                                 **Breeze**
=================================== ================================================================ ============
``airflow-config-yaml``               Checks that airflow config YAML is 1-1 with the code
----------------------------------- ---------------------------------------------------------------- ------------
``airflow-providers-available``       Checks that providers are properly declared by extras
----------------------------------- ---------------------------------------------------------------- ------------
``airflow-provider-yaml-files-ok``    Checks that providers YAML files are valid
----------------------------------- ---------------------------------------------------------------- ------------
``base-operator``                     Checks that BaseOperator is imported properly
----------------------------------- ---------------------------------------------------------------- ------------
``bats-tests``                        Runs BATS bash unit tests
----------------------------------- ---------------------------------------------------------------- ------------
``bats-in-container-tests``           Run in Breeze container bats tests                                   *
----------------------------------- ---------------------------------------------------------------- ------------
``black``                             Runs Black (the uncompromising Python code formatter)
----------------------------------- ---------------------------------------------------------------- ------------
``build``                             Builds image for mypy, pylint, flake8                                *
----------------------------------- ---------------------------------------------------------------- ------------
``build-providers-dependencies``      Regenerates the JSON file with cross-provider dependencies
----------------------------------- ---------------------------------------------------------------- ------------
``check-apache-license``              Checks compatibility with Apache License requirements
----------------------------------- ---------------------------------------------------------------- ------------
``check-builtin-literals``            Require literal syntax when initializing Python builtin types
----------------------------------- ---------------------------------------------------------------- ------------
``check-executables-have-shebangs``   Checks that executables have shebang
----------------------------------- ---------------------------------------------------------------- ------------
``check-hooks-apply``                 Checks which hooks are applicable to the repository
----------------------------------- ---------------------------------------------------------------- ------------
``check-integrations``                Checks if integration list is synchronized in code
----------------------------------- ---------------------------------------------------------------- ------------
``check-merge-conflicts``             Checks that merge conflicts are not being committed
----------------------------------- ---------------------------------------------------------------- ------------
``check-xml``                         Checks XML files with xmllint
----------------------------------- ---------------------------------------------------------------- ------------
``consistent-pylint``                 Consistent usage of pylint enable/disable with space
----------------------------------- ---------------------------------------------------------------- ------------
``daysago-import-check``              Checks if daysago is properly imported
----------------------------------- ---------------------------------------------------------------- ------------
``debug-statements``                  Detects accidentally committed debug statements
----------------------------------- ---------------------------------------------------------------- ------------
``detect-private-key``                Detects if private key is added to the repository
----------------------------------- ---------------------------------------------------------------- ------------
``doctoc``                            Refreshes the table of contents for MD files
----------------------------------- ---------------------------------------------------------------- ------------
``dont-use-safe-filter``              Don't use safe in templates
----------------------------------- ---------------------------------------------------------------- ------------
``no-providers-in-core-examples``     Don't use providers imports in core example DAGs
----------------------------------- ---------------------------------------------------------------- ------------
``no-relative-imports``               Use absolute imports, not relative
----------------------------------- ---------------------------------------------------------------- ------------
``end-of-file-fixer``                 Makes sure that there is an empty line at the end.
----------------------------------- ---------------------------------------------------------------- ------------
``fix-encoding-pragma``               Removes encoding header from python files
----------------------------------- ---------------------------------------------------------------- ------------
``pyupgrade``                         Runs pyupgrade
----------------------------------- ---------------------------------------------------------------- ------------
``flake8``                            Runs flake8                                                          *
----------------------------------- ---------------------------------------------------------------- ------------
``flynt``                             Runs flynt
----------------------------------- ---------------------------------------------------------------- ------------
``forbid-tabs``                       Fails if tabs are used in the project
----------------------------------- ---------------------------------------------------------------- ------------
``helm-lint``                         Verifies if helm lint passes for the chart
----------------------------------- ---------------------------------------------------------------- ------------
``identity``                          Prints inputs to the static check hooks for troubleshooting
----------------------------------- ---------------------------------------------------------------- ------------
``incorrect-use-of-LoggingMixin``     Checks if LoggingMixin is properly imported
----------------------------------- ---------------------------------------------------------------- ------------
``insert-license``                    Adds licenses for most file types
----------------------------------- ---------------------------------------------------------------- ------------
``isort``                             Sorts imports in python files
----------------------------------- ---------------------------------------------------------------- ------------
``json-schema``                       Checks that the files complies with the JSON Schema spec
----------------------------------- ---------------------------------------------------------------- ------------
``language-matters``                  Check for language that we do not accept as community
----------------------------------- ---------------------------------------------------------------- ------------
``lint-dockerfile``                   Lints a dockerfile
----------------------------------- ---------------------------------------------------------------- ------------
``lint-openapi``                      Lints openapi specification
----------------------------------- ---------------------------------------------------------------- ------------
``markdownlint``                      Lints Markdown files
----------------------------------- ---------------------------------------------------------------- ------------
``mermaid``                           Generates diagrams from mermaid files
----------------------------------- ---------------------------------------------------------------- ------------
``mixed-line-ending``                 Detects if mixed line ending is used (\r vs. \r\n)
----------------------------------- ---------------------------------------------------------------- ------------
``mypy``                              Runs mypy                                                            *
----------------------------------- ---------------------------------------------------------------- ------------
``mypy-helm``                         Runs mypy                                                            *
----------------------------------- ---------------------------------------------------------------- ------------
``pre-commit-descriptions``           Check if all pre-commits are described in docs
----------------------------------- ---------------------------------------------------------------- ------------
``pre-commit-hook-names``             Check that hook names are not overly long
----------------------------------- ---------------------------------------------------------------- ------------
``provide-create-sessions``           Make sure provide-session and create-session imports are OK
----------------------------------- ---------------------------------------------------------------- ------------
``providers-init-file``               Check that provider's __init__.py file is removed
----------------------------------- ---------------------------------------------------------------- ------------
``provider-yamls``                    Checks that provider.yaml files have the correct content
----------------------------------- ---------------------------------------------------------------- ------------
``pydevd``                            Check for accidentally committed pydevd statements
----------------------------------- ---------------------------------------------------------------- ------------
``pydocstyle``                        Runs pydocstyle
----------------------------------- ---------------------------------------------------------------- ------------
``pylint``                            Runs pylint check                                                    *
----------------------------------- ---------------------------------------------------------------- ------------
``python-no-log-warn``                Checks if there are no deprecate log warn
----------------------------------- ---------------------------------------------------------------- ------------
``restrict-start_date``               'start_date' should not be in default_args in example_dags
----------------------------------- ---------------------------------------------------------------- ------------
``rst-backticks``                     Checks if RST files use double backticks for code
----------------------------------- ---------------------------------------------------------------- ------------
``setup-order``                       Checks for an order of dependencies in setup.py
----------------------------------- ---------------------------------------------------------------- ------------
``setup-extra-packages``              Checks if all the libraries in setup.py are listed in docs
----------------------------------- ---------------------------------------------------------------- ------------
``shellcheck``                        Checks shell files with shellcheck
----------------------------------- ---------------------------------------------------------------- ------------
``sort-in-the-wild``                  Sort INTHEWILD.md alphabetically
----------------------------------- ---------------------------------------------------------------- ------------
``sort-spelling-wordlist``            Sort alphabetically and uniquify spelling_wordlist.txt
----------------------------------- ---------------------------------------------------------------- ------------
``stylelint``                         Checks CSS files with stylelint
----------------------------------- ---------------------------------------------------------------- ------------
``trailing-whitespace``               Removes trailing whitespace at end of line
----------------------------------- ---------------------------------------------------------------- ------------
``ui-lint``                           Lint the React UI
----------------------------------- ---------------------------------------------------------------- ------------
``ui-test``                           Test the React UI
----------------------------------- ---------------------------------------------------------------- ------------
``update-breeze-file``                Update output of breeze command in BREEZE.rst
----------------------------------- ---------------------------------------------------------------- ------------
``update-extras``                     Updates extras in the documentation
----------------------------------- ---------------------------------------------------------------- ------------
``update-local-yml-file``             Updates mounts in local.yml file
----------------------------------- ---------------------------------------------------------------- ------------
``update-setup-cfg-file``             Update setup.cfg file with all licenses
----------------------------------- ---------------------------------------------------------------- ------------
``update-extras``                     Updates extras in the documentation
----------------------------------- ---------------------------------------------------------------- ------------
``yamllint``                          Checks YAML files with yamllint
=================================== ================================================================ ============

The pre-commit hooks only check the files you are currently working on and make
them fast. Yet, these checks use exactly the same environment as the CI tests
use. So, you can be sure your modifications will also work for CI if they pass
pre-commit hooks.

We have integrated the fantastic `pre-commit <https://pre-commit.com>`__ framework
in our development workflow. To install and use it, you need Python 3.6 locally.

It is the best to use pre-commit hooks when you have your local virtualenv for
Airflow activated since then pre-commit hooks and other dependencies are
automatically installed. You can also install the pre-commit hooks manually
using ``pip install``.

The pre-commit hooks require the Docker Engine to be configured as the static
checks are executed in the Docker environment. You should build the images
locally before installing pre-commit checks as described in `BREEZE.rst <BREEZE.rst>`__.
In case you do not have your local images built, the
pre-commit hooks fail and provide instructions on what needs to be done.

Prerequisites for Pre-commit Hooks
..................................

The pre-commit hooks use several external linters that need to be installed before pre-commit is run.

Each of the checks installs its own environment, so you do not need to install those, but there are some
checks that require locally installed binaries. On Linux, you typically install
them with ``sudo apt install``, on macOS - with ``brew install``.

The current list of prerequisites is limited to ``xmllint``:

- on Linux, install via ``sudo apt install libxml2-utils``;

- on macOS, install via ``brew install libxml2``.

Enabling Pre-commit Hooks
.........................

To turn on pre-commit checks for ``commit`` operations in git, enter:

.. code-block:: bash

    pre-commit install


To install the checks also for ``pre-push`` operations, enter:

.. code-block:: bash

    pre-commit install -t pre-push


For details on advanced usage of the install method, use:

.. code-block:: bash

   pre-commit install --help


Using Pre-commit Hooks
......................

After installation, pre-commit hooks are run automatically when you commit the
code. But you can run pre-commit hooks manually as needed.

-   Run all checks on your staged files by using:

.. code-block:: bash

    pre-commit run


-   Run only mypy check on your staged files by using:

.. code-block:: bash

    pre-commit run mypy


-   Run only mypy checks on all files by using:

.. code-block:: bash

    pre-commit run mypy --all-files


-   Run all checks on all files by using:

.. code-block:: bash

    pre-commit run --all-files


-   Skip one or more of the checks by specifying a comma-separated list of
    checks to skip in the SKIP variable:

.. code-block:: bash

    SKIP=pylint,mypy pre-commit run --all-files


You can always skip running the tests by providing ``--no-verify`` flag to the
``git commit`` command.

To check other usage types of the pre-commit framework, see `Pre-commit website <https://pre-commit.com/>`__.

Pylint Static Code Checks
-------------------------

We are in the process of fixing the code flagged with pylint checks for the whole Airflow project.
This is a huge task so we implemented an incremental approach for the process.
Currently most of the code is excluded from pylint checks via scripts/ci/pylint_todo.txt.
We have an open JIRA issue AIRFLOW-4364 which has a number of sub-tasks for each of
the modules that should be made compatible. Fixing problems identified with pylint is one of
straightforward and easy tasks to do (but time-consuming), so if you are a first-time
contributor to Airflow, you can choose one of the sub-tasks as your first issue to fix.

To fix a pylint issue, do the following:

1.  Remove module/modules from the
    `scripts/ci/static_checks/pylint_todo.txt <scripts/ci/pylint_todo.txt>`__.

2.  Run `<scripts/ci/static_checks/pylint.sh>`__.

3.  Fix all the issues reported by pylint.

4.  Re-run `<scripts/ci/static_checks/pylint.sh>`__.

5.  If you see "success", submit a PR following
    `Pull Request guidelines <#pull-request-guidelines>`__.


These are guidelines for fixing errors reported by pylint:

-   Fix the errors rather than disable pylint checks. Often you can easily
    refactor the code (IntelliJ/PyCharm might be helpful when extracting methods
    in complex code or moving methods around).

-   If disabling a particular problem, make sure to disable only that error by
    using the symbolic name of the error as reported by pylint.

.. code-block:: python

    import airflow.*  # pylint: disable=wildcard-import


-   If there is a single line where you need to disable a particular error,
    consider adding a comment to the line that causes the problem. For example:

.. code-block:: python

    def  MakeSummary(pcoll, metric_fn, metric_keys): # pylint: disable=invalid-name


-   For multiple lines/block of code, to disable an error, you can surround the
    block with ``pylint:disable/pylint:enable`` comment lines. For example:

.. code-block:: python

    # pylint: disable=too-few-public-methods
    class  LoginForm(Form):
        """Form for the user"""
        username = StringField('Username', [InputRequired()])
        password = PasswordField('Password', [InputRequired()])
    # pylint: enable=too-few-public-methods


Running Static Code Checks via Breeze
-------------------------------------

The static code checks can be launched using the Breeze environment.

You run the static code checks via ``./breeze static-check`` or commands.

Note that it may take a lot of time to run checks for all files with pylint on macOS due to a slow
filesystem for macOS Docker. As a workaround, you can add their arguments after ``--`` as extra arguments.
For example ``--files`` flag. By default those checks are run only on the files you've changed in your
commit, but you can also add ``-- --all-files`` flag to run check on all files.

You can see the list of available static checks either via ``--help`` flag or by using the autocomplete
option. Note that the ``all`` static check runs all configured static checks. Also since pylint tests take
a lot of time, you can run a special ``all-but-pylint`` check that skips pylint checks.

Run the ``mypy`` check for the currently staged changes:

.. code-block:: bash

     ./breeze static-check mypy

Run the ``mypy`` check for all files:

.. code-block:: bash

     ./breeze static-check mypy -- --all-files

Run the ``flake8`` check for the ``tests.core.py`` file with verbose output:

.. code-block:: bash

     ./breeze static-check flake8 -- --files tests/core.py --verbose

Run the ``flake8`` check for the ``tests.core`` package with verbose output:

.. code-block:: bash

     ./breeze static-check mypy -- --files tests/hooks/test_druid_hook.py

Run all tests for the currently staged files:

.. code-block:: bash

     ./breeze static-check all

Run all tests for all files:

.. code-block:: bash

     ./breeze static-check all -- --all-files

Run all tests but pylint for all files:

.. code-block:: bash

     ./breeze static-check all-but-pylint --all-files

Run pylint checks for all changed files:

.. code-block:: bash

     ./breeze static-check pylint

Run pylint checks for selected files:

.. code-block:: bash

     ./breeze static-check pylint -- --files airflow/configuration.py


Run pylint checks for all files:

.. code-block:: bash

     ./breeze static-check pylint -- --all-files


The ``license`` check is run via a separate script and a separate Docker image containing the
Apache RAT verification tool that checks for Apache-compatibility of licenses within the codebase.
It does not take pre-commit parameters as extra arguments.

.. code-block:: bash

     ./breeze static-check licenses

Running Static Code Checks via Scripts from the Host
....................................................

You can trigger the static checks from the host environment, without entering the Docker container. To do
this, run the following scripts:

* `<scripts/ci/docs/ci_docs.sh>`_ - checks that documentation can be built without warnings.
* `<scripts/ci/static_checks/check_license.sh>`_ - checks the licenses.
* `<scripts/ci/static_checks/flake8.sh>`_ - runs Flake8 source code style enforcement tool.
* `<scripts/ci/static_checks/lint_dockerfile.sh>`_ - runs lint checker for the dockerfiles.
* `<scripts/ci/static_checks/mypy.sh>`_ - runs a check for Mypy type annotation consistency.
* `<scripts/ci/static_checks/pylint.sh>`_ - runs Pylint static code checker.

The scripts may ask you to rebuild the images, if needed.

You can force rebuilding the images by deleting the ``.build`` directory. This directory keeps cached
information about the images already built and you can safely delete it if you want to start from scratch.

After documentation is built, the HTML results are available in the ``docs/_build/html``
folder. This folder is mounted from the host so you can access those files on your host as well.

Running Static Code Checks in the Docker Container
..................................................

If you are already in the Breeze Docker environment (by running the ``./breeze`` command),
you can also run the same static checks via run_scripts:

* Mypy: ``./scripts/in_container/run_mypy.sh airflow tests``
* Pylint: ``./scripts/in_container/run_pylint.sh``
* Flake8: ``./scripts/in_container/run_flake8.sh``
* License check: ``./scripts/in_container/run_check_licence.sh``
* Documentation: ``./scripts/in_container/run_docs_build.sh``

Running Static Code Checks for Selected Files
.............................................

In all static check scripts, both in the container and host versions, you can also pass a module/file path as
parameters of the scripts to only check selected modules or files. For example:

In the Docker container:

.. code-block::

  ./scripts/in_container/run_pylint.sh ./airflow/example_dags/

or

.. code-block::

  ./scripts/in_container/run_pylint.sh ./airflow/example_dags/test_utils.py

On the host:

.. code-block::

  ./scripts/ci/static_checks/pylint.sh ./airflow/example_dags/

.. code-block::

  ./scripts/ci/static_checks/pylint.sh ./airflow/example_dags/test_utils.py
