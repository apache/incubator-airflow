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

Welcome
=============

Welcome to Apache Airflow! Contributions from the community are the heartbeat of the project. Every little bit helps,
and credit will always be given.

The guide below contains instructions on how to contribute to core Apache Airflow code. If you are a new contributor,
follow `Contributors Quick Start <https://github.com/apache/airflow/blob/master
/CONTRIBUTORS_QUICK_START.rst>`__ first for easy, step-by-step guidelines on how to set up your development
environment and make your first contribution.

If you're interested in contributing to other parts of the project, refer to
`Contribute to Airflow Documentation <https://github.com/apache/airflow/blob/master
/CONTROBUTE-DOCS.rst>`__ or `Contribute to Airflow Providers <https://airflow.apache.org/docs/apache-airflow-providers/index.html#creating-your-own-providers>`__.

Where to Contribute
--------------------
To get started, decide where or what you want to contribute. For ideas, you can browse:

- `All GitHub Issues <https://github.com/apache/airflow/issues>`__
- `Reported Bugs <https://github.com/apache/airflow/issues?q=is%3Aopen+is%3Aissue+label%3Akind%3Abug>`__
- `Requested Features <https://github.com/apache/airflow/labels/kind%3Afeature>`__

Once you decide on a GitHub issue that you want to work on, assign yourself to it. An unassigned feature request is
open for anyone in the community to own.

If you're interested in contributing a feature or bug fix that hasn't been reported, we always recommend that you create
a corresponding GitHub issue to allow for community feedback, but you do not have to. Follow the guidelines below
to submit a PR directly with the appropriate labels.

Contribution Workflow Overview
------------------------------

In general, your contribution includes the following stages:

.. image:: images/workflow.png
    :align: center
    :alt: Contribution Workflow

1. Fork the Apache Airflow GitHub repository.

2. Configure your local virtual environment.

3. Create a branch and make local changes.

4. Create a Pull Request from your fork.

5. Connect with people in the community.

6. Pass PR review.

Step 1: Fork the Apache Airflow Repo
------------------------------------
From the `apache/airflow <https://github.com/apache/airflow>`_ repo,
`create a fork <https://help.github.com/en/github/getting-started-with-github/fork-a-repo>`_:

.. image:: images/fork.png
    :align: center
    :alt: Creating a fork

Step 2: Configure Your Local Virtual Environment
------------------------------------------------
Configure the Docker-based `Breeze development environment <https://github.com/apache/airflow/blob/master/BREEZE.rst>`__.

You can use the default Breeze configuration as follows:

1. Install the latest versions of the Docker Community Edition
   and Docker Compose and add them to the PATH.

2. Enter Breeze: ``./breeze``

   Breeze starts with downloading the Airflow CI image from
   the Docker Hub and installing all required dependencies.

3. Enter the Docker environment and mount your local sources
   to make them immediately visible in the environment.

4. Create a `Local Virtual Env <https://github.com/apache/airflow/blob/master/LOCAL_VIRTUALENV.rst>`__. For example:

.. code-block:: bash

   mkvirtualenv myenv --python=python3.6

5. Initialize the created environment:

.. code-block:: bash

   ./breeze initialize-local-virtualenv --python 3.6

6. Open your IDE (for example, PyCharm) and select the virtualenv you created
   as the project's default virtualenv in your IDE.

Step 3: Create a Branch & Make Local Changes
------------------
Create a local branch in your forked GitHub repository to track your local changes. Make sure to use latest ``apache/master`` as the base for the branch, and that your fork's master branch is synced with Apache Airflow's master as well. 
To bring in the latest changes from the Apache Airflow repo into your master at any given time, run ``git pull apache/master`` from the ``master`` branch in your fork. If you have conflicts and want to override your local master,
you can override your local changes with ``git fetch apache; git reset --hard apache/master``.

For detailed guidelines, read the `How to sync your fork <#how-to-sync-your-fork>`_ and `How to Rebase PR <#how-to-rebase-pr>`_ sections below.

Note: While we recommend that you always create a local branch for your development, some people develop their changes directly in a ``master`` branch within their forked repository.
This allows you to easily compare changes, especially if you or your team are working on multiple changes at once. 

Step 4: Prepare a PR
------------------

Now that you've made your changes locally, create a `Pull Request from your fork <https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork>`__.

1. Update the local sources to address the issue.

   For example, to address this example issue, do the following:

   * Read about `email configuration in Airflow </docs/howto/email-config.rst>`__.

   * Find the class you should modify. For the example GitHub issue,
     this is `email.py <https://github.com/apache/airflow/blob/master/airflow/utils/email.py>`__.

   * Find the test class where you should add tests. For the example ticket,
     this is `test_email.py <https://github.com/apache/airflow/blob/master/tests/utils/test_email.py>`__.

   * Modify the class and add necessary code and unit tests.

   * Run the unit tests from the `IDE <TESTING.rst#running-unit-tests-from-ide>`__
     or `local virtualenv <TESTING.rst#running-unit-tests-from-local-virtualenv>`__ as you see fit.

   * Run the tests in `Breeze <TESTING.rst#running-unit-tests-inside-breeze>`__.

   * Run and fix all the `static checks <STATIC_CODE_CHECKS.rst>`__. If you have
     `pre-commits installed <STATIC_CODE_CHECKS.rst#pre-commit-hooks>`__,
     this step is automatically run while you are committing your code. If not, you can do it manually
     via ``git add`` and then ``pre-commit run``.

2. Rebase your fork, squash commits, and resolve all conflicts. See `How to rebase PR <#how-to-rebase-pr>`_
   if you need help with rebasing your change. Remember to rebase often if your PR takes a lot of time to
   review/fix. This will make rebase process much easier and less painful and the more often you do it,
   the more comfortable you will feel doing it.

3. Re-run static code checks again.

4. Make sure your commit has a good title and description of the context of your change, enough
   for the committer reviewing it to understand why you are proposing a change. Make sure to follow other
   PR guidelines described in `Pull Request Guidelines <#pull-request-guidelines>`_.
   Create Pull Request! Make yourself ready for the discussion!

Step 4: Connect with People
---------------------------

As you prepare your PR and begin the review process, the following community groups may be helpful resources:

- Mailing lists:

  - Developer’s mailing list `<dev-subscribe@airflow.apache.org>`_
    (quite substantial traffic on this list)

  - All commits mailing list: `<commits-subscribe@airflow.apache.org>`_
    (very high traffic on this list)

  - Airflow users mailing list: `<users-subscribe@airflow.apache.org>`_
    (reasonably small traffic on this list)

- `Issues on GitHub <https://github.com/apache/airflow/issues>`__

- `Airflow Community Slack Channel <https://s.apache.org/airflow-slack>`__

You're free to post a link to your PR and ask for feedback, or use these resources for other general questions.

Step 5: Pass PR Review
----------------------

.. image:: images/review.png
    :align: center
    :alt: PR Review

Depending on the "scope" of your changes, your Pull Request will be sent through one of a few review paths.
   Using GitHub Actions, we run an automated workflow with a high degree of automation that allows us to optimize the usage
   of queue slots. Those automated workflows determine the "scope" of changes in your PR and send it through the right path:

  * In case of a "no-code" change, approval will generate a comment that the PR can be merged and no
     tests are needed. This is usually when the change modifies some non-documentation related .rst
     files, such as this file. No Python tests are run and no CI images are built for such PR. Unless there's a long queue of jobs,
     these changes can typically be approved and merged soon after the PR is submitted.

  * In case of a change that involves python code or documentation, a subset of our full test matrix
     will be executed. This subset of tests performs a set of tests that are relevant for a single combination of python,
     backend version, and only builds one CI image and one PROD image. Here the scope of tests depends on the
     scope of your changes:

    * 1. If your change does not affect Apache Airflow's "core" codebase (e.g. Airflow CLI, WWW, Helm Chart), you'll see a
       comment that your PR is likely ok to merge without running our "full matrix" of tests, though the decision
       is ultimately left to the committer who approves your change. The committer may or may not add a "full tests needed"
       label to your PR and request that you rebase your request or re-run all jobs. PRs with the "full tests needed" label
       are expected to be run against Airflow's full test matrix.

    * 2. If your change does affect Apache Airflow's "core" database, the "full tests needed" label will automatically
       be added to your PR. An additional check is set that prevents you or a committer from accidental merging the PR
       before the full matrix of tests are successful.

   For more details on the PR workflow, read `PULL_REQUEST_WORKFLOW.rst <PULL_REQUEST_WORKFLOW.rst>`_.

Note: Committers will use **Squash and Merge** instead of **Rebase and Merge** when merging PRs and your commit will be squashed to single commit.

At least 1 Airflow Committer needs to review and approve your PR in order to have it merged, though 2 Committer reviews are often required.
If you are a committer yourself, another committer must review your changes.

Pull Request Guidelines
=======================

Before you submit a pull request (PR) from your forked repo, check that it meets
these guidelines:

-   Include tests, either as doctests, unit tests, or both, to your pull
    request.

    The airflow repo uses `GitHub Actions <https://help.github.com/en/actions>`__ to
    run the tests and `codecov <https://codecov.io/gh/apache/airflow>`__ to track
    coverage. You can set up both for free on your fork. It will help you make sure you do not
    break the build with your PR and that you help increase coverage.

-   Follow our project's `Coding style and best practices`_.

    These are things that aren't currently enforced programmatically (either because they are too hard or just
    not yet done.)

-   `Rebase your fork <http://stackoverflow.com/a/7244456/1110993>`__, squash
    commits, and resolve all conflicts.

-   When merging PRs, wherever possible try to use **Squash and Merge** instead of **Rebase and Merge**.

-   Add an `Apache License <http://www.apache.org/legal/src-headers.html>`__ header
    to all new files.

    If you have `pre-commit hooks <STATIC_CODE_CHECKS.rst#pre-commit-hooks>`__ enabled, they automatically add
    license headers during commit.

-   If your pull request adds functionality, make sure to update the docs as part
    of the same PR. Doc string is often sufficient. Make sure to follow the
    Sphinx compatible standards.

-   Make sure your code fulfils all the
    `static code checks <STATIC_CODE_CHECKS.rst#pre-commit-hooks>`__ we have in our code. The easiest way
    to make sure of that is to use `pre-commit hooks <STATIC_CODE_CHECKS.rst#pre-commit-hooks>`__

-   Run tests locally before opening PR.

-   Make sure the pull request works for Python 3.6 and 3.7.

-   Adhere to guidelines for commit messages described in this `article <http://chris.beams.io/posts/git-commit/>`__.
    This makes the lives of those who come after you a lot easier.

Airflow Git Branches
====================

All new development in Airflow happens in the ``master`` branch. All PRs should target that branch.

We also have a ``v2-0-test`` branch that is used to test ``2.0.x`` series of Airflow and where committers
cherry-pick selected commits from the master branch.

Cherry-picking is done with the ``-x`` flag.

The ``v2-0-test`` branch might be broken at times during testing. Expect force-pushes there so
committers should coordinate between themselves on who is working on the ``v2-0-test`` branch -
usually these are developers with the release manager permissions.

The ``v2-0-stable`` branch is rather stable - there are minimum changes coming from approved PRs that
passed the tests. This means that the branch is rather, well, "stable".

Once the ``v2-0-test`` branch stabilises, the ``v2-0-stable`` branch is synchronized with ``v2-0-test``.
The ``v2-0-stable`` branch is used to release ``2.0.x`` releases.

The general approach is that cherry-picking a commit that has already had a PR and unit tests run
against main is done to ``v2-0-test`` branch, but PRs from contributors towards 2.0 should target
``v2-0-stable`` branch.

The ``v2-0-test`` branch and ``v2-0-stable`` ones are merged just before the release and that's the
time when they converge.

The production images are build in DockerHub from:

* master branch for development
* v2-0-test branch for testing 2.0.x release
* ``2.0.*``, ``2.0.*rc*`` releases from the ``v2-0-stable`` branch when we prepare release candidates and
  final releases. There are no production images prepared from v2-0-stable branch.

Similar rules apply to ``1.10.x`` releases until June 2020. We have ``v1-10-test`` and ``v1-10-stable``
branches there.

Development Environments
========================

There are two environments, available on Linux and macOS, that you can use to
develop Apache Airflow:

-   `Local virtualenv development environment <#local-virtualenv-development-environment>`_
    that supports running unit tests and can be used in your IDE.

-   `Breeze Docker-based development environment <#breeze-development-environment>`_ that provides
    an end-to-end CI solution with all software dependencies covered.

The table below summarizes differences between the two environments:


========================= ================================ =====================================
**Property**              **Local virtualenv**             **Breeze environment**
========================= ================================ =====================================
Test coverage             - (-) unit tests only            - (+) integration and unit tests
------------------------- -------------------------------- -------------------------------------
Setup                     - (+) automated with breeze cmd  - (+) automated with breeze cmd
------------------------- -------------------------------- -------------------------------------
Installation difficulty   - (-) depends on the OS setup    - (+) works whenever Docker works
------------------------- -------------------------------- -------------------------------------
Team synchronization      - (-) difficult to achieve       - (+) reproducible within team
------------------------- -------------------------------- -------------------------------------
Reproducing CI failures   - (-) not possible in many cases - (+) fully reproducible
------------------------- -------------------------------- -------------------------------------
Ability to update         - (-) requires manual updates    - (+) automated update via breeze cmd
------------------------- -------------------------------- -------------------------------------
Disk space and CPU usage  - (+) relatively lightweight     - (-) uses GBs of disk and many CPUs
------------------------- -------------------------------- -------------------------------------
IDE integration           - (+) straightforward            - (-) via remote debugging only
========================= ================================ =====================================


Typically, you are recommended to use both of these environments depending on your needs.

Local virtualenv Development Environment
----------------------------------------

All details about using and running local virtualenv environment for Airflow can be found
in `LOCAL_VIRTUALENV.rst <LOCAL_VIRTUALENV.rst>`__.

Benefits:

-   Packages are installed locally. No container environment is required.

-   You can benefit from local debugging within your IDE.

-   With the virtualenv in your IDE, you can benefit from autocompletion and running tests directly from the IDE.

Limitations:

-   You have to maintain your dependencies and local environment consistent with
    other development environments that you have on your local machine.

-   You cannot run tests that require external components, such as mysql,
    postgres database, hadoop, mongo, cassandra, redis, etc.

    The tests in Airflow are a mixture of unit and integration tests and some of
    them require these components to be set up. Local virtualenv supports only
    real unit tests. Technically, to run integration tests, you can configure
    and install the dependencies on your own, but it is usually complex.
    Instead, you are recommended to use
    `Breeze development environment <#breeze-development-environment>`__ with all required packages
    pre-installed.

-   You need to make sure that your local environment is consistent with other
    developer environments. This often leads to a "works for me" syndrome. The
    Breeze container-based solution provides a reproducible environment that is
    consistent with other developers.

-   You are **STRONGLY** encouraged to also install and use `pre-commit hooks <STATIC_CODE_CHECKS.rst#pre-commit-hooks>`_
    for your local virtualenv development environment.
    Pre-commit hooks can speed up your development cycle a lot.

Breeze Development Environment
------------------------------

All details about using and running Airflow Breeze can be found in
`BREEZE.rst <BREEZE.rst>`__.

The Airflow Breeze solution is intended to ease your local development as "*It's
a Breeze to develop Airflow*".

Benefits:

-   Breeze is a complete environment that includes external components, such as
    mysql database, hadoop, mongo, cassandra, redis, etc., required by some of
    Airflow tests. Breeze provides a preconfigured Docker Compose environment
    where all these services are available and can be used by tests
    automatically.

-   Breeze environment is almost the same as used in the CI automated builds.
    So, if the tests run in your Breeze environment, they will work in the CI as well.
    See `<CI.rst>`_ for details about Airflow CI.

Limitations:

-   Breeze environment takes significant space in your local Docker cache. There
    are separate environments for different Python and Airflow versions, and
    each of the images takes around 3GB in total.

-   Though Airflow Breeze setup is automated, it takes time. The Breeze
    environment uses pre-built images from DockerHub and it takes time to
    download and extract those images. Building the environment for a particular
    Python version takes less than 10 minutes.

-   Breeze environment runs in the background taking precious resources, such as
    disk space and CPU. You can stop the environment manually after you use it
    or even use a ``bare`` environment to decrease resource usage.

**NOTE:** Breeze CI images are not supposed to be used in production environments.
They are optimized for repeatability of tests, maintainability and speed of building rather
than production performance. The production images are not yet officially published.

Dependency management
=====================

Airflow is not a standard python project. Most of the python projects fall into one of two types -
application or library. As described in
[StackOverflow Question](https://stackoverflow.com/questions/28509481/should-i-pin-my-python-dependencies-versions)
decision whether to pin (freeze) dependency versions for a python project depends on the type. For
applications, dependencies should be pinned, but for libraries, they should be open.

For application, pinning the dependencies makes it more stable to install in the future - because new
(even transitive) dependencies might cause installation to fail. For libraries - the dependencies should
be open to allow several different libraries with the same requirements to be installed at the same time.

The problem is that Apache Airflow is a bit of both - application to install and library to be used when
you are developing your own operators and DAGs.

This - seemingly unsolvable - puzzle is solved by having pinned constraints files. Those are available
as of airflow 1.10.10 and further improved with 1.10.12 (moved to separate orphan branches)

Pinned constraint files
=======================

.. note::

   On November 2020, new version of PIP (20.3) has been released with a new, 2020 resolver. This resolver
   might work with Apache Airflow as of 20.3.3, but it might lead to errors in installation. It might
   depend on your choice of extras. In order to install Airflow you might need to either downgrade
   pip to version 20.2.4 ``pip install --upgrade pip==20.2.4`` or, in case you use Pip 20.3,
   you need to add option ``--use-deprecated legacy-resolver`` to your pip install command.

   While ``pip 20.3.3`` solved most of the ``teething`` problems of 20.3, this note will remain here until we
   set ``pip 20.3`` as official version in our CI pipeline where we are testing the installation as well.
   Due to those constraints, only ``pip`` installation is currently officially supported.

   While they are some successes with using other tools like `poetry <https://python-poetry.org/>`_ or
   `pip-tools <https://pypi.org/project/pip-tools/>`_, they do not share the same workflow as
   ``pip`` - especially when it comes to constraint vs. requirements management.
   Installing via ``Poetry`` or ``pip-tools`` is not currently supported.

   If you wish to install airflow using those tools you should use the constraint files and convert
   them to appropriate format and workflow that your tool requires.


When you install the ``apache-airflow`` package, the dependencies are as open as possible by default.
This means that the ``apache-airflow`` package might fail to install if a direct or transitive dependency is released
that breaks the installation. If that happens, you may need to provide additional constraints. For example,
``pip install apache-airflow==1.10.2 Werkzeug<1.0.0``.

With that said, we now have ``constraints-<PYTHON_MAJOR_MINOR_VERSION>.txt`` files that are generated
automatically and committed to ``constraints-master``, ``constraints-2-0` and ``constraints-1-10`` branches based on
the set of all latest working and tested dependency versions.

These ``constraints-<PYTHON_MAJOR_MINOR_VERSION>.txt`` files
can be used as constraints file when installing Apache Airflow, either from the sources:

.. code-block:: bash

  pip install -e . \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-master/constraints-3.6.txt"


or from the PyPI package:

.. code-block:: bash

  pip install apache-airflow \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-master/constraints-3.6.txt"


This works also with extras, for example:

.. code-block:: bash

  pip install .[ssh] \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-master/constraints-3.6.txt"


As of Airflow 1.10.12, it is also possible to use constraints directly from GitHub using specific
tag/hash name. We tag commits working for particular release with constraints-<version> tag. For example,
fixed valid constraints 1.10.12 can be used by using ``constraints-1.10.12`` tag:

.. code-block:: bash

  pip install apache-airflow[ssh]==1.10.12 \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-1.10.12/constraints-3.6.txt"

There are different sets of fixed constraint files for different python major/minor versions and you should
use the right file for the right Python version.

The ``constraints-<PYTHON_MAJOR_MINOR_VERSION>.txt`` will be automatically regenerated by CI cron job
every time the ``setup.py`` is updated and pushed if the tests are successful. There are separate
jobs for each Python version.

Coding style and best practices
===============================

Most of our coding style rules are enforced programmatically by flake8 and pylint (which are run automatically
on every pull request), but there are some rules that are not yet automated and are more Airflow specific or
semantic than style

Database Session Handling
-------------------------

**Explicit is better than implicit.** If a function accepts a ``session`` parameter it should not commit the
transaction itself. Session management is up to the caller.

To make this easier there is the ``create_session`` helper:

.. code-block:: python

    from airflow.utils.session import create_session

    def my_call(*args, session):
      ...
      # You MUST not commit the session here.

    with create_session() as session:
        my_call(*args, session=session)

If this function is designed to be called by "end-users" (i.e. DAG authors) then using the ``@provide_session`` wrapper is okay:

.. code-block:: python

    from airflow.utils.session import provide_session

    ...

    @provide_session
    def my_method(arg, arg, session=None)
      ...
      # You SHOULD not commit the session here. The wrapper will take care of commit()/rollback() if exception

Don't use time() for duration calcuations
-----------------------------------------

If you wish to compute the time difference between two events with in the same process, use
``time.monotonic()``, not ``time.time()`` nor ``timzeone.utcnow()``.

If you are measuring duration for performance reasons, then ``time.perf_counter()`` should be used. (On many
platforms, this uses the same underlying clock mechanism as monotonic, but ``perf_counter`` is guaranteed to be
the highest accuracy clock on the system, monotonic is simply "guaranteed" to not go backwards.)

If you wish to time how long a block of code takes, use ``Stats.timer()`` -- either with a metric name, which
will be timed and submitted automatically:

.. code-block:: python

    from airflow.stats import Stats

    ...

    with Stats.timer("my_timer_metric"):
        ...

or to time but not send a metric:

.. code-block:: python

    from airflow.stats import Stats

    ...

    with Stats.timer() as timer:
        ...

    log.info("Code took %.3f seconds", timer.duration)

For full docs on ``timer()`` check out `airflow/stats.py <https://github.com/apache/airflow/blob/master/airflow/stats.py>`_.

If the start_date of a duration calculation needs to be stored in a database, then this has to be done using
datetime objects. In all other cases, using datetime for duration calculation MUST be avoided as creating and
diffing datetime operations are (comparatively) slow.

Test Infrastructure
===================

We support the following types of tests:

* **Unit tests** are Python tests launched with ``pytest``.
  Unit tests are available both in the `Breeze environment <BREEZE.rst>`_
  and `local virtualenv <LOCAL_VIRTUALENV.rst>`_.

* **Integration tests** are available in the Breeze development environment
  that is also used for Airflow's CI tests. Integration test are special tests that require
  additional services running, such as Postgres, Mysql, Kerberos, etc.

* **System tests** are automatic tests that use external systems like
  Google Cloud. These tests are intended for an end-to-end DAG execution.

For details on running different types of Airflow tests, see `TESTING.rst <TESTING.rst>`_.

Metadata Database Updates
=========================

When developing features, you may need to persist information to the metadata
database. Airflow has `Alembic <https://github.com/sqlalchemy/alembic>`__ built-in
module to handle all schema changes. Alembic must be installed on your
development machine before continuing with migration.


.. code-block:: bash

    # starting at the root of the project
    $ pwd
    ~/airflow
    # change to the airflow directory
    $ cd airflow
    $ alembic revision -m "add new field to db"
       Generating
    ~/airflow/airflow/migrations/versions/12341123_add_new_field_to_db.py


Node.js Environment Setup
=========================

``airflow/www/`` contains all yarn-managed, front-end assets. Flask-Appbuilder
itself comes bundled with jQuery and bootstrap. While they may be phased out
over time, these packages are currently not managed with yarn.

Make sure you are using recent versions of node and yarn. No problems have been
found with node\>=8.11.3 and yarn\>=1.19.1.

Note: This is only applicable to Airflow UI-related changes.

Installing yarn and its packages
--------------------------------

Make sure yarn is available in your environment.

To install yarn on macOS:

1.  Run the following commands (taken from `this source <https://gist.github.com/DanHerbert/9520689>`__):

.. code-block:: bash

    brew install node
    brew install yarn
    yarn config set prefix ~/.yarn


2.  Add ``~/.yarn/bin`` to your ``PATH`` so that commands you are installing
    could be used globally.

3.  Set up your ``.bashrc`` file and then ``source ~/.bashrc`` to reflect the
    change.

.. code-block:: bash

    export PATH="$HOME/.yarn/bin:$PATH"

4.  Install third-party libraries defined in ``package.json`` by running the
    following commands within the ``airflow/www/`` directory:


.. code-block:: bash

    # from the root of the repository, move to where our JS package.json lives
    cd airflow/www/
    # run yarn install to fetch all the dependencies
    yarn install


These commands install the libraries in a new ``node_modules/`` folder within
``www/``.

Should you add or upgrade a node package, run
``yarn add --dev <package>`` for packages needed in development or
``yarn add <package>`` for packages used by the code.
Then push the newly generated ``package.json`` and ``yarn.lock`` file so that we
could get a reproducible build. See the `Yarn docs
<https://yarnpkg.com/en/docs/cli/add#adding-dependencies->`_ for more details.


Generate Bundled Files with yarn
--------------------------------

To parse and generate bundled files for Airflow, run either of the following
commands:

.. code-block:: bash

    # Compiles the production / optimized js & css
    yarn run prod

    # Starts a web server that manages and updates your assets as you modify them
    yarn run dev


Follow JavaScript Style Guide
-----------------------------

We try to enforce a more consistent style and follow the JS community
guidelines.

Once you add or modify any JavaScript code in the project, please make sure it
follows the guidelines defined in `Airbnb
JavaScript Style Guide <https://github.com/airbnb/javascript>`__.

Apache Airflow uses `ESLint <https://eslint.org/>`__ as a tool for identifying and
reporting on patterns in JavaScript. To use it, run any of the following
commands:

.. code-block:: bash

    # Check JS code in .js and .html files, and report any errors/warnings
    yarn run lint

    # Check JS code in .js and .html files, report any errors/warnings and fix them if possible
    yarn run lint:fix

How to sync your fork
=====================

When you have your fork, you should periodically synchronize the master of your fork with the
Apache Airflow master. In order to do that you can ``git pull --rebase`` to your local git repository from
apache remote and push the master (often with ``--force`` to your fork). There is also an easy
way using ``Force sync master from apache/airflow`` workflow. You can go to "Actions" in your repository and
choose the workflow and manually trigger the workflow using "Run workflow" command.

This will force-push the master from apache/airflow to the master in your fork. Note that in case you
modified the master in your fork, you might loose those changes.


How to rebase PR
================

A lot of people are unfamiliar with the rebase workflow in Git, but we think it is an excellent workflow,
providing a better alternative to the merge workflow. We've therefore written a short guide for those who would like to learn it.

As opposed to the merge workflow, the rebase workflow allows us to
clearly separate your changes from the changes of others. It puts the responsibility of rebasing on the
author of the change. It also produces a "single-line" series of commits on the master branch. This
makes it easier to understand what was going on and to find reasons for problems (it is especially
useful for "bisecting" when looking for a commit that introduced some bugs).

First of all, we suggest you read about the rebase workflow here:
`Merging vs. rebasing <https://www.atlassian.com/git/tutorials/merging-vs-rebasing>`_. This is an
excellent article that describes all the ins/outs of the rebase workflow. I recommend keeping it for future reference.

The goal of rebasing your PR on top of ``apache/master`` is to "transplant" your change on top of
the latest changes that are merged by others. It also allows you to fix all the conflicts
that arise as a result of other people changing the same files as you and merging the changes to ``apache/master``.

Here is how rebase looks in practice (you can find a summary below these detailed steps):

1. You first need to add the Apache project remote to your git repository. This is only necessary once,
so if it's not the first time you are following this tutorial you can skip this step. In this example,
we will be adding the remote
as "apache" so you can refer to it easily:

* If you use ssh: ``git remote add apache git@github.com:apache/airflow.git``
* If you use https: ``git remote add apache https://github.com/apache/airflow.git``

2. You then need to make sure that you have the latest master fetched from the ``apache`` repository. You can do this
   via:

   ``git fetch apache`` (to fetch apache remote)

   ``git fetch --all``  (to fetch all remotes)

3. Assuming that your feature is in a branch in your repository called ``my-branch`` you can easily check
   what is the base commit you should rebase from by:

   ``git merge-base my-branch apache/master``

   This will print the HASH of the base commit which you should use to rebase your feature from.
   For example: ``5abce471e0690c6b8d06ca25685b0845c5fd270f``. Copy that HASH and go to the next step.

   Optionally, if you want better control you can also find this commit hash manually.

   Run:

   ``git log``

   And find the first commit that you DO NOT want to "transplant".

   Performing:

   ``git rebase HASH``

   Will "transplant" all commits after the commit with the HASH.

4. Providing that you weren't already working on your branch, check out your feature branch locally via:

   ``git checkout my-branch``

5. Rebase:

   ``git rebase HASH --onto apache/master``

   For example:

   ``git rebase 5abce471e0690c6b8d06ca25685b0845c5fd270f --onto apache/master``

6. If you have no conflicts - that's cool. You rebased. You can now run ``git push --force-with-lease`` to
   push your changes to your repository. That should trigger the build in our CI if you have a
   Pull Request (PR) opened already.

7. While rebasing you might have conflicts. Read carefully what git tells you when it prints information
   about the conflicts. You need to solve the conflicts manually. This is sometimes the most difficult
   part and requires deliberately correcting your code and looking at what has changed since you developed your
   changes.

   There are various tools that can help you with this. You can use:

   ``git mergetool``

   You can configure different merge tools with it. You can also use IntelliJ/PyCharm's excellent merge tool.
   When you open a project in PyCharm which has conflicts, you can go to VCS > Git > Resolve Conflicts and there
   you have a very intuitive and helpful merge tool. For more information, see
   `Resolve conflicts <https://www.jetbrains.com/help/idea/resolving-conflicts.html>`_.

8. After you've solved your conflict run:

   ``git rebase --continue``

   And go either to point 6. or 7, depending on whether you have more commits that cause conflicts in your PR (rebasing applies each
   commit from your PR one-by-one).

Summary
-------------

Useful when you understand the flow but don't remember the steps and want a quick reference.

``git fetch --all``
``git merge-base my-branch apache/master``
``git checkout my-branch``
``git rebase HASH --onto apache/master``
``git push --force-with-lease``

Commit Policy
=============

The following commit policy passed by a vote 8(binding FOR) to 0 against on May 27, 2016 on the dev list
and slightly modified and consensus reached in October 2020:

* Commits need a +1 vote from a committer who is not the author
* Do not merge a PR that regresses linting or does not pass CI tests (unless we have
  justification such as clearly transient error).
* When we do AIP voting, both PMC and committer +1s are considered as binding vote.

Resources & Links
=================
- `Airflow’s official documentation <https://airflow.apache.org/>`__

- `More resources and links to Airflow related content on the Wiki <https://cwiki.apache.org/confluence/display/AIRFLOW/Airflow+Links>`__