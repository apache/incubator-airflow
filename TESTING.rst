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

Airflow Test Infrastructure
===========================

* **Unit tests** are Python tests that do not require any additional integrations.
  Unit tests are available both in the `Breeze environment <BREEZE.rst>`__
  and local virtualenv.

* **Integration tests** are available in the Breeze development environment
  that is also used for Airflow CI tests. Integration tests are special tests that require
  additional services running, such as Postgres, MySQL, Kerberos, etc.

* **System tests** are automatic tests that use external systems like
  Google Cloud. These tests are intended for an end-to-end DAG execution.
  The tests can be executed on both the current version of Apache Airflow and any older
  versions from 1.10.* series.

This document is about running Python tests. Before the tests are run, use
`static code checks <STATIC_CODE_CHECKS.rst>`__ that enable catching typical errors in the code.

Airflow Unit Tests
==================

All tests for Apache Airflow are run using `pytest <http://doc.pytest.org/en/latest/>`_ .

Writing Unit Tests
------------------

Follow the guidelines when writing unit tests:

* For standard unit tests that do not require integrations with external systems, make sure to simulate all communications.
* All Airflow tests are run with ``pytest``. Make sure to set your IDE/runners (see below) to use ``pytest`` by default.
* For new tests, use standard "asserts" of Python and ``pytest`` decorators/context managers for testing
  rather than ``unittest`` ones. See `pytest docs <http://doc.pytest.org/en/latest/assert.html>`_ for details.
* Use a parameterized framework for tests that have variations in parameters.

**NOTE:** We plan to convert all unit tests to standard "asserts" semi-automatically, but this will be done later
in Airflow 2.0 development phase. That will include setUp/tearDown/context managers and decorators.

Running Unit Tests from IDE
---------------------------

To run unit tests from the IDE, create the `local virtualenv <LOCAL_VIRTUALENV.rst>`_,
select it as the default project's environment, then configure your test runner:

.. image:: images/configure_test_runner.png
    :align: center
    :alt: Configuring test runner

and run unit tests as follows:

.. image:: images/running_unittests.png
    :align: center
    :alt: Running unit tests

**NOTE:** You can run the unit tests in the standalone local virtualenv
(with no Breeze installed) if they do not have dependencies such as
Postgres/MySQL/Hadoop/etc.


Running Unit Tests
--------------------------------
To run unit, integration, and system tests from the Breeze and your
virtualenv, you can use the `pytest <http://doc.pytest.org/en/latest/>`_ framework.

Custom ``pytest`` plugin runs ``airflow db init`` and ``airflow db reset`` the first
time you launch them. So, you can count on the database being initialized. Currently,
when you run tests not supported **in the local virtualenv, they may either fail
or provide an error message**.

There are many available options for selecting a specific test in ``pytest``. Details can be found
in the official documentation, but here are a few basic examples:

.. code-block:: bash

    pytest tests/core -k "TestCore and not check"

This runs the ``TestCore`` class but skips tests of this class that include 'check' in their names.
For better performance (due to a test collection), run:

.. code-block:: bash

    pytest tests/core/test_core.py -k "TestCore and not bash"

This flag is useful when used to run a single test like this:

.. code-block:: bash

    pytest tests/core/test_core.py -k "test_check_operators"

This can also be done by specifying a full path to the test:

.. code-block:: bash

    pytest tests/core/test_core.py::TestCore::test_check_operators

To run the whole test class, enter:

.. code-block:: bash

    pytest tests/core/test_core.py::TestCore

You can use all available ``pytest`` flags. For example, to increase a log level
for debugging purposes, enter:

.. code-block:: bash

    pytest --log-cli-level=DEBUG tests/core/test_core.py::TestCore


Running Tests for a Specified Target Using Breeze from the Host
---------------------------------------------------------------

If you wish to only run tests and not to drop into the shell, apply the
``tests`` command. You can add extra targets and pytest flags after the ``--`` command. Note that
often you want to run the tests with a clean/reset db, so usually you want to add ``--db-reset`` flag
to breeze.

.. code-block:: bash

     ./breeze tests tests/providers/http/hooks/test_http.py tests/core/test_core.py --db-reset -- --log-cli-level=DEBUG

You can run the whole test suite without adding the test target:

.. code-block:: bash

    ./breeze tests --db-reset

You can also specify individual tests or a group of tests:

.. code-block:: bash

    ./breeze tests --db-reset tests/core/test_core.py::TestCore


Running Tests of a specified type from the Host
-----------------------------------------------

You can also run tests for a specific test type. For the stability and performance point of view,
we separated tests into different test types to be run separately.

You can select the test type by adding ``--test-type TEST_TYPE`` before the test command. There are two
kinds of test types:

* Per-directories types are added to select subset of the tests based on sub-directories in ``tests`` folder.
  Example test types there - Core, Providers, CLI. The only action that happens when you choose the right
  test folders are pre-selected. It is only useful for those types of tests to choose the test type
  when you do not specify test to run.

  Runs all core tests:

  .. code-block:: bash

       ./breeze --test-type Core  --db-reset tests

  Runs all provider tests:

  .. code-block:: bash

       ./breeze --test-type Providers --db-reset tests

* Special kinds of tests - Integration, Quarantined, Postgres, MySQL, which are marked with pytest
  marks and for those you need to select the type using test-type switch. If you want to run such tests
  using breeze, you need to pass appropriate ``--test-type`` otherwise the test will be skipped.
  Similarly to the per-directory tests if you do not specify the test or tests to run,
  all tests of a given type are run

  Run quarantined test_task_command.py test:

  .. code-block:: bash

       ./breeze --test-type Quarantined tests tests/cli/commands/test_task_command.py --db-reset

  Run all Quarantined tests:

  .. code-block:: bash

       ./breeze --test-type Quarantined tests --db-reset

Helm Unit Tests
===============

On the Airflow Project, we have decided to stick with pythonic testing for our Helm chart. This makes our chart
easier to test, easier to modify, and able to run with the same testing infrastructure. To add Helm unit tests
go to the ``chart/tests`` directory and add your unit test by creating a class that extends ``unittest.TestCase``

.. code-block:: python

    class TestBaseChartTest(unittest.TestCase):

To render the chart create a YAML string with the nested dictionary of options you wish to test. You can then
use our ``render_chart`` function to render the object of interest into a testable Python dictionary. Once the chart
has been rendered, you can use the ``render_k8s_object`` function to create a k8s model object. It simultaneously
ensures that the object created properly conforms to the expected resource spec and allows you to use object values
instead of nested dictionaries.

Example test here:

.. code-block:: python

    from .helm_template_generator import render_chart, render_k8s_object

    git_sync_basic = """
    dags:
      gitSync:
      enabled: true
    """


    class TestGitSyncScheduler(unittest.TestCase):

        def test_basic(self):
            helm_settings = yaml.safe_load(git_sync_basic)
            res = render_chart('GIT-SYNC', helm_settings,
                               show_only=["templates/scheduler/scheduler-deployment.yaml"])
            dep: k8s.V1Deployment = render_k8s_object(res[0], k8s.V1Deployment)
            assert "dags" == dep.spec.template.spec.volumes[1].name

To run tests using breeze run the following command

.. code-block:: bash

    ./breeze --test-type Helm tests

Airflow Integration Tests
=========================

Some of the tests in Airflow are integration tests. These tests require ``airflow`` Docker
image and extra images with integrations (such as ``redis``, ``mongodb``, etc.).


Enabling Integrations
---------------------

Airflow integration tests cannot be run in the local virtualenv. They can only run in the Breeze
environment with enabled integrations and in the CI. See `<CI.yml>`_ for details about Airflow CI.

When you are in the Breeze environment, by default, all integrations are disabled. This enables only true unit tests
to be executed in Breeze. You can enable the integration by passing the ``--integration <INTEGRATION>``
switch when starting Breeze. You can specify multiple integrations by repeating the ``--integration`` switch
or using the ``--integration all`` switch that enables all integrations.

NOTE: Every integration requires a separate container with the corresponding integration image.
These containers take precious resources on your PC, mainly the memory. The started integrations are not stopped
until you stop the Breeze environment with the ``stop`` command  and restart it
via ``restart`` command.

The following integrations are available:

.. list-table:: Airflow Test Integrations
   :widths: 15 80
   :header-rows: 1

   * - Integration
     - Description
   * - cassandra
     - Integration required for Cassandra hooks
   * - kerberos
     - Integration that provides Kerberos authentication
   * - mongo
     - Integration required for MongoDB hooks
   * - openldap
     - Integration required for OpenLDAP hooks
   * - pinot
     - Integration required for Apache Pinot hooks
   * - presto
     - Integration required for Presto hooks
   * - rabbitmq
     - Integration required for Celery executor tests
   * - redis
     - Integration required for Celery executor tests

To start the ``mongo`` integration only, enter:

.. code-block:: bash

    ./breeze --integration mongo

To start ``mongo`` and ``cassandra`` integrations, enter:

.. code-block:: bash

    ./breeze --integration mongo --integration cassandra

To start all integrations, enter:

.. code-block:: bash

    ./breeze --integration all

In the CI environment, integrations can be enabled by specifying the ``ENABLED_INTEGRATIONS`` variable
storing a space-separated list of integrations to start. Thanks to that, we can run integration and
integration-less tests separately in different jobs, which is desired from the memory usage point of view.

Note that Kerberos is a special kind of integration. Some tests run differently when
Kerberos integration is enabled (they retrieve and use a Kerberos authentication token) and differently when the
Kerberos integration is disabled (they neither retrieve nor use the token). Therefore, one of the test jobs
for the CI system should run all tests with the Kerberos integration enabled to test both scenarios.

Running Integration Tests
-------------------------

All tests using an integration are marked with a custom pytest marker ``pytest.mark.integration``.
The marker has a single parameter - the name of integration.

Example of the ``redis`` integration test:

.. code-block:: python

    @pytest.mark.integration("redis")
    def test_real_ping(self):
        hook = RedisHook(redis_conn_id='redis_default')
        redis = hook.get_conn()

        assert redis.ping(), 'Connection to Redis with PING works.'

The markers can be specified at the test level or the class level (then all tests in this class
require an integration). You can add multiple markers with different integrations for tests that
require more than one integration.

If such a marked test does not have a required integration enabled, it is skipped.
The skip message clearly says what is needed to use the test.

To run all tests with a certain integration, use the custom pytest flag ``--integration``.
You can pass several integration flags if you want to enable several integrations at once.

**NOTE:** If an integration is not enabled in Breeze or CI,
the affected test will be skipped.

To run only ``mongo`` integration tests:

.. code-block:: bash

    pytest --integration mongo

To run integration tests for ``mongo`` and ``rabbitmq``:

.. code-block:: bash

    pytest --integration mongo --integration rabbitmq

Note that collecting all tests takes some time. So, if you know where your tests are located, you can
speed up the test collection significantly by providing the folder where the tests are located.

Here is an example of the collection limited to the ``providers/apache`` directory:

.. code-block:: bash

    pytest --integration cassandra tests/providers/apache/

Running Backend-Specific Tests
------------------------------

Tests that are using a specific backend are marked with a custom pytest marker ``pytest.mark.backend``.
The marker has a single parameter - the name of a backend. It corresponds to the ``--backend`` switch of
the Breeze environment (one of ``mysql``, ``sqlite``, or ``postgres``). Backend-specific tests only run when
the Breeze environment is running with the right backend. If you specify more than one backend
in the marker, the test runs for all specified backends.

Example of the ``postgres`` only test:

.. code-block:: python

    @pytest.mark.backend("postgres")
    def test_copy_expert(self):
        ...


Example of the ``postgres,mysql`` test (they are skipped with the ``sqlite`` backend):

.. code-block:: python

    @pytest.mark.backend("postgres", "mysql")
    def test_celery_executor(self):
        ...


You can use the custom ``--backend`` switch in pytest to only run tests specific for that backend.
Here is an example of running only postgres-specific backend tests:

.. code-block:: bash

    pytest --backend postgres

Running Long-running tests
--------------------------

Some of the tests rung for a long time. Such tests are marked with ``@pytest.mark.long_running`` annotation.
Those tests are skipped by default. You can enable them with ``--include-long-running`` flag. You
can also decide to only run tests with ``-m long-running`` flags to run only those tests.

Quarantined tests
-----------------

Some of our tests are quarantined. This means that this test will be run in isolation and that it will be
re-run several times. Also when quarantined tests fail, the whole test suite will not fail. The quarantined
tests are usually flaky tests that need some attention and fix.

Those tests are marked with ``@pytest.mark.quarantined`` annotation.
Those tests are skipped by default. You can enable them with ``--include-quarantined`` flag. You
can also decide to only run tests with ``-m quarantined`` flag to run only those tests.


Airflow test types
==================

Airflow tests in the CI environment are split into several test types:

* Always - those are tests that should be always executed (always folder)
* Core - for the core Airflow functionality (core folder)
* API - Tests for the Airflow API (api and api_connexion folders)
* CLI - Tests for the Airflow CLI (cli folder)
* WWW - Tests for the Airflow webserver (www folder)
* Providers - Tests for all Providers of Airflow (providers folder)
* Other - all other tests (all other folders that are not part of any of the above)

This is done for three reasons:

1. in order to selectively run only subset of the test types for some PRs
2. in order to allow parallel execution of the tests on Self-Hosted runners

For case 1. see `Pull Request Workflow <PULL_REQUEST_WORKFLOW.rst#selective-ci-checks>`_  for details.

For case 2. We can utilise memory and CPUs available on both CI and local development machines to run
test in parallel. This way we can decrease the time of running all tests in self-hosted runners from
60 minutes to ~15 minutes.

.. note::

  We need to split tests manually into separate suites rather than utilise
  ``pytest-xdist`` or ``pytest-parallel`` which could ba a simpler and much more "native" parallelization
  mechanism. Unfortunately, we cannot utilise those tools because our tests are not truly ``unit`` tests that
  can run in parallel. A lot of our tests rely on shared databases - and they update/reset/cleanup the
  databases while they are executing. They are also exercising features of the Database such as locking which
  further increases cross-dependency between tests. Until we make all our tests truly unit tests (and not
  touching the database or until we isolate all such tests to a separate test type, we cannot really rely on
  frameworks that run tests in parallel. In our solution each of the test types is run in parallel with its
  own database (!) so when we have 8 test types running in parallel, there are in fact 8 databases run
  behind the scenes to support them and each of the test types executes its own tests sequentially.


Running full Airflow test suite in parallel
===========================================

If you run ``./scripts/ci/testing/ci_run_airflow_testing.sh`` tests run in parallel
on your development machine - maxing out the number of parallel runs at the number of cores you
have available in your Docker engine.

In case you do not have enough memory available to your Docker (~32 GB), the ``Integration`` test type
is always run sequentially - after all tests are completed (docker cleanup is performed in-between).

This allows for massive speedup in full test execution. On 8 CPU machine with 16 cores and 64 GB memory
and fast SSD disk, the whole suite of tests completes in about 5 minutes (!). Same suite of tests takes
more than 30 minutes on the same machine when tests are run sequentially.

.. note::

  On MacOS you might have less CPUs and less memory available to run the tests than you have in the host,
  simply because your Docker engine runs in a Linux Virtual Machine under-the-hood. If you want to make
  use of the paralllelism and memory usage for the CI tests you might want to increase the resources available
  to your docker engine. See the `Resources <https://docs.docker.com/docker-for-mac/#resources>`_ chapter
  in the ``Docker for Mac`` documentation on how to do it.

You can also limit the parallelism by specifying the maximum number of parallel jobs via
MAX_PARALLEL_TEST_JOBS variable. If you set it to "1", all the test types will be run sequentially.

.. code-block:: bash

    MAX_PARALLEL_TEST_JOBS="1" ./scripts/ci/testing/ci_run_airflow_testing.sh

.. note::

  In case you would like to cleanup after execution of such tests you might have to cleanup
  some of the docker containers running in case you use ctrl-c to stop execution. You can easily do it by
  running this command (it will kill all docker containers running so do not use it if you want to keep some
  docker containers running):

  .. code-block:: bash

      docker kill $(docker ps -q)


Running Tests with provider packages
====================================

Airflow 2.0 introduced the concept of splitting the monolithic Airflow package into separate
providers packages. The main "apache-airflow" package contains the bare Airflow implementation,
and additionally we have 70+ providers that we can install additionally to get integrations with
external services. Those providers live in the same monorepo as Airflow, but we build separate
packages for them and the main "apache-airflow" package does not contain the providers.

Most of the development in Breeze happens by iterating on sources and when you run
your tests during development, you usually do not want to build packages and install them separately.
Therefore by default, when you enter Breeze airflow and all providers are available directly from
sources rather than installed from packages. This is for example to test the "provider discovery"
mechanism available that reads provider information from the package meta-data.

When Airflow is run from sources, the metadata is read from provider.yaml
files, but when Airflow is installed from packages, it is read via the package entrypoint
``apache_airflow_provider``.

By default, all packages are prepared in wheel format. To install Airflow from packages you
need to run the following steps:

1. Prepare provider packages

.. code-block:: bash

     ./breeze prepare-provider-packages [PACKAGE ...]

If you run this command without packages, you will prepare all packages. However, You can specify
providers that you would like to build if you just want to build few provider packages.
The packages are prepared in ``dist`` folder. Note that this command cleans up the ``dist`` folder
before running, so you should run it before generating ``apache-airflow`` package.

2. Prepare airflow packages

.. code-block:: bash

     ./breeze prepare-airflow-packages

This prepares airflow .whl package in the dist folder.

3. Enter breeze installing both airflow and providers from the packages

This installs airflow and enters

.. code-block:: bash

     ./breeze --install-airflow-version wheel --install-packages-from-dist --skip-mounting-local-sources



Running Tests with Kubernetes
=============================

Airflow has tests that are run against real Kubernetes cluster. We are using
`Kind <https://kind.sigs.k8s.io/>`_ to create and run the cluster. We integrated the tools to start/stop/
deploy and run the cluster tests in our repository and into Breeze development environment.

Configuration for the cluster is kept in ``./build/.kube/config`` file in your Airflow source repository, and
our scripts set the ``KUBECONFIG`` variable to it. If you want to interact with the Kind cluster created
you can do it from outside of the scripts by exporting this variable and point it to this file.

Starting Kubernetes Cluster
---------------------------

For your testing, you manage Kind cluster with ``kind-cluster`` breeze command:

.. code-block:: bash

    ./breeze kind-cluster [ start | stop | recreate | status | deploy | test | shell | k9s ]

The command allows you to start/stop/recreate/status Kind Kubernetes cluster, deploy Airflow via Helm
chart as well as interact with the cluster (via test and shell commands).

Setting up the Kind Kubernetes cluster takes some time, so once you started it, the cluster continues running
until it is stopped with the ``kind-cluster stop`` command or until ``kind-cluster recreate``
command is used (it will stop and recreate the cluster image).

The cluster name follows the pattern ``airflow-python-X.Y-vA.B.C`` where X.Y is a Python version
and A.B.C is a Kubernetes version. This way you can have multiple clusters set up and running at the same
time for different Python versions and different Kubernetes versions.


Deploying Airflow to Kubernetes Cluster
---------------------------------------

Deploying Airflow to the Kubernetes cluster created is also done via ``kind-cluster deploy`` breeze command:

.. code-block:: bash

    ./breeze kind-cluster deploy

The deploy command performs those steps:

1. It rebuilds the latest ``apache/airflow:master-pythonX.Y`` production images using the
   latest sources using local caching. It also adds example DAGs to the image, so that they do not
   have to be mounted inside.
2. Loads the image to the Kind Cluster using the ``kind load`` command.
3. Starts airflow in the cluster using the official helm chart (in ``airflow`` namespace)
4. Forwards Local 8080 port to the webserver running in the cluster
5. Applies the volumes.yaml to get the volumes deployed to ``default`` namespace - this is where
   KubernetesExecutor starts its pods.

Running tests with Kubernetes Cluster
-------------------------------------

You can either run all tests or you can select which tests to run. You can also enter interactive virtualenv
to run the tests manually one by one.

Running Kubernetes tests via shell:

.. code-block:: bash

      ./scripts/ci/kubernetes/ci_run_kubernetes_tests.sh                      - runs all kubernetes tests
      ./scripts/ci/kubernetes/ci_run_kubernetes_tests.sh TEST [TEST ...]      - runs selected kubernetes tests (from kubernetes_tests folder)


Running Kubernetes tests via breeze:

.. code-block:: bash

      ./breeze kind-cluster test
      ./breeze kind-cluster test -- TEST TEST [TEST ...]


Entering shell with Kubernetes Cluster
--------------------------------------

This shell is prepared to run Kubernetes tests interactively. It has ``kubectl`` and ``kind`` cli tools
available in the path, it has also activated virtualenv environment that allows you to run tests via pytest.

You can enter the shell via those scripts

      ./scripts/ci/kubernetes/ci_run_kubernetes_tests.sh [-i|--interactive]   - Activates virtual environment ready to run tests and drops you in
      ./scripts/ci/kubernetes/ci_run_kubernetes_tests.sh [--help]             - Prints this help message


.. code-block:: bash

      ./breeze kind-cluster shell


K9s CLI - debug Kubernetes in style!
------------------------------------

Breeze has built-in integration with fantastic k9s CLI tool, that allows you to debug the Kubernetes
installation effortlessly and in style. K9S provides terminal (but windowed) CLI that helps you to:

- easily observe what's going on in the Kubernetes cluster
- observe the resources defined (pods, secrets, custom resource definitions)
- enter shell for the Pods/Containers running,
- see the log files and more.

You can read more about k9s at `https://k9scli.io/ <https://k9scli.io/>`_

Here is the screenshot of k9s tools in operation:

.. image:: images/testing/k9s.png
    :align: center
    :alt: K9S tool


You can enter the k9s tool via breeze (after you deployed Airflow):

.. code-block:: bash

      ./breeze kind-cluster k9s

You can exit k9s by pressing Ctrl-C.

Typical testing pattern for Kubernetes tests
--------------------------------------------

The typical session for tests with Kubernetes looks like follows:

1. Start the Kind cluster:

.. code-block:: bash

    ./breeze kind-cluster start

    Starts Kind Kubernetes cluster

       Use CI image.

       Branch name:             master
       Docker image:            apache/airflow:master-python3.7-ci

       Airflow source version:  2.0.0.dev0
       Python version:          3.7
       DockerHub user:          apache
       DockerHub repo:          airflow
       Backend:                 postgres 9.6

    No kind clusters found.

    Creating cluster

    Creating cluster "airflow-python-3.7-v1.17.0" ...
     ✓ Ensuring node image (kindest/node:v1.17.0) 🖼
     ✓ Preparing nodes 📦 📦
     ✓ Writing configuration 📜
     ✓ Starting control-plane 🕹️
     ✓ Installing CNI 🔌
    Could not read storage manifest, falling back on old k8s.io/host-path default ...
     ✓ Installing StorageClass 💾
     ✓ Joining worker nodes 🚜
    Set kubectl context to "kind-airflow-python-3.7-v1.17.0"
    You can now use your cluster with:

    kubectl cluster-info --context kind-airflow-python-3.7-v1.17.0

    Have a question, bug, or feature request? Let us know! https://kind.sigs.k8s.io/#community 🙂

    Created cluster airflow-python-3.7-v1.17.0


2. Check the status of the cluster

.. code-block:: bash

    ./breeze kind-cluster status

    Checks status of Kind Kubernetes cluster

       Use CI image.

       Branch name:             master
       Docker image:            apache/airflow:master-python3.7-ci

       Airflow source version:  2.0.0.dev0
       Python version:          3.7
       DockerHub user:          apache
       DockerHub repo:          airflow
       Backend:                 postgres 9.6

    airflow-python-3.7-v1.17.0-control-plane
    airflow-python-3.7-v1.17.0-worker

3. Deploy Airflow to the cluster

.. code-block:: bash

    ./breeze kind-cluster deploy

4. Run Kubernetes tests

Note that the tests are executed in production container not in the CI container.
There is no need for the tests to run inside the Airflow CI container image as they only
communicate with the Kubernetes-run Airflow deployed via the production image.
Those Kubernetes tests require virtualenv to be created locally with airflow installed.
The virtualenv required will be created automatically when the scripts are run.

4a) You can run all the tests

.. code-block:: bash

    ./breeze kind-cluster test


4b) You can enter an interactive shell to run tests one-by-one

This prepares and enters the virtualenv in ``.build/.kubernetes_venv_<YOUR_CURRENT_PYTHON_VERSION>`` folder:

.. code-block:: bash

    ./breeze kind-cluster shell

Once you enter the environment, you receive this information:


.. code-block:: bash

    Activating the virtual environment for kubernetes testing

    You can run kubernetes testing via 'pytest kubernetes_tests/....'
    You can add -s to see the output of your tests on screen

    The webserver is available at http://localhost:8080/

    User/password: admin/admin

    You are entering the virtualenv now. Type exit to exit back to the original shell

In a separate terminal you can open the k9s CLI:

.. code-block:: bash

    ./breeze kind-cluster k9s

Use it to observe what's going on in your cluster.

6. Debugging in IntelliJ/PyCharm

It is very easy to running/debug Kubernetes tests with IntelliJ/PyCharm. Unlike the regular tests they are
in ``kubernetes_tests`` folder and if you followed the previous steps and entered the shell using
``./breeze kind-cluster shell`` command, you can setup your IDE very easy to run (and debug) your
tests using the standard IntelliJ Run/Debug feature. You just need a few steps:

a) Add the virtualenv as interpreter for the project:

.. image:: images/testing/kubernetes-virtualenv.png
    :align: center
    :alt: Kubernetes testing virtualenv

The virtualenv is created in your "Airflow" source directory in the
``.build/.kubernetes_venv_<YOUR_CURRENT_PYTHON_VERSION>`` folder and you
have to find ``python`` binary and choose it when selecting interpreter.

b) Choose pytest as test runner:

.. image:: images/testing/pytest-runner.png
    :align: center
    :alt: Pytest runner

c) Run/Debug tests using standard "Run/Debug" feature of IntelliJ

.. image:: images/testing/run-test.png
    :align: center
    :alt: Run/Debug tests


NOTE! The first time you run it, it will likely fail with
``kubernetes.config.config_exception.ConfigException``:
``Invalid kube-config file. Expected key current-context in kube-config``. You need to add KUBECONFIG
environment variable copying it from the result of "./breeze kind-cluster test":

.. code-block:: bash

    echo ${KUBECONFIG}

    /home/jarek/code/airflow/.build/.kube/config


.. image:: images/testing/kubeconfig-env.png
    :align: center
    :alt: Run/Debug tests


The configuration for Kubernetes is stored in your "Airflow" source directory in ".build/.kube/config" file
and this is where KUBECONFIG env should point to.

You can iterate with tests while you are in the virtualenv. All the tests requiring Kubernetes cluster
are in "kubernetes_tests" folder. You can add extra ``pytest`` parameters then (for example ``-s`` will
print output generated test logs and print statements to the terminal immediately.

.. code-block:: bash

    pytest kubernetes_tests/test_kubernetes_executor.py::TestKubernetesExecutor::test_integration_run_dag_with_scheduler_failure -s


You can modify the tests or KubernetesPodOperator and re-run them without re-deploying
Airflow to KinD cluster.


Sometimes there are side effects from running tests. You can run ``redeploy_airflow.sh`` without
recreating the whole cluster. This will delete the whole namespace, including the database data
and start a new Airflow deployment in the cluster.

.. code-block:: bash

    ./scripts/ci/redeploy_airflow.sh

If needed you can also delete the cluster manually:


.. code-block:: bash

    kind get clusters
    kind delete clusters <NAME_OF_THE_CLUSTER>

Kind has also useful commands to inspect your running cluster:

.. code-block:: text

    kind --help


However, when you change Kubernetes executor implementation, you need to redeploy
Airflow to the cluster.

.. code-block:: bash

    ./breeze kind-cluster deploy


7. Stop KinD cluster when you are done

.. code-block:: bash

    ./breeze kind-cluster stop


Airflow System Tests
====================

System tests need to communicate with external services/systems that are available
if you have appropriate credentials configured for your tests.
The system tests derive from the ``tests.test_utils.system_test_class.SystemTests`` class. They should also
be marked with ``@pytest.marker.system(SYSTEM)`` where ``system`` designates the system
to be tested (for example, ``google.cloud``). These tests are skipped by default.

You can execute the system tests by providing the ``--system SYSTEM`` flag to ``pytest``. You can
specify several --system flags if you want to execute tests for several systems.

The system tests execute a specified example DAG file that runs the DAG end-to-end.

See more details about adding new system tests below.

Environment for System Tests
----------------------------

**Prerequisites:** You may need to set some variables to run system tests. If you need to
add some initialization of environment variables to Breeze, you can add a
``variables.env`` file in the ``files/airflow-breeze-config/variables.env`` file. It will be automatically
sourced when entering the Breeze environment. You can also add some additional
initialization commands in this file if you want to execute something
always at the time of entering Breeze.

There are several typical operations you might want to perform such as:

* generating a file with the random value used across the whole Breeze session (this is useful if
  you want to use this random number in names of resources that you create in your service
* generate variables that will be used as the name of your resources
* decrypt any variables and resources you keep as encrypted in your configuration files
* install additional packages that are needed in case you are doing tests with 1.10.* Airflow series
  (see below)

Example variables.env file is shown here (this is part of the variables.env file that is used to
run Google Cloud system tests.

.. code-block:: bash

  # Build variables. This file is sourced by Breeze.
  # Also it is sourced during continuous integration build in Cloud Build

  # Auto-export all variables
  set -a

  echo
  echo "Reading variables"
  echo

  # Generate random number that will be used across your session
  RANDOM_FILE="/random.txt"

  if [[ ! -f "${RANDOM_FILE}" ]]; then
      echo "${RANDOM}" > "${RANDOM_FILE}"
  fi

  RANDOM_POSTFIX=$(cat "${RANDOM_FILE}")


To execute system tests, specify the ``--system SYSTEM``
flag where ``SYSTEM`` is a system to run the system tests for. It can be repeated.


Forwarding Authentication from the Host
----------------------------------------------------

For system tests, you can also forward authentication from the host to your Breeze container. You can specify
the ``--forward-credentials`` flag when starting Breeze. Then, it will also forward the most commonly used
credentials stored in your ``home`` directory. Use this feature with care as it makes your personal credentials
visible to anything that you have installed inside the Docker container.

Currently forwarded credentials are:
  * credentials stored in ``${HOME}/.aws`` for ``aws`` - Amazon Web Services client
  * credentials stored in ``${HOME}/.azure`` for ``az`` - Microsoft Azure client
  * credentials stored in ``${HOME}/.config`` for ``gcloud`` - Google Cloud client (among others)
  * credentials stored in ``${HOME}/.docker`` for ``docker`` client

Adding a New System Test
--------------------------

We are working on automating system tests execution (AIP-4) but for now, system tests are skipped when
tests are run in our CI system. But to enable the test automation, we encourage you to add system
tests whenever an operator/hook/sensor is added/modified in a given system.

* To add your own system tests, derive them from the
  ``tests.test_utils.system_tests_class.SystemTest`` class and mark with the
  ``@pytest.mark.system(SYSTEM_NAME)`` marker. The system name should follow the path defined in
  the ``providers`` package (for example, the system tests from ``tests.providers.google.cloud``
  package should be marked with ``@pytest.mark.system("google.cloud")``.

* If your system tests need some credential files to be available for an
  authentication with external systems, make sure to keep these credentials in the
  ``files/airflow-breeze-config/keys`` directory. Mark your tests with
  ``@pytest.mark.credential_file(<FILE>)`` so that they are skipped if such a credential file is not there.
  The tests should read the right credentials and authenticate them on their own. The credentials are read
  in Breeze from the ``/files`` directory. The local "files" folder is mounted to the "/files" folder in Breeze.

* If your system tests are long-running ones (i.e., require more than 20-30 minutes
  to complete), mark them with the ```@pytest.markers.long_running`` marker.
  Such tests are skipped by default unless you specify the ``--long-running`` flag to pytest.

* The system test itself (python class) does not have any logic. Such a test runs
  the DAG specified by its ID. This DAG should contain the actual DAG logic
  to execute. Make sure to define the DAG in ``providers/<SYSTEM_NAME>/example_dags``. These example DAGs
  are also used to take some snippets of code out of them when documentation is generated. So, having these
  DAGs runnable is a great way to make sure the documentation is describing a working example. Inside
  your test class/test method, simply use ``self.run_dag(<DAG_ID>,<DAG_FOLDER>)`` to run the DAG. Then,
  the system class will take care about running the DAG. Note that the DAG_FOLDER should be
  a subdirectory of the ``tests.test_utils.AIRFLOW_MAIN_FOLDER`` + ``providers/<SYSTEM_NAME>/example_dags``.


A simple example of a system test is available in:

``tests/providers/google/cloud/operators/test_compute_system.py``.

It runs two DAGs defined in ``airflow.providers.google.cloud.example_dags.example_compute.py`` and
``airflow.providers.google.cloud.example_dags.example_compute_igm.py``.

Preparing provider packages for System Tests for Airflow 1.10.* series
----------------------------------------------------------------------

To run system tests with the older Airflow version, you need to prepare provider packages. This
can be done by running ``./breeze prepare-provider-packages <PACKAGES TO BUILD>``. For
example, the below command will build google, postgres and mysql wheel packages:

.. code-block:: bash

  ./breeze prepare-provider-packages -- google postgres mysql

Those packages will be prepared in ./dist folder. This folder is mapped to /dist folder
when you enter Breeze, so it is easy to automate installing those packages for testing.

The typical system test session
-------------------------------

Here is the typical session that you need to do to run system tests:

1. Enter breeze

.. code-block:: bash

   ./breeze --python 3.6 --db-reset --forward-credentials restart

This will:

* restarts the whole environment (i.e. recreates metadata database from the scratch)
* run Breeze with python 3.6 version
* reset the Airflow database
* forward your local credentials to Breeze

3. Run the tests:

.. code-block:: bash

   pytest -o faulthandler_timeout=2400 \
      --system=google tests/providers/google/cloud/operators/test_compute_system.py


Iteration with System Tests if your resources are slow to create
----------------------------------------------------------------

When you want to iterate on system tests, you might want to create slow resources first.

If you need to set up some external resources for your tests (for example compute instances in Google Cloud)
you should set them up and teardown in the setUp/tearDown methods of your tests.
Since those resources might be slow to create, you might want to add some helpers that
set them up and tear them down separately via manual operations. This way you can iterate on
the tests without waiting for setUp and tearDown with every test.

In this case, you should build in a mechanism to skip setUp and tearDown in case you manually
created the resources. A somewhat complex example of that can be found in
``tests.providers.google.cloud.operators.test_cloud_sql_system.py`` and the helper is
available in ``tests.providers.google.cloud.operators.test_cloud_sql_system_helper.py``.

When the helper is run with ``--action create`` to create cloud sql instances which are very slow
to create and set-up so that you can iterate on running the system tests without
losing the time for creating theme every time. A temporary file is created to prevent from
setting up and tearing down the instances when running the test.

This example also shows how you can use the random number generated at the entry of Breeze if you
have it in your variables.env (see the previous chapter). In the case of Cloud SQL, you cannot reuse the
same instance name for a week so we generate a random number that is used across the whole session
and store it in ``/random.txt`` file so that the names are unique during tests.


!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! Important !!!!!!!!!!!!!!!!!!!!!!!!!!!!

Do not forget to delete manually created resources before leaving the
Breeze session. They are usually expensive to run.

!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! Important !!!!!!!!!!!!!!!!!!!!!!!!!!!!

1. Enter breeze

.. code-block:: bash

   ./breeze --python 3.6 --db-reset --forward-credentials restart

2. Run create action in helper (to create slowly created resources):

.. code-block:: bash

    python tests/providers/google/cloud/operators/test_cloud_sql_system_helper.py --action create

3. Run the tests:

.. code-block:: bash

   pytest -o faulthandler_timeout=2400 \
      --system=google tests/providers/google/cloud/operators/test_compute_system.py

4. Run delete action in helper:

.. code-block:: bash

    python tests/providers/google/cloud/operators/test_cloud_sql_system_helper.py --action delete


Local and Remote Debugging in IDE
=================================

One of the great benefits of using the local virtualenv and Breeze is an option to run
local debugging in your IDE graphical interface.

When you run example DAGs, even if you run them using unit tests within IDE, they are run in a separate
container. This makes it a little harder to use with IDE built-in debuggers.
Fortunately, IntelliJ/PyCharm provides an effective remote debugging feature (but only in paid versions).
See additional details on
`remote debugging <https://www.jetbrains.com/help/pycharm/remote-debugging-with-product.html>`_.

You can set up your remote debugging session as follows:

.. image:: images/setup_remote_debugging.png
    :align: center
    :alt: Setup remote debugging

Note that on macOS, you have to use a real IP address of your host rather than the default
localhost because on macOS the container runs in a virtual machine with a different IP address.

Make sure to configure source code mapping in the remote debugging configuration to map
your local sources to the ``/opt/airflow`` location of the sources within the container:

.. image:: images/source_code_mapping_ide.png
    :align: center
    :alt: Source code mapping

Setup VM on GCP with SSH forwarding
-----------------------------------

Below are the steps you need to take to set up your virtual machine in the Google Cloud.

1. The next steps will assume that you have configured environment variables with the name of the network and
   a virtual machine, project ID and the zone where the virtual machine will be created

    .. code-block:: bash

      PROJECT_ID="<PROJECT_ID>"
      GCP_ZONE="europe-west3-a"
      GCP_NETWORK_NAME="airflow-debugging"
      GCP_INSTANCE_NAME="airflow-debugging-ci"

2. It is necessary to configure the network and firewall for your machine.
   The firewall must have unblocked access to port 22 for SSH traffic and any other port for the debugger.
   In the example for the debugger, we will use port 5555.

    .. code-block:: bash

      gcloud compute --project="${PROJECT_ID}" networks create "${GCP_NETWORK_NAME}" \
        --subnet-mode=auto

      gcloud compute --project="${PROJECT_ID}" firewall-rules create "${GCP_NETWORK_NAME}-allow-ssh" \
        --network "${GCP_NETWORK_NAME}" \
        --allow tcp:22 \
        --source-ranges 0.0.0.0/0

      gcloud compute --project="${PROJECT_ID}" firewall-rules create "${GCP_NETWORK_NAME}-allow-debugger" \
        --network "${GCP_NETWORK_NAME}" \
        --allow tcp:5555 \
        --source-ranges 0.0.0.0/0

3. If you have a network, you can create a virtual machine. To save costs, you can create a `Preemptible
   virtual machine <https://cloud.google.com/preemptible-vms>` that is automatically deleted for up
   to 24 hours.

    .. code-block:: bash

      gcloud beta compute --project="${PROJECT_ID}" instances create "${GCP_INSTANCE_NAME}" \
        --zone="${GCP_ZONE}" \
        --machine-type=f1-micro \
        --subnet="${GCP_NETWORK_NAME}" \
        --image=debian-10-buster-v20200210 \
        --image-project=debian-cloud \
        --preemptible

    To check the public IP address of the machine, you can run the command

    .. code-block:: bash

      gcloud compute --project="${PROJECT_ID}" instances describe "${GCP_INSTANCE_NAME}" \
        --zone="${GCP_ZONE}" \
        --format='value(networkInterfaces[].accessConfigs[0].natIP.notnull().list())'

4. The SSH Daemon's default configuration does not allow traffic forwarding to public addresses.
   To change it, modify the ``GatewayPorts`` options in the ``/etc/ssh/sshd_config`` file to ``Yes``
   and restart the SSH daemon.

    .. code-block:: bash

      gcloud beta compute --project="${PROJECT_ID}" ssh "${GCP_INSTANCE_NAME}" \
        --zone="${GCP_ZONE}" -- \
        sudo sed -i "s/#\?\s*GatewayPorts no/GatewayPorts Yes/" /etc/ssh/sshd_config

      gcloud beta compute --project="${PROJECT_ID}" ssh "${GCP_INSTANCE_NAME}" \
        --zone="${GCP_ZONE}" -- \
        sudo service sshd restart

5. To start port forwarding, run the following command:

    .. code-block:: bash

      gcloud beta compute --project="${PROJECT_ID}" ssh "${GCP_INSTANCE_NAME}" \
        --zone="${GCP_ZONE}" -- \
        -N \
        -R 0.0.0.0:5555:localhost:5555 \
        -v

If you have finished using the virtual machine, remember to delete it.

    .. code-block:: bash

      gcloud beta compute --project="${PROJECT_ID}" instances delete "${GCP_INSTANCE_NAME}" \
        --zone="${GCP_ZONE}"

You can use the GCP service for free if you use the `Free Tier <https://cloud.google.com/free>`__.

DAG Testing
===========

To ease and speed up the process of developing DAGs, you can use
py:class:`~airflow.executors.debug_executor.DebugExecutor`, which is a single process executor
for debugging purposes. Using this executor, you can run and debug DAGs from your IDE.

To set up the IDE:

1. Add ``main`` block at the end of your DAG file to make it runnable.
It will run a backfill job:

.. code-block:: python

  if __name__ == '__main__':
    from airflow.utils.state import State
    dag.clear(dag_run_state=State.NONE)
    dag.run()


2. Set up ``AIRFLOW__CORE__EXECUTOR=DebugExecutor`` in the run configuration of your IDE.
   Make sure to also set up all environment variables required by your DAG.

3. Run and debug the DAG file.

Additionally, ``DebugExecutor`` can be used in a fail-fast mode that will make
all other running or scheduled tasks fail immediately. To enable this option, set
``AIRFLOW__DEBUG__FAIL_FAST=True`` or adjust ``fail_fast`` option in your ``airflow.cfg``.

Also, with the Airflow CLI command ``airflow dags test``, you can execute one complete run of a DAG:

.. code-block:: bash

    # airflow dags test [dag_id] [execution_date]
    airflow dags test example_branch_operator 2018-01-01

By default ``/files/dags`` folder is mounted from your local ``<AIRFLOW_SOURCES>/files/dags`` and this is
the directory used by airflow scheduler and webserver to scan dags for. You can place your dags there
to test them.

The DAGs can be run in the master version of Airflow but they also work
with older versions.

To run the tests for Airflow 1.10.* series, you need to run Breeze with
``--install-airflow-version==<VERSION>`` to install a different version of Airflow.
If ``current`` is specified (default), then the current version of Airflow is used.
Otherwise, the released version of Airflow is installed.

You should also consider running it with ``restart`` command when you change the installed version.
This will clean-up the database so that you start with a clean DB and not DB installed in a previous version.
So typically you'd run it like ``breeze --install-airflow-version=1.10.9 restart``.

Tracking SQL statements
=======================

You can run tests with SQL statements tracking. To do this, use the ``--trace-sql`` option and pass the
columns to be displayed as an argument. Each query will be displayed on a separate line.
Supported values:

* ``num`` -  displays the query number;
* ``time`` - displays the query execution time;
* ``trace`` - displays the simplified (one-line) stack trace;
* ``sql`` - displays the SQL statements;
* ``parameters`` - display SQL statement parameters.

If you only provide ``num``, then only the final number of queries will be displayed.

By default, pytest does not display output for successful tests, if you still want to see them, you must
pass the ``--capture=no`` option.

If you run the following command:

.. code-block:: bash

    pytest --trace-sql=num,sql,parameters --capture=no \
      tests/jobs/test_scheduler_job.py -k test_process_dags_queries_count_05

On the screen you will see database queries for the given test.

SQL query tracking does not work properly if your test runs subprocesses. Only queries from the main process
are tracked.

BASH Unit Testing (BATS)
========================

We have started adding tests to cover Bash scripts we have in our codebase.
The tests are placed in the ``tests\bats`` folder.
They require BAT CLI to be installed if you want to run them on your
host or via a Docker image.

Installing BATS CLI
---------------------

You can find an installation guide as well as information on how to write
the bash tests in `BATS Installation <https://github.com/bats-core/bats-core#installation>`_.

Running BATS Tests on the Host
------------------------------

To run all tests:

.. code-block:: bash

   bats -r tests/bats/

To run a single test:

.. code-block:: bash

   bats tests/bats/your_test_file.bats

Running BATS Tests via Docker
-----------------------------

To run all tests:

.. code-block:: bash

   docker run -it --workdir /airflow -v $(pwd):/airflow  bats/bats:latest -r /airflow/tests/bats

To run a single test:

.. code-block:: bash

   docker run -it --workdir /airflow -v $(pwd):/airflow  bats/bats:latest /airflow/tests/bats/your_test_file.bats

Using BATS
----------

You can read more about using BATS CLI and writing tests in
`BATS Usage <https://github.com/bats-core/bats-core#usage>`_.
