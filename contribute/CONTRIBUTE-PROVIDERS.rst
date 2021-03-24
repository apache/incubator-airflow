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

Overview of Airflow Dependencies
====================

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


Extras
------

There are a number of extras that can be specified when installing Airflow. Those
extras can be specified after the usual pip install - for example
``pip install -e .[ssh]``. For development purpose there is a ``devel`` extra that
installs all development dependencies. There is also ``devel_ci`` that installs
all dependencies needed in the CI environment.

This is the full list of those extras:

  .. START EXTRAS HERE

all, all_dbs, amazon, apache.atlas, apache.beam, apache.cassandra, apache.druid, apache.hdfs,
apache.hive, apache.kylin, apache.livy, apache.pig, apache.pinot, apache.spark, apache.sqoop,
apache.webhdfs, async, atlas, aws, azure, cassandra, celery, cgroups, cloudant, cncf.kubernetes,
crypto, dask, databricks, datadog, devel, devel_all, devel_ci, devel_hadoop, dingding, discord, doc,
docker, druid, elasticsearch, exasol, facebook, ftp, gcp, gcp_api, github_enterprise, google,
google_auth, grpc, hashicorp, hdfs, hive, http, imap, jdbc, jenkins, jira, kerberos, kubernetes,
ldap, microsoft.azure, microsoft.mssql, microsoft.winrm, mongo, mssql, mysql, neo4j, odbc, openfaas,
opsgenie, oracle, pagerduty, papermill, password, pinot, plexus, postgres, presto, qds, qubole,
rabbitmq, redis, s3, salesforce, samba, segment, sendgrid, sentry, sftp, singularity, slack,
snowflake, spark, sqlite, ssh, statsd, tableau, telegram, vertica, virtualenv, webhdfs, winrm,
yandex, zendesk

  .. END EXTRAS HERE

Provider packages
-----------------

Airflow 2.0 is split into core and providers. They are delivered as separate packages:

* ``apache-airflow`` - core of Apache Airflow
* ``apache-airflow-providers-*`` - More than 50 provider packages to communicate with external services

In Airflow 1.10 all those providers were installed together within one single package and when you installed
airflow locally, from sources, they were also installed. In Airflow 2.0, providers are separated out,
and not packaged together with the core, unless you set ``INSTALL_PROVIDERS_FROM_SOURCES`` environment
variable to ``true``.

In Breeze - which is a development environment, ``INSTALL_PROVIDERS_FROM_SOURCES`` variable is set to true,
but you can add ``--skip-installing-airflow-providers-from-sources`` flag to Breeze to skip installing providers when
building the images.

One watch-out - providers are still always installed (or rather available) if you install airflow from
sources using ``-e`` (or ``--editable``) flag. In such case airflow is read directly from the sources
without copying airflow packages to the usual installation location, and since 'providers' folder is
in this airflow folder - the providers package is importable.

Some of the packages have cross-dependencies with other providers packages. This typically happens for
transfer operators where operators use hooks from the other providers in case they are transferring
data between the providers. The list of dependencies is maintained (automatically with pre-commits)
in the ``airflow/providers/dependencies.json``. Pre-commits are also used to generate dependencies.
The dependency list is automatically used during PyPI packages generation.

Cross-dependencies between provider packages are converted into extras - if you need functionality from
the other provider package you can install it adding [extra] after the
``apache-airflow-backport-providers-PROVIDER`` for example:
``pip install apache-airflow-backport-providers-google[amazon]`` in case you want to use GCP
transfer operators from Amazon ECS.

If you add a new dependency between different providers packages, it will be detected automatically during
pre-commit phase and pre-commit will fail - and add entry in dependencies.json so that the package extra
dependencies are properly added when package is installed.

You can regenerate the whole list of provider dependencies by running this command (you need to have
``pre-commits`` installed).

.. code-block:: bash

  pre-commit run build-providers-dependencies


Here is the list of packages and their extras:


  .. START PACKAGE DEPENDENCIES HERE

========================== ===========================
Package                    Extras
========================== ===========================
amazon                     apache.hive,exasol,ftp,google,imap,mongo,mysql,postgres,ssh
apache.beam                google
apache.druid               apache.hive
apache.hive                amazon,microsoft.mssql,mysql,presto,samba,vertica
apache.livy                http
dingding                   http
discord                    http
google                     amazon,apache.beam,apache.cassandra,cncf.kubernetes,facebook,microsoft.azure,microsoft.mssql,mysql,oracle,postgres,presto,salesforce,sftp,ssh
hashicorp                  google
microsoft.azure            google,oracle
microsoft.mssql            odbc
mysql                      amazon,presto,vertica
opsgenie                   http
postgres                   amazon
sftp                       ssh
slack                      http
snowflake                  slack
========================== ===========================

  .. END PACKAGE DEPENDENCIES HERE


Developing community managed provider packages
===============================================

While you can develop your own providers, Apache Airflow has 60+ providers that are managed by the community.
They are part of the same repository as Apache Airflow (we use ``monorepo`` approach where different
parts of the system are developed in the same repository but then they are packaged and released separately).
All the community-managed providers are in the 'airflow/providers' folder and they are all sub-packages of the
'airflow.providers' package. All the providers are available as ``apache-airflow-providers-<PROVIDER_ID>``
packages.

The capabilities of the community-managed providers are the same as the third-party ones. When
the providers are installed from PyPI, they provide the entry-point containing the metadata as described
in the previous chapter. However when they are locally developed, together with Airflow, the mechanism
of discovery of the providers is based on ``provider.yaml`` file that is placed in the top-folder of
the provider. Similarly as in case of the ``provider.yaml`` file is compliant with the
`json-schema specification <https://github.com/apache/airflow/blob/master/airflow/provider.yaml.schema.json>`_.
Thanks to that mechanism, you can develop community managed providers in a seamless way directly from
Airflow sources, without preparing and releasing them as packages. This is achieved by:

* When Airflow is installed locally in editable mode (``pip install -e``) the provider packages installed
  from PyPI are uninstalled and the provider discovery mechanism finds the providers in the Airflow
  sources by searching for provider.yaml files.

* When you want to install Airflow from sources you can set ``INSTALL_PROVIDERS_FROM_SOURCES`` variable
  to ``true`` and then the providers will not be installed from PyPI packages, but they will be installed
  from local sources as part of the ``apache-airflow`` package, but additionally the ``provider.yaml`` files
  are copied together with the sources, so that capabilities and names of the providers can be discovered.
  This mode is especially useful when you are developing a new provider, that cannot be installed from
  PyPI and you want to check if it installs cleanly.

Regardless if you plan to contribute your provider, when you are developing your own, custom providers,
you can use the above functionality to make your development easier. You can add your provider
as a sub-folder of the ``airflow.providers`` package, add the ``provider.yaml`` file and install airflow
in development mode - then capabilities of your provider will be discovered by airflow and you will see
the provider among other providers in ``airflow providers`` command output.

Documentation for the community managed providers
-------------------------------------------------

When you are developing a community-managed provider, you are supposed to make sure it is well tested
and documented. Part of the documentation is ``provider.yaml`` file ``integration`` information and
``version`` information. This information is stripped-out from provider info available at runtime,
however it is used to automatically generate documentation for the provider.

If you have pre-commits installed, pre-commit will warn you and let you know what changes need to be
done in the ``provider.yaml`` file when you add a new Operator, Hooks, Sensor or Transfer. You can
also take a look at the other ``provider.yaml`` files as examples.

Well documented provider contains those:

* index.rst with references to packages, API used and example dags
* configuration reference
* class documentation generated from PyDoc in the code
* example dags
* how-to guides

You can see for example ``google`` provider which has very comprehensive documentation:

* `Documentation <docs/apache-airflow-providers-google>`_
* `Example DAGs <airflow/providers/google/cloud/example_dags>`_

Part of the documentation are example dags. We are using the example dags for various purposes in
providers:

* showing real examples of how your provider classes (Operators/Sensors/Transfers) can be used
* snippets of the examples are embedded in the documentation via ``exampleinclude::`` directive
* examples are executable as system tests

Testing the community managed providers
---------------------------------------

We have high requirements when it comes to testing the community managed providers. We have to be sure
that we have enough coverage and ways to tests for regressions before the community accepts such
providers.

* Unit tests have to be comprehensive and they should tests for possible regressions and edge cases
  not only "green path"

* Integration tests where 'local' integration with a component is possible (for example tests with
  MySQL/Postgres DB/Presto/Kerberos all have integration tests which run with real, dockerised components

* System Tests which provide end-to-end testing, usually testing together several operators, sensors,
  transfers connecting to a real external system

You can read more about out approach for tests in `TESTING.rst <TESTING.rst>`_ but here
are some highlights.


Backport providers
------------------

You can also build backport provider packages for Airflow 1.10. They aim to provide a bridge when users
of Airflow 1.10 want to migrate to Airflow 2.0. The backport packages are named similarly to the
provider packages, but with "backport" added:

* ``apache-airflow-backport-provider-*``

Those backport providers are automatically refactored to work with Airflow 1.10.* and have a few
limitations described in those packages.

Naming Conventions for provider packages
----------------------------------------

In Airflow 2.0 we standardized and enforced naming for provider packages, modules and classes.
those rules (introduced as AIP-21) were not only introduced but enforced using automated checks
that verify if the naming conventions are followed. Here is a brief summary of the rules, for
detailed discussion you can go to [AIP-21 Changes in import paths](https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-21%3A+Changes+in+import+paths)

The rules are as follows:

* Provider packages are all placed in 'airflow.providers'

* Providers are usually direct sub-packages of the 'airflow.providers' package but in some cases they can be
  further split into sub-packages (for example 'apache' package has 'cassandra', 'druid' ... providers ) out
  of which several different provider packages are produced (apache.cassandra, apache.druid). This is
  case when the providers are connected under common umbrella but very loosely coupled on the code level.

* In some cases the package can have sub-packages but they are all delivered as single provider
  package (for example 'google' package contains 'ads', 'cloud' etc. sub-packages). This is in case
  the providers are connected under common umbrella and they are also tightly coupled on the code level.

* Typical structure of provider package:
    * example_dags -> example DAGs are stored here (used for documentation and System Tests)
    * hooks -> hooks are stored here
    * operators -> operators are stored here
    * sensors -> sensors are stored here
    * secrets -> secret backends are stored here
    * transfers -> transfer operators are stored here

* Module names do not contain word "hooks", "operators" etc. The right type comes from
  the package. For example 'hooks.datastore' module contains DataStore hook and 'operators.datastore'
  contains DataStore operators.

* Class names contain 'Operator', 'Hook', 'Sensor' - for example DataStoreHook, DataStoreExportOperator

* Operator name usually follows the convention: ``<Subject><Action><Entity>Operator``
  (BigQueryExecuteQueryOperator) is a good example

* Transfer Operators are those that actively push data from one service/provider and send it to another
  service (might be for the same or another provider). This usually involves two hooks. The convention
  for those ``<Source>To<Destination>Operator``. They are not named *TransferOperator nor *Transfer.

* Operators that use external service to perform transfer (for example CloudDataTransferService operators
  are not placed in "transfers" package and do not have to follow the naming convention for
  transfer operators.

* It is often debatable where to put transfer operators but we agreed to the following criteria:

  * We use "maintainability" of the operators as the main criteria - so the transfer operator
    should be kept at the provider which has highest "interest" in the transfer operator

  * For Cloud Providers or Service providers that usually means that the transfer operators
    should land at the "target" side of the transfer

* Secret Backend name follows the convention: ``<SecretEngine>Backend``.

* Tests are grouped in parallel packages under "tests.providers" top level package. Module name is usually
  ``test_<object_to_test>.py``,

* System tests (not yet fully automated but allowing to run e2e testing of particular provider) are
  named with _system.py suffix.