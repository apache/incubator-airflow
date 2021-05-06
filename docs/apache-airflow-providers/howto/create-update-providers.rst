
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

Community Providers
===================

.. contents:: :local:

How-to creating a new community provider
----------------------------------------

This document gathers the necessary steps to create a new community provider and also guidelines for updating
the existing ones. You should be aware that providers may have distinctions that may not be covered in
this guide. The sequence described was designed to meet the most linear flow possible in order to develop a
new provider.

Another recommendation that will help you is to look for a provider that works similar to yours. That way it will
help you to set up tests and other dependencies.

First, you need to set up your local development environment. See `Contribution Quick Start <https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst>`_
if you did not set up your local environment yet. We recommend using ``breeze`` to develop locally. This way you
easily be able to have an environment more similar to the one executed by GitHub CI workflow.

  .. code-block:: bash

      ./breeze

Using the code above you will set up Docker containers. These containers your local code to internal volumes.
In this way, the changes made in your IDE are already applied to the code inside the container and tests can
be carried out quickly.

In this how-to guide our example provider name will be ``<NEW_PROVIDER>``.
When you see this placeholder you must change for your provider name.


Initial Code and Unit Tests
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Most likely you have developed a version of the provider using some local customization and now you need to
transfer this code to the Airflow project. Below is described all the initial code structure that
the provider may need. Understand that not all providers will need all the components described in this structure.
If you still have doubts about building your provider, we recommend that you read the initial provider guide and
open a issue on GitHub so the community can help you.

  .. code-block:: bash

      airflow/
      ├── providers/<NEW_PROVIDER>/
      │   ├── __init__.py
      │   ├── example_dags/
      │   │   ├── __init__.py
      │   │   └── example_<NEW_PROVIDER>.py
      │   ├── hooks/
      │   │   ├── __init__.py
      │   │   └── <NEW_PROVIDER>.py
      │   ├── operators/
      │   │   ├── __init__.py
      │   │   └── <NEW_PROVIDER>.py
      │   ├── sensors/
      │   │   ├── __init__.py
      │   │   └── <NEW_PROVIDER>.py
      │   └── transfers/
      │       ├── __init__.py
      │       └── <NEW_PROVIDER>.py
      └── tests/providers/<NEW_PROVIDER>/
          ├── __init__.py
          ├── hooks/
          │   ├── __init__.py
          │   └── test_<NEW_PROVIDER>.py
          ├── operators/
          │   ├── __init__.py
          │   ├── test_<NEW_PROVIDER>.py
          │   └── test_<NEW_PROVIDER>_system.py
          ├── sensors/
          │   ├── __init__.py
          │   └── test_<NEW_PROVIDER>.py
          └── transfers/
              ├── __init__.py
              └── test_<NEW_PROVIDER>.py

Considering that you have already transferred your provider's code to the above structure, it will now be necessary
to create unit tests for each component you created. The example below I have already set up an environment using
breeze and I'll run unit tests for my Hook.

  .. code-block:: bash

      root@fafd8d630e46:/opt/airflow# python -m pytest tests/providers/<NEW_PROVIDER>/hook/<NEW_PROVIDER>.py

Update Airflow validation tests
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

There are some tests that Airflow performs to ensure consistency that is related to the providers.

  .. code-block:: bash

      airflow/scripts/in_container/
      └── run_install_and_test_provider_packages.sh
      tests/core/
      └── test_providers_manager.py

Change expected number of providers, hooks and connections if needed in ``run_install_and_test_provider_packages.sh`` file.

Add your provider information in the following variables in ``test_providers_manager.py``:

- add your provider to ``ALL_PROVIDERS`` list;
- add your provider into ``CONNECTIONS_LIST`` if your provider create a new connection type.


Integration tests
^^^^^^^^^^^^^^^^^

See `Airflow Integration Tests <https://github.com/apache/airflow/blob/master/TESTING.rst#airflow-integration-tests>`_


Documentation
^^^^^^^^^^^^^

An important part of building a new provider is the documentation.
Some steps for documentation occurs automatically by ``pre-commit`` see `Installing pre-commit guide <https://github.com/apache/airflow/blob/master/CONTRIBUTORS_QUICK_START.rst#pre-commit>`_

  .. code-block:: bash

      airflow/
      ├── INSTALL
      ├── CONTRIBUTING.rst
      ├── setup.py
      ├── docs/
      │   ├── spelling_wordlist.txt
      │   ├── apache-airflow/
      │   │   └── extra-packages-ref.rst
      │   ├── integration-logos/<NEW_PROVIDER>/
      │   │   └── <NEW_PROVIDER>.png
      │   └── apache-airflow-providers-<NEW_PROVIDER>/
      │       ├── index.rst
      │       ├── commits.rst
      │       ├── connections.rst
      │       └── operators/
      │           └── <NEW_PROVIDER>.rst
      └── providers/
          ├── dependencies.json
          └── <NEW_PROVIDER>/
              ├── provider.yaml
              └── CHANGELOG.rst


Files automatically updated by pre-commit:

- ``airflow/providers/dependencies.json``
- ``INSTALL``

Files automatically created when the provider is released:

- ``docs/apache-airflow-providers-<NEW_PROVIDER>/commits.rst``
- ``/airflow/providers/<NEW_PROVIDER>/CHANGELOG``

There is a chance that your provider's name is not a common English word.
In this case is necessary to add it to the file ``docs/spelling_wordlist.txt``. This file begin with capitalized words and
lowercase in the second block.

  .. code-block:: bash

    Namespace
    Neo4j
    Nextdoor
    <NEW_PROVIDER> (new line)
    Nones
    NotFound
    Nullable
    ...
    neo4j
    neq
    networkUri
    <NEW_PROVIDER> (new line)
    nginx
    nobr
    nodash

Add your provider dependencies into **PROVIDER_REQUIREMENTS** variable in ``setup.py``. If your provider doesn't have
any dependency add a empty list.

  .. code-block:: python

      PROVIDERS_REQUIREMENTS: Dict[str, List[str]] = {
          ...
          'microsoft.winrm': winrm,
          'mongo': mongo,
          'mysql': mysql,
          'neo4j': neo4j,
          '<NEW_PROVIDER>': [],
          'odbc': odbc,
          ...
          }

In the ``CONTRIBUTING.rst`` adds:

- your provider name in the list in the **Extras** section
- your provider dependencies in the **Provider Packages** section table, only if your provider has external dependencies.

In the ``docs/apache-airflow-providers-<NEW_PROVIDER>/connections.rst``:

- add information how to configure connection for your provider.

In the ``docs/apache-airflow-providers-<NEW_PROVIDER>/operators/<NEW_PROVIDER>.rst``:

- add information how to use the Operator. It's important to add examples and additional information if your Operator has extra-parameters.

  .. code-block:: RST

      .. _howto/operator:NewProviderOperator:

      NewProviderOperator
      ===================

      Use the :class:`~airflow.providers.<NEW_PROVIDER>.operators.NewProviderOperator` to do something
      amazing with Airflow!

      Using the Operator
      ^^^^^^^^^^^^^^^^^^

      The NewProviderOperator requires a ``connection_id`` and this other awesome parameter.
      You can see an example below:

      .. exampleinclude:: /../../airflow/providers/<NEW_PROVIDER>/example_dags/example_<NEW_PROVIDER>.py
          :language: python
          :start-after: [START howto_operator_<NEW_PROVIDER>]
          :end-before: [END howto_operator_<NEW_PROVIDER>]


In the ``docs/apache-airflow-providers-new_provider/index.rst``:

- add all information of the purpose of your provider. It is recommended to check with another provider to help you complete this document as best as possible.

In the ``airflow/providers/<NEW_PROVIDER>/provider.yaml`` add information of your provider:

  .. code-block:: yaml

      package-name: apache-airflow-providers-<NEW_PROVIDER>
      name: <NEW_PROVIDER>
      description: |
        `<NEW_PROVIDER> <https://example.io/>`__
      versions:
        - 1.0.0

      integrations:
        - integration-name: <NEW_PROVIDER>
          external-doc-url: https://www.example.io/
          logo: /integration-logos/<NEW_PROVIDER>/<NEW_PROVIDER>.png
          how-to-guide:
            - /docs/apache-airflow-providers-<NEW_PROVIDER>/operators/<NEW_PROVIDER>.rst
          tags: [service]

      operators:
        - integration-name: <NEW_PROVIDER>
          python-modules:
            - airflow.providers.<NEW_PROVIDER>.operators.<NEW_PROVIDER>

      hooks:
        - integration-name: <NEW_PROVIDER>
          python-modules:
            - airflow.providers.<NEW_PROVIDER>.hooks.<NEW_PROVIDER>

      sensors:
        - integration-name: <NEW_PROVIDER>
          python-modules:
            - airflow.providers.<NEW_PROVIDER>.sensors.<NEW_PROVIDER>

      hook-class-names:
        - airflow.providers.<NEW_PROVIDER>.hooks.<NEW_PROVIDER>.NewProviderHook

You only need to add ``hook-class-names`` in case you have some hooks that have customized UI behavior.
For more information see `Custom connection types <http://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html#custom-connection-types>`_


After changing and creating these files you can build the documentation locally. The two commands below will
serve to accomplish this. The first will build your provider's documentation. The second will ensure that the
main Airflow documentation that involves some steps with the providers is also working.

  .. code-block:: bash

    ./breeze build-docs -- --package-filter apache-airflow-providers-<NEW_PROVIDER>
    ./breeze build-docs -- --package-filter apache-airflow

How-to Update a community provider
----------------------------------

See `Provider packages versioning <https://github.com/apache/airflow/blob/master/dev/README_RELEASE_PROVIDER_PACKAGES.md#provider-packages-versioning>`_
