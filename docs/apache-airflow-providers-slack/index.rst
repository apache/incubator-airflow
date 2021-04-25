
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

``apache-airflow-providers-slack``
==================================

Content
-------

.. toctree::
    :maxdepth: 1
    :caption: References

    Python API <_api/airflow/providers/slack/index>

.. toctree::
    :maxdepth: 1
    :caption: Resources

    PyPI Repository <https://pypi.org/project/apache-airflow-providers-slack/>

.. THE REMINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


Package apache-airflow-providers-slack
------------------------------------------------------

`Slack <https://slack.com/>`__


Release: 3.0.0

Provider package
----------------

This is a provider package for ``slack`` provider. All classes for this provider package
are in ``airflow.providers.slack`` python package.

Installation
------------

.. note::

    On November 2020, new version of PIP (20.3) has been released with a new, 2020 resolver. This resolver
    does not yet work with Apache Airflow and might lead to errors in installation - depends on your choice
    of extras. In order to install Airflow you need to either downgrade pip to version 20.2.4
    ``pip install --upgrade pip==20.2.4`` or, in case you use Pip 20.3, you need to add option
    ``--use-deprecated legacy-resolver`` to your pip install command.


You can install this package on top of an existing airflow 2.* installation via
``pip install apache-airflow-providers-slack``

PIP requirements
----------------

=============  ==================
PIP package    Version required
=============  ==================
``slack_sdk``  ``>=3.0.0,<4.0.0``
=============  ==================

Cross provider package dependencies
-----------------------------------

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified provider packages in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

.. code-block:: bash

    pip install apache-airflow-providers-slack[http]


================================================================================================  ========
Dependent package                                                                                 Extra
================================================================================================  ========
`apache-airflow-providers-http <https://airflow.apache.org/docs/apache-airflow-providers-http>`_  ``http``
================================================================================================  ========

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


Changelog
---------

3.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* ``Don't allow SlackHook.call method accept *args (#14289)``


2.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

We updated the support for ``slack_sdk`` from ``>=2.0.0,<3.0.0`` to ``>=3.0.0,<4.0.0``. In most cases,
this doesn't mean any breaking changes to the DAG files, but if you used this library directly
then you have to make the changes. For details, see
`the Migration Guide <https://slack.dev/python-slack-sdk/v3-migration/index.html#from-slackclient-2-x>`_
for Python Slack SDK.

* ``Upgrade slack_sdk to v3 (#13745)``


1.0.0
.....

Initial version of the provider.
