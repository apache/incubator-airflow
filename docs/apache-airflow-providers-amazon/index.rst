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

``apache-airflow-providers-amazon``
===================================

Content
-------

.. toctree::
    :maxdepth: 1
    :caption: Guides

    Connection types <connections/aws>
    Operators <operators/index>
    Secrets backends <secrets-backends/index>
    Logging for Tasks <logging/index>

.. toctree::
    :maxdepth: 1
    :caption: References

    Python API <_api/airflow/providers/amazon/index>

.. toctree::
    :maxdepth: 1
    :caption: Resources

    Example DAGs <https://github.com/apache/airflow/tree/master/airflow/providers/amazon/aws/example_dags>
    PyPI Repository <https://pypi.org/project/apache-airflow-providers-amazon/>

.. THE REMINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


Package apache-airflow-providers-amazon
------------------------------------------------------

Amazon integration (including `Amazon Web Services (AWS) <https://aws.amazon.com/>`__).


Release: 1.2.0

Provider package
----------------

This is a provider package for ``amazon`` provider. All classes for this provider package
are in ``airflow.providers.amazon`` python package.

Installation
------------

.. note::

    On November 2020, new version of PIP (20.3) has been released with a new, 2020 resolver. This resolver
    does not yet work with Apache Airflow and might lead to errors in installation - depends on your choice
    of extras. In order to install Airflow you need to either downgrade pip to version 20.2.4
    ``pip install --upgrade pip==20.2.4`` or, in case you use Pip 20.3, you need to add option
    ``--use-deprecated legacy-resolver`` to your pip install command.


You can install this package on top of an existing airflow 2.* installation via
``pip install apache-airflow-providers-amazon``

PIP requirements
----------------

==============  ====================
PIP package     Version required
==============  ====================
``boto3``       ``>=1.15.0,<1.16.0``
``botocore``    ``>=1.18.0,<1.19.0``
``watchtower``  ``~=0.7.3``
==============  ====================

Cross provider package dependencies
-----------------------------------

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified provider packages in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

.. code-block:: bash

    pip install apache-airflow-providers-amazon[apache.hive]


==============================================================================================================  ===============
Dependent package                                                                                               Extra
==============================================================================================================  ===============
`apache-airflow-providers-apache-hive <https://airflow.apache.org/docs/apache-airflow-providers-apache-hive>`_  ``apache.hive``
`apache-airflow-providers-exasol <https://airflow.apache.org/docs/apache-airflow-providers-exasol>`_            ``exasol``
`apache-airflow-providers-ftp <https://airflow.apache.org/docs/apache-airflow-providers-ftp>`_                  ``ftp``
`apache-airflow-providers-google <https://airflow.apache.org/docs/apache-airflow-providers-google>`_            ``google``
`apache-airflow-providers-imap <https://airflow.apache.org/docs/apache-airflow-providers-imap>`_                ``imap``
`apache-airflow-providers-mongo <https://airflow.apache.org/docs/apache-airflow-providers-mongo>`_              ``mongo``
`apache-airflow-providers-mysql <https://airflow.apache.org/docs/apache-airflow-providers-mysql>`_              ``mysql``
`apache-airflow-providers-postgres <https://airflow.apache.org/docs/apache-airflow-providers-postgres>`_        ``postgres``
`apache-airflow-providers-ssh <https://airflow.apache.org/docs/apache-airflow-providers-ssh>`_                  ``ssh``
==============================================================================================================  ===============

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

1.2.0
.....

Features
~~~~~~~~

* ``Avoid using threads in S3 remote logging upload (#14414)``
* ``Allow AWS Operator RedshiftToS3Transfer To Run a Custom Query (#14177)``
* ``includes the STS token if STS credentials are used (#11227)``

1.1.0
.....

Features
~~~~~~~~

* ``Adding support to put extra arguments for Glue Job. (#14027)``
* ``Add aws ses email backend for use with EmailOperator. (#13986)``
* ``Add bucket_name to template fileds in S3 operators (#13973)``
* ``Add ExasolToS3Operator (#13847)``
* ``AWS Glue Crawler Integration (#13072)``
* ``Add acl_policy to S3CopyObjectOperator (#13773)``
* ``AllowDiskUse parameter and docs in MongotoS3Operator (#12033)``
* ``Add S3ToFTPOperator (#11747)``
* ``add xcom push for ECSOperator (#12096)``
* ``[AIRFLOW-3723] Add Gzip capability to mongo_to_S3 operator (#13187)``
* ``Add S3KeySizeSensor (#13049)``
* ``Add 'mongo_collection' to template_fields in MongoToS3Operator (#13361)``
* ``Allow Tags on AWS Batch Job Submission (#13396)``

Bug fixes
~~~~~~~~~

* ``Fix bug in GCSToS3Operator (#13718)``
* ``Fix S3KeysUnchangedSensor so that template_fields work (#13490)``


1.0.0
.....


Initial version of the provider.
