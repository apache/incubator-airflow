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

``apache-airflow-providers-google``
===================================

Content
-------

.. toctree::
    :maxdepth: 1
    :caption: Guides

    Connection types <connections/index>
    Logging handlers <logging/index>
    Secrets backends <secrets-backends/google-cloud-secret-manager-backend>
    API Authentication backend <api-auth-backend/google-openid>
    Operators <operators/index>

.. toctree::
    :maxdepth: 1
    :caption: References

    Python API <_api/airflow/providers/google/index>
    Configuration <configurations-ref>

.. toctree::
    :maxdepth: 1
    :caption: Resources

    Example DAGs <example-dags>
    PyPI Repository <https://pypi.org/project/apache-airflow-providers-google/>

.. THE REMINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


Package apache-airflow-providers-google
------------------------------------------------------

Google services including:

  - `Google Ads <https://ads.google.com/>`__
  - `Google Cloud (GCP) <https://cloud.google.com/>`__
  - `Google Firebase <https://firebase.google.com/>`__
  - `Google Marketing Platform <https://marketingplatform.google.com/>`__
  - `Google Workspace <https://workspace.google.pl/>`__ (formerly Google Suite)
  - `Google LevelDB <https://github.com/google/leveldb>`__


Release: 2.1.0

Provider package
----------------

This is a provider package for ``google`` provider. All classes for this provider package
are in ``airflow.providers.google`` python package.

Installation
------------

.. note::

    On November 2020, new version of PIP (20.3) has been released with a new, 2020 resolver. This resolver
    does not yet work with Apache Airflow and might lead to errors in installation - depends on your choice
    of extras. In order to install Airflow you need to either downgrade pip to version 20.2.4
    ``pip install --upgrade pip==20.2.4`` or, in case you use Pip 20.3, you need to add option
    ``--use-deprecated legacy-resolver`` to your pip install command.


You can install this package on top of an existing airflow 2.* installation via
``pip install apache-airflow-providers-google``

PIP requirements
----------------

======================================  ===================
PIP package                             Version required
======================================  ===================
``PyOpenSSL``
``google-ads``                          ``>=4.0.0,<8.0.0``
``google-api-core``                     ``>=1.25.1,<2.0.0``
``google-api-python-client``            ``>=1.6.0,<2.0.0``
``google-auth-httplib2``                ``>=0.0.1``
``google-auth``                         ``>=1.0.0,<2.0.0``
``google-cloud-automl``                 ``>=2.1.0,<3.0.0``
``google-cloud-bigquery-datatransfer``  ``>=3.0.0,<4.0.0``
``google-cloud-bigtable``               ``>=1.0.0,<2.0.0``
``google-cloud-container``              ``>=0.1.1,<2.0.0``
``google-cloud-datacatalog``            ``>=3.0.0,<4.0.0``
``google-cloud-dataproc``               ``>=2.2.0,<3.0.0``
``google-cloud-dlp``                    ``>=0.11.0,<2.0.0``
``google-cloud-kms``                    ``>=2.0.0,<3.0.0``
``google-cloud-language``               ``>=1.1.1,<2.0.0``
``google-cloud-logging``                ``>=2.1.1,<3.0.0``
``google-cloud-memcache``               ``>=0.2.0``
``google-cloud-monitoring``             ``>=2.0.0,<3.0.0``
``google-cloud-os-login``               ``>=2.0.0,<3.0.0``
``google-cloud-pubsub``                 ``>=2.0.0,<3.0.0``
``google-cloud-redis``                  ``>=2.0.0,<3.0.0``
``google-cloud-secret-manager``         ``>=0.2.0,<2.0.0``
``google-cloud-spanner``                ``>=1.10.0,<2.0.0``
``google-cloud-speech``                 ``>=0.36.3,<2.0.0``
``google-cloud-storage``                ``>=1.30,<2.0.0``
``google-cloud-tasks``                  ``>=2.0.0,<3.0.0``
``google-cloud-texttospeech``           ``>=0.4.0,<2.0.0``
``google-cloud-translate``              ``>=1.5.0,<2.0.0``
``google-cloud-videointelligence``      ``>=1.7.0,<2.0.0``
``google-cloud-vision``                 ``>=0.35.2,<2.0.0``
``google-cloud-workflows``              ``>=0.1.0,<2.0.0``
``grpcio-gcp``                          ``>=0.2.2``
``json-merge-patch``                    ``~=0.2``
``pandas-gbq``
``plyvel``
======================================  ===================

Cross provider package dependencies
-----------------------------------

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified provider packages in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

.. code-block:: bash

    pip install apache-airflow-providers-google[amazon]


========================================================================================================================  ====================
Dependent package                                                                                                         Extra
========================================================================================================================  ====================
`apache-airflow-providers-amazon <https://airflow.apache.org/docs/apache-airflow-providers-amazon>`_                      ``amazon``
`apache-airflow-providers-apache-beam <https://airflow.apache.org/docs/apache-airflow-providers-apache-beam>`_            ``apache.beam``
`apache-airflow-providers-apache-cassandra <https://airflow.apache.org/docs/apache-airflow-providers-apache-cassandra>`_  ``apache.cassandra``
`apache-airflow-providers-cncf-kubernetes <https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes>`_    ``cncf.kubernetes``
`apache-airflow-providers-facebook <https://airflow.apache.org/docs/apache-airflow-providers-facebook>`_                  ``facebook``
`apache-airflow-providers-microsoft-azure <https://airflow.apache.org/docs/apache-airflow-providers-microsoft-azure>`_    ``microsoft.azure``
`apache-airflow-providers-microsoft-mssql <https://airflow.apache.org/docs/apache-airflow-providers-microsoft-mssql>`_    ``microsoft.mssql``
`apache-airflow-providers-mysql <https://airflow.apache.org/docs/apache-airflow-providers-mysql>`_                        ``mysql``
`apache-airflow-providers-oracle <https://airflow.apache.org/docs/apache-airflow-providers-oracle>`_                      ``oracle``
`apache-airflow-providers-postgres <https://airflow.apache.org/docs/apache-airflow-providers-postgres>`_                  ``postgres``
`apache-airflow-providers-presto <https://airflow.apache.org/docs/apache-airflow-providers-presto>`_                      ``presto``
`apache-airflow-providers-salesforce <https://airflow.apache.org/docs/apache-airflow-providers-salesforce>`_              ``salesforce``
`apache-airflow-providers-sftp <https://airflow.apache.org/docs/apache-airflow-providers-sftp>`_                          ``sftp``
`apache-airflow-providers-ssh <https://airflow.apache.org/docs/apache-airflow-providers-ssh>`_                            ``ssh``
========================================================================================================================  ====================

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

2.1.0
.....

Features
~~~~~~~~

* ``Corrects order of argument in docstring in GCSHook.download method (#14497)``
* ``Refactor SQL/BigQuery/Qubole/Druid Check operators (#12677)``
* ``Add GoogleDriveToLocalOperator (#14191)``
* ``Add 'exists_ok' flag to BigQueryCreateEmptyTable(Dataset)Operator (#14026)``
* ``Add materialized view support for BigQuery (#14201)``
* ``Add BigQueryUpdateTableOperator (#14149)``
* ``Add param to CloudDataTransferServiceOperator (#14118)``
* ``Add gdrive_to_gcs operator, drive sensor, additional functionality to drive hook  (#13982)``
* ``Improve GCSToSFTPOperator paths handling (#11284)``

Bug Fixes
~~~~~~~~~

* ``Fixes to dataproc operators and hook (#14086)``
* ``#9803 fix bug in copy operation without wildcard  (#13919)``

2.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

Updated ``google-cloud-*`` libraries
````````````````````````````````````

This release of the provider package contains third-party library updates, which may require updating your
DAG files or custom hooks and operators, if you were using objects from those libraries.
Updating of these libraries is necessary to be able to use new features made available by new versions of
the libraries and to obtain bug fixes that are only available for new versions of the library.

Details are covered in the UPDATING.md files for each library, but there are some details
that you should pay attention to.


+-----------------------------------------------------------------------------------------------------+----------------------+---------------------+---------------------------------------------------------------------------------------------------------------------------------------+
| Library name                                                                                        | Previous constraints | Current constraints | Upgrade Documentation                                                                                                                 |
+=====================================================================================================+======================+=====================+=======================================================================================================================================+
| `google-cloud-automl <https://pypi.org/project/google-cloud-automl/>`_                              | ``>=0.4.0,<2.0.0``   | ``>=2.1.0,<3.0.0``  | `Upgrading google-cloud-automl <https://github.com/googleapis/python-automl/blob/master/UPGRADING.md>`_                               |
+-----------------------------------------------------------------------------------------------------+----------------------+---------------------+---------------------------------------------------------------------------------------------------------------------------------------+
| `google-cloud-bigquery-datatransfer <https://pypi.org/project/google-cloud-bigquery-datatransfer>`_ | ``>=0.4.0,<2.0.0``   | ``>=3.0.0,<4.0.0``  | `Upgrading google-cloud-bigquery-datatransfer <https://github.com/googleapis/python-bigquery-datatransfer/blob/master/UPGRADING.md>`_ |
+-----------------------------------------------------------------------------------------------------+----------------------+---------------------+---------------------------------------------------------------------------------------------------------------------------------------+
| `google-cloud-datacatalog <https://pypi.org/project/google-cloud-datacatalog>`_                     | ``>=0.5.0,<0.8``     | ``>=3.0.0,<4.0.0``  | `Upgrading google-cloud-datacatalog <https://github.com/googleapis/python-datacatalog/blob/master/UPGRADING.md>`_                     |
+-----------------------------------------------------------------------------------------------------+----------------------+---------------------+---------------------------------------------------------------------------------------------------------------------------------------+
| `google-cloud-dataproc <https://pypi.org/project/google-cloud-dataproc/>`_                          | ``>=1.0.1,<2.0.0``   | ``>=2.2.0,<3.0.0``  | `Upgrading google-cloud-dataproc <https://github.com/googleapis/python-dataproc/blob/master/UPGRADING.md>`_                           |
+-----------------------------------------------------------------------------------------------------+----------------------+---------------------+---------------------------------------------------------------------------------------------------------------------------------------+
| `google-cloud-kms <https://pypi.org/project/google-cloud-kms>`_                                     | ``>=1.2.1,<2.0.0``   | ``>=2.0.0,<3.0.0``  | `Upgrading google-cloud-kms <https://github.com/googleapis/python-kms/blob/master/UPGRADING.md>`_                                     |
+-----------------------------------------------------------------------------------------------------+----------------------+---------------------+---------------------------------------------------------------------------------------------------------------------------------------+
| `google-cloud-logging <https://pypi.org/project/google-cloud-logging/>`_                            | ``>=1.14.0,<2.0.0``  | ``>=2.0.0,<3.0.0``  | `Upgrading google-cloud-logging <https://github.com/googleapis/python-logging/blob/master/UPGRADING.md>`_                             |
+-----------------------------------------------------------------------------------------------------+----------------------+---------------------+---------------------------------------------------------------------------------------------------------------------------------------+
| `google-cloud-monitoring <https://pypi.org/project/google-cloud-monitoring>`_                       | ``>=0.34.0,<2.0.0``  | ``>=2.0.0,<3.0.0``  | `Upgrading google-cloud-monitoring <https://github.com/googleapis/python-monitoring/blob/master/UPGRADING.md)>`_                      |
+-----------------------------------------------------------------------------------------------------+----------------------+---------------------+---------------------------------------------------------------------------------------------------------------------------------------+
| `google-cloud-os-login <https://pypi.org/project/google-cloud-os-login>`_                           | ``>=1.0.0,<2.0.0``   | ``>=2.0.0,<3.0.0``  | `Upgrading google-cloud-os-login <https://github.com/googleapis/python-oslogin/blob/master/UPGRADING.md>`_                            |
+-----------------------------------------------------------------------------------------------------+----------------------+---------------------+---------------------------------------------------------------------------------------------------------------------------------------+
| `google-cloud-pubsub <https://pypi.org/project/google-cloud-pubsub>`_                               | ``>=1.0.0,<2.0.0``   | ``>=2.0.0,<3.0.0``  | `Upgrading google-cloud-pubsub <https://github.com/googleapis/python-pubsub/blob/master/UPGRADING.md>`_                               |
+-----------------------------------------------------------------------------------------------------+----------------------+---------------------+---------------------------------------------------------------------------------------------------------------------------------------+
| `google-cloud-tasks <https://pypi.org/project/google-cloud-tasks>`_                                 | ``>=1.2.1,<2.0.0``   | ``>=2.0.0,<3.0.0``  | `Upgrading google-cloud-task <https://github.com/googleapis/python-tasks/blob/master/UPGRADING.md>`_                                  |
+-----------------------------------------------------------------------------------------------------+----------------------+---------------------+---------------------------------------------------------------------------------------------------------------------------------------+

The field names use the snake_case convention
`````````````````````````````````````````````

If your DAG uses an object from the above mentioned libraries passed by XCom, it is necessary to update the
naming convention of the fields that are read. Previously, the fields used the CamelSnake convention,
now the snake_case convention is used.

**Before:**

.. code-block:: python

    set_acl_permission = GCSBucketCreateAclEntryOperator(
        task_id="gcs-set-acl-permission",
        bucket=BUCKET_NAME,
        entity="user-{{ task_instance.xcom_pull('get-instance')['persistenceIamIdentity']"
        ".split(':', 2)[1] }}",
        role="OWNER",
    )


**After:**

.. code-block:: python

    set_acl_permission = GCSBucketCreateAclEntryOperator(
        task_id="gcs-set-acl-permission",
        bucket=BUCKET_NAME,
        entity="user-{{ task_instance.xcom_pull('get-instance')['persistence_iam_identity']"
        ".split(':', 2)[1] }}",
        role="OWNER",
    )


Features
~~~~~~~~

* ``Add Apache Beam operators (#12814)``
* ``Add Google Cloud Workflows Operators (#13366)``
* ``Replace 'google_cloud_storage_conn_id' by 'gcp_conn_id' when using 'GCSHook' (#13851)``
* ``Add How To Guide for Dataflow (#13461)``
* ``Generalize MLEngineStartTrainingJobOperator to custom images (#13318)``
* ``Add Parquet data type to BaseSQLToGCSOperator (#13359)``
* ``Add DataprocCreateWorkflowTemplateOperator (#13338)``
* ``Add OracleToGCS Transfer (#13246)``
* ``Add timeout option to gcs hook methods. (#13156)``
* ``Add regional support to dataproc workflow template operators (#12907)``
* ``Add project_id to client inside BigQuery hook update_table method (#13018)``

Bug fixes
~~~~~~~~~

* ``Fix four bugs in StackdriverTaskHandler (#13784)``
* ``Decode Remote Google Logs (#13115)``
* ``Fix and improve GCP BigTable hook and system test (#13896)``
* ``updated Google DV360 Hook to fix SDF issue (#13703)``
* ``Fix insert_all method of BigQueryHook to support tables without schema (#13138)``
* ``Fix Google BigQueryHook method get_schema() (#13136)``
* ``Fix Data Catalog operators (#13096)``


1.0.0
.....

Initial version of the provider.
