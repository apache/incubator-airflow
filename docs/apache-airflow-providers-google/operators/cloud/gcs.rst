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



Google Cloud Storage Operators
==============================

Cloud Storage allows world-wide storage and retrieval of any amount of data at any time.
You can use Cloud Storage for a range of scenarios including serving website content,
storing data for archival and disaster recovery, or distributing large data objects to users via direct download.

.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include::/operators/_partials/prerequisite_tasks.rst

Operators
^^^^^^^^^

.. _howto/operator:GCSToBigQueryOperator:

GCSToBigQueryOperator
---------------------

Use the
:class:`~airflow.providers.google.cloud.transfers.gcs_to_bigquery.GCSToBigQueryOperator`
to execute a BigQuery load job.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_gcs_to_bigquery.py
    :language: python
    :start-after: [START howto_operator_gcs_to_bigquery]
    :end-before: [END howto_operator_gcs_to_bigquery]


.. _howto/operator:GCSTimeSpanFileTransformOperator:

GCSTimeSpanFileTransformOperator
--------------------------------

Use the
:class:`~airflow.providers.google.cloud.operators.gcs.GCSTimeSpanFileTransformOperator`
to transform files that were modified in a specific time span. The time span is defined
by the DAG instance logical execution timestamp (``execution_date``, start of time span)
and the timestamp when the next DAG instance execution is scheduled (end of time span). If a DAG
does not have a *next* DAG instance scheduled, the time span end infinite, meaning the operator
processes all files older than ``execution_date``.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_gcs_timespan_file_transform.py
    :language: python
    :start-after: [START howto_operator_gcs_timespan_file_transform_operator_Task]
    :end-before: [END howto_operator_gcs_timespan_file_transform_operator_Task]


.. _howto/operator:GCSBucketCreateAclEntryOperator:

GCSBucketCreateAclEntryOperator
-------------------------------

Creates a new ACL entry on the specified bucket.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.gcs.GCSBucketCreateAclEntryOperator`

Using the operator
""""""""""""""""""

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcs_bucket_create_acl_entry_task]
    :end-before: [END howto_operator_gcs_bucket_create_acl_entry_task]

Templating
""""""""""

.. literalinclude:: /../../airflow/providers/google/cloud/operators/gcs.py
    :language: python
    :dedent: 4
    :start-after: [START gcs_bucket_create_acl_template_fields]
    :end-before: [END gcs_bucket_create_acl_template_fields]

More information
""""""""""""""""

See Google Cloud Storage Documentation to `create a new ACL entry for a bucket
<https://cloud.google.com/storage/docs/json_api/v1/bucketAccessControls/insert>`_.

.. _howto/operator:GCSObjectCreateAclEntryOperator:

GCSObjectCreateAclEntryOperator
-------------------------------

Creates a new ACL entry on the specified object.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.gcs.GCSObjectCreateAclEntryOperator`

Using the operator
""""""""""""""""""

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcs_object_create_acl_entry_task]
    :end-before: [END howto_operator_gcs_object_create_acl_entry_task]

Templating
""""""""""

.. literalinclude:: /../../airflow/providers/google/cloud/operators/gcs.py
    :language: python
    :dedent: 4
    :start-after: [START gcs_object_create_acl_template_fields]
    :end-before: [END gcs_object_create_acl_template_fields]

More information
""""""""""""""""

See Google Cloud Storage insert documentation to `create a ACL entry for ObjectAccess
<https://cloud.google.com/storage/docs/json_api/v1/objectAccessControls/insert>`_.


.. _howto/operator:GCSDeleteBucketOperator:

Deleting Bucket
^^^^^^^^^^^^^^^

Deleting Bucket allows you to remove bucket object from the Google Cloud Storage.
It is performed through the
:class:`~airflow.providers.google.cloud.operators.gcs.GCSDeleteBucketOperator` operator.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcs_delete_bucket]
    :end-before: [END howto_operator_gcs_delete_bucket]


You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.gcs.GCSDeleteBucketOperator`
parameters which allows you to dynamically determine values.

Reference
---------

For further information, look at:

* `Client Library Documentation <https://googleapis.dev/python/storage/latest/buckets.html>`__
* `Product Documentation <https://cloud.google.com/storage/docs/json_api/v1/buckets>`__

Sensors
^^^^^^^

.. _howto/sensor:GCSObjectExistenceSensor:

GCSObjectExistenceSensor
------------------------

Use the :class:`~airflow.providers.google.cloud.sensors.gcs.GCSObjectExistenceSensor` to wait (poll) for the existence of a file in Google Cloud Storage.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_object_exists_task]
    :end-before: [END howto_sensor_object_exists_task]

.. _howto/sensor:GCSObjectsWithPrefixExistenceSensor:

GCSObjectsWithPrefixExistenceSensor
-----------------------------------

Use the :class:`~airflow.providers.google.cloud.sensors.gcs.GCSObjectsWithPrefixExistenceSensor` to wait (poll) for the existence of a file with a specified prefix in Google Cloud Storage.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_object_with_prefix_exists_task]
    :end-before: [END howto_sensor_object_with_prefix_exists_task]

More information
""""""""""""""""

Sensors have different modes that determine the behaviour of resources while the task is executing.
See `Airflow sensors documentation
<https://airflow.apache.org/docs/apache-airflow/stable/concepts.html#sensors>`_ for best practices when using sensors.


Reference
^^^^^^^^^

For further information, look at:

* `Client Library Documentation <https://googleapis.github.io/google-cloud-python/latest/storage/index.html>`__
* `Product Documentation <https://cloud.google.com/storage/docs/>`__
