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

.. _howto/operator:AirbyteTriggerSyncOperator:

AirbyteTriggerSyncOperator
==========================

Use the :class:`~airflow.providers.airbyte.operators.AirbyteTriggerSyncOperator` to
trigger an existing ConnectionId sync job in Airbyte.

.. warning::
  This operator triggers a synchronization job in Airbyte.
  If triggered again, this operator does not guarantee idempotency.
  You must be aware of the source (database, API, etc) you are updating/sync and
  the method applied to perform the operation in Airbyte.


Using the Operator
^^^^^^^^^^^^^^^^^^

The AirbyteTriggerSyncOperator requires the ``connection_id`` this is the uuid identifier
create in Airbyte between a source and destination synchronization job.
Use the ``airbyte_conn_id`` parameter to specify the Airbyte connection to use to
connect to your account.

You can trigger a synchronization job in Airflow in two ways with the Operator. The first one
is a synchronous process. This will trigger the Airbyte job and the Operator manage the status
of the job. Another way is use the flag ``async = True`` so the Operator only trigger the job and
return the ``job_id`` that should be pass to the AirbyteSensor.

An example using the synchronous way:

.. exampleinclude:: /../../airflow/providers/airbyte/example_dags/example_airbyte_trigger_job.py
    :language: python
    :start-after: [START howto_operator_airbyte_synchronous]
    :end-before: [END howto_operator_airbyte_synchronous]

An example using the async way:

.. exampleinclude:: /../../airflow/providers/airbyte/example_dags/example_airbyte_trigger_job.py
    :language: python
    :start-after: [START howto_operator_airbyte_asynchronous]
    :end-before: [END howto_operator_airbyte_asynchronous]
