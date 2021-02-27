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



.. _howto/operator:AsanaCreateTaskOperator:

AsanaCreateTaskOperator
=======================

Use the :class:`~airflow.providers.asana.operators.AsanaCreateTaskOperator` to
create an Asana task.


Using the Operator
^^^^^^^^^^^^^^^^^^

Use the ``asana_conn_id`` argument to connect to your Asana account. Pass your
personal access token (https://developers.asana.com/docs/personal-access-token) into the
password parameter of the connection metadata.

The AsanaCreateTaskOperator minimally requires a task name. There are many other
task parameters you can specify - a complete list is available here
https://developers.asana.com/docs/create-a-task - through the `optional_task_parameters`.
You must specify at least one of 'workspace', 'parent', or 'projects' in the
`optional_task_parameters`.


.. _howto/operator:AsanaCreateTaskOperator:

AsanaUpdateTaskOperator
=======================

Use the :class:`~airflow.providers.asana.operators.AsanaUpdateTaskOperator` to
update an existing Asana task.


Using the Operator
^^^^^^^^^^^^^^^^^^

Use the ``asana_conn_id`` argument to connect to your Asana account. Pass your
personal access token (https://developers.asana.com/docs/personal-access-token) into the
password parameter of the connection metadata.

The AsanaUpdateTaskOperator minimally requires a task id. There are many other
task parameters you can specify - a complete list is available here
https://developers.asana.com/docs/create-a-task - through the `optional_task_parameters`.


.. _howto/operator:AsanaCreateTaskOperator:

AsanaDeleteTaskOperator
=======================

Use the :class:`~airflow.providers.asana.operators.AsanaDeleteTaskOperator` to
update an existing Asana task.


Using the Operator
^^^^^^^^^^^^^^^^^^

Use the ``asana_conn_id`` argument to connect to your Asana account. Pass your
personal access token (https://developers.asana.com/docs/personal-access-token) into the
password parameter of the connection metadata.

The AsanaDeleteTaskOperator requires a task id to delete.
