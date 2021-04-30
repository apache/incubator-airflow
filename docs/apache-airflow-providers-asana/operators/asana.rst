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

The AsanaCreateTaskOperator minimally requires the new task's name and
the Asana connection to use to connect to your account (``conn_id``). There are many other
`task attributes you can specify <https://developers.asana.com/docs/create-a-task>`_
through the ``task_parameters``. You must specify at least one of ``workspace``,
``parent``, or ``projects`` in the ``task_parameters`` or in the connection.


.. _howto/operator:AsanaDeleteTaskOperator:

AsanaDeleteTaskOperator
=======================

Use the :class:`~airflow.providers.asana.operators.AsanaDeleteTaskOperator` to
delete an existing Asana task.


Using the Operator
^^^^^^^^^^^^^^^^^^

The AsanaDeleteTaskOperator requires the task id to delete. Use the ``conn_id``
parameter to specify the Asana connection to use to connect to your account.


.. _howto/operator:AsanaFindTaskOperator:

AsanaFindTaskOperator
=======================

Use the :class:`~airflow.providers.asana.operators.AsanaFindTaskOperator` to
search for Asana tasks that fit some criteria.


Using the Operator
^^^^^^^^^^^^^^^^^^

The AsanaFindTaskOperator requires a dict of search parameters following the description
`here <https://developers.asana.com/docs/get-multiple-tasks>`_.
Use the ``conn_id`` parameter to specify the Asana connection to use to connect
to your account. Any parameters provided through the connection will be used in the
search if not overridden in the ``search_parameters``.

.. _howto/operator:AsanaUpdateTaskOperator:

AsanaUpdateTaskOperator
=======================

Use the :class:`~airflow.providers.asana.operators.AsanaUpdateTaskOperator` to
update an existing Asana task.


Using the Operator
^^^^^^^^^^^^^^^^^^

The AsanaUpdateTaskOperator minimally requires the task id to update and
the Asana connection to use to connect to your account (``conn_id``). There are many other
`task attributes you can overwrite <https://developers.asana.com/docs/update-a-task>`_
through the ``task_parameters``.
