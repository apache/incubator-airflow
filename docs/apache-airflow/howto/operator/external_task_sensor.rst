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

.. _howto/operator:Cross-DAG Dependencies:

Cross-DAG Dependencies
======================

When two DAGs have dependency relationships, it is worth considering combining them into a single
DAG, which is usually simpler to understand. Airflow also offers better visual representation of
dependencies for tasks on the same DAG. However, it is sometimes not practical to put all related
tasks on the same DAG. For example:

- Two DAGs may have different schedules. E.g. a weekly DAG may have tasks that depend on other tasks
  on a daily DAG.
- Different teams are responsible for different DAGs, but these DAGs have some cross-DAG
  dependencies.
- A task may depend on another task on the same DAG, but for a different ``execution_date``.
- Use ``execution_delta`` for tasks running at different times, like ``execution_delta=timedelta(hours=1)``
  to check against a task that runs 1 hour earlier.

``ExternalTaskSensor`` can be used to establish such dependencies across different DAGs. When it is
used together with ``ExternalTaskMarker``, clearing dependent tasks can also happen across different
DAGs.

ExternalTaskSensor
^^^^^^^^^^^^^^^^^^

Use the :class:`~airflow.sensors.external_task.ExternalTaskSensor` to make tasks on a DAG
wait for another task on a different DAG for a specific ``execution_date``.

ExternalTaskSensor also provide options to set if the Task on a remote DAG succeeded or failed
via ``allowed_states`` and ``failed_states`` parameters.

.. exampleinclude:: /../../airflow/example_dags/example_external_task_marker_dag.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_external_task_sensor]
    :end-before: [END howto_operator_external_task_sensor]



ExternalTaskMarker
^^^^^^^^^^^^^^^^^^
If it is desirable that whenever ``parent_task`` on ``parent_dag`` is cleared, ``child_task1``
on ``child_dag`` for a specific ``execution_date`` should also be cleared, ``ExternalTaskMarker``
should be used. Note that ``child_task1`` will only be cleared if "Recursive" is selected when the
user clears ``parent_task``.

.. exampleinclude:: /../../airflow/example_dags/example_external_task_marker_dag.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_external_task_marker]
    :end-before: [END howto_operator_external_task_marker]
