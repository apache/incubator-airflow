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

.. _write-logs-loki:

Writing Logs to Grafana Loki
----------------------------------

Airflow can be configured to read and write task logs in `Grafana Loki <https://github.com/grafana/loki/>`__.
It uses an existing Airflow connection to read or write logs. If you don't have a connection properly setup,
this process will fail.

To enable this feature, ``airflow.cfg`` must be configured as in this

.. code-block:: ini

    [logging]
    # Airflow can store logs remotely in AWS S3, Google Cloud Storage, Elastic Search or Loki.
    # Users must supply an Airflow connection id that provides access to the storage
    # location. If remote_logging is set to true, see UPDATING.md for additional
    # configuration requirements.
    remote_logging = True
    remote_base_log_folder = 'loki:///airflow'
    remote_log_conn_id = <name of the Loki connection>

All configuration options are in the ``[logging]`` section.

The value of field ``remote_logging`` must always be set to ``True`` for this feature to work.
Turning this option off will result in data not being sent to Loki.
The ``remote_base_log_folder`` option contains the URL that specifies the type of handler to be used.
For integration with Loki, this option should start with ``loki:///``.

The path section of the URL specifies the label of the log e.g. ``loki://airflow-prod`` writes
logs under the label ``airflow_name=airflow-prod``.

By default it will attach ``airflow_dag_id``, ``airflow_task_id``, ``airflow_try_number`` labels.
