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

Configuring Airflow
-------------------

All Airflow configuration parameters (equivalent of ``airflow.cfg``) are
stored in
`values.yaml <https://github.com/apache/airflow/blob/master/chart/values.yaml>`__
under the ``config`` key . The following code demonstrates how one would
allow webserver users to view the config from within the webserver
application. See the bottom line of the example:

.. code-block:: yaml

   # Config settings to go into the mounted airflow.cfg
   #
   # Please note that these values are passed through the ``tpl`` function, so are
   # all subject to being rendered as go templates. If you need to include a
   # literal ``{{`` in a value, it must be expressed like this:
   #
   #    a: '{{ "{{ not a template }}" }}'
   #
   # yamllint disable rule:line-length
   config:
     core:
       dags_folder: '{{ include "airflow_dags" . }}'
       load_examples: 'False'   # <<<<<<<< This is ignored when used with the official Docker image, see below on how to load examples
       executor: '{{ .Values.executor }}'
       # For Airflow 1.10, backward compatibility
       colored_console_log: 'False'
       remote_logging: '{{- ternary "True" "False" .Values.elasticsearch.enabled }}'
     # Authentication backend used for the experimental API
     api:
       auth_backend: airflow.api.auth.backend.deny_all
     logging:
       remote_logging: '{{- ternary "True" "False" .Values.elasticsearch.enabled }}'
       colored_console_log: 'False'
       logging_level: DEBUG
     metrics:
       statsd_on: '{{ ternary "True" "False" .Values.statsd.enabled }}'
       statsd_port: 9125
       statsd_prefix: airflow
       statsd_host: '{{ printf "%s-statsd" .Release.Name }}'
     webserver:
       enable_proxy_fix: 'True'
       expose_config: 'True'   # <<<<<<<<<< BY DEFAULT THIS IS 'False' BUT WE CHANGE IT TO 'True' PRIOR TO INSTALLING THE CHART

Generally speaking, it is useful to familiarize oneself with the Airflow
configuration prior to installing and deploying the service.

.. note::

  The recommended way to load example DAGs using the official Docker image and chart is to configure the ``AIRFLOW__CORE__LOAD_EXAMPLES`` environment variable
  in ``extraEnv`` (see :doc:`Parameters reference <parameters-ref>`). Because the official Docker image has ``AIRFLOW__CORE__LOAD_EXAMPLES=False``
  set within the image, so you need to override it when deploying the chart.
