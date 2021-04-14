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

Google Cloud Dataflow Operators
===============================

`Dataflow <https://cloud.google.com/dataflow/>`__ is a managed service for
executing a wide variety of data processing patterns. These pipelines are created
using the Apache Beam programming model which allows for both batch and streaming processing.

.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include::/operators/_partials/prerequisite_tasks.rst

Ways to run a data pipeline
^^^^^^^^^^^^^^^^^^^^^^^^^^^

There are several ways to run a Dataflow pipeline depending on your environment, source files:

- **Non-templated pipeline**: Developer can run the pipeline as a local process on the Airflow worker
  if you have a '*.jar' file for Java or a '* .py` file for Python. This also means that the necessary system
  dependencies must be installed on the worker.  For Java, worker must have the JRE Runtime installed.
  For Python, the Python interpreter. The runtime versions must be compatible with the pipeline versions.
  This is the fastest way to start a pipeline, but because of its frequent problems with system dependencies,
  it may cause problems. See: :ref:`howto/operator:DataflowCreateJavaJobOperator`,
  :ref:`howto/operator:DataflowCreatePythonJobOperator` for more detailed information.
- **Templated pipeline**: The programmer can make the pipeline independent of the environment by preparing
  a template that will then be run on a machine managed by Google. This way, changes to the environment
  won't affect your pipeline. There are two types of the templates:

  - **Classic templates**. Developers run the pipeline and create a template. The Apache Beam SDK stages
    files in Cloud Storage, creates a template file (similar to job request),
    and saves the template file in Cloud Storage. See: :ref:`howto/operator:DataflowTemplatedJobStartOperator`
  - **Flex Templates**. Developers package the pipeline into a Docker image and then use the ``gcloud``
    command-line tool to build and save the Flex Template spec file in Cloud Storage. See:
    :ref:`howto/operator:DataflowStartFlexTemplateOperator`

- **SQL pipeline**: Developer can write pipeline as SQL statement and then execute it in Dataflow. See:
  :ref:`howto/operator:DataflowStartSqlJobOperator`

It is a good idea to test your pipeline using the non-templated pipeline,
and then run the pipeline in production using the templates.

For details on the differences between the pipeline types, see
`Dataflow templates <https://cloud.google.com/dataflow/docs/concepts/dataflow-templates>`__
in the Google Cloud documentation.

Starting Non-templated pipeline
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To create a new pipeline using the source file (JAR in Java or Python file) use
the create job operators. The source file can be located on GCS or on the local filesystem.
:class:`~airflow.providers.google.cloud.operators.dataflow.DataflowCreateJavaJobOperator`
or
:class:`~airflow.providers.google.cloud.operators.dataflow.DataflowCreatePythonJobOperator`

.. _howto/operator:DataflowCreateJavaJobOperator:

Java SDK pipelines
""""""""""""""""""

For Java pipeline the ``jar`` argument must be specified for
:class:`~airflow.providers.google.cloud.operators.dataflow.DataflowCreateJavaJobOperator`
as it contains the pipeline to be executed on Dataflow. The JAR can be available on GCS that Airflow
has the ability to download or available on the local filesystem (provide the absolute path to it).

Here is an example of creating and running a pipeline in Java with jar stored on GCS:

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_dataflow.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_start_java_job_jar_on_gcs]
    :end-before: [END howto_operator_start_java_job_jar_on_gcs]

Here is an example of creating and running a pipeline in Java with jar stored on local file system:

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_dataflow.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_start_java_job_local_jar]
    :end-before: [END howto_operator_start_java_job_local_jar]

.. _howto/operator:DataflowCreatePythonJobOperator:

Python SDK pipelines
""""""""""""""""""""

The ``py_file`` argument must be specified for
:class:`~airflow.providers.google.cloud.operators.dataflow.DataflowCreatePythonJobOperator`
as it contains the pipeline to be executed on Dataflow. The Python file can be available on GCS that Airflow
has the ability to download or available on the local filesystem (provide the absolute path to it).

The ``py_interpreter`` argument specifies the Python version to be used when executing the pipeline, the default
is ``python3`. If your Airflow instance is running on Python 2 - specify ``python2`` and ensure your ``py_file`` is
in Python 2. For best results, use Python 3.

If ``py_requirements`` argument is specified a temporary Python virtual environment with specified requirements will be created
and within it pipeline will run.

The ``py_system_site_packages`` argument specifies whether or not all the Python packages from your Airflow instance,
will be accessible within virtual environment (if ``py_requirements`` argument is specified),
recommend avoiding unless the Dataflow job requires it.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_dataflow.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_start_python_job]
    :end-before: [END howto_operator_start_python_job]

Execution models
^^^^^^^^^^^^^^^^

Dataflow has multiple options of executing pipelines. It can be done in the following modes:
batch asynchronously (fire and forget), batch blocking (wait until completion), or streaming (run indefinitely).
In Airflow it is best practice to use asynchronous batch pipelines or streams and use sensors to listen for expected job state.

By default :class:`~airflow.providers.google.cloud.operators.dataflow.DataflowCreateJavaJobOperator`,
:class:`~airflow.providers.google.cloud.operators.dataflow.DataflowCreatePythonJobOperator`,
:class:`~airflow.providers.google.cloud.operators.dataflow.DataflowTemplatedJobStartOperator` and
:class:`~airflow.providers.google.cloud.operators.dataflow.DataflowStartFlexTemplateOperator`
have argument ``wait_until_finished`` set to ``None`` which cause different behaviour depends om the type of pipeline:

* for the streaming pipeline, wait for jobs to start,
* for the batch pipeline, wait for the jobs to complete.

If ``wait_until_finished`` is set to ``True`` operator will always wait for end of pipeline execution.
If set to ``False`` only submits the jobs.

See: `Configuring PipelineOptions for execution on the Cloud Dataflow service <https://cloud.google.com/dataflow/docs/guides/specifying-exec-params#configuring-pipelineoptions-for-execution-on-the-cloud-dataflow-service>`_

Asynchronous execution
""""""""""""""""""""""

Dataflow batch jobs are by default asynchronous - however this is dependent on the application code (contained in the JAR
or Python file) and how it is written. In order for the Dataflow job to execute asynchronously, ensure the
pipeline objects are not being waited upon (not calling ``waitUntilFinish`` or ``wait_until_finish`` on the
``PipelineResult`` in your application code).

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_dataflow.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_start_python_job_async]
    :end-before: [END howto_operator_start_python_job_async]

Blocking execution
""""""""""""""""""

In order for a Dataflow job to execute and wait until completion, ensure the pipeline objects are waited upon
in the application code. This can be done for the Java SDK by calling ``waitUntilFinish`` on the ``PipelineResult``
returned from ``pipeline.run()`` or for the Python SDK by calling ``wait_until_finish`` on the ``PipelineResult``
returned from ``pipeline.run()``.

Blocking jobs should be avoided as there is a background process that occurs when run on Airflow. This process is
continuously being run to wait for the Dataflow job to be completed and increases the consumption of resources by
Airflow in doing so.

Streaming execution
"""""""""""""""""""

To execute a streaming Dataflow job, ensure the streaming option is set (for Python) or read from an unbounded data
source, such as Pub/Sub, in your pipeline (for Java).

Setting argument ``drain_pipeline`` to ``True`` allows to stop streaming job by draining it
instead of canceling during killing task instance.

See the `Stopping a running pipeline
<https://cloud.google.com/dataflow/docs/guides/stopping-a-pipeline>`_.


.. _howto/operator:DataflowTemplatedJobStartOperator:
.. _howto/operator:DataflowStartFlexTemplateOperator:

Templated jobs
""""""""""""""

Templates give the ability to stage a pipeline on Cloud Storage and run it from there. This
provides flexibility in the development workflow as it separates the development of a pipeline
from the staging and execution steps. There are two types of templates for Dataflow: Classic and Flex.
See the `official documentation for Dataflow templates
<https://cloud.google.com/dataflow/docs/concepts/dataflow-templates>`_ for more information.

Here is an example of running Classic template with
:class:`~airflow.providers.google.cloud.operators.dataflow.DataflowTemplatedJobStartOperator`:

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_dataflow.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_start_template_job]
    :end-before: [END howto_operator_start_template_job]

See the `list of Google-provided templates that can be used with this operator
<https://cloud.google.com/dataflow/docs/guides/templates/provided-templates>`_.

Here is an example of running Flex template with
:class:`~airflow.providers.google.cloud.operators.dataflow.DataflowStartFlexTemplateOperator`:

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_dataflow_flex_template.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_start_template_job]
    :end-before: [END howto_operator_start_template_job]

.. _howto/operator:DataflowStartSqlJobOperator:

Dataflow SQL
""""""""""""
Dataflow SQL supports a variant of the ZetaSQL query syntax and includes additional streaming
extensions for running Dataflow streaming jobs.

Here is an example of running Dataflow SQL job with
:class:`~airflow.providers.google.cloud.operators.dataflow.DataflowStartSqlJobOperator`:

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_dataflow_sql.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_start_sql_job]
    :end-before: [END howto_operator_start_sql_job]

.. warning::
    This operator requires ``gcloud`` command (Google Cloud SDK) must be installed on the Airflow worker
    <https://cloud.google.com/sdk/docs/install>`__

See the `Dataflow SQL reference
<https://cloud.google.com/dataflow/docs/reference/sql>`_.

.. _howto/operator:DataflowJobStatusSensor:
.. _howto/operator:DataflowJobMetricsSensor:
.. _howto/operator:DataflowJobMessagesSensor:
.. _howto/operator:DataflowJobAutoScalingEventsSensor:

Sensors
^^^^^^^

When job is triggered asynchronously sensors may be used to run checks for specific job properties.

:class:`~airflow.providers.google.cloud.sensors.dataflow.DataflowJobStatusSensor`.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_dataflow.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_wait_for_job_status]
    :end-before: [END howto_sensor_wait_for_job_status]

:class:`~airflow.providers.google.cloud.sensors.dataflow.DataflowJobMetricsSensor`.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_dataflow.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_wait_for_job_metric]
    :end-before: [END howto_sensor_wait_for_job_metric]

:class:`~airflow.providers.google.cloud.sensors.dataflow.DataflowJobMessagesSensor`.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_dataflow.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_wait_for_job_message]
    :end-before: [END howto_sensor_wait_for_job_message]

:class:`~airflow.providers.google.cloud.sensors.dataflow.DataflowJobAutoScalingEventsSensor`.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_dataflow.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_wait_for_job_autoscaling_event]
    :end-before: [END howto_sensor_wait_for_job_autoscaling_event]

Reference
^^^^^^^^^

For further information, look at:

* `Google Cloud API Documentation <https://cloud.google.com/dataflow/docs/apis>`__
* `Apache Beam Documentation <https://beam.apache.org/documentation/>`__
* `Product Documentation <https://cloud.google.com/dataflow/docs/>`__
* `Dataflow Monitoring Interface <https://cloud.google.com/dataflow/docs/guides/using-monitoring-intf/>`__
* `Dataflow Command-line Interface <https://cloud.google.com/dataflow/docs/guides/using-command-line-intf/>`__
