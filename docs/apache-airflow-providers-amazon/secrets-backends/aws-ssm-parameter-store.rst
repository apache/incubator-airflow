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

.. _ssm_parameter_store_secrets:

AWS SSM Parameter Store Secrets Backend
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To enable SSM parameter store, specify :py:class:`~airflow.providers.amazon.aws.secrets.systems_manager.SystemsManagerParameterStoreBackend`
as the ``backend`` in  ``[secrets]`` section of ``airflow.cfg``.

Here is a sample configuration:

.. code-block:: ini

    [secrets]
    backend = airflow.providers.amazon.aws.secrets.systems_manager.SystemsManagerParameterStoreBackend
    backend_kwargs = {"connections_prefix": "/airflow/connections", "variables_prefix": "/airflow/variables", "profile_name": "default"}

Optional lookup
"""""""""""""""

Optionally connections, variables, or config may be looked up exclusive of each other or in any combination.
This will prevent requests being sent to AWS SSM Parameter Store for the excluded type.

If you want to look up some and not others in AWS SSM Parameter Store you may do so by setting the relevant ``*_prefix`` parameter of the ones to be excluded as ``null``.

For example, if you want to set parameter ``connections_prefix`` to ``"/airflow/connections"`` and not look up variables, your configuration file should look like this:

.. code-block:: ini

    [secrets]
    backend = airflow.providers.amazon.aws.secrets.systems_manager.SystemsManagerParameterStoreBackend
    backend_kwargs = {"connections_prefix": "/airflow/connections", "variables_prefix": null, "profile_name": "default"}

Storing and Retrieving Connections
""""""""""""""""""""""""""""""""""

If you have set ``connections_prefix`` as ``/airflow/connections``, then for a connection id of ``smtp_default``,
you would want to store your connection at ``/airflow/connections/smtp_default``.

Optionally you can supply a profile name to reference aws profile, e.g. defined in ``~/.aws/config``.

The value of the SSM parameter must be the :ref:`connection URI representation <generating_connection_uri>`
of the connection object.

In some cases, URI's you will need stored in Secrets Manager may not be intuitive, for example when using HTTP / HTTPS or SPARK, you may need URI's that will look like this:

.. code-block:: ini

    http://https%3A%2F%2Fexample.com

    spark://spark%3A%2F%2Fspark-master-0.spark-master.spark:7077

This is a known situation, where schema and protocol parts of the URI are independent and in some cases, need to be specified explicitly.

See GitHub issue `#10256 <https://github.com/apache/airflow/pull/10256>`__ and `#10913 <https://github.com/apache/airflow/issues/10913>`__ for more detailed discussion that led to this documentation update. This may get resolved in the future.

Storing and Retrieving Variables
""""""""""""""""""""""""""""""""

If you have set ``variables_prefix`` as ``/airflow/variables``, then for an Variable key of ``hello``,
you would want to store your Variable at ``/airflow/variables/hello``.

Optionally you can supply a profile name to reference aws profile, e.g. defined in ``~/.aws/config``.
