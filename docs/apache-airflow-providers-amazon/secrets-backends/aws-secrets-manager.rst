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

AWS Secrets Manager Backend
^^^^^^^^^^^^^^^^^^^^^^^^^^^

To enable Secrets Manager, specify :py:class:`~airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend`
as the ``backend`` in  ``[secrets]`` section of ``airflow.cfg``.

Here is a sample configuration:

.. code-block:: ini

    [secrets]
    backend = airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend
    backend_kwargs = {"connections_prefix": "airflow/connections", "variables_prefix": "airflow/variables", "profile_name": "default"}

To authenticate you can either supply a profile name to reference aws profile, e.g. defined in ``~/.aws/config`` or set
environment variables like ``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY``.

Optional lookup
"""""""""""""""

Optionally connections, variables, or config may be looked up exclusive of each other or in any combination.
This will prevent requests being sent to AWS Secrets Manager for the excluded type.

If you want to look up some and not others in AWS Secrets Manager you may do so by setting the relevant ``*_prefix`` parameter of the ones to be excluded as ``null``.

For example, if you want to set parameter ``connections_prefix`` to ``"airflow/connections"`` and not look up variables, your configuration file should look like this:

.. code-block:: ini

    [secrets]
    backend = airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend
    backend_kwargs = {"connections_prefix": "airflow/connections", "variables_prefix": null, "profile_name": "default"}

Storing and Retrieving Connections
""""""""""""""""""""""""""""""""""

If you have set ``connections_prefix`` as ``airflow/connections``, then for a connection id of ``smtp_default``,
you would want to store your connection at ``airflow/connections/smtp_default``.

Example:

.. code-block:: bash

    aws secretsmanager put-secret-value \
        --secret-id airflow/connections/smtp_default \
        --secret-string "smtps://user:host@relay.example.com:465"

Verify that you can get the secret:

.. code-block:: console

    ❯ aws secretsmanager get-secret-value --secret-id airflow/connections/smtp_default
    {
        "ARN": "arn:aws:secretsmanager:us-east-2:314524341751:secret:airflow/connections/smtp_default-7meuul",
        "Name": "airflow/connections/smtp_default",
        "VersionId": "34f90eff-ea21-455a-9c8f-5ee74b21be672",
        "SecretString": "smtps://user:host@relay.example.com:465",
        "VersionStages": [
            "AWSCURRENT"
        ],
        "CreatedDate": "2020-04-08T02:10:35.132000+01:00"
    }

The value of the secret must be the :ref:`connection URI representation <generating_connection_uri>`
of the connection object.

Storing and Retrieving Variables
""""""""""""""""""""""""""""""""

If you have set ``variables_prefix`` as ``airflow/variables``, then for an Variable key of ``hello``,
you would want to store your Variable at ``airflow/variables/hello``.
