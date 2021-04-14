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

.. _google_cloud_secret_manager_backend:

Google Cloud Secret Manager Backend
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This topic describes how to configure Airflow to use `Secret Manager <https://cloud.google.com/secret-manager/docs>`__ as
a secret backend and how to manage secrets.

Before you begin
""""""""""""""""

Before you start, make sure you have performed the following tasks:

1.  Include sendgrid subpackage as part of your Airflow installation

    .. code-block:: bash

        pip install apache-airflow[google]

2. `Configure Secret Manager and your local environment <https://cloud.google.com/secret-manager/docs/configuring-secret-manager>`__, once per project.

Enabling the secret backend
"""""""""""""""""""""""""""

To enable the secret backend for Google Cloud Secrets Manager to retrieve connection/variables,
specify :py:class:`~airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend`
as the ``backend`` in  ``[secrets]`` section of ``airflow.cfg``.

Here is a sample configuration if you want to use it:

.. code-block:: ini

    [secrets]
    backend = airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend

You can also set this with environment variables.

.. code-block:: bash

    export AIRFLOW__SECRETS__BACKEND=airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend

You can verify the correct setting of the configuration options with the ``airflow config get-value`` command.

.. code-block:: console

    $ airflow config get-value secrets backend
    airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend

Backend parameters
""""""""""""""""""

The next step is to configure backend parameters using the ``backend_kwargs`` options. You can pass
the following parameters:

* ``connections_prefix``: Specifies the prefix of the secret to read to get Connections. Default: ``"airflow-connections"``
* ``variables_prefix``: Specifies the prefix of the secret to read to get Variables. Default: ``"airflow-variables"``
* ``gcp_key_path``: Path to Google Cloud Service Account Key file (JSON).
* ``gcp_keyfile_dict``: Dictionary of keyfile parameters.
* ``gcp_scopes``: Comma-separated string containing OAuth2 scopes.
* ``sep``: Separator used to concatenate connections_prefix and conn_id. Default: ``"-"``
* ``project_id``: Project ID to read the secrets from. If not passed, the project ID from credentials will be used.

All options should be passed as a JSON dictionary.

For example, if you want to set parameter ``connections_prefix`` to ``"example-connections-prefix"`` and parameter ``variables_prefix`` to ``"example-variables-prefix"``, your configuration file should look like this:

.. code-block:: ini

    [secrets]
    backend = airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend
    backend_kwargs = {"connections_prefix": "example-connections-prefix", "variables_prefix": "example-variables-prefix"}

Set-up credentials
""""""""""""""""""

You can configure the credentials in three ways:

* By default, Application Default Credentials (ADC) is used obtain credentials.
* ``gcp_key_path`` option in ``backend_kwargs`` option - allows you to configure authorizations with a service account stored in local file.
* ``gcp_keyfile_dict`` option in ``backend_kwargs`` option - allows you to configure authorizations with a service account stored in Airflow configuration.

.. note::

    For more information about the Application Default Credentials (ADC), see:

      * `google.auth.default <https://google-auth.readthedocs.io/en/latest/reference/google.auth.html#google.auth.default>`__
      * `Setting Up Authentication for Server to Server Production Applications <https://cloud.google.com/docs/authentication/production>`__

Managing secrets
""""""""""""""""

If you want to configure a connection, you need to save it as a :ref:`connection URI representation <generating_connection_uri>`.
Variables should be saved as plain text.

In order to manage secrets, you can use the ``gcloud`` tool or other supported tools. For more information, take a look at:
`Managing secrets <https://cloud.google.com/secret-manager/docs/creating-and-accessing-secrets>`__ in Google Cloud Documentation.

The name of the secret must fit the following formats:

 * for connections: ``[connections_prefix][sep][connection_name]``
 * for variables: ``[variables_prefix][sep][variable_name]``
 * for Airflow config: ``[config_prefix][sep][config_name]``

where:

 * ``connections_prefix`` - fixed value defined in the ``connections_prefix`` parameter in backend configuration. Default: ``airflow-connections``.
 * ``variables_prefix`` - fixed value defined in the ``variables_prefix`` parameter in backend configuration. Default: ``airflow-variables``.
 * ``config_prefix`` - fixed value defined in the ``config_prefix`` parameter in backend configuration. Default: ``airflow-config``.
 * ``sep`` - fixed value defined in the ``sep`` parameter in backend configuration. Default: ``-``.

The Cloud Secrets Manager secret name should follow the pattern ``^[a-zA-Z0-9-_]*$``.

If you have the default backend configuration and you want to create a connection with ``conn_id``
equals ``first-connection``, you should create secret named ``airflow-connections-first-connection``.
You can do it with the gcloud tools as in the example below.

.. code-block:: bash

    $ echo "mysql://example.org" | gcloud beta secrets create \
        airflow-connections-first-connection \
        --data-file=- \
        --replication-policy=automatic
    Created version [1] of the secret [airflow-variables-first-connection].

If you have the default backend configuration and you want to create a variable named ``first-variable``,
you should create a secret named ``airflow-variables-first-variable``. You can do it with the gcloud
command as in the example below.

.. code-block:: bash

    $ echo "secret_content" | gcloud beta secrets create \
        airflow-variables-first-variable \
        --data-file=-\
        --replication-policy=automatic
    Created version [1] of the secret [airflow-variables-first-variable].

Checking configuration
======================

You can use the ``airflow connections get`` command to check if the connection is correctly read from the backend secret:

.. code-block:: console

    $ airflow connections get first-connection
    Id: null
    Conn Id: first-connection
    Conn Type: mysql
    Host: example.org
    Schema: ''
    Login: null
    Password: null
    Port: null
    Is Encrypted: null
    Is Extra Encrypted: null
    Extra: {}
    URI: mysql://example.org

To check the variables is correctly read from the backend secret, you can use ``airflow variables get``:

.. code-block:: console

    $ airflow variables get first-variable
    secret_content

Clean up
========

To avoid incurring charges to your Google Cloud account for the resources used in this guide,
delete secrets by running ``gcloud beta secrets delete``:

.. code-block:: bash

    gcloud beta secrets delete airflow-connections-first-connection
    gcloud beta secrets delete airflow-variables-first-variable
