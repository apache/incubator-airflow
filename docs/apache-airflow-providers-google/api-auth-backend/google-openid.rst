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

Google OpenID authentication
''''''''''''''''''''''''''''

You can also configure
`Google OpenID <https://developers.google.com/identity/protocols/oauth2/openid-connect>`__
for authentication. To enable it, set the following option in the configuration:

.. code-block:: ini

    [api]
    auth_backend = airflow.providers.google.common.auth_backend.google_openid

It is also highly recommended to configure an OAuth2 audience so that the generated tokens are restricted to
use by Airflow only.

.. code-block:: ini

    [api]
    google_oauth2_audience = project-id-random-value.apps.googleusercontent.com

You can also configure the CLI to send request to a remote API instead of making a query to a local database.

.. code-block:: ini

    [cli]
    api_client = airflow.api.client.json_client
    endpoint_url = http://remote-host.example.org/

You can also set up a service account key. If omitted, authorization based on `the Application Default
Credentials <https://cloud.google.com/docs/authentication/production#finding_credentials_automatically>`__
will be used.

.. code-block:: ini

    [cli]
    google_key_path = <KEY_PATH>

You can get the authorization token with the ``gcloud auth print-identity-token`` command. An example request
look like the following.

  .. code-block:: bash

      ENDPOINT_URL="http://localhost:8080/"

      AUDIENCE="project-id-random-value.apps.googleusercontent.com"
      ID_TOKEN="$(gcloud auth print-identity-token "--audience=${AUDIENCE}")"

      curl -X GET  \
          "${ENDPOINT_URL}/api/experimental/pools" \
          -H 'Content-Type: application/json' \
          -H 'Cache-Control: no-cache' \
          -H "Authorization: Bearer ${ID_TOKEN}"
