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



.. _howto/connection:gcp:

Google Cloud Connection
================================

The Google Cloud connection type enables the Google Cloud Integrations.

Authenticating to Google Cloud
------------------------------

There are three ways to connect to Google Cloud using Airflow.

1. Use `Application Default Credentials
   <https://google-auth.readthedocs.io/en/latest/reference/google.auth.html#google.auth.default>`_,
2. Use a `service account
   <https://cloud.google.com/docs/authentication/#service_accounts>`_ key
   file (JSON format) on disk - ``Keyfile Path``.
3. Use a service account key file (JSON format) from connection configuration - ``Keyfile JSON``.

Only one authorization method can be used at a time. If you need to manage multiple keys then you should
configure multiple connections.

Default Connection IDs
----------------------

All hooks and operators related to Google Cloud use ``google_cloud_default`` by default.


Note On Application Default Credentials
---------------------------------------
Application Default Credentials are inferred by the GCE metadata server when running
Airflow on Google Compute Engine or the GKE metadata server
when running on GKE which allows mapping Kubernetes Service Accounts to GCP service accounts
`Workload Identity
<https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity>`_.
This can be useful when managing minimum permissions for multiple Airflow instances on a single GKE cluster which
each have a different IAM footprint. Simply assign KSAs for your worker / webserver deployments and workload identity
will map them to separate GCP Service Accounts (rather than sharing a cluster-level GCE service account).
From a security perspective it has the benefit of not storing Google Service Account
keys  on disk nor in the Airflow database, making it impossible
to leak the sensitive long lived credential key material.

From an Airflow perspective Application Default Credentials can be used for
a connection by specifying an empty URI.

For example:

.. code-block:: bash

   export AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT='google-cloud-platform://'

Configuring the Connection
--------------------------

Project Id (optional)
    The Google Cloud project ID to connect to. It is used as default project id by operators using it and
    can usually be overridden at the operator level.

Keyfile Path
    Path to a `service account
    <https://cloud.google.com/docs/authentication/#service_accounts>`_ key
    file (JSON format) on disk.

    Not required if using application default credentials.

Keyfile JSON
    Contents of a `service account
    <https://cloud.google.com/docs/authentication/#service_accounts>`_ key
    file (JSON format) on disk.

    Not required if using application default credentials.

Scopes (comma separated)
    A list of comma-separated `Google Cloud scopes
    <https://developers.google.com/identity/protocols/googlescopes>`_ to
    authenticate with.

Number of Retries
    Integer, number of times to retry with randomized
    exponential backoff. If all retries fail, the :class:`googleapiclient.errors.HttpError`
    represents the last request. If zero (default), we attempt the
    request only once.

    When specifying the connection in environment variable you should specify
    it using URI syntax, with the following requirements:

      * scheme part should be equals ``google-cloud-platform`` (Note: look for a
        hyphen character)
      * authority (username, password, host, port), path is ignored
      * query parameters contains information specific to this type of
        connection. The following keys are accepted:

        * ``extra__google_cloud_platform__project`` - Project Id
        * ``extra__google_cloud_platform__key_path`` - Keyfile Path
        * ``extra__google_cloud_platform__keyfile_dict`` - Keyfile JSON
        * ``extra__google_cloud_platform__scope`` - Scopes
        * ``extra__google_cloud_platform__num_retries`` - Number of Retries

    Note that all components of the URI should be URL-encoded.

    For example:

    .. code-block:: bash

       export AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT='google-cloud-platform://?extra__google_cloud_platform__key_path=%2Fkeys%2Fkey.json&extra__google_cloud_platform__scope=https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fcloud-platform&extra__google_cloud_platform__project=airflow&extra__google_cloud_platform__num_retries=5'

.. _howto/connection:gcp:impersonation:

Direct impersonation of a service account
-----------------------------------------

Google operators support `direct impersonation of a service account
<https://cloud.google.com/iam/docs/understanding-service-accounts#directly_impersonating_a_service_account>`_
via ``impersonation_chain`` argument (``google_impersonation_chain`` in case of operators
that also communicate with services of other cloud providers).

For example:

.. code-block:: python

        import os

        from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator

        IMPERSONATION_CHAIN = "impersonated_account@your_project_id.iam.gserviceaccount.com"

        create_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id="create-dataset",
            gcp_conn_id="google_cloud_default",
            dataset_id="test_dataset",
            location="southamerica-east1",
            impersonation_chain=IMPERSONATION_CHAIN,
        )

In order for this example to work, the account ``impersonated_account`` must grant the
``Service Account Token Creator`` IAM role to the service account specified in the
``google_cloud_default`` Connection. This will allow to generate ``impersonated_account``'s
access token, which will allow to act on its behalf using its permissions. ``impersonated_account``
does not even need to have a generated key.

.. warning::
  :class:`~airflow.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperator`,
  :class:`~airflow.providers.google.cloud.operators.dataflow.DataflowCreateJavaJobOperator` and
  :class:`~airflow.providers.google.cloud.operators.dataflow.DataflowCreatePythonJobOperator`
  do not support direct impersonation as of now.

In case of operators that connect to multiple Google services, all hooks use the same value of
``impersonation_chain`` (if applicable). You can also impersonate accounts from projects
other than the project of the originating account. In that case, the project id of the impersonated
account will be used as the default project id in operator's logic, unless you have explicitly
specified the Project Id in Connection's configuration or in operator's arguments.

Impersonation can also be used in chain: if the service account specified in Connection has
``Service Account Token Creator`` role granted on account A, and account A has this role on account
B, then we are able to impersonate account B.

For example, with the following ``terraform`` setup...

.. code-block:: terraform

        terraform {
          required_version = "> 0.11.14"
        }
        provider "google" {
        }
        variable "project_id" {
          type = "string"
        }
        resource "google_service_account" "sa_1" {
          account_id   = "impersonation-chain-1"
          project = "${var.project_id}"
        }
        resource "google_service_account" "sa_2" {
          account_id   = "impersonation-chain-2"
          project = "${var.project_id}"
        }
        resource "google_service_account" "sa_3" {
          account_id   = "impersonation-chain-3"
          project = "${var.project_id}"
        }
        resource "google_service_account" "sa_4" {
          account_id   = "impersonation-chain-4"
          project = "${var.project_id}"
        }
        resource "google_service_account_iam_member" "sa_4_member" {
          service_account_id = "${google_service_account.sa_4.name}"
          role               = "roles/iam.serviceAccountTokenCreator"
          member             = "serviceAccount:${google_service_account.sa_3.email}"
        }
        resource "google_service_account_iam_member" "sa_3_member" {
          service_account_id = "${google_service_account.sa_3.name}"
          role               = "roles/iam.serviceAccountTokenCreator"
          member             = "serviceAccount:${google_service_account.sa_2.email}"
        }
        resource "google_service_account_iam_member" "sa_2_member" {
          service_account_id = "${google_service_account.sa_2.name}"
          role               = "roles/iam.serviceAccountTokenCreator"
          member             = "serviceAccount:${google_service_account.sa_1.email}"
        }

...we should configure Airflow Connection to use ``impersonation-chain-1`` account's key and provide
following value for ``impersonation_chain`` argument...

.. code-block:: python

        PROJECT_ID = os.environ.get("TF_VAR_project_id", "your_project_id")
        IMPERSONATION_CHAIN = [
            f"impersonation-chain-2@{PROJECT_ID}.iam.gserviceaccount.com",
            f"impersonation-chain-3@{PROJECT_ID}.iam.gserviceaccount.com",
            f"impersonation-chain-4@{PROJECT_ID}.iam.gserviceaccount.com",
        ]

...then requests will be executed using ``impersonation-chain-4`` account's privileges.
