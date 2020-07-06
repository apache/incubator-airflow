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



Defining Additional Execute Context Manager
===========================================

Creating new context manager
----------------------------

Users can create their own execution context manager to allow context management on a higher level.
To do so, one must define a new context manager in one of their files. This context manager is entered
before calling ``execute`` method and is exited shortly after it. Here is an example context manager
which provides authentication to Google Cloud Platform:

    .. code-block:: python

      import os
      import subprocess
      from contextlib import contextmanager
      from tempfile import TemporaryDirectory
      from unittest import mock
      from google.auth.environment_vars import CLOUD_SDK_CONFIG_DIR
      from airflow.providers.google.cloud.utils.credentials_provider import provide_gcp_conn_and_credentials

      def execute_cmd(cmd):
          with open(os.devnull, 'w') as dev_null:
            return subprocess.call(args=cmd, stdout=dev_null, stderr=subprocess.STDOUT)

      @contextmanager
      def provide_gcp_context(task_instance, execution_context):
          """
          Context manager that provides:

          - GCP credentials for application supporting `Application Default Credentials (ADC)
          strategy <https://cloud.google.com/docs/authentication/production>`__.
          - temporary value of ``AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT`` connection
          - the ``gcloud`` config directory isolated from user configuration
          """
          project_id = os.environ["GCP_PROJECT_ID"]
          key_file_path = os.environ["GCP_DEFAULT_SERVICE_KEY"]
          with provide_gcp_conn_and_credentials(key_file_path, project_id=project_id), \
                TemporaryDirectory() as gcloud_config_tmp, \
                mock.patch.dict('os.environ', {CLOUD_SDK_CONFIG_DIR: gcloud_config_tmp}):

            execute_cmd(["gcloud", "config", "set", "core/project", project_id])
            execute_cmd(["gcloud", "auth", "activate-service-account", f"--key-file={key_file_path}"])
            yield
            execute_cmd(["gcloud", "config", "set", "account", "none", f"--project={project_id}"])

Your custom context manager has to accept two arguments:
1. ``task_instance`` - the executing task instance object (can also be retrieved from execution context via ``"ti"`` key.
2. ``execution_context`` - the execution context that is provided to an operator's ``execute`` function.
