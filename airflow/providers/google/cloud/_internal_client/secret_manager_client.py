# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import re
from typing import Optional

import google

try:
    from functools import cached_property
except ImportError:
    from cached_property import cached_property
from google.api_core.exceptions import NotFound, PermissionDenied
from google.api_core.gapic_v1.client_info import ClientInfo
from google.cloud.secretmanager_v1 import SecretManagerServiceClient

from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.version import version

SECRET_ID_PATTERN = r"^[a-zA-Z0-9-_]*$"


class _SecretManagerClient(LoggingMixin):
    """
    Retrieves Secrets object from Google Cloud Secrets Manager. This is a common class reused between
    SecretsManager and Secrets Hook that provides the shared authentication and verification mechanisms.
    This class should not be used directly, use SecretsManager or SecretsHook instead


    :param credentials: Credentials used to authenticate to GCP
    :type credentials: google.auth.credentials.Credentials
    """

    def __init__(
        self,
        credentials: google.auth.credentials.Credentials,
    ) -> None:
        super().__init__()
        self.credentials = credentials

    @staticmethod
    def is_valid_secret_name(secret_name: str) -> bool:
        """
        Returns true if the secret name is valid.
        :param secret_name: name of the secret
        :type secret_name: str
        :return:
        """
        return bool(re.match(SECRET_ID_PATTERN, secret_name))

    @cached_property
    def client(self) -> SecretManagerServiceClient:
        """Create an authenticated KMS client"""
        _client = SecretManagerServiceClient(
            credentials=self.credentials, client_info=ClientInfo(client_library_version='airflow_v' + version)
        )
        return _client

    def get_secret(self, secret_id: str, project_id: str, secret_version: str = 'latest') -> Optional[str]:
        """
        Get secret value from the Secret Manager.

        :param secret_id: Secret Key
        :type secret_id: str
        :param project_id: Project id to use
        :type project_id: str
        :param secret_version: version of the secret (default is 'latest')
        :type secret_version: str
        """
        name = self.client.secret_version_path(project_id, secret_id, secret_version)
        try:
            response = self.client.access_secret_version(name)
            value = response.payload.data.decode('UTF-8')
            return value
        except NotFound:
            self.log.error("Google Cloud API Call Error (NotFound): Secret ID %s not found.", secret_id)
            return None
        except PermissionDenied:
            self.log.error(
                """Google Cloud API Call Error (PermissionDenied): No access for Secret ID %s.
                Did you add 'secretmanager.versions.access' permission?""",
                secret_id,
            )
            return None
