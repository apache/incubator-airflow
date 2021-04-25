#
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
"""Objects relating to sourcing connections from AWS SSM Parameter Store"""
from typing import Optional

import boto3

try:
    from functools import cached_property
except ImportError:
    from cached_property import cached_property

from airflow.secrets import BaseSecretsBackend
from airflow.utils.log.logging_mixin import LoggingMixin


class SystemsManagerParameterStoreBackend(BaseSecretsBackend, LoggingMixin):
    """
    Retrieves Connection or Variables from AWS SSM Parameter Store

    Configurable via ``airflow.cfg`` like so:

    .. code-block:: ini

        [secrets]
        backend = airflow.providers.amazon.aws.secrets.systems_manager.SystemsManagerParameterStoreBackend
        backend_kwargs = {"connections_prefix": "/airflow/connections", "profile_name": null}

    For example, if ssm path is ``/airflow/connections/smtp_default``, this would be accessible
    if you provide ``{"connections_prefix": "/airflow/connections"}`` and request conn_id ``smtp_default``.
    And if ssm path is ``/airflow/variables/hello``, this would be accessible
    if you provide ``{"variables_prefix": "/airflow/variables"}`` and request conn_id ``hello``.

    :param connections_prefix: Specifies the prefix of the secret to read to get Connections.
        If set to None (null), requests for connections will not be sent to AWS SSM Parameter Store.
    :type connections_prefix: str
    :param variables_prefix: Specifies the prefix of the secret to read to get Variables.
        If set to None (null), requests for variables will not be sent to AWS SSM Parameter Store.
    :type variables_prefix: str
    :param config_prefix: Specifies the prefix of the secret to read to get Variables.
        If set to None (null), requests for configurations will not be sent to AWS SSM Parameter Store.
    :type config_prefix: str
    :param profile_name: The name of a profile to use. If not given, then the default profile is used.
    :type profile_name: str
    """

    def __init__(
        self,
        connections_prefix: str = '/airflow/connections',
        variables_prefix: str = '/airflow/variables',
        config_prefix: str = '/airflow/config',
        profile_name: Optional[str] = None,
        **kwargs,
    ):
        super().__init__()
        if connections_prefix is not None:
            self.connections_prefix = connections_prefix.rstrip("/")
        else:
            self.connections_prefix = connections_prefix
        if variables_prefix is not None:
            self.variables_prefix = variables_prefix.rstrip('/')
        else:
            self.variables_prefix = variables_prefix
        if config_prefix is not None:
            self.config_prefix = config_prefix.rstrip('/')
        else:
            self.config_prefix = config_prefix
        self.profile_name = profile_name
        self.kwargs = kwargs

    @cached_property
    def client(self):
        """Create a SSM client"""
        session = boto3.Session(profile_name=self.profile_name)
        return session.client("ssm", **self.kwargs)

    def get_conn_uri(self, conn_id: str) -> Optional[str]:
        """
        Get param value

        :param conn_id: connection id
        :type conn_id: str
        """
        if self.connections_prefix is None:
            return None

        return self._get_secret(self.connections_prefix, conn_id)

    def get_variable(self, key: str) -> Optional[str]:
        """
        Get Airflow Variable from Environment Variable

        :param key: Variable Key
        :return: Variable Value
        """
        if self.variables_prefix is None:
            return None

        return self._get_secret(self.variables_prefix, key)

    def get_config(self, key: str) -> Optional[str]:
        """
        Get Airflow Configuration

        :param key: Configuration Option Key
        :return: Configuration Option Value
        """
        if self.config_prefix is None:
            return None

        return self._get_secret(self.config_prefix, key)

    def _get_secret(self, path_prefix: str, secret_id: str) -> Optional[str]:
        """
        Get secret value from Parameter Store.

        :param path_prefix: Prefix for the Path to get Secret
        :type path_prefix: str
        :param secret_id: Secret Key
        :type secret_id: str
        """
        ssm_path = self.build_path(path_prefix, secret_id)
        try:
            response = self.client.get_parameter(Name=ssm_path, WithDecryption=True)
            value = response["Parameter"]["Value"]
            return value
        except self.client.exceptions.ParameterNotFound:
            self.log.info(
                "An error occurred (ParameterNotFound) when calling the GetParameter operation: "
                "Parameter %s not found.",
                ssm_path,
            )
            return None
