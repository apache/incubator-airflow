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
"""Objects relating to sourcing connections & variables from Hashicorp Vault"""
from typing import Optional

from airflow.models.connection import Connection
from airflow.providers.hashicorp._internal_client.vault_client import _VaultClient  # noqa
from airflow.secrets import BaseSecretsBackend
from airflow.secrets.local_filesystem import _create_connection
from airflow.utils.log.logging_mixin import LoggingMixin


# pylint: disable=too-many-instance-attributes,too-many-locals
class VaultBackend(BaseSecretsBackend, LoggingMixin):
    """
    Retrieves Connections and Variables from Hashicorp Vault.

    Configurable via ``airflow.cfg`` as follows:

    .. code-block:: ini

        [secrets]
        backend = airflow.providers.hashicorp.secrets.vault.VaultBackend
        backend_kwargs = {
            "connections_path": "connections",
            "url": "http://127.0.0.1:8200",
            "mount_point": "airflow"
            }

    For example, if your keys are under ``connections`` path in ``airflow`` mount_point, this
    would be accessible if you provide ``{"connections_path": "connections"}`` and request
    conn_id ``smtp_default``.

    :param connections_path: Specifies the path of the secret to read to get Connections.
        (default: 'connections'). If set to None (null), requests for connections will not be sent to Vault.
    :type connections_path: str
    :param variables_path: Specifies the path of the secret to read to get Variable.
        (default: 'variables'). If set to None (null), requests for variables will not be sent to Vault.
    :type variables_path: str
    :param config_path: Specifies the path of the secret to read Airflow Configurations
        (default: 'config'). If set to None (null), requests for configurations will not be sent to Vault.
    :type config_path: str
    :param url: Base URL for the Vault instance being addressed.
    :type url: str
    :param auth_type: Authentication Type for Vault. Default is ``token``. Available values are:
        ('approle', 'aws_iam', 'azure', 'github', 'gcp', 'kubernetes', 'ldap', 'radius', 'token', 'userpass')
    :type auth_type: str
    :param auth_mount_point: It can be used to define mount_point for authentication chosen
          Default depends on the authentication method used.
    :type auth_mount_point: str
    :param mount_point: The "path" the secret engine was mounted on. Default is "secret". Note that
         this mount_point is not used for authentication if authentication is done via a
         different engine. For authentication mount_points see, auth_mount_point.
    :type mount_point: str
    :param kv_engine_version: Select the version of the engine to run (``1`` or ``2``, default: ``2``).
    :type kv_engine_version: int
    :param token: Authentication token to include in requests sent to Vault.
        (for ``token`` and ``github`` auth_type)
    :type token: str
    :param token_path: path to file containing authentication token to include in requests sent to Vault
        (for ``token`` and ``github`` auth_type).
    :type token_path: str
    :param username: Username for Authentication (for ``ldap`` and ``userpass`` auth_type).
    :type username: str
    :param password: Password for Authentication (for ``ldap`` and ``userpass`` auth_type).
    :type password: str
    :param key_id: Key ID for Authentication (for ``aws_iam`` and ''azure`` auth_type).
    :type key_id: str
    :param secret_id: Secret ID for Authentication (for ``approle``, ``aws_iam`` and ``azure`` auth_types).
    :type secret_id: str
    :param role_id: Role ID for Authentication (for ``approle``, ``aws_iam`` auth_types).
    :type role_id: str
    :param kubernetes_role: Role for Authentication (for ``kubernetes`` auth_type).
    :type kubernetes_role: str
    :param kubernetes_jwt_path: Path for kubernetes jwt token (for ``kubernetes`` auth_type, default:
        ``/var/run/secrets/kubernetes.io/serviceaccount/token``).
    :type kubernetes_jwt_path: str
    :param gcp_key_path: Path to Google Cloud Service Account key file (JSON) (for ``gcp`` auth_type).
           Mutually exclusive with gcp_keyfile_dict.
    :type gcp_key_path: str
    :param gcp_keyfile_dict: Dictionary of keyfile parameters. (for ``gcp`` auth_type).
           Mutually exclusive with gcp_key_path.
    :type gcp_keyfile_dict: dict
    :param gcp_scopes: Comma-separated string containing OAuth2 scopes (for ``gcp`` auth_type).
    :type gcp_scopes: str
    :param azure_tenant_id: The tenant id for the Azure Active Directory (for ``azure`` auth_type).
    :type azure_tenant_id: str
    :param azure_resource: The configured URL for the application registered in Azure Active Directory
           (for ``azure`` auth_type).
    :type azure_resource: str
    :param radius_host: Host for radius (for ``radius`` auth_type).
    :type radius_host: str
    :param radius_secret: Secret for radius (for ``radius`` auth_type).
    :type radius_secret: str
    :param radius_port: Port for radius (for ``radius`` auth_type).
    :type radius_port: str
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        connections_path: str = 'connections',
        variables_path: str = 'variables',
        config_path: str = 'config',
        url: Optional[str] = None,
        auth_type: str = 'token',
        auth_mount_point: Optional[str] = None,
        mount_point: str = 'secret',
        kv_engine_version: int = 2,
        token: Optional[str] = None,
        token_path: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        key_id: Optional[str] = None,
        secret_id: Optional[str] = None,
        role_id: Optional[str] = None,
        kubernetes_role: Optional[str] = None,
        kubernetes_jwt_path: str = '/var/run/secrets/kubernetes.io/serviceaccount/token',
        gcp_key_path: Optional[str] = None,
        gcp_keyfile_dict: Optional[dict] = None,
        gcp_scopes: Optional[str] = None,
        azure_tenant_id: Optional[str] = None,
        azure_resource: Optional[str] = None,
        radius_host: Optional[str] = None,
        radius_secret: Optional[str] = None,
        radius_port: Optional[int] = None,
        **kwargs,
    ):
        super().__init__()
        if connections_path is not None:
            self.connections_path = connections_path.rstrip('/')
        else:
            self.connections_path = connections_path
        if variables_path is not None:
            self.variables_path = variables_path.rstrip('/')
        else:
            self.variables_path = variables_path
        if config_path is not None:
            self.config_path = config_path.rstrip('/')
        else:
            self.config_path = config_path
        self.mount_point = mount_point
        self.kv_engine_version = kv_engine_version
        self.vault_client = _VaultClient(
            url=url,
            auth_type=auth_type,
            auth_mount_point=auth_mount_point,
            mount_point=mount_point,
            kv_engine_version=kv_engine_version,
            token=token,
            token_path=token_path,
            username=username,
            password=password,
            key_id=key_id,
            secret_id=secret_id,
            role_id=role_id,
            kubernetes_role=kubernetes_role,
            kubernetes_jwt_path=kubernetes_jwt_path,
            gcp_key_path=gcp_key_path,
            gcp_keyfile_dict=gcp_keyfile_dict,
            gcp_scopes=gcp_scopes,
            azure_tenant_id=azure_tenant_id,
            azure_resource=azure_resource,
            radius_host=radius_host,
            radius_secret=radius_secret,
            radius_port=radius_port,
            **kwargs,
        )

    def get_response(self, conn_id: str) -> Optional[dict]:
        """
        Get data from Vault

        :type conn_id: str
        :rtype: dict
        :return: The data from the Vault path if exists
        """
        if self.connections_path is None:
            return None

        secret_path = self.build_path(self.connections_path, conn_id)
        return self.vault_client.get_secret(secret_path=secret_path)

    def get_conn_uri(self, conn_id: str) -> Optional[str]:
        """
        Get secret value from Vault. Store the secret in the form of URI

        :param conn_id: The connection id
        :type conn_id: str
        :rtype: str
        :return: The connection uri retrieved from the secret
        """
        response = self.get_response(conn_id)

        return response.get("conn_uri") if response else None

    def get_connection(self, conn_id: str) -> Optional[Connection]:
        """
        Get connection from Vault as secret. Prioritize conn_uri if exists,
        if not fall back to normal Connection creation.

        :type conn_id: str
        :rtype: Connection
        :return: A Connection object constructed from Vault data
        """
        response = self.get_response(conn_id)
        if response is None:
            return None

        uri = response.get("conn_uri")
        if uri:
            return uri

        return _create_connection(conn_id, response)

    def get_variable(self, key: str) -> Optional[str]:
        """
        Get Airflow Variable

        :param key: Variable Key
        :type key: str
        :rtype: str
        :return: Variable Value retrieved from the vault
        """
        if self.variables_path is None:
            return None
        else:
            secret_path = self.build_path(self.variables_path, key)
            response = self.vault_client.get_secret(secret_path=secret_path)
            return response.get("value") if response else None

    def get_config(self, key: str) -> Optional[str]:
        """
        Get Airflow Configuration

        :param key: Configuration Option Key
        :type key: str
        :rtype: str
        :return: Configuration Option Value retrieved from the vault
        """
        if self.config_path is None:
            return None
        else:
            secret_path = self.build_path(self.config_path, key)
            response = self.vault_client.get_secret(secret_path=secret_path)
            return response.get("value") if response else None
