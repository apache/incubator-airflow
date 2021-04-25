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

from unittest import TestCase, mock

from moto import mock_secretsmanager

from airflow.providers.amazon.aws.secrets.secrets_manager import SecretsManagerBackend


class TestSecretsManagerBackend(TestCase):
    @mock.patch("airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend.get_conn_uri")
    def test_aws_secrets_manager_get_connections(self, mock_get_uri):
        mock_get_uri.return_value = "scheme://user:pass@host:100"
        conn_list = SecretsManagerBackend().get_connections("fake_conn")
        conn = conn_list[0]
        assert conn.host == 'host'

    @mock_secretsmanager
    def test_get_conn_uri(self):

        secret_id = 'airflow/connections/test_postgres'
        create_param = {
            'Name': secret_id,
        }

        param = {
            'SecretId': secret_id,
            'SecretString': 'postgresql://airflow:airflow@host:5432/airflow',
        }

        secrets_manager_backend = SecretsManagerBackend()
        secrets_manager_backend.client.create_secret(**create_param)
        secrets_manager_backend.client.put_secret_value(**param)

        returned_uri = secrets_manager_backend.get_conn_uri(conn_id="test_postgres")
        assert 'postgresql://airflow:airflow@host:5432/airflow' == returned_uri

    @mock_secretsmanager
    def test_get_conn_uri_non_existent_key(self):
        """
        Test that if the key with connection ID is not present,
        SecretsManagerBackend.get_connections should return None
        """
        conn_id = "test_mysql"

        secret_id = 'airflow/connections/test_postgres'
        create_param = {
            'Name': secret_id,
        }

        param = {
            'SecretId': secret_id,
            'SecretString': 'postgresql://airflow:airflow@host:5432/airflow',
        }

        secrets_manager_backend = SecretsManagerBackend()
        secrets_manager_backend.client.create_secret(**create_param)
        secrets_manager_backend.client.put_secret_value(**param)

        assert secrets_manager_backend.get_conn_uri(conn_id=conn_id) is None
        assert [] == secrets_manager_backend.get_connections(conn_id=conn_id)

    @mock_secretsmanager
    def test_get_variable(self):

        secret_id = 'airflow/variables/hello'
        create_param = {
            'Name': secret_id,
        }

        param = {'SecretId': secret_id, 'SecretString': 'world'}

        secrets_manager_backend = SecretsManagerBackend()
        secrets_manager_backend.client.create_secret(**create_param)
        secrets_manager_backend.client.put_secret_value(**param)

        returned_uri = secrets_manager_backend.get_variable('hello')
        assert 'world' == returned_uri

    @mock_secretsmanager
    def test_get_variable_non_existent_key(self):
        """
        Test that if Variable key is not present,
        SystemsManagerParameterStoreBackend.get_variables should return None
        """
        secret_id = 'airflow/variables/hello'
        create_param = {
            'Name': secret_id,
        }
        param = {'SecretId': secret_id, 'SecretString': 'world'}

        secrets_manager_backend = SecretsManagerBackend()
        secrets_manager_backend.client.create_secret(**create_param)
        secrets_manager_backend.client.put_secret_value(**param)

        assert secrets_manager_backend.get_variable("test_mysql") is None

    @mock.patch("airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend._get_secret")
    def test_connection_prefix_none_value(self, mock_get_secret):
        """
        Test that if Variable key is not present in AWS Secrets Manager,
        SecretsManagerBackend.get_conn_uri should return None,
        SecretsManagerBackend._get_secret should not be called
        """
        kwargs = {'connections_prefix': None}

        secrets_manager_backend = SecretsManagerBackend(**kwargs)

        assert secrets_manager_backend.get_conn_uri("test_mysql") is None
        mock_get_secret.assert_not_called()

    @mock.patch("airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend._get_secret")
    def test_variable_prefix_none_value(self, mock_get_secret):
        """
        Test that if Variable key is not present in AWS Secrets Manager,
        SecretsManagerBackend.get_variables should return None,
        SecretsManagerBackend._get_secret should not be called
        """
        kwargs = {'variables_prefix': None}

        secrets_manager_backend = SecretsManagerBackend(**kwargs)

        assert secrets_manager_backend.get_variable("hello") is None
        mock_get_secret.assert_not_called()

    @mock.patch("airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend._get_secret")
    def test_config_prefix_none_value(self, mock_get_secret):
        """
        Test that if Variable key is not present in AWS Secrets Manager,
        SecretsManagerBackend.get_config should return None,
        SecretsManagerBackend._get_secret should not be called
        """
        kwargs = {'config_prefix': None}

        secrets_manager_backend = SecretsManagerBackend(**kwargs)

        assert secrets_manager_backend.get_config("config") is None
        mock_get_secret.assert_not_called()
