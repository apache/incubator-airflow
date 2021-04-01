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
#

import base64
import json
import unittest

from airflow.providers.amazon.aws.hooks.secrets_manager import SecretsManagerHook

try:
    from moto import mock_secretsmanager
except ImportError:
    mock_secretsmanager = None


class TestSecretsManagerHook(unittest.TestCase):
    @unittest.skipIf(mock_secretsmanager is None, 'mock_secretsmanager package not present')
    @mock_secretsmanager
    def test_get_conn_returns_a_boto3_connection(self):
        hook = SecretsManagerHook(aws_conn_id='aws_default')
        assert hook.get_conn() is not None

    @unittest.skipIf(mock_secretsmanager is None, 'mock_secretsmanager package not present')
    @mock_secretsmanager
    def test_get_secret_string(self):
        secret_name = "arn:aws:secretsmanager:us-east-2:999999999999:secret:db_cluster-YYYYYYY"
        secret_value = '{"user": "test"}'
        hook = SecretsManagerHook(aws_conn_id='aws_default')

        create_param = {
            'Name': secret_name,
        }

        put_param = {
            'SecretId': secret_name,
            'SecretString': secret_value,
        }

        hook.get_conn().create_secret(**create_param)
        hook.get_conn().put_secret_value(**put_param)

        secret = hook.get_secret(secret_name)
        assert secret == secret_value

    @unittest.skipIf(mock_secretsmanager is None, 'mock_secretsmanager package not present')
    @mock_secretsmanager
    def test_get_secret_dict(self):
        secret_name = "arn:aws:secretsmanager:us-east-2:999999999999:secret:db_cluster-YYYYYYY"
        secret_value = '{"user": "test"}'
        hook = SecretsManagerHook(aws_conn_id='aws_default')

        create_param = {
            'Name': secret_name,
        }

        put_param = {
            'SecretId': secret_name,
            'SecretString': secret_value,
        }

        hook.get_conn().create_secret(**create_param)
        hook.get_conn().put_secret_value(**put_param)

        secret = hook.get_secret_as_dict(secret_name)
        assert secret == json.loads(secret_value)

    @unittest.skipIf(mock_secretsmanager is None, 'mock_secretsmanager package not present')
    @mock_secretsmanager
    def test_get_secret_binary(self):
        secret_name = "arn:aws:secretsmanager:us-east-2:999999999999:secret:db_cluster-YYYYYYY"
        secret_value_binary = base64.b64encode(b'{"username": "test"}')
        hook = SecretsManagerHook(aws_conn_id='aws_default')
        create_param = {
            'Name': secret_name,
        }

        put_param = {
            'SecretId': secret_name,
            'SecretBinary': secret_value_binary,
        }

        hook.get_conn().create_secret(**create_param)
        hook.get_conn().put_secret_value(**put_param)

        secret = hook.get_secret(secret_name)
        assert secret == base64.b64decode(secret_value_binary)
