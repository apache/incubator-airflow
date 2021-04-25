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
from unittest.mock import patch

from airflow.providers.amazon.aws.hooks.lambda_function import AwsLambdaHook

try:
    from moto import mock_lambda
except ImportError:
    mock_lambda = None


class TestAwsLambdaHook:
    @mock_lambda
    def test_get_conn_returns_a_boto3_connection(self):
        hook = AwsLambdaHook(
            aws_conn_id='aws_default', function_name="test_function", region_name="us-east-1"
        )
        assert hook.conn is not None

    @mock_lambda
    def test_invoke_lambda_function(self):

        hook = AwsLambdaHook(
            aws_conn_id='aws_default', function_name="test_function", region_name="us-east-1"
        )

        with patch.object(hook.conn, 'invoke') as mock_invoke:
            payload = '{"hello": "airflow"}'
            hook.invoke_lambda(payload=payload)

        mock_invoke.asset_called_once_with(
            FunctionName="test_function",
            InvocationType="RequestResponse",
            LogType="None",
            Payload=payload,
            Qualifier="$LATEST",
        )
