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

import unittest
from unittest import mock

from airflow.models import DAG
from airflow.providers.snowflake.transfers.snowflake_to_slack import SnowflakeToSlackOperator
from airflow.utils import timezone

TEST_DAG_ID = 'snowflake_to_slack_unit_test'
DEFAULT_DATE = timezone.datetime(2017, 1, 1)


class TestSnowflakeToSlackOperator(unittest.TestCase):
    def setUp(self):
        self.example_dag = DAG('unit_test_dag_snowflake_to_slack', start_date=DEFAULT_DATE)

    @staticmethod
    def _construct_operator(**kwargs):
        operator = SnowflakeToSlackOperator(task_id=TEST_DAG_ID, **kwargs)
        return operator

    @mock.patch('airflow.providers.snowflake.transfers.snowflake_to_slack.SnowflakeHook')
    @mock.patch('airflow.providers.snowflake.transfers.snowflake_to_slack.SlackWebhookHook')
    def test_hooks_and_rendering(self, mock_slack_hook_class, mock_snowflake_hook_class):
        operator_args = {
            'snowflake_conn_id': 'snowflake_connection',
            'slack_conn_id': 'slack_connection',
            'sql': "sql {{ ds }}",
            'results_df_name': 'xxxx',
            'warehouse': 'test_warehouse',
            'database': 'test_database',
            'role': 'test_role',
            'schema': 'test_schema',
            'parameters': ['1', '2', '3'],
            'slack_message': 'message: {{ ds }}, {{ xxxx }}',
            'slack_token': 'test_token',
            'dag': self.example_dag,
        }
        snowflake_to_slack_operator = self._construct_operator(**operator_args)

        snowflake_hook = mock_snowflake_hook_class.return_value
        snowflake_hook.get_pandas_df.return_value = '1234'
        slack_webhook_hook = mock_slack_hook_class.return_value

        snowflake_to_slack_operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        # Test that the Snowflake hook is instantiated with the right parameters
        mock_snowflake_hook_class.assert_called_once_with(
            database='test_database',
            role='test_role',
            schema='test_schema',
            snowflake_conn_id='snowflake_connection',
            warehouse='test_warehouse',
        )

        # Test that the get_pandas_df method is executed on the Snowflake hook with the pre-rendered sql and
        # correct params
        snowflake_hook.get_pandas_df.assert_called_once_with('sql 2017-01-01', parameters=['1', '2', '3'])

        # Test that the Slack hook is instantiated with the right parameters
        mock_slack_hook_class.assert_called_once_with(
            http_conn_id='slack_connection', message='message: 2017-01-01, 1234', webhook_token='test_token'
        )

        # Test that the Slack hook's execute method gets run once
        slack_webhook_hook.execute.assert_called_once()
