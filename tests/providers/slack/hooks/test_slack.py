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

import unittest
from unittest import mock

import pytest
from slack_sdk.errors import SlackApiError

from airflow.exceptions import AirflowException
from airflow.providers.slack.hooks.slack import SlackHook


class TestSlackHook(unittest.TestCase):
    def test_get_token_with_token_only(self):
        """tests `__get_token` method when only token is provided """
        # Given
        test_token = 'test_token'
        test_conn_id = None

        # Run
        hook = SlackHook(test_token, test_conn_id)

        # Assert
        output = hook.token
        expected = test_token
        assert output == expected

    @mock.patch('airflow.providers.slack.hooks.slack.WebClient')
    @mock.patch('airflow.providers.slack.hooks.slack.SlackHook.get_connection')
    def test_get_token_with_valid_slack_conn_id_only(self, get_connection_mock, mock_slack_client):
        """tests `__get_token` method when only connection is provided """
        # Given
        test_token = None
        test_conn_id = 'x'
        test_password = 'test_password'

        # Mock
        get_connection_mock.return_value = mock.Mock(password=test_password)

        # Run
        hook = SlackHook(test_token, test_conn_id)

        # Assert
        output = hook.token
        expected = test_password
        assert output == expected
        mock_slack_client.assert_called_once_with(test_password)

    @mock.patch('airflow.providers.slack.hooks.slack.SlackHook.get_connection')
    def test_get_token_with_no_password_slack_conn_id_only(self, get_connection_mock):
        """tests `__get_token` method when only connection is provided """

        # Mock
        conn = mock.Mock()
        del conn.password
        get_connection_mock.return_value = conn

        # Assert
        with pytest.raises(AirflowException):
            SlackHook(token=None, slack_conn_id='x')

    @mock.patch('airflow.providers.slack.hooks.slack.SlackHook.get_connection')
    def test_get_token_with_empty_password_slack_conn_id_only(self, get_connection_mock):
        """tests `__get_token` method when only connection is provided """

        # Mock
        get_connection_mock.return_value = mock.Mock(password=None)

        # Assert
        with pytest.raises(AirflowException):
            SlackHook(token=None, slack_conn_id='x')

    def test_get_token_with_token_and_slack_conn_id(self):
        """tests `__get_token` method when both arguments are provided """
        # Given
        test_token = 'test_token'
        test_conn_id = 'x'

        # Run
        hook = SlackHook(test_token, test_conn_id)

        # Assert
        output = hook.token
        expected = test_token
        assert output == expected

    def test_get_token_with_out_token_nor_slack_conn_id(self):
        """tests `__get_token` method when no arguments are provided """

        with pytest.raises(AirflowException):
            SlackHook(token=None, slack_conn_id=None)

    @mock.patch('airflow.providers.slack.hooks.slack.WebClient')
    def test_call_with_failure(self, slack_client_class_mock):
        slack_client_mock = mock.Mock()
        slack_client_class_mock.return_value = slack_client_mock
        expected_exception = SlackApiError(message='foo', response='bar')
        slack_client_mock.api_call = mock.Mock(side_effect=expected_exception)

        test_token = 'test_token'
        test_slack_conn_id = 'test_slack_conn_id'
        slack_hook = SlackHook(token=test_token, slack_conn_id=test_slack_conn_id)
        test_method = 'test_method'
        test_api_params = {'key1': 'value1', 'key2': 'value2'}

        with pytest.raises(SlackApiError):
            slack_hook.call(test_method, data=test_api_params)

    @mock.patch('airflow.providers.slack.hooks.slack.WebClient.api_call', autospec=True)
    @mock.patch('airflow.providers.slack.hooks.slack.WebClient')
    def test_api_call(self, mock_slack_client, mock_slack_api_call):
        slack_hook = SlackHook(token='test_token')
        test_api_json = {'channel': 'test_channel'}

        slack_hook.call("chat.postMessage", json=test_api_json)
        mock_slack_api_call.assert_called_once_with(mock_slack_client, "chat.postMessage", json=test_api_json)
