# -*- coding: utf-8 -*-
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

from slack.errors import SlackApiError

from airflow.exceptions import AirflowException
from airflow.hooks.slack_hook import SlackHook
from tests.compat import mock


class SlackHookTestCase(unittest.TestCase):

    def test___get_token_with_token_only(self):
        """ tests `__get_token` method when only token is provided """
        # Given
        test_token = 'test_token'
        test_conn_id = None

        # Creating a dummy subclass is the easiest way to avoid running init
        #  which is actually using the method we are testing
        class DummySlackHook(SlackHook):
            def __init__(self, *args, **kwargs):
                pass

        # Run
        hook = DummySlackHook()

        # Assert
        output = hook._SlackHook__get_token(test_token, test_conn_id)  # pylint: disable=E1101
        expected = test_token
        self.assertEqual(output, expected)

    @mock.patch('airflow.hooks.slack_hook.SlackHook.get_connection')
    def test___get_token_with_valid_slack_conn_id_only(self, get_connection_mock):
        """ tests `__get_token` method when only connection is provided """
        # Given
        test_token = None
        test_conn_id = 'x'
        test_password = 'test_password'

        # Mock
        get_connection_mock.return_value = mock.Mock(password=test_password)

        # Creating a dummy subclass is the easiest way to avoid running init
        #  which is actually using the method we are testing
        class DummySlackHook(SlackHook):
            def __init__(self, *args, **kwargs):
                pass

        # Run
        hook = DummySlackHook()

        # Assert
        output = hook._SlackHook__get_token(test_token, test_conn_id)  # pylint: disable=E1101
        expected = test_password
        self.assertEqual(output, expected)

    @mock.patch('airflow.hooks.slack_hook.SlackHook.get_connection')
    def test___get_token_with_no_password_slack_conn_id_only(self, get_connection_mock):
        """ tests `__get_token` method when only connection is provided """

        # Mock
        conn = mock.Mock()
        del conn.password
        get_connection_mock.return_value = conn

        # Assert
        self.assertRaises(AirflowException, SlackHook, token=None, slack_conn_id='x')

    @mock.patch('airflow.hooks.slack_hook.SlackHook.get_connection')
    def test___get_token_with_empty_password_slack_conn_id_only(self, get_connection_mock):
        """ tests `__get_token` method when only connection is provided """

        # Mock
        get_connection_mock.return_value = mock.Mock(password=None)

        # Assert
        self.assertRaises(AirflowException, SlackHook, token=None, slack_conn_id='x')

    def test___get_token_with_token_and_slack_conn_id(self):
        """ tests `__get_token` method when both arguments are provided """
        # Given
        test_token = 'test_token'
        test_conn_id = 'x'

        # Creating a dummy subclass is the easiest way to avoid running init
        #  which is actually using the method we are testing
        class DummySlackHook(SlackHook):
            def __init__(self, *args, **kwargs):
                pass

        # Run
        hook = DummySlackHook()

        # Assert
        output = hook._SlackHook__get_token(test_token, test_conn_id)  # pylint: disable=E1101
        expected = test_token
        self.assertEqual(output, expected)

    def test___get_token_with_out_token_nor_slack_conn_id(self):
        """ tests `__get_token` method when no arguments are provided """

        self.assertRaises(AirflowException, SlackHook, token=None, slack_conn_id=None)

    @mock.patch('airflow.hooks.slack_hook.WebClient')
    def test_call_with_success(self, slack_client_class_mock):
        slack_client_mock = mock.Mock()
        slack_client_class_mock.return_value = slack_client_mock
        slack_response = mock.Mock()
        slack_client_mock.api_call.return_value = slack_response
        slack_response.validate.return_value = True

        test_token = 'test_token'
        test_slack_conn_id = 'test_slack_conn_id'
        slack_hook = SlackHook(token=test_token, slack_conn_id=test_slack_conn_id)
        test_method = 'test_method'
        test_api_params = {'key1': 'value1', 'key2': 'value2'}

        slack_hook.call(test_method, test_api_params)

        slack_client_class_mock.assert_called_with(test_token)
        slack_client_mock.api_call.assert_called_with(test_method, **test_api_params)
        self.assertEqual(slack_response.validate.call_count, 1)

    @mock.patch('airflow.hooks.slack_hook.WebClient')
    def test_call_with_failure(self, slack_client_class_mock):
        slack_client_mock = mock.Mock()
        slack_client_class_mock.return_value = slack_client_mock
        slack_response = mock.Mock()
        slack_client_mock.api_call.return_value = slack_response
        expected_exception = SlackApiError(message='foo', response='bar')
        slack_response.validate = mock.Mock(side_effect=expected_exception)

        test_token = 'test_token'
        test_slack_conn_id = 'test_slack_conn_id'
        slack_hook = SlackHook(token=test_token, slack_conn_id=test_slack_conn_id)
        test_method = 'test_method'
        test_api_params = {'key1': 'value1', 'key2': 'value2'}

        try:
            slack_hook.call(test_method, test_api_params)
            self.fail()
        except AirflowException as exc:
            self.assertIn("foo", str(exc))
            self.assertIn("bar", str(exc))
