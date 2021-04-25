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

import json
import unittest
from unittest import mock

from google.api_core.gapic_v1.method import DEFAULT
from google.cloud.monitoring_v3 import AlertPolicy, NotificationChannel

from airflow.providers.google.cloud.operators.stackdriver import (
    StackdriverDeleteAlertOperator,
    StackdriverDeleteNotificationChannelOperator,
    StackdriverDisableAlertPoliciesOperator,
    StackdriverDisableNotificationChannelsOperator,
    StackdriverEnableAlertPoliciesOperator,
    StackdriverEnableNotificationChannelsOperator,
    StackdriverListAlertPoliciesOperator,
    StackdriverListNotificationChannelsOperator,
    StackdriverUpsertAlertOperator,
    StackdriverUpsertNotificationChannelOperator,
)

TEST_TASK_ID = 'test-stackdriver-operator'
TEST_FILTER = 'filter'
TEST_ALERT_POLICY_1 = {
    "combiner": "OR",
    "name": "projects/sd-project/alertPolicies/12345",
    "enabled": True,
    "display_name": "test display",
    "conditions": [
        {
            "condition_threshold": {
                "comparison": "COMPARISON_GT",
                "aggregations": [{"alignment_eriod": {'seconds': 60}, "per_series_aligner": "ALIGN_RATE"}],
            },
            "display_name": "Condition display",
            "name": "projects/sd-project/alertPolicies/123/conditions/456",
        }
    ],
}

TEST_ALERT_POLICY_2 = {
    "combiner": "OR",
    "name": "projects/sd-project/alertPolicies/6789",
    "enabled": False,
    "display_name": "test display",
    "conditions": [
        {
            "condition_threshold": {
                "comparison": "COMPARISON_GT",
                "aggregations": [{"alignment_period": {'seconds': 60}, "per_series_aligner": "ALIGN_RATE"}],
            },
            "display_name": "Condition display",
            "name": "projects/sd-project/alertPolicies/456/conditions/789",
        }
    ],
}

TEST_NOTIFICATION_CHANNEL_1 = {
    "displayName": "sd",
    "enabled": True,
    "labels": {"auth_token": "top-secret", "channel_name": "#channel"},
    "name": "projects/sd-project/notificationChannels/12345",
    "type": "slack",
}

TEST_NOTIFICATION_CHANNEL_2 = {
    "displayName": "sd",
    "enabled": False,
    "labels": {"auth_token": "top-secret", "channel_name": "#channel"},
    "name": "projects/sd-project/notificationChannels/6789",
    "type": "slack",
}


class TestStackdriverListAlertPoliciesOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.stackdriver.StackdriverHook')
    def test_execute(self, mock_hook):
        operator = StackdriverListAlertPoliciesOperator(task_id=TEST_TASK_ID, filter_=TEST_FILTER)
        mock_hook.return_value.list_alert_policies.return_value = [AlertPolicy(name="test-name")]
        result = operator.execute(None)
        mock_hook.return_value.list_alert_policies.assert_called_once_with(
            project_id=None,
            filter_=TEST_FILTER,
            format_=None,
            order_by=None,
            page_size=None,
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=None,
        )
        assert [
            {
                'combiner': 0,
                'conditions': [],
                'display_name': '',
                'name': 'test-name',
                'notification_channels': [],
                'user_labels': {},
            }
        ] == result


class TestStackdriverEnableAlertPoliciesOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.stackdriver.StackdriverHook')
    def test_execute(self, mock_hook):
        operator = StackdriverEnableAlertPoliciesOperator(task_id=TEST_TASK_ID, filter_=TEST_FILTER)
        operator.execute(None)
        mock_hook.return_value.enable_alert_policies.assert_called_once_with(
            project_id=None, filter_=TEST_FILTER, retry=DEFAULT, timeout=DEFAULT, metadata=None
        )


class TestStackdriverDisableAlertPoliciesOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.stackdriver.StackdriverHook')
    def test_execute(self, mock_hook):
        operator = StackdriverDisableAlertPoliciesOperator(task_id=TEST_TASK_ID, filter_=TEST_FILTER)
        operator.execute(None)
        mock_hook.return_value.disable_alert_policies.assert_called_once_with(
            project_id=None, filter_=TEST_FILTER, retry=DEFAULT, timeout=DEFAULT, metadata=None
        )


class TestStackdriverUpsertAlertsOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.stackdriver.StackdriverHook')
    def test_execute(self, mock_hook):
        operator = StackdriverUpsertAlertOperator(
            task_id=TEST_TASK_ID, alerts=json.dumps({"policies": [TEST_ALERT_POLICY_1, TEST_ALERT_POLICY_2]})
        )
        operator.execute(None)
        mock_hook.return_value.upsert_alert.assert_called_once_with(
            alerts=json.dumps({"policies": [TEST_ALERT_POLICY_1, TEST_ALERT_POLICY_2]}),
            project_id=None,
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=None,
        )


class TestStackdriverDeleteAlertOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.stackdriver.StackdriverHook')
    def test_execute(self, mock_hook):
        operator = StackdriverDeleteAlertOperator(
            task_id=TEST_TASK_ID,
            name='test-alert',
        )
        operator.execute(None)
        mock_hook.return_value.delete_alert_policy.assert_called_once_with(
            name='test-alert', retry=DEFAULT, timeout=DEFAULT, metadata=None
        )


class TestStackdriverListNotificationChannelsOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.stackdriver.StackdriverHook')
    def test_execute(self, mock_hook):
        operator = StackdriverListNotificationChannelsOperator(task_id=TEST_TASK_ID, filter_=TEST_FILTER)
        mock_hook.return_value.list_notification_channels.return_value = [
            NotificationChannel(name="test-123")
        ]

        result = operator.execute(None)
        mock_hook.return_value.list_notification_channels.assert_called_once_with(
            project_id=None,
            filter_=TEST_FILTER,
            format_=None,
            order_by=None,
            page_size=None,
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=None,
        )
        # Depending on the version of google-apitools installed we might receive the response either with or
        # without mutation_records.
        assert result in [
            [
                {
                    'description': '',
                    'display_name': '',
                    'labels': {},
                    'name': 'test-123',
                    'type_': '',
                    'user_labels': {},
                    'verification_status': 0,
                }
            ],
            [
                {
                    'description': '',
                    'display_name': '',
                    'labels': {},
                    'mutation_records': [],
                    'name': 'test-123',
                    'type_': '',
                    'user_labels': {},
                    'verification_status': 0,
                }
            ],
        ]


class TestStackdriverEnableNotificationChannelsOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.stackdriver.StackdriverHook')
    def test_execute(self, mock_hook):
        operator = StackdriverEnableNotificationChannelsOperator(task_id=TEST_TASK_ID, filter_=TEST_FILTER)
        operator.execute(None)
        mock_hook.return_value.enable_notification_channels.assert_called_once_with(
            project_id=None, filter_=TEST_FILTER, retry=DEFAULT, timeout=DEFAULT, metadata=None
        )


class TestStackdriverDisableNotificationChannelsOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.stackdriver.StackdriverHook')
    def test_execute(self, mock_hook):
        operator = StackdriverDisableNotificationChannelsOperator(task_id=TEST_TASK_ID, filter_=TEST_FILTER)
        operator.execute(None)
        mock_hook.return_value.disable_notification_channels.assert_called_once_with(
            project_id=None, filter_=TEST_FILTER, retry=DEFAULT, timeout=DEFAULT, metadata=None
        )


class TestStackdriverUpsertChannelOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.stackdriver.StackdriverHook')
    def test_execute(self, mock_hook):
        operator = StackdriverUpsertNotificationChannelOperator(
            task_id=TEST_TASK_ID,
            channels=json.dumps({"channels": [TEST_NOTIFICATION_CHANNEL_1, TEST_NOTIFICATION_CHANNEL_2]}),
        )
        operator.execute(None)
        mock_hook.return_value.upsert_channel.assert_called_once_with(
            channels=json.dumps({"channels": [TEST_NOTIFICATION_CHANNEL_1, TEST_NOTIFICATION_CHANNEL_2]}),
            project_id=None,
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=None,
        )


class TestStackdriverDeleteNotificationChannelOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.stackdriver.StackdriverHook')
    def test_execute(self, mock_hook):
        operator = StackdriverDeleteNotificationChannelOperator(
            task_id=TEST_TASK_ID,
            name='test-channel',
        )
        operator.execute(None)
        mock_hook.return_value.delete_notification_channel.assert_called_once_with(
            name='test-channel', retry=DEFAULT, timeout=DEFAULT, metadata=None
        )
