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
from google.protobuf.field_mask_pb2 import FieldMask

from airflow.providers.google.cloud.hooks import stackdriver

PROJECT_ID = "sd-project"
CREDENTIALS = "sd-credentials"
TEST_FILTER = "filter"
TEST_ALERT_POLICY_1 = {
    "combiner": "OR",
    "name": "projects/sd-project/alertPolicies/12345",
    "enabled": True,
    "display_name": "test display",
    "conditions": [
        {
            "condition_threshold": {
                "comparison": "COMPARISON_GT",
                "aggregations": [{"alignment_period": {'seconds': 60}, "per_series_aligner": "ALIGN_RATE"}],
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
    "display_name": "sd",
    "enabled": True,
    "labels": {"auth_token": "top-secret", "channel_name": "#channel"},
    "name": "projects/sd-project/notificationChannels/12345",
    "type_": "slack",
}

TEST_NOTIFICATION_CHANNEL_2 = {
    "display_name": "sd",
    "enabled": False,
    "labels": {"auth_token": "top-secret", "channel_name": "#channel"},
    "name": "projects/sd-project/notificationChannels/6789",
    "type_": "slack",
}


class TestStackdriverHookMethods(unittest.TestCase):
    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook._get_credentials_and_project_id',
        return_value=(CREDENTIALS, PROJECT_ID),
    )
    @mock.patch('airflow.providers.google.cloud.hooks.stackdriver.StackdriverHook._get_policy_client')
    def test_stackdriver_list_alert_policies(self, mock_policy_client, mock_get_creds_and_project_id):
        method = mock_policy_client.return_value.list_alert_policies
        hook = stackdriver.StackdriverHook()
        hook.list_alert_policies(
            filter_=TEST_FILTER,
            project_id=PROJECT_ID,
        )
        method.assert_called_once_with(
            request=dict(name=f'projects/{PROJECT_ID}', filter=TEST_FILTER, order_by=None, page_size=None),
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=(),
        )

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook._get_credentials_and_project_id',
        return_value=(CREDENTIALS, PROJECT_ID),
    )
    @mock.patch('airflow.providers.google.cloud.hooks.stackdriver.StackdriverHook._get_policy_client')
    def test_stackdriver_enable_alert_policy(self, mock_policy_client, mock_get_creds_and_project_id):
        hook = stackdriver.StackdriverHook()

        alert_policy_enabled = AlertPolicy(**TEST_ALERT_POLICY_1)
        alert_policy_disabled = AlertPolicy(**TEST_ALERT_POLICY_2)

        alert_policies = [alert_policy_enabled, alert_policy_disabled]

        mock_policy_client.return_value.list_alert_policies.return_value = alert_policies
        hook.enable_alert_policies(
            filter_=TEST_FILTER,
            project_id=PROJECT_ID,
        )
        mock_policy_client.return_value.list_alert_policies.assert_called_once_with(
            request=dict(name=f'projects/{PROJECT_ID}', filter=TEST_FILTER, order_by=None, page_size=None),
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=(),
        )
        mask = FieldMask(paths=["enabled"])
        alert_policy_disabled.enabled = True  # pylint: disable=no-member
        mock_policy_client.return_value.update_alert_policy.assert_called_once_with(
            request=dict(alert_policy=alert_policy_disabled, update_mask=mask),
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=(),
        )

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook._get_credentials_and_project_id',
        return_value=(CREDENTIALS, PROJECT_ID),
    )
    @mock.patch('airflow.providers.google.cloud.hooks.stackdriver.StackdriverHook._get_policy_client')
    def test_stackdriver_disable_alert_policy(self, mock_policy_client, mock_get_creds_and_project_id):
        hook = stackdriver.StackdriverHook()
        alert_policy_enabled = AlertPolicy(**TEST_ALERT_POLICY_1)
        alert_policy_disabled = AlertPolicy(**TEST_ALERT_POLICY_2)

        mock_policy_client.return_value.list_alert_policies.return_value = [
            alert_policy_enabled,
            alert_policy_disabled,
        ]
        hook.disable_alert_policies(
            filter_=TEST_FILTER,
            project_id=PROJECT_ID,
        )
        mock_policy_client.return_value.list_alert_policies.assert_called_once_with(
            request=dict(name=f'projects/{PROJECT_ID}', filter=TEST_FILTER, order_by=None, page_size=None),
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=(),
        )
        mask = FieldMask(paths=["enabled"])
        alert_policy_enabled.enabled = False  # pylint: disable=no-member
        mock_policy_client.return_value.update_alert_policy.assert_called_once_with(
            request=dict(alert_policy=alert_policy_enabled, update_mask=mask),
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=(),
        )

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook._get_credentials_and_project_id',
        return_value=(CREDENTIALS, PROJECT_ID),
    )
    @mock.patch('airflow.providers.google.cloud.hooks.stackdriver.StackdriverHook._get_policy_client')
    @mock.patch('airflow.providers.google.cloud.hooks.stackdriver.StackdriverHook._get_channel_client')
    def test_stackdriver_upsert_alert_policy(
        self, mock_channel_client, mock_policy_client, mock_get_creds_and_project_id
    ):
        hook = stackdriver.StackdriverHook()
        existing_alert_policy = AlertPolicy(**TEST_ALERT_POLICY_1)
        alert_policy_to_create = AlertPolicy(**TEST_ALERT_POLICY_2)

        mock_policy_client.return_value.list_alert_policies.return_value = [existing_alert_policy]
        mock_channel_client.return_value.list_notification_channels.return_value = []

        hook.upsert_alert(
            alerts=json.dumps({"policies": [TEST_ALERT_POLICY_1, TEST_ALERT_POLICY_2], "channels": []}),
            project_id=PROJECT_ID,
        )
        mock_channel_client.return_value.list_notification_channels.assert_called_once_with(
            request=dict(
                name=f'projects/{PROJECT_ID}',
                filter=None,
                order_by=None,
                page_size=None,
            ),
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=(),
        )
        mock_policy_client.return_value.list_alert_policies.assert_called_once_with(
            request=dict(name=f'projects/{PROJECT_ID}', filter=None, order_by=None, page_size=None),
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=(),
        )
        alert_policy_to_create.name = None
        alert_policy_to_create.creation_record = None
        alert_policy_to_create.mutation_record = None
        # pylint: disable=unsubscriptable-object
        alert_policy_to_create.conditions[0].name = None
        mock_policy_client.return_value.create_alert_policy.assert_called_once_with(
            request=dict(
                name=f'projects/{PROJECT_ID}',
                alert_policy=alert_policy_to_create,
            ),
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=(),
        )
        existing_alert_policy.creation_record = None
        existing_alert_policy.mutation_record = None
        mock_policy_client.return_value.update_alert_policy.assert_called_once_with(
            request=dict(alert_policy=existing_alert_policy), retry=DEFAULT, timeout=DEFAULT, metadata=()
        )

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook._get_credentials_and_project_id',
        return_value=(CREDENTIALS, PROJECT_ID),
    )
    @mock.patch('airflow.providers.google.cloud.hooks.stackdriver.StackdriverHook._get_policy_client')
    @mock.patch('airflow.providers.google.cloud.hooks.stackdriver.StackdriverHook._get_channel_client')
    def test_stackdriver_upsert_alert_policy_without_channel(
        self, mock_channel_client, mock_policy_client, mock_get_creds_and_project_id
    ):
        hook = stackdriver.StackdriverHook()
        existing_alert_policy = AlertPolicy(**TEST_ALERT_POLICY_1)

        mock_policy_client.return_value.list_alert_policies.return_value = [existing_alert_policy]
        mock_channel_client.return_value.list_notification_channels.return_value = []

        hook.upsert_alert(
            alerts=json.dumps({"policies": [TEST_ALERT_POLICY_1, TEST_ALERT_POLICY_2]}),
            project_id=PROJECT_ID,
        )
        mock_channel_client.return_value.list_notification_channels.assert_called_once_with(
            request=dict(name=f'projects/{PROJECT_ID}', filter=None, order_by=None, page_size=None),
            metadata=(),
            retry=DEFAULT,
            timeout=DEFAULT,
        )
        mock_policy_client.return_value.list_alert_policies.assert_called_once_with(
            request=dict(name=f'projects/{PROJECT_ID}', filter=None, order_by=None, page_size=None),
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=(),
        )

        existing_alert_policy.creation_record = None
        existing_alert_policy.mutation_record = None
        mock_policy_client.return_value.update_alert_policy.assert_called_once_with(
            request=dict(alert_policy=existing_alert_policy), retry=DEFAULT, timeout=DEFAULT, metadata=()
        )

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook._get_credentials_and_project_id',
        return_value=(CREDENTIALS, PROJECT_ID),
    )
    @mock.patch('airflow.providers.google.cloud.hooks.stackdriver.StackdriverHook._get_policy_client')
    def test_stackdriver_delete_alert_policy(self, mock_policy_client, mock_get_creds_and_project_id):
        hook = stackdriver.StackdriverHook()
        hook.delete_alert_policy(
            name='test-alert',
        )
        mock_policy_client.return_value.delete_alert_policy.assert_called_once_with(
            request=dict(name='test-alert'),
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=(),
        )

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook._get_credentials_and_project_id',
        return_value=(CREDENTIALS, PROJECT_ID),
    )
    @mock.patch('airflow.providers.google.cloud.hooks.stackdriver.StackdriverHook._get_channel_client')
    def test_stackdriver_list_notification_channel(self, mock_channel_client, mock_get_creds_and_project_id):
        hook = stackdriver.StackdriverHook()
        hook.list_notification_channels(
            filter_=TEST_FILTER,
            project_id=PROJECT_ID,
        )
        mock_channel_client.return_value.list_notification_channels.assert_called_once_with(
            request=dict(name=f'projects/{PROJECT_ID}', filter=TEST_FILTER, order_by=None, page_size=None),
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=(),
        )

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook._get_credentials_and_project_id',
        return_value=(CREDENTIALS, PROJECT_ID),
    )
    @mock.patch('airflow.providers.google.cloud.hooks.stackdriver.StackdriverHook._get_channel_client')
    def test_stackdriver_enable_notification_channel(
        self, mock_channel_client, mock_get_creds_and_project_id
    ):
        hook = stackdriver.StackdriverHook()
        notification_channel_enabled = NotificationChannel(**TEST_NOTIFICATION_CHANNEL_1)
        notification_channel_disabled = NotificationChannel(**TEST_NOTIFICATION_CHANNEL_2)

        mock_channel_client.return_value.list_notification_channels.return_value = [
            notification_channel_enabled,
            notification_channel_disabled,
        ]

        hook.enable_notification_channels(
            filter_=TEST_FILTER,
            project_id=PROJECT_ID,
        )

        notification_channel_disabled.enabled = True  # pylint: disable=no-member
        mask = FieldMask(paths=['enabled'])
        mock_channel_client.return_value.update_notification_channel.assert_called_once_with(
            request=dict(notification_channel=notification_channel_disabled, update_mask=mask),
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=(),
        )

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook._get_credentials_and_project_id',
        return_value=(CREDENTIALS, PROJECT_ID),
    )
    @mock.patch('airflow.providers.google.cloud.hooks.stackdriver.StackdriverHook._get_channel_client')
    def test_stackdriver_disable_notification_channel(
        self, mock_channel_client, mock_get_creds_and_project_id
    ):
        hook = stackdriver.StackdriverHook()
        notification_channel_enabled = NotificationChannel(**TEST_NOTIFICATION_CHANNEL_1)
        notification_channel_disabled = NotificationChannel(**TEST_NOTIFICATION_CHANNEL_2)
        mock_channel_client.return_value.list_notification_channels.return_value = [
            notification_channel_enabled,
            notification_channel_disabled,
        ]

        hook.disable_notification_channels(
            filter_=TEST_FILTER,
            project_id=PROJECT_ID,
        )

        notification_channel_enabled.enabled = False  # pylint: disable=no-member
        mask = FieldMask(paths=['enabled'])
        mock_channel_client.return_value.update_notification_channel.assert_called_once_with(
            request=dict(notification_channel=notification_channel_enabled, update_mask=mask),
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=(),
        )

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook._get_credentials_and_project_id',
        return_value=(CREDENTIALS, PROJECT_ID),
    )
    @mock.patch('airflow.providers.google.cloud.hooks.stackdriver.StackdriverHook._get_channel_client')
    def test_stackdriver_upsert_channel(self, mock_channel_client, mock_get_creds_and_project_id):
        hook = stackdriver.StackdriverHook()
        existing_notification_channel = NotificationChannel(**TEST_NOTIFICATION_CHANNEL_1)
        notification_channel_to_be_created = NotificationChannel(**TEST_NOTIFICATION_CHANNEL_2)

        mock_channel_client.return_value.list_notification_channels.return_value = [
            existing_notification_channel
        ]
        hook.upsert_channel(
            channels=json.dumps({"channels": [TEST_NOTIFICATION_CHANNEL_1, TEST_NOTIFICATION_CHANNEL_2]}),
            project_id=PROJECT_ID,
        )
        mock_channel_client.return_value.list_notification_channels.assert_called_once_with(
            request=dict(name=f'projects/{PROJECT_ID}', filter=None, order_by=None, page_size=None),
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=(),
        )
        mock_channel_client.return_value.update_notification_channel.assert_called_once_with(
            request=dict(notification_channel=existing_notification_channel),
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=(),
        )
        notification_channel_to_be_created.name = None
        mock_channel_client.return_value.create_notification_channel.assert_called_once_with(
            request=dict(
                name=f'projects/{PROJECT_ID}', notification_channel=notification_channel_to_be_created
            ),
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=(),
        )

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook._get_credentials_and_project_id',
        return_value=(CREDENTIALS, PROJECT_ID),
    )
    @mock.patch('airflow.providers.google.cloud.hooks.stackdriver.StackdriverHook._get_channel_client')
    def test_stackdriver_delete_notification_channel(
        self, mock_channel_client, mock_get_creds_and_project_id
    ):
        hook = stackdriver.StackdriverHook()
        hook.delete_notification_channel(
            name='test-channel',
        )
        mock_channel_client.return_value.delete_notification_channel.assert_called_once_with(
            request=dict(name='test-channel'), retry=DEFAULT, timeout=DEFAULT, metadata=()
        )
