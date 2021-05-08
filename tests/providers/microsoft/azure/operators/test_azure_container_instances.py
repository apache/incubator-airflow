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

import unittest
from collections import namedtuple
from unittest import mock
from unittest.mock import MagicMock

import pytest
from azure.mgmt.containerinstance.models import ContainerState, Event

from airflow.exceptions import AirflowException
from airflow.providers.microsoft.azure.operators.azure_container_instances import (
    AzureContainerInstancesOperator,
)


def make_mock_cg(container_state, events=None):
    """
    Make a mock Container Group as the underlying azure Models have read-only attributes
    See https://docs.microsoft.com/en-us/rest/api/container-instances/containergroups
    """
    events = events or []
    instance_view_dict = {"current_state": container_state, "events": events}
    instance_view = namedtuple("InstanceView", instance_view_dict.keys())(*instance_view_dict.values())

    container_dict = {"instance_view": instance_view}
    container = namedtuple("Container", container_dict.keys())(*container_dict.values())

    container_g_dict = {"containers": [container]}
    container_g = namedtuple("ContainerGroup", container_g_dict.keys())(*container_g_dict.values())
    return container_g


def make_mock_cg_with_missing_events(container_state):
    """
    Make a mock Container Group as the underlying azure Models have read-only attributes
    See https://docs.microsoft.com/en-us/rest/api/container-instances/containergroups
    This creates the Container Group without events.
    This can happen, when the container group is provisioned, but not started.
    """
    instance_view_dict = {"current_state": container_state, "events": None}
    instance_view = namedtuple("InstanceView", instance_view_dict.keys())(*instance_view_dict.values())

    container_dict = {"instance_view": instance_view}
    container = namedtuple("Container", container_dict.keys())(*container_dict.values())

    container_g_dict = {"containers": [container]}
    container_g = namedtuple("ContainerGroup", container_g_dict.keys())(*container_g_dict.values())
    return container_g


class TestACIOperator(unittest.TestCase):
    @mock.patch(
        "airflow.providers.microsoft.azure.operators." "azure_container_instances.AzureContainerInstanceHook"
    )
    def test_execute(self, aci_mock):
        expected_c_state = ContainerState(state='Terminated', exit_code=0, detail_status='test')
        expected_cg = make_mock_cg(expected_c_state)

        aci_mock.return_value.get_state.return_value = expected_cg
        aci_mock.return_value.exists.return_value = False

        aci = AzureContainerInstancesOperator(
            ci_conn_id=None,
            registry_conn_id=None,
            resource_group='resource-group',
            name='container-name',
            image='container-image',
            region='region',
            task_id='task',
        )
        aci.execute(None)

        assert aci_mock.return_value.create_or_update.call_count == 1
        (called_rg, called_cn, called_cg), _ = aci_mock.return_value.create_or_update.call_args

        assert called_rg == 'resource-group'
        assert called_cn == 'container-name'

        assert called_cg.location == 'region'
        assert called_cg.image_registry_credentials is None
        assert called_cg.restart_policy == 'Never'
        assert called_cg.os_type == 'Linux'

        called_cg_container = called_cg.containers[0]
        assert called_cg_container.name == 'container-name'
        assert called_cg_container.image == 'container-image'

        assert aci_mock.return_value.delete.call_count == 1

    @mock.patch(
        "airflow.providers.microsoft.azure.operators." "azure_container_instances.AzureContainerInstanceHook"
    )
    def test_execute_with_failures(self, aci_mock):
        expected_c_state = ContainerState(state='Terminated', exit_code=1, detail_status='test')
        expected_cg = make_mock_cg(expected_c_state)

        aci_mock.return_value.get_state.return_value = expected_cg
        aci_mock.return_value.exists.return_value = False

        aci = AzureContainerInstancesOperator(
            ci_conn_id=None,
            registry_conn_id=None,
            resource_group='resource-group',
            name='container-name',
            image='container-image',
            region='region',
            task_id='task',
        )
        with pytest.raises(AirflowException):
            aci.execute(None)

        assert aci_mock.return_value.delete.call_count == 1

    @mock.patch(
        "airflow.providers.microsoft.azure.operators." "azure_container_instances.AzureContainerInstanceHook"
    )
    def test_execute_with_tags(self, aci_mock):
        expected_c_state = ContainerState(state='Terminated', exit_code=0, detail_status='test')
        expected_cg = make_mock_cg(expected_c_state)
        tags = {"testKey": "testValue"}

        aci_mock.return_value.get_state.return_value = expected_cg
        aci_mock.return_value.exists.return_value = False

        aci = AzureContainerInstancesOperator(
            ci_conn_id=None,
            registry_conn_id=None,
            resource_group='resource-group',
            name='container-name',
            image='container-image',
            region='region',
            task_id='task',
            tags=tags,
        )
        aci.execute(None)

        assert aci_mock.return_value.create_or_update.call_count == 1
        (called_rg, called_cn, called_cg), _ = aci_mock.return_value.create_or_update.call_args

        assert called_rg == 'resource-group'
        assert called_cn == 'container-name'

        assert called_cg.location == 'region'
        assert called_cg.image_registry_credentials is None
        assert called_cg.restart_policy == 'Never'
        assert called_cg.os_type == 'Linux'
        assert called_cg.tags == tags

        called_cg_container = called_cg.containers[0]
        assert called_cg_container.name == 'container-name'
        assert called_cg_container.image == 'container-image'

        assert aci_mock.return_value.delete.call_count == 1

    @mock.patch(
        "airflow.providers.microsoft.azure.operators." "azure_container_instances.AzureContainerInstanceHook"
    )
    def test_execute_with_messages_logs(self, aci_mock):
        events = [Event(message="test"), Event(message="messages")]
        expected_c_state1 = ContainerState(state='Succeeded', exit_code=0, detail_status='test')
        expected_cg1 = make_mock_cg(expected_c_state1, events)
        expected_c_state2 = ContainerState(state='Running', exit_code=0, detail_status='test')
        expected_cg2 = make_mock_cg(expected_c_state2, events)
        expected_c_state3 = ContainerState(state='Terminated', exit_code=0, detail_status='test')
        expected_cg3 = make_mock_cg(expected_c_state3, events)

        aci_mock.return_value.get_state.side_effect = [expected_cg1, expected_cg2, expected_cg3]
        aci_mock.return_value.get_logs.return_value = ["test", "logs"]
        aci_mock.return_value.exists.return_value = False

        aci = AzureContainerInstancesOperator(
            ci_conn_id=None,
            registry_conn_id=None,
            resource_group='resource-group',
            name='container-name',
            image='container-image',
            region='region',
            task_id='task',
        )
        aci.execute(None)

        assert aci_mock.return_value.create_or_update.call_count == 1
        assert aci_mock.return_value.get_state.call_count == 3
        assert aci_mock.return_value.get_logs.call_count == 3

        assert aci_mock.return_value.delete.call_count == 1

    def test_name_checker(self):
        valid_names = ['test-dash', 'name-with-length---63' * 3]

        invalid_names = [
            'test_underscore',
            'name-with-length---84' * 4,
            'name-ending-with-dash-',
            '-name-starting-with-dash',
        ]
        for name in invalid_names:
            with pytest.raises(AirflowException):
                AzureContainerInstancesOperator._check_name(name)

        for name in valid_names:
            checked_name = AzureContainerInstancesOperator._check_name(name)
            assert checked_name == name

    @mock.patch(
        "airflow.providers.microsoft.azure.operators.azure_container_instances.AzureContainerInstanceHook"
    )
    def test_execute_with_ipaddress(self, aci_mock):
        expected_c_state = ContainerState(state='Terminated', exit_code=0, detail_status='test')
        expected_cg = make_mock_cg(expected_c_state)
        ipaddress = MagicMock()

        aci_mock.return_value.get_state.return_value = expected_cg
        aci_mock.return_value.exists.return_value = False

        aci = AzureContainerInstancesOperator(
            ci_conn_id=None,
            registry_conn_id=None,
            resource_group='resource-group',
            name='container-name',
            image='container-image',
            region='region',
            task_id='task',
            ip_address=ipaddress,
        )
        aci.execute(None)
        assert aci_mock.return_value.create_or_update.call_count == 1
        (_, _, called_cg), _ = aci_mock.return_value.create_or_update.call_args

        assert called_cg.ip_address == ipaddress

    @mock.patch(
        "airflow.providers.microsoft.azure.operators.azure_container_instances.AzureContainerInstanceHook"
    )
    def test_execute_with_windows_os_and_diff_restart_policy(self, aci_mock):
        expected_c_state = ContainerState(state='Terminated', exit_code=0, detail_status='test')
        expected_cg = make_mock_cg(expected_c_state)

        aci_mock.return_value.get_state.return_value = expected_cg
        aci_mock.return_value.exists.return_value = False

        aci = AzureContainerInstancesOperator(
            ci_conn_id=None,
            registry_conn_id=None,
            resource_group='resource-group',
            name='container-name',
            image='container-image',
            region='region',
            task_id='task',
            restart_policy="Always",
            os_type='Windows',
        )
        aci.execute(None)
        assert aci_mock.return_value.create_or_update.call_count == 1
        (_, _, called_cg), _ = aci_mock.return_value.create_or_update.call_args

        assert called_cg.restart_policy == 'Always'
        assert called_cg.os_type == 'Windows'

    @mock.patch(
        "airflow.providers.microsoft.azure.operators.azure_container_instances.AzureContainerInstanceHook"
    )
    def test_execute_fails_with_incorrect_os_type(self, aci_mock):
        expected_c_state = ContainerState(state='Terminated', exit_code=0, detail_status='test')
        expected_cg = make_mock_cg(expected_c_state)

        aci_mock.return_value.get_state.return_value = expected_cg
        aci_mock.return_value.exists.return_value = False

        with pytest.raises(AirflowException) as ctx:
            AzureContainerInstancesOperator(
                ci_conn_id=None,
                registry_conn_id=None,
                resource_group='resource-group',
                name='container-name',
                image='container-image',
                region='region',
                task_id='task',
                os_type='MacOs',
            )

        assert (
            str(ctx.value) == "Invalid value for the os_type argument. "
            "Please set 'Linux' or 'Windows' as the os_type. "
            "Found `MacOs`."
        )

    @mock.patch(
        "airflow.providers.microsoft.azure.operators.azure_container_instances.AzureContainerInstanceHook"
    )
    def test_execute_fails_with_incorrect_restart_policy(self, aci_mock):
        expected_c_state = ContainerState(state='Terminated', exit_code=0, detail_status='test')
        expected_cg = make_mock_cg(expected_c_state)

        aci_mock.return_value.get_state.return_value = expected_cg
        aci_mock.return_value.exists.return_value = False

        with pytest.raises(AirflowException) as ctx:
            AzureContainerInstancesOperator(
                ci_conn_id=None,
                registry_conn_id=None,
                resource_group='resource-group',
                name='container-name',
                image='container-image',
                region='region',
                task_id='task',
                restart_policy='Everyday',
            )

        assert (
            str(ctx.value) == "Invalid value for the restart_policy argument. "
            "Please set one of 'Always', 'OnFailure','Never' as the restart_policy. "
            "Found `Everyday`"
        )

    @mock.patch(
        "airflow.providers.microsoft.azure.operators.azure_container_instances.AzureContainerInstanceHook"
    )
    @mock.patch(
        'airflow.providers.microsoft.azure.operators.azure_container_instances.sleep'
    )
    def test_execute_correct_sleep_cycle(self, sleep_mock, aci_mock):
        expected_c_state1 = ContainerState(state='Running', exit_code=0, detail_status='test')
        expected_cg1 = make_mock_cg(expected_c_state1)
        expected_c_state2 = ContainerState(state='Terminated', exit_code=0, detail_status='test')
        expected_cg2 = make_mock_cg(expected_c_state2)

        aci_mock.return_value.get_state.side_effect = [expected_cg1, expected_cg1, expected_cg2]
        aci_mock.return_value.exists.return_value = False

        aci = AzureContainerInstancesOperator(
            ci_conn_id=None,
            registry_conn_id=None,
            resource_group='resource-group',
            name='container-name',
            image='container-image',
            region='region',
            task_id='task',
        )
        aci.execute(None)

        # sleep is called at the end of cycles. Thus, the Terminated call does not trigger sleep
        assert sleep_mock.call_count == 2

    @mock.patch(
        "airflow.providers.microsoft.azure.operators.azure_container_instances.AzureContainerInstanceHook"
    )
    @mock.patch("logging.Logger.exception")
    def test_execute_with_missing_events(self, log_mock, aci_mock):
        expected_c_state1 = ContainerState(state='Running', exit_code=0, detail_status='test')
        expected_cg1 = make_mock_cg_with_missing_events(expected_c_state1)
        expected_c_state2 = ContainerState(state='Terminated', exit_code=0, detail_status='test')
        expected_cg2 = make_mock_cg(expected_c_state2)

        aci_mock.return_value.get_state.side_effect = [expected_cg1, expected_cg2]
        aci_mock.return_value.exists.return_value = False

        aci = AzureContainerInstancesOperator(
            ci_conn_id=None,
            registry_conn_id=None,
            resource_group='resource-group',
            name='container-name',
            image='container-image',
            region='region',
            task_id='task',
        )

        aci.execute(None)

        assert log_mock.call_count == 0
