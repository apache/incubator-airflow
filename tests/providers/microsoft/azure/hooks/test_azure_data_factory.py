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

# pylint: disable=redefined-outer-name,unused-argument

import json
from unittest.mock import MagicMock, Mock

import pytest
from pytest import fixture

from airflow.exceptions import AirflowException
from airflow.models.connection import Connection
from airflow.providers.microsoft.azure.hooks.azure_data_factory import (
    AzureDataFactoryHook,
    provide_targeted_factory,
)
from airflow.utils import db

DEFAULT_RESOURCE_GROUP = "defaultResourceGroup"
RESOURCE_GROUP = "testResourceGroup"

DEFAULT_FACTORY = "defaultFactory"
FACTORY = "testFactory"

MODEL = object()
NAME = "testName"
ID = "testId"


def setup_module():
    connection = Connection(
        conn_id="azure_data_factory_test",
        conn_type="azure_data_factory",
        login="clientId",
        password="clientSecret",
        extra=json.dumps(
            {
                "tenantId": "tenantId",
                "subscriptionId": "subscriptionId",
                "resourceGroup": DEFAULT_RESOURCE_GROUP,
                "factory": DEFAULT_FACTORY,
            }
        ),
    )

    db.merge_conn(connection)


@fixture
def hook():
    client = AzureDataFactoryHook(conn_id="azure_data_factory_test")
    client._conn = MagicMock(
        spec=[
            "factories",
            "linked_services",
            "datasets",
            "pipelines",
            "pipeline_runs",
            "triggers",
            "trigger_runs",
        ]
    )

    return client


def parametrize(explicit_factory, implicit_factory):
    def wrapper(func):
        return pytest.mark.parametrize(
            ("user_args", "sdk_args"),
            (explicit_factory, implicit_factory),
            ids=("explicit factory", "implicit factory"),
        )(func)

    return wrapper


def test_provide_targeted_factory():
    def echo(_, resource_group_name=None, factory_name=None):
        return resource_group_name, factory_name

    conn = MagicMock()
    hook = MagicMock()
    hook.get_connection.return_value = conn

    conn.extra_dejson = {}
    assert provide_targeted_factory(echo)(hook, RESOURCE_GROUP, FACTORY) == (RESOURCE_GROUP, FACTORY)

    conn.extra_dejson = {"resourceGroup": DEFAULT_RESOURCE_GROUP, "factory": DEFAULT_FACTORY}
    assert provide_targeted_factory(echo)(hook) == (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY)

    with pytest.raises(AirflowException):
        conn.extra_dejson = {}
        provide_targeted_factory(echo)(hook)


@parametrize(
    explicit_factory=((RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY)),
    implicit_factory=((), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY)),
)
def test_get_factory(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.get_factory(*user_args)

    hook._conn.factories.get.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((MODEL, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, MODEL)),
    implicit_factory=((MODEL,), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, MODEL)),
)
def test_create_factory(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.create_factory(*user_args)

    hook._conn.factories.create_or_update.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((MODEL, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, MODEL)),
    implicit_factory=((MODEL,), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, MODEL)),
)
def test_update_factory(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook._factory_exists = Mock(return_value=True)
    hook.update_factory(*user_args)

    hook._conn.factories.create_or_update.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((MODEL, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, MODEL)),
    implicit_factory=((MODEL,), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, MODEL)),
)
def test_update_factory_non_existent(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook._factory_exists = Mock(return_value=False)

    with pytest.raises(AirflowException, match=r"Factory .+ does not exist"):
        hook.update_factory(*user_args)


@parametrize(
    explicit_factory=((RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY)),
    implicit_factory=((), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY)),
)
def test_delete_factory(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.delete_factory(*user_args)

    hook._conn.factories.delete.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME)),
    implicit_factory=((NAME,), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME)),
)
def test_get_linked_service(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.get_linked_service(*user_args)

    hook._conn.linked_services.get.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, MODEL, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME, MODEL)),
    implicit_factory=((NAME, MODEL), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME, MODEL)),
)
def test_create_linked_service(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.create_linked_service(*user_args)

    hook._conn.linked_services.create_or_update(*sdk_args)


@parametrize(
    explicit_factory=((NAME, MODEL, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME, MODEL)),
    implicit_factory=((NAME, MODEL), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME, MODEL)),
)
def test_update_linked_service(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook._linked_service_exists = Mock(return_value=True)
    hook.update_linked_service(*user_args)

    hook._conn.linked_services.create_or_update(*sdk_args)


@parametrize(
    explicit_factory=((NAME, MODEL, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME, MODEL)),
    implicit_factory=((NAME, MODEL), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME, MODEL)),
)
def test_update_linked_service_non_existent(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook._linked_service_exists = Mock(return_value=False)

    with pytest.raises(AirflowException, match=r"Linked service .+ does not exist"):
        hook.update_linked_service(*user_args)


@parametrize(
    explicit_factory=((NAME, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME)),
    implicit_factory=((NAME,), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME)),
)
def test_delete_linked_service(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.delete_linked_service(*user_args)

    hook._conn.linked_services.delete.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME)),
    implicit_factory=((NAME,), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME)),
)
def test_get_dataset(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.get_dataset(*user_args)

    hook._conn.datasets.get.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, MODEL, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME, MODEL)),
    implicit_factory=((NAME, MODEL), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME, MODEL)),
)
def test_create_dataset(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.create_dataset(*user_args)

    hook._conn.datasets.create_or_update.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, MODEL, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME, MODEL)),
    implicit_factory=((NAME, MODEL), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME, MODEL)),
)
def test_update_dataset(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook._dataset_exists = Mock(return_value=True)
    hook.update_dataset(*user_args)

    hook._conn.datasets.create_or_update.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, MODEL, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME, MODEL)),
    implicit_factory=((NAME, MODEL), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME, MODEL)),
)
def test_update_dataset_non_existent(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook._dataset_exists = Mock(return_value=False)

    with pytest.raises(AirflowException, match=r"Dataset .+ does not exist"):
        hook.update_dataset(*user_args)


@parametrize(
    explicit_factory=((NAME, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME)),
    implicit_factory=((NAME,), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME)),
)
def test_delete_dataset(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.delete_dataset(*user_args)

    hook._conn.datasets.delete.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME)),
    implicit_factory=((NAME,), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME)),
)
def test_get_pipeline(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.get_pipeline(*user_args)

    hook._conn.pipelines.get.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, MODEL, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME, MODEL)),
    implicit_factory=((NAME, MODEL), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME, MODEL)),
)
def test_create_pipeline(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.create_pipeline(*user_args)

    hook._conn.pipelines.create_or_update.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, MODEL, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME, MODEL)),
    implicit_factory=((NAME, MODEL), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME, MODEL)),
)
def test_update_pipeline(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook._pipeline_exists = Mock(return_value=True)
    hook.update_pipeline(*user_args)

    hook._conn.pipelines.create_or_update.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, MODEL, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME, MODEL)),
    implicit_factory=((NAME, MODEL), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME, MODEL)),
)
def test_update_pipeline_non_existent(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook._pipeline_exists = Mock(return_value=False)

    with pytest.raises(AirflowException, match=r"Pipeline .+ does not exist"):
        hook.update_pipeline(*user_args)


@parametrize(
    explicit_factory=((NAME, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME)),
    implicit_factory=((NAME,), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME)),
)
def test_delete_pipeline(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.delete_pipeline(*user_args)

    hook._conn.pipelines.delete.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME)),
    implicit_factory=((NAME,), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME)),
)
def test_run_pipeline(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.run_pipeline(*user_args)

    hook._conn.pipelines.create_run.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((ID, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, ID)),
    implicit_factory=((ID,), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, ID)),
)
def test_get_pipeline_run(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.get_pipeline_run(*user_args)

    hook._conn.pipeline_runs.get.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((ID, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, ID)),
    implicit_factory=((ID,), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, ID)),
)
def test_cancel_pipeline_run(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.cancel_pipeline_run(*user_args)

    hook._conn.pipeline_runs.cancel.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME)),
    implicit_factory=((NAME,), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME)),
)
def test_get_trigger(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.get_trigger(*user_args)

    hook._conn.triggers.get.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, MODEL, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME, MODEL)),
    implicit_factory=((NAME, MODEL), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME, MODEL)),
)
def test_create_trigger(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.create_trigger(*user_args)

    hook._conn.triggers.create_or_update.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, MODEL, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME, MODEL)),
    implicit_factory=((NAME, MODEL), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME, MODEL)),
)
def test_update_trigger(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook._trigger_exists = Mock(return_value=True)
    hook.update_trigger(*user_args)

    hook._conn.triggers.create_or_update.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, MODEL, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME, MODEL)),
    implicit_factory=((NAME, MODEL), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME, MODEL)),
)
def test_update_trigger_non_existent(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook._trigger_exists = Mock(return_value=False)

    with pytest.raises(AirflowException, match=r"Trigger .+ does not exist"):
        hook.update_trigger(*user_args)


@parametrize(
    explicit_factory=((NAME, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME)),
    implicit_factory=((NAME,), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME)),
)
def test_delete_trigger(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.delete_trigger(*user_args)

    hook._conn.triggers.delete.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME)),
    implicit_factory=((NAME,), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME)),
)
def test_start_trigger(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.start_trigger(*user_args)

    hook._conn.triggers.begin_start.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME)),
    implicit_factory=((NAME,), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME)),
)
def test_stop_trigger(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.stop_trigger(*user_args)

    hook._conn.triggers.begin_stop.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, ID, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME, ID)),
    implicit_factory=((NAME, ID), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME, ID)),
)
def test_rerun_trigger(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.rerun_trigger(*user_args)

    hook._conn.trigger_runs.rerun.assert_called_with(*sdk_args)


@parametrize(
    explicit_factory=((NAME, ID, RESOURCE_GROUP, FACTORY), (RESOURCE_GROUP, FACTORY, NAME, ID)),
    implicit_factory=((NAME, ID), (DEFAULT_RESOURCE_GROUP, DEFAULT_FACTORY, NAME, ID)),
)
def test_cancel_trigger(hook: AzureDataFactoryHook, user_args, sdk_args):
    hook.cancel_trigger(*user_args)

    hook._conn.trigger_runs.cancel.assert_called_with(*sdk_args)
