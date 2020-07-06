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

import contextlib
import os
import sys
from datetime import datetime, timedelta
from unittest import mock
from unittest.mock import MagicMock, Mock

import pytest

from airflow import DAG, configuration
from airflow.exceptions import AirflowException
from airflow.models import TaskInstance
from airflow.models.baseoperator import BaseOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    "owner": "test",
    "depends_on_past": True,
    "start_date": days_ago(1),
    "end_date": datetime.today(),
    "schedule_interval": "@once",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@contextlib.contextmanager
def user_defined_contextmanager(task_instance, execution_context):  # pylint: disable=W0613
    try:
        yield True
    finally:
        pass


@contextlib.contextmanager
def incorrect_user_defined_context_func():
    return 8


def not_ctx_manager(task_instance, execution_context):  # pylint: disable=W0613
    return None


def get_user_contextmanager(section="core", key="additional_execute_contextmanager",
                            value="test_additional_execute_contextmanager.user_defined_contextmanager"):
    configuration.conf.set(section, key, value)
    sys.path.append(os.path.dirname(__file__))
    ti = Mock(TaskInstance)
    ti.configure_mock(
        get_additional_execution_contextmanager=TaskInstance.get_additional_execution_contextmanager)
    context = ti.get_additional_execution_contextmanager(None, None)

    return context


class TestUserDefinedContextLoad:
    def test_config_read(self):
        user_ctx = get_user_contextmanager()
        assert user_ctx

    def test_assert_exception_on_invalid_value(self):
        with pytest.raises(AirflowException):
            get_user_contextmanager(value="INVALID_PACKAGE.INVALID_MODULE.INVALID_FUNC")

    def test_user_func_incorrect_signature(self):
        with pytest.raises(TypeError):
            get_user_contextmanager(
                value="test_additional_execute_contextmanager.incorrect_user_defined_context_func")

    def test_user_func_not_ctx_manager(self):
        with pytest.raises(AirflowException):
            get_user_contextmanager(value="test_additional_execute_contextmanager.not_ctx_manager")

    def test_enter_exit_exists(self):
        user_ctx = get_user_contextmanager()
        assert user_ctx
        # Ensure these attributes were loaded
        assert user_ctx.__enter__
        assert user_ctx.__exit__


class TestUserDefinedContextRuntime:
    marker = MagicMock()
    enter_counter = 0
    exit_counter = 0

    @staticmethod
    def increment_enter_counter(p):  # pylint: disable=W0613
        TestUserDefinedContextRuntime.enter_counter += 1

    @staticmethod
    def increment_exit_counter(p1, p2, p3, p4):  # pylint: disable=W0613
        TestUserDefinedContextRuntime.exit_counter += 1

    def test_simple_runtime(self):
        # Configure mock so user context manager received is our mock marker object:
        # (TestUserDefinedContextRuntime.marker)
        attrs = {"__enter__": TestUserDefinedContextRuntime.increment_enter_counter,
                 "__exit__": TestUserDefinedContextRuntime.increment_exit_counter}
        TestUserDefinedContextRuntime.marker.configure_mock(**attrs)

        with mock.patch("test_additional_execute_contextmanager.user_defined_contextmanager",
                        return_value=TestUserDefinedContextRuntime.marker):
            configuration.conf.set(
                "core", "additional_execute_contextmanager", ""
                                                             "test_additional_execute_contextmanager"
                                                             ".user_defined_contextmanager")
            sys.path.append(os.path.dirname(__file__))

            with DAG(dag_id="context_runtime_dag", default_args=DEFAULT_ARGS):
                op = self.MySimpleOperator(task_id="check_affected_value")
            op.run(ignore_ti_state=True, ignore_first_depends_on_past=True)

            assert TestUserDefinedContextRuntime.marker.call_count == 1
            assert TestUserDefinedContextRuntime.enter_counter == 1
            assert TestUserDefinedContextRuntime.exit_counter == 1

    class MySimpleOperator(BaseOperator):
        def execute(self, context):
            TestUserDefinedContextRuntime.marker()
