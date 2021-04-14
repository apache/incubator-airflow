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
from datetime import datetime
from unittest import mock

import pytest
from qds_sdk.commands import HiveCommand

from airflow.exceptions import AirflowException
from airflow.models import DAG
from airflow.providers.qubole.hooks.qubole import QuboleHook
from airflow.providers.qubole.hooks.qubole_check import QuboleCheckHook
from airflow.providers.qubole.operators.qubole_check import (
    QuboleCheckOperator,
    QuboleValueCheckOperator,
    SQLCheckOperator,
    SQLValueCheckOperator,
    _QuboleCheckOperatorMixin,
)


# pylint: disable=unused-argument
@pytest.mark.parametrize(
    "operator_class, kwargs, parent_check_operator",
    [
        (QuboleCheckOperator, dict(sql='Select * from test_table'), SQLCheckOperator),
        (
            QuboleValueCheckOperator,
            dict(sql='Select * from test_table', pass_value=95),
            SQLValueCheckOperator,
        ),
    ],
)
class TestQuboleCheckMixin:
    def setup(self):
        self.task_id = 'test_task'

    def __construct_operator(self, operator_class, **kwargs):
        dag = DAG('test_dag', start_date=datetime(2017, 1, 1))
        return operator_class(task_id=self.task_id, dag=dag, command_type='hivecmd', **kwargs)

    def test_get_hook_with_context(self, operator_class, kwargs, parent_check_operator):
        operator = self.__construct_operator(operator_class=operator_class, **kwargs)
        assert isinstance(operator.get_hook(), QuboleCheckHook)

        context = {'exec_date': 'today'}
        operator._hook_context = context
        hook = operator.get_hook()
        assert hook.context == context

    @mock.patch.object(_QuboleCheckOperatorMixin, "get_db_hook")
    @mock.patch.object(_QuboleCheckOperatorMixin, "get_hook")
    def test_get_db_hook(
        self, mock_get_hook, mock_get_db_hook, operator_class, kwargs, parent_check_operator
    ):
        operator = self.__construct_operator(operator_class=operator_class, **kwargs)
        operator.get_db_hook()
        mock_get_db_hook.assert_called_once()

        operator.get_hook()
        mock_get_hook.assert_called_once()

    def test_execute(self, operator_class, kwargs, parent_check_operator):
        operator = self.__construct_operator(operator_class=operator_class, **kwargs)

        with mock.patch.object(parent_check_operator, 'execute') as mock_execute:
            operator.execute()
            mock_execute.assert_called_once()

    @mock.patch('airflow.providers.qubole.operators.qubole_check.handle_airflow_exception')
    def test_execute_fail(self, mock_handle_airflow_exception, operator_class, kwargs, parent_check_operator):
        operator = self.__construct_operator(operator_class=operator_class, **kwargs)

        with mock.patch.object(parent_check_operator, 'execute') as mock_execute:
            mock_execute.side_effect = AirflowException()
            operator.execute()
            mock_execute.assert_called_once()
            mock_handle_airflow_exception.assert_called_once()


class TestQuboleValueCheckOperator(unittest.TestCase):
    def setUp(self):
        self.task_id = 'test_task'
        self.conn_id = 'default_conn'

    def __construct_operator(self, query, pass_value, tolerance=None, results_parser_callable=None):

        dag = DAG('test_dag', start_date=datetime(2017, 1, 1))

        return QuboleValueCheckOperator(
            dag=dag,
            task_id=self.task_id,
            conn_id=self.conn_id,
            query=query,
            pass_value=pass_value,
            results_parser_callable=results_parser_callable,
            command_type='hivecmd',
            tolerance=tolerance,
        )

    def test_pass_value_template(self):
        pass_value_str = "2018-03-22"
        operator = self.__construct_operator('select date from tab1;', "{{ ds }}")
        result = operator.render_template(operator.pass_value, {'ds': pass_value_str})

        assert operator.task_id == self.task_id
        assert result == pass_value_str

    @mock.patch.object(QuboleValueCheckOperator, 'get_hook')
    def test_execute_pass(self, mock_get_hook):

        mock_hook = mock.Mock()
        mock_hook.get_first.return_value = [10]
        mock_get_hook.return_value = mock_hook

        query = 'select value from tab1 limit 1;'

        operator = self.__construct_operator(query, 5, 1)

        operator.execute(None)

        mock_hook.get_first.assert_called_once_with(query)

    @mock.patch.object(QuboleValueCheckOperator, 'get_hook')
    def test_execute_assertion_fail(self, mock_get_hook):

        mock_cmd = mock.Mock()
        mock_cmd.status = 'done'
        mock_cmd.id = 123
        mock_cmd.is_success = mock.Mock(return_value=HiveCommand.is_success(mock_cmd.status))

        mock_hook = mock.Mock()
        mock_hook.get_first.return_value = [11]
        mock_hook.cmd = mock_cmd
        mock_get_hook.return_value = mock_hook

        operator = self.__construct_operator('select value from tab1 limit 1;', 5, 1)

        with pytest.raises(AirflowException, match='Qubole Command Id: ' + str(mock_cmd.id)):
            operator.execute()

        mock_cmd.is_success.assert_called_once_with(mock_cmd.status)

    @mock.patch.object(QuboleValueCheckOperator, 'get_hook')
    def test_execute_assert_query_fail(self, mock_get_hook):

        mock_cmd = mock.Mock()
        mock_cmd.status = 'error'
        mock_cmd.id = 123
        mock_cmd.is_success = mock.Mock(return_value=HiveCommand.is_success(mock_cmd.status))

        mock_hook = mock.Mock()
        mock_hook.get_first.return_value = [11]
        mock_hook.cmd = mock_cmd
        mock_get_hook.return_value = mock_hook

        operator = self.__construct_operator('select value from tab1 limit 1;', 5, 1)

        with pytest.raises(AirflowException) as ctx:
            operator.execute()

        assert 'Qubole Command Id: ' not in str(ctx.value)
        mock_cmd.is_success.assert_called_once_with(mock_cmd.status)

    @mock.patch.object(QuboleCheckHook, 'get_query_results')
    @mock.patch.object(QuboleHook, 'execute')
    def test_results_parser_callable(self, mock_execute, mock_get_query_results):

        mock_execute.return_value = None

        pass_value = 'pass_value'
        mock_get_query_results.return_value = pass_value

        results_parser_callable = mock.Mock()
        results_parser_callable.return_value = [pass_value]

        operator = self.__construct_operator(
            'select value from tab1 limit 1;', pass_value, None, results_parser_callable
        )
        operator.execute()
        results_parser_callable.assert_called_once_with([pass_value])
