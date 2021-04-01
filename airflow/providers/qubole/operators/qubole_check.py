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
from typing import Callable, Iterable, Optional, Union

from airflow.exceptions import AirflowException
from airflow.operators.sql import SQLCheckOperator, SQLValueCheckOperator
from airflow.providers.qubole.hooks.qubole_check import QuboleCheckHook
from airflow.providers.qubole.operators.qubole import QuboleOperator
from airflow.utils.decorators import apply_defaults


class _QuboleCheckOperatorMixin:
    """This is a Mixin for Qubole related check operators"""

    def execute(self, context=None) -> None:
        """Execute a check operation against Qubole"""
        try:
            self._hook_context = context
            super().execute(context=context)
        except AirflowException as e:
            handle_airflow_exception(e, self.get_hook())

    def get_db_hook(self) -> QuboleCheckHook:
        """Get QuboleCheckHook"""
        return self.get_hook()

    # this overwrite the original QuboleOperator.get_hook() which returns a QuboleHook.
    def get_hook(self) -> QuboleCheckHook:
        """Reinitialising the hook, as some template fields might have changed"""
        return QuboleCheckHook(
            context=self._hook_context, results_parser_callable=self.results_parser_callable, **self.kwargs
        )


# pylint: disable=too-many-ancestors
class QuboleCheckOperator(_QuboleCheckOperatorMixin, SQLCheckOperator, QuboleOperator):
    """
    Performs checks against Qubole Commands. ``QuboleCheckOperator`` expects
    a command that will be executed on QDS.
    By default, each value on first row of the result of this Qubole Command
    is evaluated using python ``bool`` casting. If any of the
    values return ``False``, the check is failed and errors out.

    Note that Python bool casting evals the following as ``False``:

    * ``False``
    * ``0``
    * Empty string (``""``)
    * Empty list (``[]``)
    * Empty dictionary or set (``{}``)

    Given a query like ``SELECT COUNT(*) FROM foo``, it will fail only if
    the count ``== 0``. You can craft much more complex query that could,
    for instance, check that the table has the same number of rows as
    the source table upstream, or that the count of today's partition is
    greater than yesterday's partition, or that a set of metrics are less
    than 3 standard deviation for the 7 day average.

    This operator can be used as a data quality check in your pipeline, and
    depending on where you put it in your DAG, you have the choice to
    stop the critical path, preventing from
    publishing dubious data, or on the side and receive email alerts
    without stopping the progress of the DAG.

    :param qubole_conn_id: Connection id which consists of qds auth_token
    :type qubole_conn_id: str

    kwargs:

        Arguments specific to Qubole command can be referred from QuboleOperator docs.

        :results_parser_callable: This is an optional parameter to
            extend the flexibility of parsing the results of Qubole
            command to the users. This is a python callable which
            can hold the logic to parse list of rows returned by Qubole command.
            By default, only the values on first row are used for performing checks.
            This callable should return a list of records on
            which the checks have to be performed.

    .. note:: All fields in common with template fields of
        QuboleOperator and SQLCheckOperator are template-supported.

    """

    template_fields: Iterable[str] = set(QuboleOperator.template_fields) | set(
        SQLCheckOperator.template_fields
    )
    template_ext = QuboleOperator.template_ext
    ui_fgcolor = '#000'

    @apply_defaults
    def __init__(
        self, *, qubole_conn_id: str = "qubole_default", results_parser_callable: Callable = None, **kwargs
    ) -> None:
        sql = get_sql_from_qbol_cmd(kwargs)
        kwargs.pop('sql', None)
        super().__init__(qubole_conn_id=qubole_conn_id, sql=sql, **kwargs)
        self.results_parser_callable = results_parser_callable
        self.on_failure_callback = QuboleCheckHook.handle_failure_retry
        self.on_retry_callback = QuboleCheckHook.handle_failure_retry
        self._hook_context = None


# TODO(xinbinhuang): refactor to reduce levels of inheritance
# pylint: disable=too-many-ancestors
class QuboleValueCheckOperator(_QuboleCheckOperatorMixin, SQLValueCheckOperator, QuboleOperator):
    """
    Performs a simple value check using Qubole command.
    By default, each value on the first row of this
    Qubole command is compared with a pre-defined value.
    The check fails and errors out if the output of the command
    is not within the permissible limit of expected value.

    :param qubole_conn_id: Connection id which consists of qds auth_token
    :type qubole_conn_id: str

    :param pass_value: Expected value of the query results.
    :type pass_value: str or int or float

    :param tolerance: Defines the permissible pass_value range, for example if
        tolerance is 2, the Qubole command output can be anything between
        -2*pass_value and 2*pass_value, without the operator erring out.

    :type tolerance: int or float


    kwargs:

        Arguments specific to Qubole command can be referred from QuboleOperator docs.

        :results_parser_callable: This is an optional parameter to
            extend the flexibility of parsing the results of Qubole
            command to the users. This is a python callable which
            can hold the logic to parse list of rows returned by Qubole command.
            By default, only the values on first row are used for performing checks.
            This callable should return a list of records on
            which the checks have to be performed.


    .. note:: All fields in common with template fields of
            QuboleOperator and SQLValueCheckOperator are template-supported.
    """

    template_fields = set(QuboleOperator.template_fields) | set(SQLValueCheckOperator.template_fields)
    template_ext = QuboleOperator.template_ext
    ui_fgcolor = '#000'

    @apply_defaults
    def __init__(
        self,
        *,
        pass_value: Union[str, int, float],
        tolerance: Optional[Union[int, float]] = None,
        results_parser_callable: Callable = None,
        qubole_conn_id: str = "qubole_default",
        **kwargs,
    ) -> None:
        sql = get_sql_from_qbol_cmd(kwargs)
        kwargs.pop('sql', None)
        super().__init__(
            qubole_conn_id=qubole_conn_id, sql=sql, pass_value=pass_value, tolerance=tolerance, **kwargs
        )
        self.results_parser_callable = results_parser_callable
        self.on_failure_callback = QuboleCheckHook.handle_failure_retry
        self.on_retry_callback = QuboleCheckHook.handle_failure_retry
        self._hook_context = None


def get_sql_from_qbol_cmd(params) -> str:
    """Get Qubole sql from Qubole command"""
    sql = ''
    if 'query' in params:
        sql = params['query']
    elif 'sql' in params:
        sql = params['sql']
    return sql


def handle_airflow_exception(airflow_exception, hook: QuboleCheckHook):
    """Qubole check handle Airflow exception"""
    cmd = hook.cmd
    if cmd is not None:
        if cmd.is_success(cmd.status):
            qubole_command_results = hook.get_query_results()
            qubole_command_id = cmd.id
            exception_message = (
                '\nQubole Command Id: {qubole_command_id}'
                '\nQubole Command Results:'
                '\n{qubole_command_results}'.format(
                    qubole_command_id=qubole_command_id, qubole_command_results=qubole_command_results
                )
            )
            raise AirflowException(str(airflow_exception) + exception_message)
    raise AirflowException(str(airflow_exception))
