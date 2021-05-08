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
import logging
from io import StringIO
from typing import List, Optional, Union

from qds_sdk.commands import Command

from airflow.exceptions import AirflowException
from airflow.providers.qubole.hooks.qubole import QuboleHook

log = logging.getLogger(__name__)

COL_DELIM = '\t'
ROW_DELIM = '\r\n'


def isint(value) -> bool:
    """Whether Qubole column are integer"""
    try:
        int(value)
        return True
    except ValueError:
        return False


def isfloat(value) -> bool:
    """Whether Qubole column are float"""
    try:
        float(value)
        return True
    except ValueError:
        return False


def isbool(value) -> bool:
    """Whether Qubole column are boolean"""
    try:
        return value.lower() in ["true", "false"]
    except ValueError:
        return False


def parse_first_row(row_list) -> List[Union[bool, float, int, str]]:
    """Parse Qubole first record list"""
    record_list = []
    first_row = row_list[0] if row_list else ""

    for col_value in first_row.split(COL_DELIM):
        if isint(col_value):
            col_value = int(col_value)
        elif isfloat(col_value):
            col_value = float(col_value)
        elif isbool(col_value):
            col_value = col_value.lower() == "true"
        record_list.append(col_value)

    return record_list


class QuboleCheckHook(QuboleHook):
    """Qubole check hook"""

    def __init__(self, context, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.results_parser_callable = parse_first_row
        if 'results_parser_callable' in kwargs and kwargs['results_parser_callable'] is not None:
            if not callable(kwargs['results_parser_callable']):
                raise AirflowException('`results_parser_callable` param must be callable')
            self.results_parser_callable = kwargs['results_parser_callable']
        self.context = context

    @staticmethod
    def handle_failure_retry(context) -> None:
        ti = context['ti']
        cmd_id = ti.xcom_pull(key='qbol_cmd_id', task_ids=ti.task_id)

        if cmd_id is not None:
            cmd = Command.find(cmd_id)
            if cmd is not None:
                if cmd.status == 'running':
                    log.info('Cancelling the Qubole Command Id: %s', cmd_id)
                    cmd.cancel()

    def get_first(self, sql):  # pylint: disable=unused-argument
        """Get Qubole query first record list"""
        self.execute(context=self.context)
        query_result = self.get_query_results()
        row_list = list(filter(None, query_result.split(ROW_DELIM)))
        record_list = self.results_parser_callable(row_list)
        return record_list

    def get_query_results(self) -> Optional[str]:
        """Get Qubole query result"""
        if self.cmd is not None:
            cmd_id = self.cmd.id
            self.log.info("command id: %d", cmd_id)
            query_result_buffer = StringIO()
            self.cmd.get_results(fp=query_result_buffer, inline=True, delim=COL_DELIM, arguments=['true'])
            query_result = query_result_buffer.getvalue()
            query_result_buffer.close()
            return query_result
        else:
            self.log.error("Qubole command not found")
            return None
