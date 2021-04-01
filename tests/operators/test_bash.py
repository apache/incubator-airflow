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
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile
from unittest import mock

import pytest
from parameterized import parameterized

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models import DagRun
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.utils import timezone
from airflow.utils.state import State
from airflow.utils.types import DagRunType

DEFAULT_DATE = datetime(2016, 1, 1, tzinfo=timezone.utc)
END_DATE = datetime(2016, 1, 2, tzinfo=timezone.utc)
INTERVAL = timedelta(hours=12)


class TestBashOperator(unittest.TestCase):
    def test_echo_env_variables(self):
        """
        Test that env variables are exported correctly to the task bash environment.
        """
        utc_now = datetime.utcnow().replace(tzinfo=timezone.utc)
        dag = DAG(
            dag_id='bash_op_test',
            default_args={'owner': 'airflow', 'retries': 100, 'start_date': DEFAULT_DATE},
            schedule_interval='@daily',
            dagrun_timeout=timedelta(minutes=60),
        )

        dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            execution_date=utc_now,
            start_date=utc_now,
            state=State.RUNNING,
            external_trigger=False,
        )

        with NamedTemporaryFile() as tmp_file:
            task = BashOperator(
                task_id='echo_env_vars',
                dag=dag,
                bash_command='echo $AIRFLOW_HOME>> {0};'
                'echo $PYTHONPATH>> {0};'
                'echo $AIRFLOW_CTX_DAG_ID >> {0};'
                'echo $AIRFLOW_CTX_TASK_ID>> {0};'
                'echo $AIRFLOW_CTX_EXECUTION_DATE>> {0};'
                'echo $AIRFLOW_CTX_DAG_RUN_ID>> {0};'.format(tmp_file.name),
            )

            with mock.patch.dict(
                'os.environ', {'AIRFLOW_HOME': 'MY_PATH_TO_AIRFLOW_HOME', 'PYTHONPATH': 'AWESOME_PYTHONPATH'}
            ):
                task.run(utc_now, utc_now, ignore_first_depends_on_past=True, ignore_ti_state=True)

            with open(tmp_file.name) as file:
                output = ''.join(file.readlines())
                assert 'MY_PATH_TO_AIRFLOW_HOME' in output
                # exported in run-tests as part of PYTHONPATH
                assert 'AWESOME_PYTHONPATH' in output
                assert 'bash_op_test' in output
                assert 'echo_env_vars' in output
                assert utc_now.isoformat() in output
                assert DagRun.generate_run_id(DagRunType.MANUAL, utc_now) in output

    @parameterized.expand(
        [
            ('test-val', 'test-val'),
            ('test-val\ntest-val\n', ''),
            ('test-val\ntest-val', 'test-val'),
            ('', ''),
        ]
    )
    def test_return_value(self, val, expected):
        op = BashOperator(task_id='abc', bash_command=f'set -e; echo "{val}";')
        line = op.execute({})
        assert line == expected

    def test_raise_exception_on_non_zero_exit_code(self):
        bash_operator = BashOperator(bash_command='exit 42', task_id='test_return_value', dag=None)
        with pytest.raises(
            AirflowException, match="Bash command failed\\. The command returned a non-zero exit code\\."
        ):
            bash_operator.execute(context={})

    def test_task_retries(self):
        bash_operator = BashOperator(
            bash_command='echo "stdout"', task_id='test_task_retries', retries=2, dag=None
        )

        assert bash_operator.retries == 2

    def test_default_retries(self):
        bash_operator = BashOperator(bash_command='echo "stdout"', task_id='test_default_retries', dag=None)

        assert bash_operator.retries == 0

    def test_command_not_found(self):
        with pytest.raises(
            AirflowException, match="Bash command failed\\. The command returned a non-zero exit code\\."
        ):
            BashOperator(task_id='abc', bash_command='set -e; something-that-isnt-on-path').execute({})

    @parameterized.expand(
        [
            (None, 99, AirflowSkipException),
            ({'skip_exit_code': 100}, 100, AirflowSkipException),
            ({'skip_exit_code': 100}, 101, AirflowException),
            ({'skip_exit_code': None}, 99, AirflowException),
        ]
    )
    def test_skip(self, extra_kwargs, actual_exit_code, expected_exc):
        kwargs = dict(task_id='abc', bash_command=f'set -e; echo "hello world"; exit {actual_exit_code};')
        if extra_kwargs:
            kwargs.update(**extra_kwargs)
        with pytest.raises(expected_exc):
            BashOperator(**kwargs).execute({})
