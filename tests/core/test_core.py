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

import multiprocessing
import os
import signal
import unittest
from datetime import timedelta
from time import sleep

import pytest

from airflow import settings
from airflow.exceptions import AirflowException, AirflowTaskTimeout
from airflow.hooks.base import BaseHook
from airflow.jobs.local_task_job import LocalTaskJob
from airflow.models import DagBag, DagRun, TaskFail, TaskInstance
from airflow.models.baseoperator import BaseOperator
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.check_operator import CheckOperator, ValueCheckOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.settings import Session
from airflow.utils.state import State
from airflow.utils.timezone import datetime
from airflow.utils.types import DagRunType
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_dags, clear_db_runs

DEV_NULL = '/dev/null'
DEFAULT_DATE = datetime(2015, 1, 1)
TEST_DAG_ID = 'unit_tests'


class OperatorSubclass(BaseOperator):
    """
    An operator to test template substitution
    """

    template_fields = ['some_templated_field']

    def __init__(self, some_templated_field, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.some_templated_field = some_templated_field

    def execute(self, context):
        pass


class TestCore(unittest.TestCase):
    default_scheduler_args = {"num_runs": 1}

    def setUp(self):
        self.dagbag = DagBag(dag_folder=DEV_NULL, include_examples=True, read_dags_from_db=False)
        self.args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        self.dag = DAG(TEST_DAG_ID, default_args=self.args)
        self.dag_bash = self.dagbag.dags['example_bash_operator']
        self.runme_0 = self.dag_bash.get_task('runme_0')
        self.run_after_loop = self.dag_bash.get_task('run_after_loop')
        self.run_this_last = self.dag_bash.get_task('run_this_last')

    def tearDown(self):
        session = Session()
        session.query(DagRun).filter(DagRun.dag_id == TEST_DAG_ID).delete(synchronize_session=False)
        session.query(TaskInstance).filter(TaskInstance.dag_id == TEST_DAG_ID).delete(
            synchronize_session=False
        )
        session.query(TaskFail).filter(TaskFail.dag_id == TEST_DAG_ID).delete(synchronize_session=False)
        session.commit()
        session.close()
        clear_db_dags()
        clear_db_runs()

    def test_check_operators(self):

        conn_id = "sqlite_default"

        captain_hook = BaseHook.get_hook(conn_id=conn_id)  # quite funny :D
        captain_hook.run("CREATE TABLE operator_test_table (a, b)")
        captain_hook.run("insert into operator_test_table values (1,2)")

        self.dag.create_dagrun(run_type=DagRunType.MANUAL, state=State.RUNNING, execution_date=DEFAULT_DATE)
        op = CheckOperator(
            task_id='check', sql="select count(*) from operator_test_table", conn_id=conn_id, dag=self.dag
        )

        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        op = ValueCheckOperator(
            task_id='value_check',
            pass_value=95,
            tolerance=0.1,
            conn_id=conn_id,
            sql="SELECT 100",
            dag=self.dag,
        )
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        captain_hook.run("drop table operator_test_table")

    def test_clear_api(self):
        task = self.dag_bash.tasks[0]
        task.clear(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, upstream=True, downstream=True)
        ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)
        ti.are_dependents_done()

    def test_illegal_args(self):
        """
        Tests that Operators reject illegal arguments
        """
        msg = 'Invalid arguments were passed to BashOperator (task_id: test_illegal_args).'
        with conf_vars({('operators', 'allow_illegal_arguments'): 'True'}):
            with pytest.warns(PendingDeprecationWarning) as warnings:
                BashOperator(
                    task_id='test_illegal_args',
                    bash_command='echo success',
                    dag=self.dag,
                    illegal_argument_1234='hello?',
                )
                assert any(msg in str(w) for w in warnings)

    def test_illegal_args_forbidden(self):
        """
        Tests that operators raise exceptions on illegal arguments when
        illegal arguments are not allowed.
        """
        with pytest.raises(AirflowException) as ctx:
            BashOperator(
                task_id='test_illegal_args',
                bash_command='echo success',
                dag=self.dag,
                illegal_argument_1234='hello?',
            )
        assert 'Invalid arguments were passed to BashOperator (task_id: test_illegal_args).' in str(ctx.value)

    def test_bash_operator(self):
        op = BashOperator(task_id='test_bash_operator', bash_command="echo success", dag=self.dag)
        self.dag.create_dagrun(run_type=DagRunType.MANUAL, state=State.RUNNING, execution_date=DEFAULT_DATE)

        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_bash_operator_multi_byte_output(self):
        op = BashOperator(
            task_id='test_multi_byte_bash_operator',
            bash_command="echo \u2600",
            dag=self.dag,
            output_encoding='utf-8',
        )
        self.dag.create_dagrun(run_type=DagRunType.MANUAL, state=State.RUNNING, execution_date=DEFAULT_DATE)
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_bash_operator_kill(self):
        import psutil

        sleep_time = "100%d" % os.getpid()
        op = BashOperator(
            task_id='test_bash_operator_kill',
            execution_timeout=timedelta(seconds=1),
            bash_command=f"/bin/bash -c 'sleep {sleep_time}'",
            dag=self.dag,
        )
        with pytest.raises(AirflowTaskTimeout):
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        sleep(2)
        pid = -1
        for proc in psutil.process_iter():
            if proc.cmdline() == ['sleep', sleep_time]:
                pid = proc.pid
        if pid != -1:
            os.kill(pid, signal.SIGTERM)
            self.fail("BashOperator's subprocess still running after stopping on timeout!")

    def test_on_failure_callback(self):
        # Annoying workaround for nonlocal not existing in python 2
        data = {'called': False}

        def check_failure(context, test_case=self):  # pylint: disable=unused-argument
            data['called'] = True
            error = context.get("exception")
            test_case.assertIsInstance(error, AirflowException)

        op = BashOperator(
            task_id='check_on_failure_callback',
            bash_command="exit 1",
            dag=self.dag,
            on_failure_callback=check_failure,
        )
        with pytest.raises(AirflowException):
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
        assert data['called']

    def test_dryrun(self):
        op = BashOperator(task_id='test_dryrun', bash_command="echo success", dag=self.dag)
        op.dry_run()

    def test_sqlite(self):
        import airflow.providers.sqlite.operators.sqlite

        op = airflow.providers.sqlite.operators.sqlite.SqliteOperator(
            task_id='time_sqlite', sql="CREATE TABLE IF NOT EXISTS unitest (dummy VARCHAR(20))", dag=self.dag
        )
        self.dag.create_dagrun(run_type=DagRunType.MANUAL, state=State.RUNNING, execution_date=DEFAULT_DATE)
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_timeout(self):
        op = PythonOperator(
            task_id='test_timeout',
            execution_timeout=timedelta(seconds=1),
            python_callable=lambda: sleep(5),
            dag=self.dag,
        )
        with pytest.raises(AirflowTaskTimeout):
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_python_op(self):
        def test_py_op(templates_dict, ds, **kwargs):
            if not templates_dict['ds'] == ds:
                raise Exception("failure")

        op = PythonOperator(
            task_id='test_py_op', python_callable=test_py_op, templates_dict={'ds': "{{ ds }}"}, dag=self.dag
        )
        self.dag.create_dagrun(run_type=DagRunType.MANUAL, state=State.RUNNING, execution_date=DEFAULT_DATE)
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_complex_template(self):
        def verify_templated_field(context):
            assert context['ti'].task.some_templated_field['bar'][1] == context['ds']

        op = OperatorSubclass(
            task_id='test_complex_template',
            some_templated_field={'foo': '123', 'bar': ['baz', '{{ ds }}']},
            dag=self.dag,
        )
        op.execute = verify_templated_field
        self.dag.create_dagrun(run_type=DagRunType.MANUAL, state=State.RUNNING, execution_date=DEFAULT_DATE)
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_template_non_bool(self):
        """
        Test templates can handle objects with no sense of truthiness
        """

        class NonBoolObject:
            def __len__(self):  # pylint: disable=invalid-length-returned
                return NotImplemented

            def __bool__(self):  # pylint: disable=invalid-bool-returned, bad-option-value
                return NotImplemented

        op = OperatorSubclass(
            task_id='test_bad_template_obj', some_templated_field=NonBoolObject(), dag=self.dag
        )
        op.resolve_template_files()

    def test_task_get_template(self):
        TI = TaskInstance
        ti = TI(task=self.runme_0, execution_date=DEFAULT_DATE)
        ti.dag = self.dag_bash
        self.dag_bash.create_dagrun(
            run_type=DagRunType.MANUAL, state=State.RUNNING, execution_date=DEFAULT_DATE
        )
        ti.run(ignore_ti_state=True)
        context = ti.get_template_context()

        # DEFAULT DATE is 2015-01-01
        assert context['ds'] == '2015-01-01'
        assert context['ds_nodash'] == '20150101'

        # next_ds is 2015-01-02 as the dag interval is daily
        assert context['next_ds'] == '2015-01-02'
        assert context['next_ds_nodash'] == '20150102'

        # prev_ds is 2014-12-31 as the dag interval is daily
        assert context['prev_ds'] == '2014-12-31'
        assert context['prev_ds_nodash'] == '20141231'

        assert context['ts'] == '2015-01-01T00:00:00+00:00'
        assert context['ts_nodash'] == '20150101T000000'
        assert context['ts_nodash_with_tz'] == '20150101T000000+0000'

        assert context['yesterday_ds'] == '2014-12-31'
        assert context['yesterday_ds_nodash'] == '20141231'

        assert context['tomorrow_ds'] == '2015-01-02'
        assert context['tomorrow_ds_nodash'] == '20150102'

    def test_local_task_job(self):
        TI = TaskInstance
        ti = TI(task=self.runme_0, execution_date=DEFAULT_DATE)
        job = LocalTaskJob(task_instance=ti, ignore_ti_state=True)
        job.run()

    def test_raw_job(self):
        TI = TaskInstance
        ti = TI(task=self.runme_0, execution_date=DEFAULT_DATE)
        ti.dag = self.dag_bash
        self.dag_bash.create_dagrun(
            run_type=DagRunType.MANUAL, state=State.RUNNING, execution_date=DEFAULT_DATE
        )
        ti.run(ignore_ti_state=True)

    def test_bad_trigger_rule(self):
        with pytest.raises(AirflowException):
            DummyOperator(task_id='test_bad_trigger', trigger_rule="non_existent", dag=self.dag)

    def test_terminate_task(self):
        """If a task instance's db state get deleted, it should fail"""
        from airflow.executors.sequential_executor import SequentialExecutor

        TI = TaskInstance
        dag = self.dagbag.dags.get('test_utils')
        task = dag.task_dict.get('sleeps_forever')

        ti = TI(task=task, execution_date=DEFAULT_DATE)
        job = LocalTaskJob(task_instance=ti, ignore_ti_state=True, executor=SequentialExecutor())

        # Running task instance asynchronously
        proc = multiprocessing.Process(target=job.run)
        proc.start()
        sleep(5)
        settings.engine.dispose()
        session = settings.Session()
        ti.refresh_from_db(session=session)
        # making sure it's actually running
        assert State.RUNNING == ti.state
        ti = (
            session.query(TI)
            .filter_by(dag_id=task.dag_id, task_id=task.task_id, execution_date=DEFAULT_DATE)
            .one()
        )

        # deleting the instance should result in a failure
        session.delete(ti)
        session.commit()
        # waiting for the async task to finish
        proc.join()

        # making sure that the task ended up as failed
        ti.refresh_from_db(session=session)
        assert State.FAILED == ti.state
        session.close()

    def test_task_fail_duration(self):
        """If a task fails, the duration should be recorded in TaskFail"""

        op1 = BashOperator(task_id='pass_sleepy', bash_command='sleep 3', dag=self.dag)
        op2 = BashOperator(
            task_id='fail_sleepy',
            bash_command='sleep 5',
            execution_timeout=timedelta(seconds=3),
            retry_delay=timedelta(seconds=0),
            dag=self.dag,
        )
        session = settings.Session()
        try:
            op1.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
        except Exception:  # pylint: disable=broad-except
            pass
        try:
            op2.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
        except Exception:  # pylint: disable=broad-except
            pass
        op1_fails = (
            session.query(TaskFail)
            .filter_by(task_id='pass_sleepy', dag_id=self.dag.dag_id, execution_date=DEFAULT_DATE)
            .all()
        )
        op2_fails = (
            session.query(TaskFail)
            .filter_by(task_id='fail_sleepy', dag_id=self.dag.dag_id, execution_date=DEFAULT_DATE)
            .all()
        )

        assert 0 == len(op1_fails)
        assert 1 == len(op2_fails)
        assert sum([f.duration for f in op2_fails]) >= 3

    def test_externally_triggered_dagrun(self):
        TI = TaskInstance

        # Create the dagrun between two "scheduled" execution dates of the DAG
        execution_date = DEFAULT_DATE + timedelta(days=2)
        execution_ds = execution_date.strftime('%Y-%m-%d')
        execution_ds_nodash = execution_ds.replace('-', '')

        dag = DAG(
            TEST_DAG_ID, default_args=self.args, schedule_interval=timedelta(weeks=1), start_date=DEFAULT_DATE
        )
        task = DummyOperator(task_id='test_externally_triggered_dag_context', dag=dag)
        dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=execution_date,
            state=State.RUNNING,
            external_trigger=True,
        )
        task.run(start_date=execution_date, end_date=execution_date)

        ti = TI(task=task, execution_date=execution_date)
        context = ti.get_template_context()

        # next_ds/prev_ds should be the execution date for manually triggered runs
        assert context['next_ds'] == execution_ds
        assert context['next_ds_nodash'] == execution_ds_nodash

        assert context['prev_ds'] == execution_ds
        assert context['prev_ds_nodash'] == execution_ds_nodash

    def test_dag_params_and_task_params(self):
        # This test case guards how params of DAG and Operator work together.
        # - If any key exists in either DAG's or Operator's params,
        #   it is guaranteed to be available eventually.
        # - If any key exists in both DAG's params and Operator's params,
        #   the latter has precedence.
        TI = TaskInstance

        dag = DAG(
            TEST_DAG_ID,
            default_args=self.args,
            schedule_interval=timedelta(weeks=1),
            start_date=DEFAULT_DATE,
            params={'key_1': 'value_1', 'key_2': 'value_2_old'},
        )
        task1 = DummyOperator(
            task_id='task1',
            dag=dag,
            params={'key_2': 'value_2_new', 'key_3': 'value_3'},
        )
        task2 = DummyOperator(task_id='task2', dag=dag)
        dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
            external_trigger=True,
        )
        task1.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        task2.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        ti1 = TI(task=task1, execution_date=DEFAULT_DATE)
        ti2 = TI(task=task2, execution_date=DEFAULT_DATE)
        context1 = ti1.get_template_context()
        context2 = ti2.get_template_context()

        assert context1['params'] == {'key_1': 'value_1', 'key_2': 'value_2_new', 'key_3': 'value_3'}
        assert context2['params'] == {'key_1': 'value_1', 'key_2': 'value_2_old'}
