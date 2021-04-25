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
# pylint: disable=attribute-defined-outside-init
import datetime
import os
import shutil
import unittest
from datetime import timedelta
from tempfile import NamedTemporaryFile, mkdtemp
from time import sleep
from unittest import mock
from unittest.mock import MagicMock, patch
from zipfile import ZipFile

import psutil
import pytest
from freezegun import freeze_time
from parameterized import parameterized
from sqlalchemy import func

import airflow.example_dags
import airflow.smart_sensor_dags
from airflow import settings
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.executors.base_executor import BaseExecutor
from airflow.jobs.backfill_job import BackfillJob
from airflow.jobs.scheduler_job import DagFileProcessor, SchedulerJob
from airflow.models import DAG, DagBag, DagModel, Pool, SlaMiss, TaskInstance, errors
from airflow.models.dagrun import DagRun
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.taskinstance import SimpleTaskInstance, TaskInstanceKey
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.serialization.serialized_objects import SerializedDAG
from airflow.utils import timezone
from airflow.utils.callback_requests import DagCallbackRequest, TaskCallbackRequest
from airflow.utils.dag_processing import DagFileProcessorAgent
from airflow.utils.dates import days_ago
from airflow.utils.file import list_py_file_paths
from airflow.utils.session import create_session, provide_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from tests.test_utils.asserts import assert_queries_count
from tests.test_utils.config import conf_vars, env_vars
from tests.test_utils.db import (
    clear_db_dags,
    clear_db_import_errors,
    clear_db_jobs,
    clear_db_pools,
    clear_db_runs,
    clear_db_serialized_dags,
    clear_db_sla_miss,
    set_default_pool_slots,
)
from tests.test_utils.mock_executor import MockExecutor
from tests.test_utils.mock_operators import CustomOperator

ROOT_FOLDER = os.path.realpath(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, os.pardir)
)
PERF_DAGS_FOLDER = os.path.join(ROOT_FOLDER, "tests", "test_utils", "perf", "dags")
ELASTIC_DAG_FILE = os.path.join(PERF_DAGS_FOLDER, "elastic_dag.py")

TEST_DAG_FOLDER = os.environ['AIRFLOW__CORE__DAGS_FOLDER']
DEFAULT_DATE = timezone.datetime(2016, 1, 1)
TRY_NUMBER = 1
# Include the words "airflow" and "dag" in the file contents,
# tricking airflow into thinking these
# files contain a DAG (otherwise Airflow will skip them)
PARSEABLE_DAG_FILE_CONTENTS = '"airflow DAG"'
UNPARSEABLE_DAG_FILE_CONTENTS = 'airflow DAG'
INVALID_DAG_WITH_DEPTH_FILE_CONTENTS = "def something():\n    return airflow_DAG\nsomething()"

# Filename to be used for dags that are created in an ad-hoc manner and can be removed/
# created at runtime
TEMP_DAG_FILENAME = "temp_dag.py"


@pytest.fixture(scope="class")
def disable_load_example():
    with conf_vars({('core', 'load_examples'): 'false'}):
        with env_vars({('core', 'load_examples'): 'false'}):
            yield


@pytest.mark.usefixtures("disable_load_example")
class TestDagFileProcessor(unittest.TestCase):
    @staticmethod
    def clean_db():
        clear_db_runs()
        clear_db_pools()
        clear_db_dags()
        clear_db_sla_miss()
        clear_db_import_errors()
        clear_db_jobs()
        clear_db_serialized_dags()

    def setUp(self):
        self.clean_db()

        # Speed up some tests by not running the tasks, just look at what we
        # enqueue!
        self.null_exec = MockExecutor()
        self.scheduler_job = None

    def tearDown(self) -> None:
        if self.scheduler_job and self.scheduler_job.processor_agent:
            self.scheduler_job.processor_agent.end()
            self.scheduler_job = None
        self.clean_db()

    def create_test_dag(self, start_date=DEFAULT_DATE, end_date=DEFAULT_DATE + timedelta(hours=1), **kwargs):
        dag = DAG(
            dag_id='test_scheduler_reschedule',
            start_date=start_date,
            # Make sure it only creates a single DAG Run
            end_date=end_date,
        )
        dag.clear()
        dag.is_subdag = False
        with create_session() as session:
            orm_dag = DagModel(dag_id=dag.dag_id, is_paused=False)
            session.merge(orm_dag)
            session.commit()
        return dag

    @classmethod
    def setUpClass(cls):
        # Ensure the DAGs we are looking at from the DB are up-to-date
        non_serialized_dagbag = DagBag(read_dags_from_db=False, include_examples=False)
        non_serialized_dagbag.sync_to_db()
        cls.dagbag = DagBag(read_dags_from_db=True)

    def test_dag_file_processor_sla_miss_callback(self):
        """
        Test that the dag file processor calls the sla miss callback
        """
        session = settings.Session()

        sla_callback = MagicMock()

        # Create dag with a start of 1 day ago, but an sla of 0
        # so we'll already have an sla_miss on the books.
        test_start_date = days_ago(1)
        dag = DAG(
            dag_id='test_sla_miss',
            sla_miss_callback=sla_callback,
            default_args={'start_date': test_start_date, 'sla': datetime.timedelta()},
        )

        task = DummyOperator(task_id='dummy', dag=dag, owner='airflow')

        session.merge(TaskInstance(task=task, execution_date=test_start_date, state='success'))

        session.merge(SlaMiss(task_id='dummy', dag_id='test_sla_miss', execution_date=test_start_date))

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        dag_file_processor.manage_slas(dag=dag, session=session)

        assert sla_callback.called

    def test_dag_file_processor_sla_miss_callback_invalid_sla(self):
        """
        Test that the dag file processor does not call the sla miss callback when
        given an invalid sla
        """
        session = settings.Session()

        sla_callback = MagicMock()

        # Create dag with a start of 1 day ago, but an sla of 0
        # so we'll already have an sla_miss on the books.
        # Pass anything besides a timedelta object to the sla argument.
        test_start_date = days_ago(1)
        dag = DAG(
            dag_id='test_sla_miss',
            sla_miss_callback=sla_callback,
            default_args={'start_date': test_start_date, 'sla': None},
        )

        task = DummyOperator(task_id='dummy', dag=dag, owner='airflow')

        session.merge(TaskInstance(task=task, execution_date=test_start_date, state='success'))

        session.merge(SlaMiss(task_id='dummy', dag_id='test_sla_miss', execution_date=test_start_date))

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        dag_file_processor.manage_slas(dag=dag, session=session)
        sla_callback.assert_not_called()

    def test_dag_file_processor_sla_miss_callback_sent_notification(self):
        """
        Test that the dag file processor does not call the sla_miss_callback when a
        notification has already been sent
        """
        session = settings.Session()

        # Mock the callback function so we can verify that it was not called
        sla_callback = MagicMock()

        # Create dag with a start of 2 days ago, but an sla of 1 day
        # ago so we'll already have an sla_miss on the books
        test_start_date = days_ago(2)
        dag = DAG(
            dag_id='test_sla_miss',
            sla_miss_callback=sla_callback,
            default_args={'start_date': test_start_date, 'sla': datetime.timedelta(days=1)},
        )

        task = DummyOperator(task_id='dummy', dag=dag, owner='airflow')

        # Create a TaskInstance for two days ago
        session.merge(TaskInstance(task=task, execution_date=test_start_date, state='success'))

        # Create an SlaMiss where notification was sent, but email was not
        session.merge(
            SlaMiss(
                task_id='dummy',
                dag_id='test_sla_miss',
                execution_date=test_start_date,
                email_sent=False,
                notification_sent=True,
            )
        )

        # Now call manage_slas and see if the sla_miss callback gets called
        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        dag_file_processor.manage_slas(dag=dag, session=session)

        sla_callback.assert_not_called()

    def test_dag_file_processor_sla_miss_callback_exception(self):
        """
        Test that the dag file processor gracefully logs an exception if there is a problem
        calling the sla_miss_callback
        """
        session = settings.Session()

        sla_callback = MagicMock(side_effect=RuntimeError('Could not call function'))

        test_start_date = days_ago(2)
        dag = DAG(
            dag_id='test_sla_miss',
            sla_miss_callback=sla_callback,
            default_args={'start_date': test_start_date},
        )

        task = DummyOperator(task_id='dummy', dag=dag, owner='airflow', sla=datetime.timedelta(hours=1))

        session.merge(TaskInstance(task=task, execution_date=test_start_date, state='Success'))

        # Create an SlaMiss where notification was sent, but email was not
        session.merge(SlaMiss(task_id='dummy', dag_id='test_sla_miss', execution_date=test_start_date))

        # Now call manage_slas and see if the sla_miss callback gets called
        mock_log = mock.MagicMock()
        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock_log)
        dag_file_processor.manage_slas(dag=dag, session=session)
        assert sla_callback.called
        mock_log.exception.assert_called_once_with(
            'Could not call sla_miss_callback for DAG %s', 'test_sla_miss'
        )

    @mock.patch('airflow.jobs.scheduler_job.send_email')
    def test_dag_file_processor_only_collect_emails_from_sla_missed_tasks(self, mock_send_email):
        session = settings.Session()

        test_start_date = days_ago(2)
        dag = DAG(
            dag_id='test_sla_miss',
            default_args={'start_date': test_start_date, 'sla': datetime.timedelta(days=1)},
        )

        email1 = 'test1@test.com'
        task = DummyOperator(
            task_id='sla_missed', dag=dag, owner='airflow', email=email1, sla=datetime.timedelta(hours=1)
        )

        session.merge(TaskInstance(task=task, execution_date=test_start_date, state='Success'))

        email2 = 'test2@test.com'
        DummyOperator(task_id='sla_not_missed', dag=dag, owner='airflow', email=email2)

        session.merge(SlaMiss(task_id='sla_missed', dag_id='test_sla_miss', execution_date=test_start_date))

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())

        dag_file_processor.manage_slas(dag=dag, session=session)

        assert len(mock_send_email.call_args_list) == 1

        send_email_to = mock_send_email.call_args_list[0][0][0]
        assert email1 in send_email_to
        assert email2 not in send_email_to

    @mock.patch('airflow.jobs.scheduler_job.Stats.incr')
    @mock.patch("airflow.utils.email.send_email")
    def test_dag_file_processor_sla_miss_email_exception(self, mock_send_email, mock_stats_incr):
        """
        Test that the dag file processor gracefully logs an exception if there is a problem
        sending an email
        """
        session = settings.Session()

        # Mock the callback function so we can verify that it was not called
        mock_send_email.side_effect = RuntimeError('Could not send an email')

        test_start_date = days_ago(2)
        dag = DAG(
            dag_id='test_sla_miss',
            default_args={'start_date': test_start_date, 'sla': datetime.timedelta(days=1)},
        )

        task = DummyOperator(
            task_id='dummy', dag=dag, owner='airflow', email='test@test.com', sla=datetime.timedelta(hours=1)
        )

        session.merge(TaskInstance(task=task, execution_date=test_start_date, state='Success'))

        # Create an SlaMiss where notification was sent, but email was not
        session.merge(SlaMiss(task_id='dummy', dag_id='test_sla_miss', execution_date=test_start_date))

        mock_log = mock.MagicMock()
        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock_log)

        dag_file_processor.manage_slas(dag=dag, session=session)
        mock_log.exception.assert_called_once_with(
            'Could not send SLA Miss email notification for DAG %s', 'test_sla_miss'
        )
        mock_stats_incr.assert_called_once_with('sla_email_notification_failure')

    def test_dag_file_processor_sla_miss_deleted_task(self):
        """
        Test that the dag file processor will not crash when trying to send
        sla miss notification for a deleted task
        """
        session = settings.Session()

        test_start_date = days_ago(2)
        dag = DAG(
            dag_id='test_sla_miss',
            default_args={'start_date': test_start_date, 'sla': datetime.timedelta(days=1)},
        )

        task = DummyOperator(
            task_id='dummy', dag=dag, owner='airflow', email='test@test.com', sla=datetime.timedelta(hours=1)
        )

        session.merge(TaskInstance(task=task, execution_date=test_start_date, state='Success'))

        # Create an SlaMiss where notification was sent, but email was not
        session.merge(
            SlaMiss(task_id='dummy_deleted', dag_id='test_sla_miss', execution_date=test_start_date)
        )

        mock_log = mock.MagicMock()
        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock_log)
        dag_file_processor.manage_slas(dag=dag, session=session)

    @parameterized.expand(
        [
            [State.NONE, None, None],
            [
                State.UP_FOR_RETRY,
                timezone.utcnow() - datetime.timedelta(minutes=30),
                timezone.utcnow() - datetime.timedelta(minutes=15),
            ],
            [
                State.UP_FOR_RESCHEDULE,
                timezone.utcnow() - datetime.timedelta(minutes=30),
                timezone.utcnow() - datetime.timedelta(minutes=15),
            ],
        ]
    )
    def test_dag_file_processor_process_task_instances(self, state, start_date, end_date):
        """
        Test if _process_task_instances puts the right task instances into the
        mock_list.
        """
        dag = DAG(dag_id='test_scheduler_process_execute_task', start_date=DEFAULT_DATE)
        BashOperator(task_id='dummy', dag=dag, owner='airflow', bash_command='echo hi')

        with create_session() as session:
            orm_dag = DagModel(dag_id=dag.dag_id)
            session.merge(orm_dag)

        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        self.scheduler_job.processor_agent = mock.MagicMock()
        self.scheduler_job.dagbag.bag_dag(dag, root_dag=dag)
        dag.clear()
        dr = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        assert dr is not None

        with create_session() as session:
            ti = dr.get_task_instances(session=session)[0]
            ti.state = state
            ti.start_date = start_date
            ti.end_date = end_date

            count = self.scheduler_job._schedule_dag_run(dr, set(), session)
            assert count == 1

            session.refresh(ti)
            assert ti.state == State.SCHEDULED

    @parameterized.expand(
        [
            [State.NONE, None, None],
            [
                State.UP_FOR_RETRY,
                timezone.utcnow() - datetime.timedelta(minutes=30),
                timezone.utcnow() - datetime.timedelta(minutes=15),
            ],
            [
                State.UP_FOR_RESCHEDULE,
                timezone.utcnow() - datetime.timedelta(minutes=30),
                timezone.utcnow() - datetime.timedelta(minutes=15),
            ],
        ]
    )
    def test_dag_file_processor_process_task_instances_with_task_concurrency(
        self,
        state,
        start_date,
        end_date,
    ):
        """
        Test if _process_task_instances puts the right task instances into the
        mock_list.
        """
        dag = DAG(dag_id='test_scheduler_process_execute_task_with_task_concurrency', start_date=DEFAULT_DATE)
        BashOperator(task_id='dummy', task_concurrency=2, dag=dag, owner='airflow', bash_command='echo Hi')

        with create_session() as session:
            orm_dag = DagModel(dag_id=dag.dag_id)
            session.merge(orm_dag)

        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        self.scheduler_job.processor_agent = mock.MagicMock()
        self.scheduler_job.dagbag.bag_dag(dag, root_dag=dag)
        dag.clear()
        dr = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        assert dr is not None

        with create_session() as session:
            ti = dr.get_task_instances(session=session)[0]
            ti.state = state
            ti.start_date = start_date
            ti.end_date = end_date

            count = self.scheduler_job._schedule_dag_run(dr, set(), session)
            assert count == 1

            session.refresh(ti)
            assert ti.state == State.SCHEDULED

    @parameterized.expand(
        [
            [State.NONE, None, None],
            [
                State.UP_FOR_RETRY,
                timezone.utcnow() - datetime.timedelta(minutes=30),
                timezone.utcnow() - datetime.timedelta(minutes=15),
            ],
            [
                State.UP_FOR_RESCHEDULE,
                timezone.utcnow() - datetime.timedelta(minutes=30),
                timezone.utcnow() - datetime.timedelta(minutes=15),
            ],
        ]
    )
    def test_dag_file_processor_process_task_instances_depends_on_past(self, state, start_date, end_date):
        """
        Test if _process_task_instances puts the right task instances into the
        mock_list.
        """
        dag = DAG(
            dag_id='test_scheduler_process_execute_task_depends_on_past',
            start_date=DEFAULT_DATE,
            default_args={
                'depends_on_past': True,
            },
        )
        BashOperator(task_id='dummy1', dag=dag, owner='airflow', bash_command='echo hi')
        BashOperator(task_id='dummy2', dag=dag, owner='airflow', bash_command='echo hi')

        with create_session() as session:
            orm_dag = DagModel(dag_id=dag.dag_id)
            session.merge(orm_dag)

        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        self.scheduler_job.processor_agent = mock.MagicMock()
        self.scheduler_job.dagbag.bag_dag(dag, root_dag=dag)
        dag.clear()
        dr = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        assert dr is not None

        with create_session() as session:
            tis = dr.get_task_instances(session=session)
            for ti in tis:
                ti.state = state
                ti.start_date = start_date
                ti.end_date = end_date

            count = self.scheduler_job._schedule_dag_run(dr, set(), session)
            assert count == 2

            session.refresh(tis[0])
            session.refresh(tis[1])
            assert tis[0].state == State.SCHEDULED
            assert tis[1].state == State.SCHEDULED

    def test_scheduler_job_add_new_task(self):
        """
        Test if a task instance will be added if the dag is updated
        """
        dag = DAG(dag_id='test_scheduler_add_new_task', start_date=DEFAULT_DATE)
        BashOperator(task_id='dummy', dag=dag, owner='airflow', bash_command='echo test')

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        self.scheduler_job.dagbag.bag_dag(dag, root_dag=dag)

        # Since we don't want to store the code for the DAG defined in this file
        with mock.patch.object(settings, "STORE_DAG_CODE", False):
            self.scheduler_job.dagbag.sync_to_db()

        session = settings.Session()
        orm_dag = session.query(DagModel).get(dag.dag_id)
        assert orm_dag is not None

        if self.scheduler_job.processor_agent:
            self.scheduler_job.processor_agent.end()
        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        self.scheduler_job.processor_agent = mock.MagicMock()
        dag = self.scheduler_job.dagbag.get_dag('test_scheduler_add_new_task', session=session)
        self.scheduler_job._create_dag_runs([orm_dag], session)

        drs = DagRun.find(dag_id=dag.dag_id, session=session)
        assert len(drs) == 1
        dr = drs[0]

        tis = dr.get_task_instances()
        assert len(tis) == 1

        BashOperator(task_id='dummy2', dag=dag, owner='airflow', bash_command='echo test')
        SerializedDagModel.write_dag(dag=dag)

        scheduled_tis = self.scheduler_job._schedule_dag_run(dr, set(), session)
        session.flush()
        assert scheduled_tis == 2

        drs = DagRun.find(dag_id=dag.dag_id, session=session)
        assert len(drs) == 1
        dr = drs[0]

        tis = dr.get_task_instances()
        assert len(tis) == 2

    def test_runs_respected_after_clear(self):
        """
        Test if _process_task_instances only schedules ti's up to max_active_runs
        (related to issue AIRFLOW-137)
        """
        dag = DAG(dag_id='test_scheduler_max_active_runs_respected_after_clear', start_date=DEFAULT_DATE)
        dag.max_active_runs = 3

        BashOperator(task_id='dummy', dag=dag, owner='airflow', bash_command='echo Hi')

        session = settings.Session()
        orm_dag = DagModel(dag_id=dag.dag_id)
        session.merge(orm_dag)
        session.commit()
        session.close()
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        self.scheduler_job.processor_agent = mock.MagicMock()
        self.scheduler_job.dagbag.bag_dag(dag, root_dag=dag)
        dag.clear()

        date = DEFAULT_DATE
        dr1 = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=date,
            state=State.RUNNING,
        )
        date = dag.following_schedule(date)
        dr2 = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=date,
            state=State.RUNNING,
        )
        date = dag.following_schedule(date)
        dr3 = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=date,
            state=State.RUNNING,
        )

        # First create up to 3 dagruns in RUNNING state.
        assert dr1 is not None
        assert dr2 is not None
        assert dr3 is not None
        assert len(DagRun.find(dag_id=dag.dag_id, state=State.RUNNING, session=session)) == 3

        # Reduce max_active_runs to 1
        dag.max_active_runs = 1

        # and schedule them in, so we can check how many
        # tasks are put on the task_instances_list (should be one, not 3)
        with create_session() as session:
            num_scheduled = self.scheduler_job._schedule_dag_run(dr1, set(), session)
            assert num_scheduled == 1
            num_scheduled = self.scheduler_job._schedule_dag_run(dr2, {dr1.execution_date}, session)
            assert num_scheduled == 0
            num_scheduled = self.scheduler_job._schedule_dag_run(dr3, {dr1.execution_date}, session)
            assert num_scheduled == 0

    @patch.object(TaskInstance, 'handle_failure_with_callback')
    def test_execute_on_failure_callbacks(self, mock_ti_handle_failure):
        dagbag = DagBag(dag_folder="/dev/null", include_examples=True, read_dags_from_db=False)
        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        with create_session() as session:
            session.query(TaskInstance).delete()
            dag = dagbag.get_dag('example_branch_operator')
            task = dag.get_task(task_id='run_this_first')

            ti = TaskInstance(task, DEFAULT_DATE, State.RUNNING)

            session.add(ti)
            session.commit()

            requests = [
                TaskCallbackRequest(
                    full_filepath="A", simple_task_instance=SimpleTaskInstance(ti), msg="Message"
                )
            ]
            dag_file_processor.execute_callbacks(dagbag, requests)
            mock_ti_handle_failure.assert_called_once_with(
                error="Message",
                test_mode=conf.getboolean('core', 'unit_test_mode'),
            )

    def test_process_file_should_failure_callback(self):
        dag_file = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), '../dags/test_on_failure_callback.py'
        )
        dagbag = DagBag(dag_folder=dag_file, include_examples=False)
        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        with create_session() as session, NamedTemporaryFile(delete=False) as callback_file:
            session.query(TaskInstance).delete()
            dag = dagbag.get_dag('test_om_failure_callback_dag')
            task = dag.get_task(task_id='test_om_failure_callback_task')

            ti = TaskInstance(task, DEFAULT_DATE, State.RUNNING)

            session.add(ti)
            session.commit()

            requests = [
                TaskCallbackRequest(
                    full_filepath=dag.full_filepath,
                    simple_task_instance=SimpleTaskInstance(ti),
                    msg="Message",
                )
            ]
            callback_file.close()

            with mock.patch.dict("os.environ", {"AIRFLOW_CALLBACK_FILE": callback_file.name}):
                dag_file_processor.process_file(dag_file, requests)
            with open(callback_file.name) as callback_file2:
                content = callback_file2.read()
            assert "Callback fired" == content
            os.remove(callback_file.name)

    def test_should_mark_dummy_task_as_success(self):
        dag_file = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), '../dags/test_only_dummy_tasks.py'
        )

        # Write DAGs to dag and serialized_dag table
        dagbag = DagBag(dag_folder=dag_file, include_examples=False, read_dags_from_db=False)
        dagbag.sync_to_db()

        self.scheduler_job_job = SchedulerJob(subdir=os.devnull)
        self.scheduler_job_job.processor_agent = mock.MagicMock()
        dag = self.scheduler_job_job.dagbag.get_dag("test_only_dummy_tasks")

        # Create DagRun
        session = settings.Session()
        orm_dag = session.query(DagModel).get(dag.dag_id)
        self.scheduler_job_job._create_dag_runs([orm_dag], session)

        drs = DagRun.find(dag_id=dag.dag_id, session=session)
        assert len(drs) == 1
        dr = drs[0]

        # Schedule TaskInstances
        self.scheduler_job_job._schedule_dag_run(dr, {}, session)
        with create_session() as session:
            tis = session.query(TaskInstance).all()

        dags = self.scheduler_job_job.dagbag.dags.values()
        assert ['test_only_dummy_tasks'] == [dag.dag_id for dag in dags]
        assert 5 == len(tis)
        assert {
            ('test_task_a', 'success'),
            ('test_task_b', None),
            ('test_task_c', 'success'),
            ('test_task_on_execute', 'scheduled'),
            ('test_task_on_success', 'scheduled'),
        } == {(ti.task_id, ti.state) for ti in tis}
        for state, start_date, end_date, duration in [
            (ti.state, ti.start_date, ti.end_date, ti.duration) for ti in tis
        ]:
            if state == 'success':
                assert start_date is not None
                assert end_date is not None
                assert 0.0 == duration
            else:
                assert start_date is None
                assert end_date is None
                assert duration is None

        self.scheduler_job_job._schedule_dag_run(dr, {}, session)
        with create_session() as session:
            tis = session.query(TaskInstance).all()

        assert 5 == len(tis)
        assert {
            ('test_task_a', 'success'),
            ('test_task_b', 'success'),
            ('test_task_c', 'success'),
            ('test_task_on_execute', 'scheduled'),
            ('test_task_on_success', 'scheduled'),
        } == {(ti.task_id, ti.state) for ti in tis}
        for state, start_date, end_date, duration in [
            (ti.state, ti.start_date, ti.end_date, ti.duration) for ti in tis
        ]:
            if state == 'success':
                assert start_date is not None
                assert end_date is not None
                assert 0.0 == duration
            else:
                assert start_date is None
                assert end_date is None
                assert duration is None


@pytest.mark.usefixtures("disable_load_example")
class TestSchedulerJob(unittest.TestCase):
    @staticmethod
    def clean_db():
        clear_db_runs()
        clear_db_pools()
        clear_db_dags()
        clear_db_sla_miss()
        clear_db_import_errors()
        clear_db_jobs()
        # DO NOT try to run clear_db_serialized_dags() here - this will break the tests
        # The tests expect DAGs to be fully loaded here via setUpClass method below

    def setUp(self):
        self.clean_db()
        self.scheduler_job = None
        # Speed up some tests by not running the tasks, just look at what we
        # enqueue!
        self.null_exec = MockExecutor()

        self.patcher = patch('airflow.utils.dag_processing.SerializedDagModel.remove_deleted_dags')
        # Since we don't want to store the code for the DAG defined in this file
        self.patcher_dag_code = patch.object(settings, "STORE_DAG_CODE", False)
        self.patcher.start()
        self.patcher_dag_code.start()

    def tearDown(self):
        if self.scheduler_job and self.scheduler_job.processor_agent:
            self.scheduler_job.processor_agent.end()
            self.scheduler_job = None
        self.patcher.stop()
        self.patcher_dag_code.stop()
        self.clean_db()

    @classmethod
    def setUpClass(cls):
        # Ensure the DAGs we are looking at from the DB are up-to-date
        non_serialized_dagbag = DagBag(read_dags_from_db=False, include_examples=False)
        non_serialized_dagbag.sync_to_db()
        cls.dagbag = DagBag(read_dags_from_db=True)

    def test_is_alive(self):
        self.scheduler_job = SchedulerJob(None, heartrate=10, state=State.RUNNING)
        assert self.scheduler_job.is_alive()

        self.scheduler_job.latest_heartbeat = timezone.utcnow() - datetime.timedelta(seconds=20)
        assert self.scheduler_job.is_alive()

        self.scheduler_job.latest_heartbeat = timezone.utcnow() - datetime.timedelta(seconds=31)
        assert not self.scheduler_job.is_alive()

        # test because .seconds was used before instead of total_seconds
        # internal repr of datetime is (days, seconds)
        self.scheduler_job.latest_heartbeat = timezone.utcnow() - datetime.timedelta(days=1)
        assert not self.scheduler_job.is_alive()

        self.scheduler_job.state = State.SUCCESS
        self.scheduler_job.latest_heartbeat = timezone.utcnow() - datetime.timedelta(seconds=10)
        assert (
            not self.scheduler_job.is_alive()
        ), "Completed jobs even with recent heartbeat should not be alive"

    def run_single_scheduler_loop_with_no_dags(self, dags_folder):
        """
        Utility function that runs a single scheduler loop without actually
        changing/scheduling any dags. This is useful to simulate the other side effects of
        running a scheduler loop, e.g. to see what parse errors there are in the
        dags_folder.

        :param dags_folder: the directory to traverse
        :type dags_folder: str
        """
        self.scheduler_job = SchedulerJob(
            executor=self.null_exec, num_times_parse_dags=1, subdir=os.path.join(dags_folder)
        )
        self.scheduler_job.heartrate = 0
        self.scheduler_job.run()

    def test_no_orphan_process_will_be_left(self):
        empty_dir = mkdtemp()
        current_process = psutil.Process()
        old_children = current_process.children(recursive=True)
        self.scheduler_job = SchedulerJob(
            subdir=empty_dir, num_runs=1, executor=MockExecutor(do_update=False)
        )
        self.scheduler_job.run()
        shutil.rmtree(empty_dir)

        # Remove potential noise created by previous tests.
        current_children = set(current_process.children(recursive=True)) - set(old_children)
        assert not current_children

    @mock.patch('airflow.jobs.scheduler_job.TaskCallbackRequest')
    @mock.patch('airflow.jobs.scheduler_job.Stats.incr')
    def test_process_executor_events(self, mock_stats_incr, mock_task_callback):
        dag_id = "test_process_executor_events"
        dag_id2 = "test_process_executor_events_2"
        task_id_1 = 'dummy_task'

        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, full_filepath="/test_path1/")
        dag2 = DAG(dag_id=dag_id2, start_date=DEFAULT_DATE, full_filepath="/test_path1/")
        task1 = DummyOperator(dag=dag, task_id=task_id_1)
        DummyOperator(dag=dag2, task_id=task_id_1)
        dag.fileloc = "/test_path1/"
        dag2.fileloc = "/test_path1/"

        executor = MockExecutor(do_update=False)
        task_callback = mock.MagicMock()
        mock_task_callback.return_value = task_callback
        self.scheduler_job = SchedulerJob(executor=executor)
        self.scheduler_job.processor_agent = mock.MagicMock()

        session = settings.Session()
        dag.sync_to_db(session=session)
        dag2.sync_to_db(session=session)

        ti1 = TaskInstance(task1, DEFAULT_DATE)
        ti1.state = State.QUEUED
        session.merge(ti1)
        session.commit()

        executor.event_buffer[ti1.key] = State.FAILED, None

        self.scheduler_job._process_executor_events(session=session)
        ti1.refresh_from_db()
        assert ti1.state == State.QUEUED
        mock_task_callback.assert_called_once_with(
            full_filepath='/test_path1/',
            simple_task_instance=mock.ANY,
            msg='Executor reports task instance '
            '<TaskInstance: test_process_executor_events.dummy_task 2016-01-01 00:00:00+00:00 [queued]> '
            'finished (failed) although the task says its queued. (Info: None) '
            'Was the task killed externally?',
        )
        self.scheduler_job.processor_agent.send_callback_to_execute.assert_called_once_with(task_callback)
        self.scheduler_job.processor_agent.reset_mock()

        # ti in success state
        ti1.state = State.SUCCESS
        session.merge(ti1)
        session.commit()
        executor.event_buffer[ti1.key] = State.SUCCESS, None

        self.scheduler_job._process_executor_events(session=session)
        ti1.refresh_from_db()
        assert ti1.state == State.SUCCESS
        self.scheduler_job.processor_agent.send_callback_to_execute.assert_not_called()

        mock_stats_incr.assert_called_once_with('scheduler.tasks.killed_externally')

    def test_process_executor_events_uses_inmemory_try_number(self):
        execution_date = DEFAULT_DATE
        dag_id = "dag_id"
        task_id = "task_id"
        try_number = 42

        executor = MagicMock()
        self.scheduler_job = SchedulerJob(executor=executor)
        self.scheduler_job.processor_agent = MagicMock()
        event_buffer = {TaskInstanceKey(dag_id, task_id, execution_date, try_number): (State.SUCCESS, None)}
        executor.get_event_buffer.return_value = event_buffer

        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE)
        task = DummyOperator(dag=dag, task_id=task_id)

        with create_session() as session:
            ti = TaskInstance(task, DEFAULT_DATE)
            ti.state = State.SUCCESS
            session.merge(ti)

        self.scheduler_job._process_executor_events()
        # Assert that the even_buffer is empty so the task was popped using right
        # task instance key
        assert event_buffer == {}

    def test_execute_task_instances_is_paused_wont_execute(self):
        dag_id = 'SchedulerJobTest.test_execute_task_instances_is_paused_wont_execute'
        task_id_1 = 'dummy_task'

        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE)
        task1 = DummyOperator(dag=dag, task_id=task_id_1)
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        session = settings.Session()

        dagmodel = DagModel(
            dag_id=dag_id,
            is_paused=False,
            concurrency=dag.concurrency,
            has_task_concurrency_limits=False,
        )
        dr1 = dag.create_dagrun(
            run_type=DagRunType.BACKFILL_JOB,
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        ti1 = TaskInstance(task1, DEFAULT_DATE)
        ti1.state = State.SCHEDULED
        session.merge(ti1)
        session.merge(dr1)
        session.add(dagmodel)
        session.flush()

        self.scheduler_job._critical_section_execute_task_instances(session)
        session.flush()
        ti1.refresh_from_db()
        assert State.SCHEDULED == ti1.state
        session.rollback()

    def test_execute_task_instances_no_dagrun_task_will_execute(self):
        """
        Tests that tasks without dagrun still get executed.
        """
        dag_id = 'SchedulerJobTest.test_execute_task_instances_no_dagrun_task_will_execute'
        task_id_1 = 'dummy_task'

        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE)
        task1 = DummyOperator(dag=dag, task_id=task_id_1)
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        session = settings.Session()

        dag_model = DagModel(
            dag_id=dag_id,
            is_paused=False,
            concurrency=dag.concurrency,
            has_task_concurrency_limits=False,
        )
        session.add(dag_model)
        dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        ti1 = TaskInstance(task1, DEFAULT_DATE)
        ti1.state = State.SCHEDULED
        ti1.execution_date = ti1.execution_date + datetime.timedelta(days=1)
        session.merge(ti1)
        session.flush()

        self.scheduler_job._critical_section_execute_task_instances(session)
        session.flush()
        ti1.refresh_from_db()
        assert State.QUEUED == ti1.state
        session.rollback()

    def test_execute_task_instances_backfill_tasks_wont_execute(self):
        """
        Tests that backfill tasks won't get executed.
        """
        dag_id = 'SchedulerJobTest.test_execute_task_instances_backfill_tasks_wont_execute'
        task_id_1 = 'dummy_task'

        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE)
        task1 = DummyOperator(dag=dag, task_id=task_id_1)
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        session = settings.Session()

        dag_model = DagModel(
            dag_id=dag_id,
            is_paused=False,
            concurrency=dag.concurrency,
            has_task_concurrency_limits=False,
        )
        session.add(dag_model)
        dr1 = dag.create_dagrun(
            run_type=DagRunType.BACKFILL_JOB,
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        ti1 = TaskInstance(task1, dr1.execution_date)
        ti1.refresh_from_db()
        ti1.state = State.SCHEDULED
        session.merge(ti1)
        session.merge(dr1)
        session.flush()

        assert dr1.is_backfill

        self.scheduler_job._critical_section_execute_task_instances(session)
        session.flush()
        ti1.refresh_from_db()
        assert State.SCHEDULED == ti1.state
        session.rollback()

    def test_find_executable_task_instances_backfill_nodagrun(self):
        dag_id = 'SchedulerJobTest.test_find_executable_task_instances_backfill_nodagrun'
        task_id_1 = 'dummy'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, concurrency=16)
        task1 = DummyOperator(dag=dag, task_id=task_id_1)
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        session = settings.Session()

        dag_model = DagModel(
            dag_id=dag_id,
            is_paused=False,
            concurrency=dag.concurrency,
            has_task_concurrency_limits=False,
        )
        session.add(dag_model)
        dr1 = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        dr2 = dag.create_dagrun(
            run_type=DagRunType.BACKFILL_JOB,
            execution_date=dag.following_schedule(dr1.execution_date),
            state=State.RUNNING,
        )

        ti_no_dagrun = TaskInstance(task1, DEFAULT_DATE - datetime.timedelta(days=1))
        ti_backfill = TaskInstance(task1, dr2.execution_date)
        ti_with_dagrun = TaskInstance(task1, dr1.execution_date)
        # ti_with_paused
        ti_no_dagrun.state = State.SCHEDULED
        ti_backfill.state = State.SCHEDULED
        ti_with_dagrun.state = State.SCHEDULED

        session.merge(dr2)
        session.merge(ti_no_dagrun)
        session.merge(ti_backfill)
        session.merge(ti_with_dagrun)
        session.flush()

        res = self.scheduler_job._executable_task_instances_to_queued(max_tis=32, session=session)

        assert 2 == len(res)
        res_keys = map(lambda x: x.key, res)
        assert ti_no_dagrun.key in res_keys
        assert ti_with_dagrun.key in res_keys
        session.rollback()

    def test_find_executable_task_instances_pool(self):
        dag_id = 'SchedulerJobTest.test_find_executable_task_instances_pool'
        task_id_1 = 'dummy'
        task_id_2 = 'dummydummy'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, concurrency=16)
        task1 = DummyOperator(dag=dag, task_id=task_id_1, pool='a')
        task2 = DummyOperator(dag=dag, task_id=task_id_2, pool='b')
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        session = settings.Session()

        dag_model = DagModel(
            dag_id=dag_id,
            is_paused=False,
            concurrency=dag.concurrency,
            has_task_concurrency_limits=False,
        )
        session.add(dag_model)
        dr1 = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        dr2 = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=dag.following_schedule(dr1.execution_date),
            state=State.RUNNING,
        )

        tis = [
            TaskInstance(task1, dr1.execution_date),
            TaskInstance(task2, dr1.execution_date),
            TaskInstance(task1, dr2.execution_date),
            TaskInstance(task2, dr2.execution_date),
        ]
        for ti in tis:
            ti.state = State.SCHEDULED
            session.merge(ti)
        pool = Pool(pool='a', slots=1, description='haha')
        pool2 = Pool(pool='b', slots=100, description='haha')
        session.add(pool)
        session.add(pool2)
        session.flush()

        res = self.scheduler_job._executable_task_instances_to_queued(max_tis=32, session=session)
        session.flush()
        assert 3 == len(res)
        res_keys = []
        for ti in res:
            res_keys.append(ti.key)
        assert tis[0].key in res_keys
        assert tis[1].key in res_keys
        assert tis[3].key in res_keys
        session.rollback()

    def test_find_executable_task_instances_in_default_pool(self):
        set_default_pool_slots(1)

        dag_id = 'SchedulerJobTest.test_find_executable_task_instances_in_default_pool'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE)
        op1 = DummyOperator(dag=dag, task_id='dummy1')
        op2 = DummyOperator(dag=dag, task_id='dummy2')
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        executor = MockExecutor(do_update=True)
        self.scheduler_job = SchedulerJob(executor=executor)
        session = settings.Session()
        dag_model = DagModel(
            dag_id=dag_id,
            is_paused=False,
            concurrency=dag.concurrency,
            has_task_concurrency_limits=False,
        )
        session.add(dag_model)
        dr1 = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        dr2 = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=dag.following_schedule(dr1.execution_date),
            state=State.RUNNING,
        )

        ti1 = TaskInstance(task=op1, execution_date=dr1.execution_date)
        ti2 = TaskInstance(task=op2, execution_date=dr2.execution_date)
        ti1.state = State.SCHEDULED
        ti2.state = State.SCHEDULED

        session.merge(ti1)
        session.merge(ti2)
        session.flush()

        # Two tasks w/o pool up for execution and our default pool size is 1
        res = self.scheduler_job._executable_task_instances_to_queued(max_tis=32, session=session)
        assert 1 == len(res)

        ti2.state = State.RUNNING
        session.merge(ti2)
        session.flush()

        # One task w/o pool up for execution and one task running
        res = self.scheduler_job._executable_task_instances_to_queued(max_tis=32, session=session)
        assert 0 == len(res)

        session.rollback()
        session.close()

    def test_nonexistent_pool(self):
        dag_id = 'SchedulerJobTest.test_nonexistent_pool'
        task_id = 'dummy_wrong_pool'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, concurrency=16)
        task = DummyOperator(dag=dag, task_id=task_id, pool="this_pool_doesnt_exist")
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        session = settings.Session()

        dag_model = DagModel(
            dag_id=dag_id,
            is_paused=False,
            concurrency=dag.concurrency,
            has_task_concurrency_limits=False,
        )
        session.add(dag_model)
        dr = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        ti = TaskInstance(task, dr.execution_date)
        ti.state = State.SCHEDULED
        session.merge(ti)
        session.commit()

        res = self.scheduler_job._executable_task_instances_to_queued(max_tis=32, session=session)
        session.flush()
        assert 0 == len(res)
        session.rollback()

    def test_find_executable_task_instances_none(self):
        dag_id = 'SchedulerJobTest.test_find_executable_task_instances_none'
        task_id_1 = 'dummy'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, concurrency=16)
        DummyOperator(dag=dag, task_id=task_id_1)
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        session = settings.Session()

        dag_model = DagModel(
            dag_id=dag_id,
            is_paused=False,
            concurrency=dag.concurrency,
            has_task_concurrency_limits=False,
        )
        session.add(dag_model)
        dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        session.flush()

        assert 0 == len(self.scheduler_job._executable_task_instances_to_queued(max_tis=32, session=session))
        session.rollback()

    def test_find_executable_task_instances_concurrency(self):
        dag_id = 'SchedulerJobTest.test_find_executable_task_instances_concurrency'
        task_id_1 = 'dummy'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, concurrency=2)
        task1 = DummyOperator(dag=dag, task_id=task_id_1)
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        session = settings.Session()

        dag_model = DagModel(
            dag_id=dag_id,
            is_paused=False,
            concurrency=dag.concurrency,
            has_task_concurrency_limits=False,
        )
        session.add(dag_model)
        dr1 = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        dr2 = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=dag.following_schedule(dr1.execution_date),
            state=State.RUNNING,
        )
        dr3 = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=dag.following_schedule(dr2.execution_date),
            state=State.RUNNING,
        )

        ti1 = TaskInstance(task1, dr1.execution_date)
        ti2 = TaskInstance(task1, dr2.execution_date)
        ti3 = TaskInstance(task1, dr3.execution_date)
        ti1.state = State.RUNNING
        ti2.state = State.SCHEDULED
        ti3.state = State.SCHEDULED
        session.merge(ti1)
        session.merge(ti2)
        session.merge(ti3)

        session.flush()

        res = self.scheduler_job._executable_task_instances_to_queued(max_tis=32, session=session)

        assert 1 == len(res)
        res_keys = map(lambda x: x.key, res)
        assert ti2.key in res_keys

        ti2.state = State.RUNNING
        session.merge(ti2)
        session.flush()

        res = self.scheduler_job._executable_task_instances_to_queued(max_tis=32, session=session)

        assert 0 == len(res)
        session.rollback()

    def test_find_executable_task_instances_concurrency_queued(self):
        dag_id = 'SchedulerJobTest.test_find_executable_task_instances_concurrency_queued'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, concurrency=3)
        task1 = DummyOperator(dag=dag, task_id='dummy1')
        task2 = DummyOperator(dag=dag, task_id='dummy2')
        task3 = DummyOperator(dag=dag, task_id='dummy3')
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        session = settings.Session()
        dag_model = DagModel(
            dag_id=dag_id,
            is_paused=False,
            concurrency=dag.concurrency,
            has_task_concurrency_limits=False,
        )
        session.add(dag_model)
        dag_run = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        ti1 = TaskInstance(task1, dag_run.execution_date)
        ti2 = TaskInstance(task2, dag_run.execution_date)
        ti3 = TaskInstance(task3, dag_run.execution_date)
        ti1.state = State.RUNNING
        ti2.state = State.QUEUED
        ti3.state = State.SCHEDULED

        session.merge(ti1)
        session.merge(ti2)
        session.merge(ti3)

        session.flush()

        res = self.scheduler_job._executable_task_instances_to_queued(max_tis=32, session=session)

        assert 1 == len(res)
        assert res[0].key == ti3.key
        session.rollback()

    # TODO: This is a hack, I think I need to just remove the setting and have it on always
    def test_find_executable_task_instances_task_concurrency(self):  # pylint: disable=too-many-statements
        dag_id = 'SchedulerJobTest.test_find_executable_task_instances_task_concurrency'
        task_id_1 = 'dummy'
        task_id_2 = 'dummy2'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, concurrency=16)
        task1 = DummyOperator(dag=dag, task_id=task_id_1, task_concurrency=2)
        task2 = DummyOperator(dag=dag, task_id=task_id_2)

        executor = MockExecutor(do_update=True)
        self.scheduler_job = SchedulerJob(executor=executor)
        session = settings.Session()

        self.scheduler_job.dagbag.bag_dag(dag, root_dag=dag)
        self.scheduler_job.dagbag.sync_to_db(session=session)
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))
        dr1 = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        dr2 = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=dag.following_schedule(dr1.execution_date),
            state=State.RUNNING,
        )
        dr3 = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=dag.following_schedule(dr2.execution_date),
            state=State.RUNNING,
        )

        ti1_1 = TaskInstance(task1, dr1.execution_date)
        ti2 = TaskInstance(task2, dr1.execution_date)

        ti1_1.state = State.SCHEDULED
        ti2.state = State.SCHEDULED
        session.merge(ti1_1)
        session.merge(ti2)
        session.flush()

        res = self.scheduler_job._executable_task_instances_to_queued(max_tis=32, session=session)

        assert 2 == len(res)

        ti1_1.state = State.RUNNING
        ti2.state = State.RUNNING
        ti1_2 = TaskInstance(task1, dr2.execution_date)
        ti1_2.state = State.SCHEDULED
        session.merge(ti1_1)
        session.merge(ti2)
        session.merge(ti1_2)
        session.flush()

        res = self.scheduler_job._executable_task_instances_to_queued(max_tis=32, session=session)

        assert 1 == len(res)

        ti1_2.state = State.RUNNING
        ti1_3 = TaskInstance(task1, dr3.execution_date)
        ti1_3.state = State.SCHEDULED
        session.merge(ti1_2)
        session.merge(ti1_3)
        session.flush()

        res = self.scheduler_job._executable_task_instances_to_queued(max_tis=32, session=session)

        assert 0 == len(res)

        ti1_1.state = State.SCHEDULED
        ti1_2.state = State.SCHEDULED
        ti1_3.state = State.SCHEDULED
        session.merge(ti1_1)
        session.merge(ti1_2)
        session.merge(ti1_3)
        session.flush()

        res = self.scheduler_job._executable_task_instances_to_queued(max_tis=32, session=session)

        assert 2 == len(res)

        ti1_1.state = State.RUNNING
        ti1_2.state = State.SCHEDULED
        ti1_3.state = State.SCHEDULED
        session.merge(ti1_1)
        session.merge(ti1_2)
        session.merge(ti1_3)
        session.flush()

        res = self.scheduler_job._executable_task_instances_to_queued(max_tis=32, session=session)

        assert 1 == len(res)
        session.rollback()

    def test_change_state_for_executable_task_instances_no_tis_with_state(self):
        dag_id = 'SchedulerJobTest.test_change_state_for__no_tis_with_state'
        task_id_1 = 'dummy'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, concurrency=2)
        task1 = DummyOperator(dag=dag, task_id=task_id_1)
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        session = settings.Session()

        date = DEFAULT_DATE
        dr1 = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=date,
            state=State.RUNNING,
        )
        date = dag.following_schedule(date)
        dr2 = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=date,
            state=State.RUNNING,
        )
        date = dag.following_schedule(date)
        dr3 = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=date,
            state=State.RUNNING,
        )

        ti1 = TaskInstance(task1, dr1.execution_date)
        ti2 = TaskInstance(task1, dr2.execution_date)
        ti3 = TaskInstance(task1, dr3.execution_date)
        ti1.state = State.RUNNING
        ti2.state = State.RUNNING
        ti3.state = State.RUNNING
        session.merge(ti1)
        session.merge(ti2)
        session.merge(ti3)

        session.flush()

        res = self.scheduler_job._executable_task_instances_to_queued(max_tis=100, session=session)
        assert 0 == len(res)

        session.rollback()

    def test_enqueue_task_instances_with_queued_state(self):
        dag_id = 'SchedulerJobTest.test_enqueue_task_instances_with_queued_state'
        task_id_1 = 'dummy'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE)
        task1 = DummyOperator(dag=dag, task_id=task_id_1)
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        session = settings.Session()

        dag_model = DagModel(
            dag_id=dag_id,
            is_paused=False,
            concurrency=dag.concurrency,
            has_task_concurrency_limits=False,
        )
        session.add(dag_model)
        dr1 = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        ti1 = TaskInstance(task1, dr1.execution_date)
        ti1.dag_model = dag_model
        session.merge(ti1)
        session.flush()

        with patch.object(BaseExecutor, 'queue_command') as mock_queue_command:
            self.scheduler_job._enqueue_task_instances_with_queued_state([ti1])

        assert mock_queue_command.called
        session.rollback()

    def test_critical_section_execute_task_instances(self):
        dag_id = 'SchedulerJobTest.test_execute_task_instances'
        task_id_1 = 'dummy_task'
        task_id_2 = 'dummy_task_nonexistent_queue'
        # important that len(tasks) is less than concurrency
        # because before scheduler._execute_task_instances would only
        # check the num tasks once so if concurrency was 3,
        # we could execute arbitrarily many tasks in the second run
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, concurrency=3)
        task1 = DummyOperator(dag=dag, task_id=task_id_1)
        task2 = DummyOperator(dag=dag, task_id=task_id_2)
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        session = settings.Session()

        # create first dag run with 1 running and 1 queued
        dag_model = DagModel(
            dag_id=dag_id,
            is_paused=False,
            concurrency=dag.concurrency,
            has_task_concurrency_limits=False,
        )
        session.add(dag_model)
        dr1 = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        ti1 = TaskInstance(task1, dr1.execution_date)
        ti2 = TaskInstance(task2, dr1.execution_date)
        ti1.refresh_from_db()
        ti2.refresh_from_db()
        ti1.state = State.RUNNING
        ti2.state = State.RUNNING
        session.merge(ti1)
        session.merge(ti2)
        session.flush()

        assert State.RUNNING == dr1.state
        assert 2 == DAG.get_num_task_instances(dag_id, dag.task_ids, states=[State.RUNNING], session=session)

        # create second dag run
        dr2 = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=dag.following_schedule(dr1.execution_date),
            state=State.RUNNING,
        )
        ti3 = TaskInstance(task1, dr2.execution_date)
        ti4 = TaskInstance(task2, dr2.execution_date)
        ti3.refresh_from_db()
        ti4.refresh_from_db()
        # manually set to scheduled so we can pick them up
        ti3.state = State.SCHEDULED
        ti4.state = State.SCHEDULED
        session.merge(ti3)
        session.merge(ti4)
        session.flush()

        assert State.RUNNING == dr2.state

        res = self.scheduler_job._critical_section_execute_task_instances(session)

        # check that concurrency is respected
        ti1.refresh_from_db()
        ti2.refresh_from_db()
        ti3.refresh_from_db()
        ti4.refresh_from_db()
        assert 3 == DAG.get_num_task_instances(
            dag_id, dag.task_ids, states=[State.RUNNING, State.QUEUED], session=session
        )
        assert State.RUNNING == ti1.state
        assert State.RUNNING == ti2.state
        assert {State.QUEUED, State.SCHEDULED} == {ti3.state, ti4.state}
        assert 1 == res

    def test_execute_task_instances_limit(self):
        dag_id = 'SchedulerJobTest.test_execute_task_instances_limit'
        task_id_1 = 'dummy_task'
        task_id_2 = 'dummy_task_2'
        # important that len(tasks) is less than concurrency
        # because before scheduler._execute_task_instances would only
        # check the num tasks once so if concurrency was 3,
        # we could execute arbitrarily many tasks in the second run
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, concurrency=16)
        task1 = DummyOperator(dag=dag, task_id=task_id_1)
        task2 = DummyOperator(dag=dag, task_id=task_id_2)
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        session = settings.Session()

        dag_model = DagModel(
            dag_id=dag_id,
            is_paused=False,
            concurrency=dag.concurrency,
            has_task_concurrency_limits=False,
        )
        session.add(dag_model)
        date = dag.start_date
        tis = []
        for _ in range(0, 4):
            dr = dag.create_dagrun(
                run_type=DagRunType.SCHEDULED,
                execution_date=date,
                state=State.RUNNING,
            )
            date = dag.following_schedule(date)
            ti1 = TaskInstance(task1, dr.execution_date)
            ti2 = TaskInstance(task2, dr.execution_date)
            tis.append(ti1)
            tis.append(ti2)
            ti1.refresh_from_db()
            ti2.refresh_from_db()
            ti1.state = State.SCHEDULED
            ti2.state = State.SCHEDULED
            session.merge(ti1)
            session.merge(ti2)
            session.flush()
        self.scheduler_job.max_tis_per_query = 2
        res = self.scheduler_job._critical_section_execute_task_instances(session)
        assert 2 == res

        self.scheduler_job.max_tis_per_query = 8
        with mock.patch.object(
            type(self.scheduler_job.executor), 'slots_available', new_callable=mock.PropertyMock
        ) as mock_slots:
            mock_slots.return_value = 2
            # Check that we don't "overfill" the executor
            assert 2 == res
            res = self.scheduler_job._critical_section_execute_task_instances(session)

        res = self.scheduler_job._critical_section_execute_task_instances(session)
        assert 4 == res
        for ti in tis:
            ti.refresh_from_db()
            assert State.QUEUED == ti.state

    def test_execute_task_instances_unlimited(self):
        """Test that max_tis_per_query=0 is unlimited"""

        dag_id = 'SchedulerJobTest.test_execute_task_instances_unlimited'
        task_id_1 = 'dummy_task'
        task_id_2 = 'dummy_task_2'

        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, concurrency=1024)
        task1 = DummyOperator(dag=dag, task_id=task_id_1)
        task2 = DummyOperator(dag=dag, task_id=task_id_2)
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        session = settings.Session()

        dag_model = DagModel(
            dag_id=dag_id,
            is_paused=False,
            concurrency=dag.concurrency,
            has_task_concurrency_limits=False,
        )
        session.add(dag_model)
        date = dag.start_date
        tis = []
        for _ in range(0, 20):
            dr = dag.create_dagrun(
                run_type=DagRunType.SCHEDULED,
                execution_date=date,
                state=State.RUNNING,
            )
            date = dag.following_schedule(date)
            ti1 = TaskInstance(task1, dr.execution_date)
            ti2 = TaskInstance(task2, dr.execution_date)
            tis.append(ti1)
            tis.append(ti2)
            ti1.refresh_from_db()
            ti2.refresh_from_db()
            ti1.state = State.SCHEDULED
            ti2.state = State.SCHEDULED
            session.merge(ti1)
            session.merge(ti2)
            session.flush()
        self.scheduler_job.max_tis_per_query = 0
        self.scheduler_job.executor = MagicMock(slots_available=36)

        res = self.scheduler_job._critical_section_execute_task_instances(session)
        # 20 dag runs * 2 tasks each = 40, but limited by number of slots available
        self.assertEqual(36, res)
        session.rollback()

    def test_change_state_for_tis_without_dagrun(self):
        dag1 = DAG(dag_id='test_change_state_for_tis_without_dagrun', start_date=DEFAULT_DATE)

        DummyOperator(task_id='dummy', dag=dag1, owner='airflow')

        DummyOperator(task_id='dummy_b', dag=dag1, owner='airflow')

        dag2 = DAG(dag_id='test_change_state_for_tis_without_dagrun_dont_change', start_date=DEFAULT_DATE)

        DummyOperator(task_id='dummy', dag=dag2, owner='airflow')

        dag3 = DAG(dag_id='test_change_state_for_tis_without_dagrun_no_dagrun', start_date=DEFAULT_DATE)

        DummyOperator(task_id='dummy', dag=dag3, owner='airflow')

        session = settings.Session()
        dr1 = dag1.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            state=State.RUNNING,
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            session=session,
        )

        dr2 = dag2.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            state=State.RUNNING,
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            session=session,
        )

        ti1a = dr1.get_task_instance(task_id='dummy', session=session)
        ti1a.state = State.SCHEDULED
        ti1b = dr1.get_task_instance(task_id='dummy_b', session=session)
        ti1b.state = State.SUCCESS
        session.commit()

        ti2 = dr2.get_task_instance(task_id='dummy', session=session)
        ti2.state = State.SCHEDULED
        session.commit()

        ti3 = TaskInstance(dag3.get_task('dummy'), DEFAULT_DATE)
        ti3.state = State.SCHEDULED
        session.merge(ti3)
        session.commit()

        dagbag = DagBag("/dev/null", include_examples=False, read_dags_from_db=False)
        dagbag.bag_dag(dag1, root_dag=dag1)
        dagbag.bag_dag(dag2, root_dag=dag2)
        dagbag.bag_dag(dag3, root_dag=dag3)
        dagbag.sync_to_db(session)
        session.commit()

        self.scheduler_job = SchedulerJob(num_runs=0)
        self.scheduler_job.dagbag.collect_dags_from_db()

        self.scheduler_job._change_state_for_tis_without_dagrun(
            old_states=[State.SCHEDULED, State.QUEUED], new_state=State.NONE, session=session
        )

        ti1a = dr1.get_task_instance(task_id='dummy', session=session)
        ti1a.refresh_from_db(session=session)
        assert ti1a.state == State.SCHEDULED

        ti1b = dr1.get_task_instance(task_id='dummy_b', session=session)
        ti1b.refresh_from_db(session=session)
        assert ti1b.state == State.SUCCESS

        ti2 = dr2.get_task_instance(task_id='dummy', session=session)
        ti2.refresh_from_db(session=session)
        assert ti2.state == State.SCHEDULED

        ti3.refresh_from_db(session=session)
        assert ti3.state == State.NONE
        assert ti3.start_date is not None
        assert ti3.end_date is None
        assert ti3.duration is None

        dr1.refresh_from_db(session=session)
        dr1.state = State.FAILED

        # Push the changes to DB
        session.merge(dr1)
        session.commit()

        self.scheduler_job._change_state_for_tis_without_dagrun(
            old_states=[State.SCHEDULED, State.QUEUED], new_state=State.NONE, session=session
        )

        # Clear the session objects
        session.expunge_all()
        ti1a.refresh_from_db(session=session)
        assert ti1a.state == State.NONE

        # don't touch ti1b
        ti1b.refresh_from_db(session=session)
        assert ti1b.state == State.SUCCESS

        # don't touch ti2
        ti2.refresh_from_db(session=session)
        assert ti2.state == State.SCHEDULED

    def test_change_state_for_tasks_failed_to_execute(self):
        dag = DAG(dag_id='dag_id', start_date=DEFAULT_DATE)

        task = DummyOperator(task_id='task_id', dag=dag, owner='airflow')
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        # If there's no left over task in executor.queued_tasks, nothing happens
        session = settings.Session()
        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        mock_logger = mock.MagicMock()
        test_executor = MockExecutor(do_update=False)
        self.scheduler_job.executor = test_executor
        self.scheduler_job._logger = mock_logger
        self.scheduler_job._change_state_for_tasks_failed_to_execute()
        mock_logger.info.assert_not_called()

        # Tasks failed to execute with QUEUED state will be set to SCHEDULED state.
        session.query(TaskInstance).delete()
        session.commit()
        key = 'dag_id', 'task_id', DEFAULT_DATE, 1
        test_executor.queued_tasks[key] = 'value'
        ti = TaskInstance(task, DEFAULT_DATE)
        ti.state = State.QUEUED
        session.merge(ti)  # pylint: disable=no-value-for-parameter
        session.commit()

        self.scheduler_job._change_state_for_tasks_failed_to_execute()

        ti.refresh_from_db()
        assert State.SCHEDULED == ti.state

        # Tasks failed to execute with RUNNING state will not be set to SCHEDULED state.
        session.query(TaskInstance).delete()
        session.commit()
        ti.state = State.RUNNING

        session.merge(ti)
        session.commit()

        self.scheduler_job._change_state_for_tasks_failed_to_execute()

        ti.refresh_from_db()
        assert State.RUNNING == ti.state

    def test_adopt_or_reset_orphaned_tasks(self):
        session = settings.Session()
        dag = DAG(
            'test_execute_helper_reset_orphaned_tasks',
            start_date=DEFAULT_DATE,
            default_args={'owner': 'owner1'},
        )

        with dag:
            op1 = DummyOperator(task_id='op1')
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        dag.clear()
        dr = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            state=State.RUNNING,
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            session=session,
        )
        dr2 = dag.create_dagrun(
            run_type=DagRunType.BACKFILL_JOB,
            state=State.RUNNING,
            execution_date=DEFAULT_DATE + datetime.timedelta(1),
            start_date=DEFAULT_DATE,
            session=session,
        )
        ti = dr.get_task_instance(task_id=op1.task_id, session=session)
        ti.state = State.SCHEDULED
        ti2 = dr2.get_task_instance(task_id=op1.task_id, session=session)
        ti2.state = State.SCHEDULED
        session.commit()

        processor = mock.MagicMock()

        self.scheduler_job = SchedulerJob(num_runs=0)
        self.scheduler_job.processor_agent = processor

        self.scheduler_job.adopt_or_reset_orphaned_tasks()

        ti = dr.get_task_instance(task_id=op1.task_id, session=session)
        assert ti.state == State.NONE

        ti2 = dr2.get_task_instance(task_id=op1.task_id, session=session)
        assert ti2.state == State.SCHEDULED, "Tasks run by Backfill Jobs should not be reset"

    @parameterized.expand(
        [
            [State.UP_FOR_RETRY, State.FAILED],
            [State.QUEUED, State.NONE],
            [State.SCHEDULED, State.NONE],
            [State.UP_FOR_RESCHEDULE, State.NONE],
        ]
    )
    def test_scheduler_loop_should_change_state_for_tis_without_dagrun(
        self, initial_task_state, expected_task_state
    ):
        session = settings.Session()
        dag_id = 'test_execute_helper_should_change_state_for_tis_without_dagrun'
        dag = DAG(dag_id, start_date=DEFAULT_DATE, default_args={'owner': 'owner1'})

        with dag:
            op1 = DummyOperator(task_id='op1')

        # Write Dag to DB
        dagbag = DagBag(dag_folder="/dev/null", include_examples=False, read_dags_from_db=False)
        dagbag.bag_dag(dag, root_dag=dag)
        dagbag.sync_to_db()

        dag = DagBag(read_dags_from_db=True, include_examples=False).get_dag(dag_id)
        # Create DAG run with FAILED state
        dag.clear()
        dr = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            state=State.FAILED,
            execution_date=DEFAULT_DATE + timedelta(days=1),
            start_date=DEFAULT_DATE + timedelta(days=1),
            session=session,
        )
        ti = dr.get_task_instance(task_id=op1.task_id, session=session)
        ti.state = initial_task_state
        session.commit()

        # This poll interval is large, bug the scheduler doesn't sleep that
        # long, instead we hit the clean_tis_without_dagrun interval instead
        self.scheduler_job = SchedulerJob(num_runs=2, processor_poll_interval=30)
        self.scheduler_job.dagbag = dagbag
        executor = MockExecutor(do_update=False)
        executor.queued_tasks
        self.scheduler_job.executor = executor
        processor = mock.MagicMock()
        processor.done = False
        self.scheduler_job.processor_agent = processor

        with mock.patch.object(settings, "USE_JOB_SCHEDULE", False), conf_vars(
            {('scheduler', 'clean_tis_without_dagrun_interval'): '0.001'}
        ):
            self.scheduler_job._run_scheduler_loop()

        ti = dr.get_task_instance(task_id=op1.task_id, session=session)
        assert ti.state == expected_task_state
        assert ti.start_date is not None
        if expected_task_state in State.finished:
            assert ti.end_date is not None
            assert ti.start_date == ti.end_date
            assert ti.duration is not None

    def test_dagrun_timeout_verify_max_active_runs(self):
        """
        Test if a a dagrun will not be scheduled if max_dag_runs
        has been reached and dagrun_timeout is not reached

        Test if a a dagrun would be scheduled if max_dag_runs has
        been reached but dagrun_timeout is also reached
        """
        dag = DAG(dag_id='test_scheduler_verify_max_active_runs_and_dagrun_timeout', start_date=DEFAULT_DATE)
        dag.max_active_runs = 1
        dag.dagrun_timeout = datetime.timedelta(seconds=60)

        DummyOperator(task_id='dummy', dag=dag, owner='airflow')

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        self.scheduler_job.dagbag.bag_dag(dag, root_dag=dag)

        self.scheduler_job.dagbag.sync_to_db()

        session = settings.Session()
        orm_dag = session.query(DagModel).get(dag.dag_id)
        assert orm_dag is not None

        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        self.scheduler_job._create_dag_runs([orm_dag], session)

        drs = DagRun.find(dag_id=dag.dag_id, session=session)
        assert len(drs) == 1
        dr = drs[0]

        # Should not be able to create a new dag run, as we are at max active runs
        assert orm_dag.next_dagrun_create_after is None
        # But we should record the date of _what run_ it would be
        assert isinstance(orm_dag.next_dagrun, datetime.datetime)

        # Should be scheduled as dagrun_timeout has passed
        dr.start_date = timezone.utcnow() - datetime.timedelta(days=1)
        session.flush()

        # Mock that processor_agent is started
        self.scheduler_job.processor_agent = mock.Mock()
        self.scheduler_job.processor_agent.send_callback_to_execute = mock.Mock()

        self.scheduler_job._schedule_dag_run(dr, {}, session)
        session.flush()

        session.refresh(dr)
        assert dr.state == State.FAILED
        session.refresh(orm_dag)
        assert isinstance(orm_dag.next_dagrun, datetime.datetime)
        assert isinstance(orm_dag.next_dagrun_create_after, datetime.datetime)

        expected_callback = DagCallbackRequest(
            full_filepath=dr.dag.fileloc,
            dag_id=dr.dag_id,
            is_failure_callback=True,
            execution_date=dr.execution_date,
            msg="timed_out",
        )

        # Verify dag failure callback request is sent to file processor
        self.scheduler_job.processor_agent.send_callback_to_execute.assert_called_once_with(expected_callback)

        session.rollback()
        session.close()

    def test_dagrun_timeout_fails_run(self):
        """
        Test if a a dagrun will be set failed if timeout, even without max_active_runs
        """
        dag = DAG(dag_id='test_scheduler_fail_dagrun_timeout', start_date=DEFAULT_DATE)
        dag.dagrun_timeout = datetime.timedelta(seconds=60)

        DummyOperator(task_id='dummy', dag=dag, owner='airflow')

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        self.scheduler_job.dagbag.bag_dag(dag, root_dag=dag)
        self.scheduler_job.dagbag.sync_to_db()

        session = settings.Session()
        orm_dag = session.query(DagModel).get(dag.dag_id)
        assert orm_dag is not None

        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        self.scheduler_job._create_dag_runs([orm_dag], session)

        drs = DagRun.find(dag_id=dag.dag_id, session=session)
        assert len(drs) == 1
        dr = drs[0]

        # Should be scheduled as dagrun_timeout has passed
        dr.start_date = timezone.utcnow() - datetime.timedelta(days=1)
        session.flush()

        # Mock that processor_agent is started
        self.scheduler_job.processor_agent = mock.Mock()
        self.scheduler_job.processor_agent.send_callback_to_execute = mock.Mock()

        self.scheduler_job._schedule_dag_run(dr, {}, session)
        session.flush()

        session.refresh(dr)
        assert dr.state == State.FAILED

        expected_callback = DagCallbackRequest(
            full_filepath=dr.dag.fileloc,
            dag_id=dr.dag_id,
            is_failure_callback=True,
            execution_date=dr.execution_date,
            msg="timed_out",
        )

        # Verify dag failure callback request is sent to file processor
        self.scheduler_job.processor_agent.send_callback_to_execute.assert_called_once_with(expected_callback)

        session.rollback()
        session.close()

    @parameterized.expand([(State.SUCCESS, "success"), (State.FAILED, "task_failure")])
    def test_dagrun_callbacks_are_called(self, state, expected_callback_msg):
        """
        Test if DagRun is successful, and if Success callbacks is defined, it is sent to DagFileProcessor.
        Also test that SLA Callback Function is called.
        """
        dag = DAG(
            dag_id='test_dagrun_callbacks_are_called',
            start_date=DEFAULT_DATE,
            on_success_callback=lambda x: print("success"),
            on_failure_callback=lambda x: print("failed"),
        )

        DummyOperator(task_id='dummy', dag=dag, owner='airflow')

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        self.scheduler_job.processor_agent = mock.Mock()
        self.scheduler_job.processor_agent.send_callback_to_execute = mock.Mock()
        self.scheduler_job._send_sla_callbacks_to_processor = mock.Mock()

        # Sync DAG into DB
        with mock.patch.object(settings, "STORE_DAG_CODE", False):
            self.scheduler_job.dagbag.bag_dag(dag, root_dag=dag)
            self.scheduler_job.dagbag.sync_to_db()

        session = settings.Session()
        orm_dag = session.query(DagModel).get(dag.dag_id)
        assert orm_dag is not None

        # Create DagRun
        self.scheduler_job._create_dag_runs([orm_dag], session)

        drs = DagRun.find(dag_id=dag.dag_id, session=session)
        assert len(drs) == 1
        dr = drs[0]

        ti = dr.get_task_instance('dummy')
        ti.set_state(state, session)

        self.scheduler_job._schedule_dag_run(dr, {}, session)

        expected_callback = DagCallbackRequest(
            full_filepath=dr.dag.fileloc,
            dag_id=dr.dag_id,
            is_failure_callback=bool(state == State.FAILED),
            execution_date=dr.execution_date,
            msg=expected_callback_msg,
        )

        # Verify dag failure callback request is sent to file processor
        self.scheduler_job.processor_agent.send_callback_to_execute.assert_called_once_with(expected_callback)
        # This is already tested separately
        # In this test we just want to verify that this function is called
        self.scheduler_job._send_sla_callbacks_to_processor.assert_called_once_with(dag)

        session.rollback()
        session.close()

    @parameterized.expand([(State.SUCCESS,), (State.FAILED,)])
    def test_dagrun_callbacks_are_not_added_when_callbacks_are_not_defined(self, state):
        """
        Test if no on_*_callback are defined on DAG, Callbacks not registered and sent to DAG Processor
        """
        dag = DAG(
            dag_id='test_dagrun_callbacks_are_not_added_when_callbacks_are_not_defined',
            start_date=DEFAULT_DATE,
        )

        BashOperator(task_id='test_task', dag=dag, owner='airflow', bash_command='echo hi')

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        self.scheduler_job.processor_agent = mock.Mock()
        self.scheduler_job.processor_agent.send_callback_to_execute = mock.Mock()
        self.scheduler_job._send_dag_callbacks_to_processor = mock.Mock()

        # Sync DAG into DB
        with mock.patch.object(settings, "STORE_DAG_CODE", False):
            self.scheduler_job.dagbag.bag_dag(dag, root_dag=dag)
            self.scheduler_job.dagbag.sync_to_db()

        session = settings.Session()
        orm_dag = session.query(DagModel).get(dag.dag_id)
        assert orm_dag is not None

        # Create DagRun
        self.scheduler_job._create_dag_runs([orm_dag], session)

        drs = DagRun.find(dag_id=dag.dag_id, session=session)
        assert len(drs) == 1
        dr = drs[0]

        ti = dr.get_task_instance('test_task')
        ti.set_state(state, session)

        self.scheduler_job._schedule_dag_run(dr, set(), session)

        # Verify Callback is not set (i.e is None) when no callbacks are set on DAG
        self.scheduler_job._send_dag_callbacks_to_processor.assert_called_once_with(dr, None)

        session.rollback()
        session.close()

    def test_do_not_schedule_removed_task(self):
        dag = DAG(dag_id='test_scheduler_do_not_schedule_removed_task', start_date=DEFAULT_DATE)
        DummyOperator(task_id='dummy', dag=dag, owner='airflow')

        session = settings.Session()
        dag.sync_to_db(session=session)
        session.flush()

        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        dr = dag.create_dagrun(
            execution_date=DEFAULT_DATE,
            run_type=DagRunType.SCHEDULED,
            state=State.RUNNING,
            session=session,
        )
        assert dr is not None

        # Re-create the DAG, but remove the task
        dag = DAG(dag_id='test_scheduler_do_not_schedule_removed_task', start_date=DEFAULT_DATE)

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        res = self.scheduler_job._executable_task_instances_to_queued(max_tis=32, session=session)

        assert [] == res
        session.rollback()
        session.close()

    @provide_session
    def evaluate_dagrun(
        self,
        dag_id,
        expected_task_states,  # dict of task_id: state
        dagrun_state,
        run_kwargs=None,
        advance_execution_date=False,
        session=None,
    ):  # pylint: disable=unused-argument

        """
        Helper for testing DagRun states with simple two-task DAGS.
        This is hackish: a dag run is created but its tasks are
        run by a backfill.
        """
        if run_kwargs is None:
            run_kwargs = {}

        dag = self.dagbag.get_dag(dag_id)
        dr = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=dag.next_dagrun_after_date(None),
            state=State.RUNNING,
        )

        if advance_execution_date:
            # run a second time to schedule a dagrun after the start_date
            dr = dag.create_dagrun(
                run_type=DagRunType.SCHEDULED,
                execution_date=dag.following_schedule(dr.execution_date),
                state=State.RUNNING,
            )
        ex_date = dr.execution_date

        for tid, state in expected_task_states.items():
            if state != State.FAILED:
                continue
            self.null_exec.mock_task_fail(dag_id, tid, ex_date)

        try:
            dag = DagBag().get_dag(dag.dag_id)
            assert not isinstance(dag, SerializedDAG)
            # This needs a _REAL_ dag, not the serialized version
            dag.run(start_date=ex_date, end_date=ex_date, executor=self.null_exec, **run_kwargs)
        except AirflowException:
            pass

        # test tasks
        for task_id, expected_state in expected_task_states.items():
            task = dag.get_task(task_id)
            ti = TaskInstance(task, ex_date)
            ti.refresh_from_db()
            assert ti.state == expected_state

        # load dagrun
        dr = DagRun.find(dag_id=dag_id, execution_date=ex_date)
        dr = dr[0]
        dr.dag = dag

        assert dr.state == dagrun_state

    def test_dagrun_fail(self):
        """
        DagRuns with one failed and one incomplete root task -> FAILED
        """
        self.evaluate_dagrun(
            dag_id='test_dagrun_states_fail',
            expected_task_states={
                'test_dagrun_fail': State.FAILED,
                'test_dagrun_succeed': State.UPSTREAM_FAILED,
            },
            dagrun_state=State.FAILED,
        )

    def test_dagrun_success(self):
        """
        DagRuns with one failed and one successful root task -> SUCCESS
        """
        self.evaluate_dagrun(
            dag_id='test_dagrun_states_success',
            expected_task_states={
                'test_dagrun_fail': State.FAILED,
                'test_dagrun_succeed': State.SUCCESS,
            },
            dagrun_state=State.SUCCESS,
        )

    def test_dagrun_root_fail(self):
        """
        DagRuns with one successful and one failed root task -> FAILED
        """
        self.evaluate_dagrun(
            dag_id='test_dagrun_states_root_fail',
            expected_task_states={
                'test_dagrun_succeed': State.SUCCESS,
                'test_dagrun_fail': State.FAILED,
            },
            dagrun_state=State.FAILED,
        )

    def test_dagrun_root_fail_unfinished(self):
        """
        DagRuns with one unfinished and one failed root task -> RUNNING
        """
        # TODO: this should live in test_dagrun.py
        # Run both the failed and successful tasks
        dag_id = 'test_dagrun_states_root_fail_unfinished'
        dag = self.dagbag.get_dag(dag_id)
        dr = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        self.null_exec.mock_task_fail(dag_id, 'test_dagrun_fail', DEFAULT_DATE)

        with pytest.raises(AirflowException):
            dag.run(start_date=dr.execution_date, end_date=dr.execution_date, executor=self.null_exec)

        # Mark the successful task as never having run since we want to see if the
        # dagrun will be in a running state despite having an unfinished task.
        with create_session() as session:
            ti = dr.get_task_instance('test_dagrun_unfinished', session=session)
            ti.state = State.NONE
            session.commit()
        dr.update_state()
        assert dr.state == State.RUNNING

    def test_dagrun_root_after_dagrun_unfinished(self):
        """
        DagRuns with one successful and one future root task -> SUCCESS

        Noted: the DagRun state could be still in running state during CI.
        """
        clear_db_dags()
        dag_id = 'test_dagrun_states_root_future'
        dag = self.dagbag.get_dag(dag_id)
        dag.sync_to_db()
        self.scheduler_job = SchedulerJob(num_runs=1, executor=self.null_exec, subdir=dag.fileloc)
        self.scheduler_job.run()

        first_run = DagRun.find(dag_id=dag_id, execution_date=DEFAULT_DATE)[0]
        ti_ids = [(ti.task_id, ti.state) for ti in first_run.get_task_instances()]

        assert ti_ids == [('current', State.SUCCESS)]
        assert first_run.state in [State.SUCCESS, State.RUNNING]

    def test_dagrun_deadlock_ignore_depends_on_past_advance_ex_date(self):
        """
        DagRun is marked a success if ignore_first_depends_on_past=True

        Test that an otherwise-deadlocked dagrun is marked as a success
        if ignore_first_depends_on_past=True and the dagrun execution_date
        is after the start_date.
        """
        self.evaluate_dagrun(
            dag_id='test_dagrun_states_deadlock',
            expected_task_states={
                'test_depends_on_past': State.SUCCESS,
                'test_depends_on_past_2': State.SUCCESS,
            },
            dagrun_state=State.SUCCESS,
            advance_execution_date=True,
            run_kwargs=dict(ignore_first_depends_on_past=True),
        )

    def test_dagrun_deadlock_ignore_depends_on_past(self):
        """
        Test that ignore_first_depends_on_past doesn't affect results
        (this is the same test as
        test_dagrun_deadlock_ignore_depends_on_past_advance_ex_date except
        that start_date == execution_date so depends_on_past is irrelevant).
        """
        self.evaluate_dagrun(
            dag_id='test_dagrun_states_deadlock',
            expected_task_states={
                'test_depends_on_past': State.SUCCESS,
                'test_depends_on_past_2': State.SUCCESS,
            },
            dagrun_state=State.SUCCESS,
            run_kwargs=dict(ignore_first_depends_on_past=True),
        )

    def test_scheduler_start_date(self):
        """
        Test that the scheduler respects start_dates, even when DAGS have run
        """
        with create_session() as session:
            dag_id = 'test_start_date_scheduling'
            dag = self.dagbag.get_dag(dag_id)
            dag.clear()
            assert dag.start_date > datetime.datetime.now(timezone.utc)

            # Deactivate other dags in this file
            other_dag = self.dagbag.get_dag('test_task_start_date_scheduling')
            other_dag.is_paused_upon_creation = True
            other_dag.sync_to_db()

            self.scheduler_job = SchedulerJob(executor=self.null_exec, subdir=dag.fileloc, num_runs=1)
            self.scheduler_job.run()

            # zero tasks ran
            assert len(session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id).all()) == 0
            session.commit()
            assert [] == self.null_exec.sorted_tasks

            # previously, running this backfill would kick off the Scheduler
            # because it would take the most recent run and start from there
            # That behavior still exists, but now it will only do so if after the
            # start date
            bf_exec = MockExecutor()
            backfill = BackfillJob(executor=bf_exec, dag=dag, start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
            backfill.run()

            # one task ran
            assert len(session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id).all()) == 1
            assert [
                (TaskInstanceKey(dag.dag_id, 'dummy', DEFAULT_DATE, 1), (State.SUCCESS, None)),
            ] == bf_exec.sorted_tasks
            session.commit()

            self.scheduler_job = SchedulerJob(dag.fileloc, executor=self.null_exec, num_runs=1)
            self.scheduler_job.run()

            # still one task
            assert len(session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id).all()) == 1
            session.commit()
            assert [] == self.null_exec.sorted_tasks

    def test_scheduler_task_start_date(self):
        """
        Test that the scheduler respects task start dates that are different from DAG start dates
        """

        dagbag = DagBag(dag_folder=os.path.join(settings.DAGS_FOLDER, "no_dags.py"), include_examples=False)
        dag_id = 'test_task_start_date_scheduling'
        dag = self.dagbag.get_dag(dag_id)
        dag.is_paused_upon_creation = False
        dagbag.bag_dag(dag=dag, root_dag=dag)

        # Deactivate other dags in this file so the scheduler doesn't waste time processing them
        other_dag = self.dagbag.get_dag('test_start_date_scheduling')
        other_dag.is_paused_upon_creation = True
        dagbag.bag_dag(dag=other_dag, root_dag=other_dag)

        dagbag.sync_to_db()

        self.scheduler_job = SchedulerJob(executor=self.null_exec, subdir=dag.fileloc, num_runs=2)
        self.scheduler_job.run()

        session = settings.Session()
        tiq = session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id)
        ti1s = tiq.filter(TaskInstance.task_id == 'dummy1').all()
        ti2s = tiq.filter(TaskInstance.task_id == 'dummy2').all()
        assert len(ti1s) == 0
        assert len(ti2s) == 2
        for task in ti2s:
            assert task.state == State.SUCCESS

    def test_scheduler_multiprocessing(self):
        """
        Test that the scheduler can successfully queue multiple dags in parallel
        """
        dag_ids = ['test_start_date_scheduling', 'test_dagrun_states_success']
        for dag_id in dag_ids:
            dag = self.dagbag.get_dag(dag_id)
            dag.clear()

        self.scheduler_job = SchedulerJob(
            executor=self.null_exec,
            subdir=os.path.join(TEST_DAG_FOLDER, 'test_scheduler_dags.py'),
            num_runs=1,
        )
        self.scheduler_job.run()

        # zero tasks ran
        dag_id = 'test_start_date_scheduling'
        session = settings.Session()
        assert len(session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id).all()) == 0

    @conf_vars({("core", "mp_start_method"): "spawn"})
    def test_scheduler_multiprocessing_with_spawn_method(self):
        """
        Test that the scheduler can successfully queue multiple dags in parallel
        when using "spawn" mode of multiprocessing. (Fork is default on Linux and older OSX)
        """
        dag_ids = ['test_start_date_scheduling', 'test_dagrun_states_success']
        for dag_id in dag_ids:
            dag = self.dagbag.get_dag(dag_id)
            dag.clear()

        self.scheduler_job = SchedulerJob(
            executor=self.null_exec,
            subdir=os.path.join(TEST_DAG_FOLDER, 'test_scheduler_dags.py'),
            num_runs=1,
        )

        self.scheduler_job.run()

        # zero tasks ran
        dag_id = 'test_start_date_scheduling'
        with create_session() as session:
            assert session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id).count() == 0

    def test_scheduler_verify_pool_full(self):
        """
        Test task instances not queued when pool is full
        """
        dag = DAG(dag_id='test_scheduler_verify_pool_full', start_date=DEFAULT_DATE)

        BashOperator(
            task_id='dummy',
            dag=dag,
            owner='airflow',
            pool='test_scheduler_verify_pool_full',
            bash_command='echo hi',
        )

        dagbag = DagBag(
            dag_folder=os.path.join(settings.DAGS_FOLDER, "no_dags.py"),
            include_examples=False,
            read_dags_from_db=True,
        )
        dagbag.bag_dag(dag=dag, root_dag=dag)
        dagbag.sync_to_db()

        session = settings.Session()
        pool = Pool(pool='test_scheduler_verify_pool_full', slots=1)
        session.add(pool)
        session.flush()

        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))
        SerializedDagModel.write_dag(dag)

        self.scheduler_job = SchedulerJob(executor=self.null_exec)
        self.scheduler_job.processor_agent = mock.MagicMock()

        # Create 2 dagruns, which will create 2 task instances.
        dr = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        self.scheduler_job._schedule_dag_run(dr, {}, session)
        dr = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=dag.following_schedule(dr.execution_date),
            state=State.RUNNING,
        )
        self.scheduler_job._schedule_dag_run(dr, {}, session)
        task_instances_list = self.scheduler_job._executable_task_instances_to_queued(
            max_tis=32, session=session
        )

        assert len(task_instances_list) == 1

    def test_scheduler_verify_pool_full_2_slots_per_task(self):
        """
        Test task instances not queued when pool is full.

        Variation with non-default pool_slots
        """
        dag = DAG(dag_id='test_scheduler_verify_pool_full_2_slots_per_task', start_date=DEFAULT_DATE)

        BashOperator(
            task_id='dummy',
            dag=dag,
            owner='airflow',
            pool='test_scheduler_verify_pool_full_2_slots_per_task',
            pool_slots=2,
            bash_command='echo hi',
        )

        dagbag = DagBag(
            dag_folder=os.path.join(settings.DAGS_FOLDER, "no_dags.py"),
            include_examples=False,
            read_dags_from_db=True,
        )
        dagbag.bag_dag(dag=dag, root_dag=dag)
        dagbag.sync_to_db()

        session = settings.Session()
        pool = Pool(pool='test_scheduler_verify_pool_full_2_slots_per_task', slots=6)
        session.add(pool)
        session.commit()
        SerializedDagModel.write_dag(dag)

        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        self.scheduler_job = SchedulerJob(executor=self.null_exec)
        self.scheduler_job.processor_agent = mock.MagicMock()

        # Create 5 dagruns, which will create 5 task instances.
        date = DEFAULT_DATE
        for _ in range(5):
            dr = dag.create_dagrun(
                run_type=DagRunType.SCHEDULED,
                execution_date=date,
                state=State.RUNNING,
            )
            self.scheduler_job._schedule_dag_run(dr, {}, session)
            date = dag.following_schedule(date)

        task_instances_list = self.scheduler_job._executable_task_instances_to_queued(
            max_tis=32, session=session
        )

        # As tasks require 2 slots, only 3 can fit into 6 available
        assert len(task_instances_list) == 3

    def test_scheduler_keeps_scheduling_pool_full(self):
        """
        Test task instances in a pool that isn't full keep getting scheduled even when a pool is full.
        """
        dag_d1 = DAG(dag_id='test_scheduler_keeps_scheduling_pool_full_d1', start_date=DEFAULT_DATE)
        BashOperator(
            task_id='test_scheduler_keeps_scheduling_pool_full_t1',
            dag=dag_d1,
            owner='airflow',
            pool='test_scheduler_keeps_scheduling_pool_full_p1',
            bash_command='echo hi',
        )

        dag_d2 = DAG(dag_id='test_scheduler_keeps_scheduling_pool_full_d2', start_date=DEFAULT_DATE)
        BashOperator(
            task_id='test_scheduler_keeps_scheduling_pool_full_t2',
            dag=dag_d2,
            owner='airflow',
            pool='test_scheduler_keeps_scheduling_pool_full_p2',
            bash_command='echo hi',
        )
        dagbag = DagBag(
            dag_folder=os.path.join(settings.DAGS_FOLDER, "no_dags.py"),
            include_examples=False,
            read_dags_from_db=True,
        )
        dagbag.bag_dag(dag=dag_d1, root_dag=dag_d1)
        dagbag.bag_dag(dag=dag_d2, root_dag=dag_d2)
        dagbag.sync_to_db()

        session = settings.Session()
        pool_p1 = Pool(pool='test_scheduler_keeps_scheduling_pool_full_p1', slots=1)
        pool_p2 = Pool(pool='test_scheduler_keeps_scheduling_pool_full_p2', slots=10)
        session.add(pool_p1)
        session.add(pool_p2)
        session.commit()

        dag_d1 = SerializedDAG.from_dict(SerializedDAG.to_dict(dag_d1))

        scheduler = SchedulerJob(executor=self.null_exec)
        scheduler.processor_agent = mock.MagicMock()

        # Create 5 dagruns for each DAG.
        # To increase the chances the TIs from the "full" pool will get retrieved first, we schedule all
        # TIs from the first dag first.
        date = DEFAULT_DATE
        for _ in range(5):
            dr = dag_d1.create_dagrun(
                run_type=DagRunType.SCHEDULED,
                execution_date=date,
                state=State.RUNNING,
            )
            scheduler._schedule_dag_run(dr, {}, session)
            date = dag_d1.following_schedule(date)

        date = DEFAULT_DATE
        for _ in range(5):
            dr = dag_d2.create_dagrun(
                run_type=DagRunType.SCHEDULED,
                execution_date=date,
                state=State.RUNNING,
            )
            scheduler._schedule_dag_run(dr, {}, session)
            date = dag_d2.following_schedule(date)

        scheduler._executable_task_instances_to_queued(max_tis=2, session=session)
        task_instances_list2 = scheduler._executable_task_instances_to_queued(max_tis=2, session=session)

        # Make sure we get TIs from a non-full pool in the 2nd list
        assert len(task_instances_list2) > 0
        assert all(
            task_instance.pool != 'test_scheduler_keeps_scheduling_pool_full_p1'
            for task_instance in task_instances_list2
        )

    def test_scheduler_verify_priority_and_slots(self):
        """
        Test task instances with higher priority are not queued
        when pool does not have enough slots.

        Though tasks with lower priority might be executed.
        """
        dag = DAG(dag_id='test_scheduler_verify_priority_and_slots', start_date=DEFAULT_DATE)

        # Medium priority, not enough slots
        BashOperator(
            task_id='test_scheduler_verify_priority_and_slots_t0',
            dag=dag,
            owner='airflow',
            pool='test_scheduler_verify_priority_and_slots',
            pool_slots=2,
            priority_weight=2,
            bash_command='echo hi',
        )
        # High priority, occupies first slot
        BashOperator(
            task_id='test_scheduler_verify_priority_and_slots_t1',
            dag=dag,
            owner='airflow',
            pool='test_scheduler_verify_priority_and_slots',
            pool_slots=1,
            priority_weight=3,
            bash_command='echo hi',
        )
        # Low priority, occupies second slot
        BashOperator(
            task_id='test_scheduler_verify_priority_and_slots_t2',
            dag=dag,
            owner='airflow',
            pool='test_scheduler_verify_priority_and_slots',
            pool_slots=1,
            priority_weight=1,
            bash_command='echo hi',
        )

        dagbag = DagBag(
            dag_folder=os.path.join(settings.DAGS_FOLDER, "no_dags.py"),
            include_examples=False,
            read_dags_from_db=True,
        )
        dagbag.bag_dag(dag=dag, root_dag=dag)
        dagbag.sync_to_db()

        session = settings.Session()
        pool = Pool(pool='test_scheduler_verify_priority_and_slots', slots=2)
        session.add(pool)
        session.commit()

        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))
        SerializedDagModel.write_dag(dag)

        self.scheduler_job = SchedulerJob(executor=self.null_exec)
        self.scheduler_job.processor_agent = mock.MagicMock()

        dr = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        self.scheduler_job._schedule_dag_run(dr, {}, session)

        task_instances_list = self.scheduler_job._executable_task_instances_to_queued(
            max_tis=32, session=session
        )

        # Only second and third
        assert len(task_instances_list) == 2

        ti0 = (
            session.query(TaskInstance)
            .filter(TaskInstance.task_id == 'test_scheduler_verify_priority_and_slots_t0')
            .first()
        )
        assert ti0.state == State.SCHEDULED

        ti1 = (
            session.query(TaskInstance)
            .filter(TaskInstance.task_id == 'test_scheduler_verify_priority_and_slots_t1')
            .first()
        )
        assert ti1.state == State.QUEUED

        ti2 = (
            session.query(TaskInstance)
            .filter(TaskInstance.task_id == 'test_scheduler_verify_priority_and_slots_t2')
            .first()
        )
        assert ti2.state == State.QUEUED

    def test_verify_integrity_if_dag_not_changed(self):
        # CleanUp
        with create_session() as session:
            session.query(SerializedDagModel).filter(
                SerializedDagModel.dag_id == 'test_verify_integrity_if_dag_not_changed'
            ).delete(synchronize_session=False)

        dag = DAG(dag_id='test_verify_integrity_if_dag_not_changed', start_date=DEFAULT_DATE)
        BashOperator(task_id='dummy', dag=dag, owner='airflow', bash_command='echo hi')

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        self.scheduler_job.dagbag.bag_dag(dag, root_dag=dag)
        self.scheduler_job.dagbag.sync_to_db()

        session = settings.Session()
        orm_dag = session.query(DagModel).get(dag.dag_id)
        assert orm_dag is not None

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        self.scheduler_job.processor_agent = mock.MagicMock()
        dag = self.scheduler_job.dagbag.get_dag('test_verify_integrity_if_dag_not_changed', session=session)
        self.scheduler_job._create_dag_runs([orm_dag], session)

        drs = DagRun.find(dag_id=dag.dag_id, session=session)
        assert len(drs) == 1
        dr = drs[0]

        # Verify that DagRun.verify_integrity is not called
        with mock.patch('airflow.jobs.scheduler_job.DagRun.verify_integrity') as mock_verify_integrity:
            scheduled_tis = self.scheduler_job._schedule_dag_run(dr, {}, session)
            mock_verify_integrity.assert_not_called()
        session.flush()

        assert scheduled_tis == 1

        tis_count = (
            session.query(func.count(TaskInstance.task_id))
            .filter(
                TaskInstance.dag_id == dr.dag_id,
                TaskInstance.execution_date == dr.execution_date,
                TaskInstance.task_id == dr.dag.tasks[0].task_id,
                TaskInstance.state == State.SCHEDULED,
            )
            .scalar()
        )
        assert tis_count == 1

        latest_dag_version = SerializedDagModel.get_latest_version_hash(dr.dag_id, session=session)
        assert dr.dag_hash == latest_dag_version

        session.rollback()
        session.close()

    def test_verify_integrity_if_dag_changed(self):
        # CleanUp
        with create_session() as session:
            session.query(SerializedDagModel).filter(
                SerializedDagModel.dag_id == 'test_verify_integrity_if_dag_changed'
            ).delete(synchronize_session=False)

        dag = DAG(dag_id='test_verify_integrity_if_dag_changed', start_date=DEFAULT_DATE)
        BashOperator(task_id='dummy', dag=dag, owner='airflow', bash_command='echo hi')

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        self.scheduler_job.dagbag.bag_dag(dag, root_dag=dag)
        self.scheduler_job.dagbag.sync_to_db()

        session = settings.Session()
        orm_dag = session.query(DagModel).get(dag.dag_id)
        assert orm_dag is not None

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        self.scheduler_job.processor_agent = mock.MagicMock()
        dag = self.scheduler_job.dagbag.get_dag('test_verify_integrity_if_dag_changed', session=session)
        self.scheduler_job._create_dag_runs([orm_dag], session)

        drs = DagRun.find(dag_id=dag.dag_id, session=session)
        assert len(drs) == 1
        dr = drs[0]

        dag_version_1 = SerializedDagModel.get_latest_version_hash(dr.dag_id, session=session)
        assert dr.dag_hash == dag_version_1
        assert self.scheduler_job.dagbag.dags == {'test_verify_integrity_if_dag_changed': dag}
        assert len(self.scheduler_job.dagbag.dags.get("test_verify_integrity_if_dag_changed").tasks) == 1

        # Now let's say the DAG got updated (new task got added)
        BashOperator(task_id='bash_task_1', dag=dag, bash_command='echo hi')
        SerializedDagModel.write_dag(dag=dag)

        dag_version_2 = SerializedDagModel.get_latest_version_hash(dr.dag_id, session=session)
        assert dag_version_2 != dag_version_1

        scheduled_tis = self.scheduler_job._schedule_dag_run(dr, {}, session)
        session.flush()

        assert scheduled_tis == 2

        drs = DagRun.find(dag_id=dag.dag_id, session=session)
        assert len(drs) == 1
        dr = drs[0]
        assert dr.dag_hash == dag_version_2
        assert self.scheduler_job.dagbag.dags == {'test_verify_integrity_if_dag_changed': dag}
        assert len(self.scheduler_job.dagbag.dags.get("test_verify_integrity_if_dag_changed").tasks) == 2

        tis_count = (
            session.query(func.count(TaskInstance.task_id))
            .filter(
                TaskInstance.dag_id == dr.dag_id,
                TaskInstance.execution_date == dr.execution_date,
                TaskInstance.state == State.SCHEDULED,
            )
            .scalar()
        )
        assert tis_count == 2

        latest_dag_version = SerializedDagModel.get_latest_version_hash(dr.dag_id, session=session)
        assert dr.dag_hash == latest_dag_version

        session.rollback()
        session.close()

    def test_retry_still_in_executor(self):
        """
        Checks if the scheduler does not put a task in limbo, when a task is retried
        but is still present in the executor.
        """
        executor = MockExecutor(do_update=False)
        dagbag = DagBag(dag_folder=os.path.join(settings.DAGS_FOLDER, "no_dags.py"), include_examples=False)
        dagbag.dags.clear()

        dag = DAG(dag_id='test_retry_still_in_executor', start_date=DEFAULT_DATE, schedule_interval="@once")
        dag_task1 = BashOperator(
            task_id='test_retry_handling_op', bash_command='exit 1', retries=1, dag=dag, owner='airflow'
        )

        dag.clear()
        dag.is_subdag = False

        with create_session() as session:
            orm_dag = DagModel(dag_id=dag.dag_id)
            orm_dag.is_paused = False
            session.merge(orm_dag)

        dagbag.bag_dag(dag=dag, root_dag=dag)
        dagbag.sync_to_db()

        @mock.patch('airflow.jobs.scheduler_job.DagBag', return_value=dagbag)
        def do_schedule(mock_dagbag):
            # Use a empty file since the above mock will return the
            # expected DAGs. Also specify only a single file so that it doesn't
            # try to schedule the above DAG repeatedly.
            self.scheduler_job = SchedulerJob(
                num_runs=1, executor=executor, subdir=os.path.join(settings.DAGS_FOLDER, "no_dags.py")
            )
            self.scheduler_job.heartrate = 0
            self.scheduler_job.run()

        do_schedule()  # pylint: disable=no-value-for-parameter
        with create_session() as session:
            ti = (
                session.query(TaskInstance)
                .filter(
                    TaskInstance.dag_id == 'test_retry_still_in_executor',
                    TaskInstance.task_id == 'test_retry_handling_op',
                )
                .first()
            )
        ti.task = dag_task1

        def run_with_error(ti, ignore_ti_state=False):
            try:
                ti.run(ignore_ti_state=ignore_ti_state)
            except AirflowException:
                pass

        assert ti.try_number == 1
        # At this point, scheduler has tried to schedule the task once and
        # heartbeated the executor once, which moved the state of the task from
        # SCHEDULED to QUEUED and then to SCHEDULED, to fail the task execution
        # we need to ignore the TaskInstance state as SCHEDULED is not a valid state to start
        # executing task.
        run_with_error(ti, ignore_ti_state=True)
        assert ti.state == State.UP_FOR_RETRY
        assert ti.try_number == 2

        with create_session() as session:
            ti.refresh_from_db(lock_for_update=True, session=session)
            ti.state = State.SCHEDULED
            session.merge(ti)

        # To verify that task does get re-queued.
        executor.do_update = True
        do_schedule()  # pylint: disable=no-value-for-parameter
        ti.refresh_from_db()
        assert ti.state == State.SUCCESS

    @pytest.mark.skip(reason="This test needs fixing. It's very wrong now and always fails")
    def test_retry_handling_job(self):
        """
        Integration test of the scheduler not accidentally resetting
        the try_numbers for a task
        """
        dag = self.dagbag.get_dag('test_retry_handling_job')
        dag_task1 = dag.get_task("test_retry_handling_op")
        dag.clear()

        self.scheduler_job = SchedulerJob(dag_id=dag.dag_id, num_runs=1)
        self.scheduler_job.heartrate = 0
        self.scheduler_job.run()

        session = settings.Session()
        ti = (
            session.query(TaskInstance)
            .filter(TaskInstance.dag_id == dag.dag_id, TaskInstance.task_id == dag_task1.task_id)
            .first()
        )
        # make sure the counter has increased
        assert ti.try_number == 2
        assert ti.state == State.UP_FOR_RETRY

    def test_dag_get_active_runs(self):
        """
        Test to check that a DAG returns its active runs
        """

        now = timezone.utcnow()
        six_hours_ago_to_the_hour = (now - datetime.timedelta(hours=6)).replace(
            minute=0, second=0, microsecond=0
        )

        start_date = six_hours_ago_to_the_hour
        dag_name1 = 'get_active_runs_test'

        default_args = {'owner': 'airflow', 'depends_on_past': False, 'start_date': start_date}
        dag1 = DAG(dag_name1, schedule_interval='* * * * *', max_active_runs=1, default_args=default_args)

        run_this_1 = DummyOperator(task_id='run_this_1', dag=dag1)
        run_this_2 = DummyOperator(task_id='run_this_2', dag=dag1)
        run_this_2.set_upstream(run_this_1)
        run_this_3 = DummyOperator(task_id='run_this_3', dag=dag1)
        run_this_3.set_upstream(run_this_2)

        session = settings.Session()
        orm_dag = DagModel(dag_id=dag1.dag_id)
        session.merge(orm_dag)
        session.commit()
        session.close()

        dag1.clear()

        dr = dag1.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=start_date,
            state=State.RUNNING,
        )

        # We had better get a dag run
        assert dr is not None

        execution_date = dr.execution_date

        running_dates = dag1.get_active_runs()

        try:
            running_date = running_dates[0]
        except Exception:  # pylint: disable=broad-except
            running_date = 'Except'

        assert execution_date == running_date, 'Running Date must match Execution Date'

    @conf_vars({("core", "dagbag_import_error_tracebacks"): "False"})
    def test_add_unparseable_file_before_sched_start_creates_import_error(self):
        dags_folder = mkdtemp()
        try:
            with env_vars({('core', 'dags_folder'): dags_folder}):
                unparseable_filename = os.path.join(dags_folder, TEMP_DAG_FILENAME)
                with open(unparseable_filename, 'w') as unparseable_file:
                    unparseable_file.writelines(UNPARSEABLE_DAG_FILE_CONTENTS)
                self.run_single_scheduler_loop_with_no_dags(dags_folder)
        finally:
            shutil.rmtree(dags_folder)

        with create_session() as session:
            import_errors = session.query(errors.ImportError).all()

        assert len(import_errors) == 1
        import_error = import_errors[0]
        assert import_error.filename == unparseable_filename
        assert import_error.stacktrace == f"invalid syntax ({TEMP_DAG_FILENAME}, line 1)"

    @conf_vars({("core", "dagbag_import_error_tracebacks"): "False"})
    def test_add_unparseable_file_after_sched_start_creates_import_error(self):
        """
        Check that new DAG files are picked up, and import errors recorded.

        This is more of an "integration" test as it checks SchedulerJob, DagFileProcessorManager and
        DagFileProcessor
        """
        dags_folder = mkdtemp()
        try:
            unparseable_filename = os.path.join(dags_folder, TEMP_DAG_FILENAME)
            self.run_single_scheduler_loop_with_no_dags(dags_folder)

            with open(unparseable_filename, 'w') as unparseable_file:
                unparseable_file.writelines(UNPARSEABLE_DAG_FILE_CONTENTS)
            print("Second run")
            self.run_single_scheduler_loop_with_no_dags(dags_folder)
        finally:
            shutil.rmtree(dags_folder)

        with create_session() as session:
            import_errors = session.query(errors.ImportError).all()

        assert len(import_errors) == 1
        import_error = import_errors[0]
        assert import_error.filename == unparseable_filename
        assert import_error.stacktrace == f"invalid syntax ({TEMP_DAG_FILENAME}, line 1)"

    def test_no_import_errors_with_parseable_dag(self):
        try:
            dags_folder = mkdtemp()
            parseable_filename = os.path.join(dags_folder, TEMP_DAG_FILENAME)

            with open(parseable_filename, 'w') as parseable_file:
                parseable_file.writelines(PARSEABLE_DAG_FILE_CONTENTS)
            self.run_single_scheduler_loop_with_no_dags(dags_folder)
        finally:
            shutil.rmtree(dags_folder)

        with create_session() as session:
            import_errors = session.query(errors.ImportError).all()

        assert len(import_errors) == 0

    @conf_vars({("core", "dagbag_import_error_tracebacks"): "False"})
    def test_new_import_error_replaces_old(self):
        try:
            dags_folder = mkdtemp()
            unparseable_filename = os.path.join(dags_folder, TEMP_DAG_FILENAME)

            # Generate original import error
            with open(unparseable_filename, 'w') as unparseable_file:
                unparseable_file.writelines(UNPARSEABLE_DAG_FILE_CONTENTS)
            self.run_single_scheduler_loop_with_no_dags(dags_folder)

            # Generate replacement import error (the error will be on the second line now)
            with open(unparseable_filename, 'w') as unparseable_file:
                unparseable_file.writelines(
                    PARSEABLE_DAG_FILE_CONTENTS + os.linesep + UNPARSEABLE_DAG_FILE_CONTENTS
                )
            self.run_single_scheduler_loop_with_no_dags(dags_folder)
        finally:
            shutil.rmtree(dags_folder)

        session = settings.Session()
        import_errors = session.query(errors.ImportError).all()

        assert len(import_errors) == 1
        import_error = import_errors[0]
        assert import_error.filename == unparseable_filename
        assert import_error.stacktrace == f"invalid syntax ({TEMP_DAG_FILENAME}, line 2)"

    def test_remove_error_clears_import_error(self):
        try:
            dags_folder = mkdtemp()
            filename_to_parse = os.path.join(dags_folder, TEMP_DAG_FILENAME)

            # Generate original import error
            with open(filename_to_parse, 'w') as file_to_parse:
                file_to_parse.writelines(UNPARSEABLE_DAG_FILE_CONTENTS)
            self.run_single_scheduler_loop_with_no_dags(dags_folder)

            # Remove the import error from the file
            with open(filename_to_parse, 'w') as file_to_parse:
                file_to_parse.writelines(PARSEABLE_DAG_FILE_CONTENTS)
            self.run_single_scheduler_loop_with_no_dags(dags_folder)
        finally:
            shutil.rmtree(dags_folder)

        session = settings.Session()
        import_errors = session.query(errors.ImportError).all()

        assert len(import_errors) == 0

    def test_remove_file_clears_import_error(self):
        try:
            dags_folder = mkdtemp()
            filename_to_parse = os.path.join(dags_folder, TEMP_DAG_FILENAME)

            # Generate original import error
            with open(filename_to_parse, 'w') as file_to_parse:
                file_to_parse.writelines(UNPARSEABLE_DAG_FILE_CONTENTS)
            self.run_single_scheduler_loop_with_no_dags(dags_folder)
        finally:
            shutil.rmtree(dags_folder)

        # Rerun the scheduler once the dag file has been removed
        self.run_single_scheduler_loop_with_no_dags(dags_folder)

        with create_session() as session:
            import_errors = session.query(errors.ImportError).all()

        assert len(import_errors) == 0

    def test_import_error_tracebacks(self):
        dags_folder = mkdtemp()
        try:
            unparseable_filename = os.path.join(dags_folder, TEMP_DAG_FILENAME)
            with open(unparseable_filename, "w") as unparseable_file:
                unparseable_file.writelines(INVALID_DAG_WITH_DEPTH_FILE_CONTENTS)
            self.run_single_scheduler_loop_with_no_dags(dags_folder)
        finally:
            shutil.rmtree(dags_folder)

        with create_session() as session:
            import_errors = session.query(errors.ImportError).all()

        assert len(import_errors) == 1
        import_error = import_errors[0]
        assert import_error.filename == unparseable_filename
        expected_stacktrace = (
            "Traceback (most recent call last):\n"
            '  File "{}", line 3, in <module>\n'
            "    something()\n"
            '  File "{}", line 2, in something\n'
            "    return airflow_DAG\n"
            "NameError: name 'airflow_DAG' is not defined\n"
        )
        assert import_error.stacktrace == expected_stacktrace.format(
            unparseable_filename, unparseable_filename
        )

    @conf_vars({("core", "dagbag_import_error_traceback_depth"): "1"})
    def test_import_error_traceback_depth(self):
        dags_folder = mkdtemp()
        try:
            unparseable_filename = os.path.join(dags_folder, TEMP_DAG_FILENAME)
            with open(unparseable_filename, "w") as unparseable_file:
                unparseable_file.writelines(INVALID_DAG_WITH_DEPTH_FILE_CONTENTS)
            self.run_single_scheduler_loop_with_no_dags(dags_folder)
        finally:
            shutil.rmtree(dags_folder)

        with create_session() as session:
            import_errors = session.query(errors.ImportError).all()

        assert len(import_errors) == 1
        import_error = import_errors[0]
        assert import_error.filename == unparseable_filename
        expected_stacktrace = (
            "Traceback (most recent call last):\n"
            '  File "{}", line 2, in something\n'
            "    return airflow_DAG\n"
            "NameError: name 'airflow_DAG' is not defined\n"
        )
        assert import_error.stacktrace == expected_stacktrace.format(unparseable_filename)

    def test_import_error_tracebacks_zip(self):
        dags_folder = mkdtemp()
        try:
            invalid_zip_filename = os.path.join(dags_folder, "test_zip_invalid.zip")
            invalid_dag_filename = os.path.join(dags_folder, "test_zip_invalid.zip", TEMP_DAG_FILENAME)
            with ZipFile(invalid_zip_filename, "w") as invalid_zip_file:
                invalid_zip_file.writestr(TEMP_DAG_FILENAME, INVALID_DAG_WITH_DEPTH_FILE_CONTENTS)
            self.run_single_scheduler_loop_with_no_dags(dags_folder)
        finally:
            shutil.rmtree(dags_folder)

        with create_session() as session:
            import_errors = session.query(errors.ImportError).all()

        assert len(import_errors) == 1
        import_error = import_errors[0]
        assert import_error.filename == invalid_zip_filename
        expected_stacktrace = (
            "Traceback (most recent call last):\n"
            '  File "{}", line 3, in <module>\n'
            "    something()\n"
            '  File "{}", line 2, in something\n'
            "    return airflow_DAG\n"
            "NameError: name 'airflow_DAG' is not defined\n"
        )
        assert import_error.stacktrace == expected_stacktrace.format(
            invalid_dag_filename, invalid_dag_filename
        )

    @conf_vars({("core", "dagbag_import_error_traceback_depth"): "1"})
    def test_import_error_tracebacks_zip_depth(self):
        dags_folder = mkdtemp()
        try:
            invalid_zip_filename = os.path.join(dags_folder, "test_zip_invalid.zip")
            invalid_dag_filename = os.path.join(dags_folder, "test_zip_invalid.zip", TEMP_DAG_FILENAME)
            with ZipFile(invalid_zip_filename, "w") as invalid_zip_file:
                invalid_zip_file.writestr(TEMP_DAG_FILENAME, INVALID_DAG_WITH_DEPTH_FILE_CONTENTS)
            self.run_single_scheduler_loop_with_no_dags(dags_folder)
        finally:
            shutil.rmtree(dags_folder)

        with create_session() as session:
            import_errors = session.query(errors.ImportError).all()

        assert len(import_errors) == 1
        import_error = import_errors[0]
        assert import_error.filename == invalid_zip_filename
        expected_stacktrace = (
            "Traceback (most recent call last):\n"
            '  File "{}", line 2, in something\n'
            "    return airflow_DAG\n"
            "NameError: name 'airflow_DAG' is not defined\n"
        )
        assert import_error.stacktrace == expected_stacktrace.format(invalid_dag_filename)

    def test_list_py_file_paths(self):
        """
        [JIRA-1357] Test the 'list_py_file_paths' function used by the
        scheduler to list and load DAGs.
        """
        detected_files = set()
        expected_files = set()
        # No_dags is empty, _invalid_ is ignored by .airflowignore
        ignored_files = {
            'no_dags.py',
            'test_invalid_cron.py',
            'test_zip_invalid_cron.zip',
            'test_ignore_this.py',
        }
        for root, _, files in os.walk(TEST_DAG_FOLDER):  # pylint: disable=too-many-nested-blocks
            for file_name in files:
                if file_name.endswith('.py') or file_name.endswith('.zip'):
                    if file_name not in ignored_files:
                        expected_files.add(f'{root}/{file_name}')
        for file_path in list_py_file_paths(TEST_DAG_FOLDER, include_examples=False):
            detected_files.add(file_path)
        assert detected_files == expected_files

        ignored_files = {
            'helper.py',
        }
        example_dag_folder = airflow.example_dags.__path__[0]
        for root, _, files in os.walk(example_dag_folder):  # pylint: disable=too-many-nested-blocks
            for file_name in files:
                if file_name.endswith('.py') or file_name.endswith('.zip'):
                    if file_name not in ['__init__.py'] and file_name not in ignored_files:
                        expected_files.add(os.path.join(root, file_name))
        detected_files.clear()
        for file_path in list_py_file_paths(TEST_DAG_FOLDER, include_examples=True):
            detected_files.add(file_path)
        assert detected_files == expected_files

        smart_sensor_dag_folder = airflow.smart_sensor_dags.__path__[0]
        for root, _, files in os.walk(smart_sensor_dag_folder):
            for file_name in files:
                if (file_name.endswith('.py') or file_name.endswith('.zip')) and file_name not in [
                    '__init__.py'
                ]:
                    expected_files.add(os.path.join(root, file_name))
        detected_files.clear()
        for file_path in list_py_file_paths(
            TEST_DAG_FOLDER, include_examples=True, include_smart_sensor=True
        ):
            detected_files.add(file_path)
        assert detected_files == expected_files

    def test_adopt_or_reset_orphaned_tasks_nothing(self):
        """Try with nothing. """
        self.scheduler_job = SchedulerJob()
        session = settings.Session()
        assert 0 == self.scheduler_job.adopt_or_reset_orphaned_tasks(session=session)

    def test_adopt_or_reset_orphaned_tasks_external_triggered_dag(self):
        dag_id = 'test_reset_orphaned_tasks_external_triggered_dag'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, schedule_interval='@daily')
        task_id = dag_id + '_task'
        DummyOperator(task_id=task_id, dag=dag)

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        session = settings.Session()

        dr1 = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            state=State.RUNNING,
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            external_trigger=True,
            session=session,
        )
        ti = dr1.get_task_instances(session=session)[0]
        ti.state = State.SCHEDULED
        session.merge(ti)
        session.merge(dr1)
        session.commit()

        num_reset_tis = self.scheduler_job.adopt_or_reset_orphaned_tasks(session=session)
        assert 1 == num_reset_tis

    def test_adopt_or_reset_orphaned_tasks_backfill_dag(self):
        dag_id = 'test_adopt_or_reset_orphaned_tasks_backfill_dag'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, schedule_interval='@daily')
        task_id = dag_id + '_task'
        DummyOperator(task_id=task_id, dag=dag)

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        session = settings.Session()
        session.add(self.scheduler_job)
        session.flush()

        dr1 = dag.create_dagrun(
            run_type=DagRunType.BACKFILL_JOB,
            state=State.RUNNING,
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            session=session,
        )
        ti = dr1.get_task_instances(session=session)[0]
        ti.state = State.SCHEDULED
        session.merge(ti)
        session.merge(dr1)
        session.flush()

        assert dr1.is_backfill
        assert 0 == self.scheduler_job.adopt_or_reset_orphaned_tasks(session=session)
        session.rollback()

    def test_reset_orphaned_tasks_nonexistent_dagrun(self):
        """Make sure a task in an orphaned state is not reset if it has no dagrun. """
        dag_id = 'test_reset_orphaned_tasks_nonexistent_dagrun'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, schedule_interval='@daily')
        task_id = dag_id + '_task'
        task = DummyOperator(task_id=task_id, dag=dag)

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        session = settings.Session()

        ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)
        session.add(ti)
        session.flush()

        ti.refresh_from_db()
        ti.state = State.SCHEDULED
        session.merge(ti)
        session.flush()

        assert 0 == self.scheduler_job.adopt_or_reset_orphaned_tasks(session=session)
        session.rollback()

    def test_reset_orphaned_tasks_no_orphans(self):
        dag_id = 'test_reset_orphaned_tasks_no_orphans'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, schedule_interval='@daily')
        task_id = dag_id + '_task'
        DummyOperator(task_id=task_id, dag=dag)

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        session = settings.Session()
        session.add(self.scheduler_job)
        session.flush()

        dr1 = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            state=State.RUNNING,
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            session=session,
        )
        tis = dr1.get_task_instances(session=session)
        tis[0].state = State.RUNNING
        tis[0].queued_by_job_id = self.scheduler_job.id
        session.merge(dr1)
        session.merge(tis[0])
        session.flush()

        assert 0 == self.scheduler_job.adopt_or_reset_orphaned_tasks(session=session)
        tis[0].refresh_from_db()
        assert State.RUNNING == tis[0].state

    def test_reset_orphaned_tasks_non_running_dagruns(self):
        """Ensure orphaned tasks with non-running dagruns are not reset."""
        dag_id = 'test_reset_orphaned_tasks_non_running_dagruns'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, schedule_interval='@daily')
        task_id = dag_id + '_task'
        DummyOperator(task_id=task_id, dag=dag)

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        session = settings.Session()
        session.add(self.scheduler_job)
        session.flush()

        dr1 = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            state=State.SUCCESS,
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            session=session,
        )
        tis = dr1.get_task_instances(session=session)
        assert 1 == len(tis)
        tis[0].state = State.SCHEDULED
        tis[0].queued_by_job_id = self.scheduler_job.id
        session.merge(dr1)
        session.merge(tis[0])
        session.flush()

        assert 0 == self.scheduler_job.adopt_or_reset_orphaned_tasks(session=session)
        session.rollback()

    def test_adopt_or_reset_orphaned_tasks_stale_scheduler_jobs(self):
        dag_id = 'test_adopt_or_reset_orphaned_tasks_stale_scheduler_jobs'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, schedule_interval='@daily')
        DummyOperator(task_id='task1', dag=dag)
        DummyOperator(task_id='task2', dag=dag)

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        session = settings.Session()
        self.scheduler_job.state = State.RUNNING
        self.scheduler_job.latest_heartbeat = timezone.utcnow()
        session.add(self.scheduler_job)

        old_job = SchedulerJob(subdir=os.devnull)
        old_job.state = State.RUNNING
        old_job.latest_heartbeat = timezone.utcnow() - timedelta(minutes=15)
        session.add(old_job)
        session.flush()

        dr1 = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
            start_date=timezone.utcnow(),
            state=State.RUNNING,
            session=session,
        )

        ti1, ti2 = dr1.get_task_instances(session=session)
        dr1.state = State.RUNNING
        ti1.state = State.SCHEDULED
        ti1.queued_by_job_id = old_job.id
        session.merge(dr1)
        session.merge(ti1)

        ti2.state = State.SCHEDULED
        ti2.queued_by_job_id = self.scheduler_job.id
        session.merge(ti2)
        session.flush()

        num_reset_tis = self.scheduler_job.adopt_or_reset_orphaned_tasks(session=session)

        assert 1 == num_reset_tis

        session.refresh(ti1)
        assert ti1.state is None
        session.refresh(ti2)
        assert State.SCHEDULED == ti2.state
        session.rollback()
        if old_job.processor_agent:
            old_job.processor_agent.end()

    def test_send_sla_callbacks_to_processor_sla_disabled(self):
        """Test SLA Callbacks are not sent when check_slas is False"""
        dag_id = 'test_send_sla_callbacks_to_processor_sla_disabled'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, schedule_interval='@daily')
        DummyOperator(task_id='task1', dag=dag)

        with patch.object(settings, "CHECK_SLAS", False):
            self.scheduler_job = SchedulerJob(subdir=os.devnull)
            mock_agent = mock.MagicMock()

            self.scheduler_job.processor_agent = mock_agent

            self.scheduler_job._send_sla_callbacks_to_processor(dag)
            self.scheduler_job.processor_agent.send_sla_callback_request_to_execute.assert_not_called()

    def test_send_sla_callbacks_to_processor_sla_no_task_slas(self):
        """Test SLA Callbacks are not sent when no task SLAs are defined"""
        dag_id = 'test_send_sla_callbacks_to_processor_sla_no_task_slas'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, schedule_interval='@daily')
        DummyOperator(task_id='task1', dag=dag)

        with patch.object(settings, "CHECK_SLAS", True):
            self.scheduler_job = SchedulerJob(subdir=os.devnull)
            mock_agent = mock.MagicMock()

            self.scheduler_job.processor_agent = mock_agent

            self.scheduler_job._send_sla_callbacks_to_processor(dag)
            self.scheduler_job.processor_agent.send_sla_callback_request_to_execute.assert_not_called()

    def test_send_sla_callbacks_to_processor_sla_with_task_slas(self):
        """Test SLA Callbacks are sent to the DAG Processor when SLAs are defined on tasks"""
        dag_id = 'test_send_sla_callbacks_to_processor_sla_with_task_slas'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, schedule_interval='@daily')
        DummyOperator(task_id='task1', dag=dag, sla=timedelta(seconds=60))

        # Used Serialized DAG as Serialized DAG is used in Scheduler
        dag = SerializedDAG.from_json(SerializedDAG.to_json(dag))

        with patch.object(settings, "CHECK_SLAS", True):
            self.scheduler_job = SchedulerJob(subdir=os.devnull)
            mock_agent = mock.MagicMock()

            self.scheduler_job.processor_agent = mock_agent

            self.scheduler_job._send_sla_callbacks_to_processor(dag)
            self.scheduler_job.processor_agent.send_sla_callback_request_to_execute.assert_called_once_with(
                full_filepath=dag.fileloc, dag_id=dag_id
            )

    @freeze_time(DEFAULT_DATE + datetime.timedelta(days=1, seconds=9))
    @mock.patch('airflow.jobs.scheduler_job.Stats.timing')
    def test_create_dag_runs(self, stats_timing):
        """
        Test various invariants of _create_dag_runs.

        - That the run created has the creating_job_id set
        - That we emit the right DagRun metrics
        """
        dag = DAG(dag_id='test_create_dag_runs', start_date=DEFAULT_DATE)

        DummyOperator(
            task_id='dummy',
            dag=dag,
        )

        dagbag = DagBag(
            dag_folder=os.devnull,
            include_examples=False,
            read_dags_from_db=True,
        )
        dagbag.bag_dag(dag=dag, root_dag=dag)
        dagbag.sync_to_db()
        dag_model = DagModel.get_dagmodel(dag.dag_id)

        self.scheduler_job = SchedulerJob(executor=self.null_exec)
        self.scheduler_job.processor_agent = mock.MagicMock()

        with create_session() as session:
            self.scheduler_job._create_dag_runs([dag_model], session)

        stats_timing.assert_called_once_with(
            "dagrun.schedule_delay.test_create_dag_runs", datetime.timedelta(seconds=9)
        )

        assert dag.get_last_dagrun().creating_job_id == self.scheduler_job.id

    def test_extra_operator_links_not_loaded_in_scheduler_loop(self):
        """
        Test that Operator links are not loaded inside the Scheduling Loop (that does not include
        DagFileProcessorProcess) especially the critical loop of the Scheduler.

        This is to avoid running User code in the Scheduler and prevent any deadlocks
        """
        dag = DAG(dag_id='test_extra_operator_links_not_loaded_in_scheduler', start_date=DEFAULT_DATE)

        # This CustomOperator has Extra Operator Links registered via plugins
        _ = CustomOperator(task_id='custom_task', dag=dag)

        dagbag = DagBag(
            dag_folder=os.path.join(settings.DAGS_FOLDER, "no_dags.py"),
            include_examples=False,
            read_dags_from_db=True,
        )
        dagbag.bag_dag(dag=dag, root_dag=dag)
        dagbag.sync_to_db()

        # Get serialized dag
        s_dag_1 = dagbag.get_dag(dag.dag_id)
        custom_task = s_dag_1.task_dict['custom_task']
        # Test that custom_task has >= 1 Operator Links (after de-serialization)
        assert custom_task.operator_extra_links

        self.scheduler_job = SchedulerJob(executor=self.null_exec)
        self.scheduler_job.processor_agent = mock.MagicMock()
        self.scheduler_job._run_scheduler_loop()

        # Get serialized dag
        s_dag_2 = self.scheduler_job.dagbag.get_dag(dag.dag_id)
        custom_task = s_dag_2.task_dict['custom_task']
        # Test that custom_task has no Operator Links (after de-serialization) in the Scheduling Loop
        assert not custom_task.operator_extra_links

    def test_scheduler_create_dag_runs_does_not_raise_error(self):
        """
        Test that scheduler._create_dag_runs does not raise an error when the DAG does not exist
        in serialized_dag table
        """
        dag = DAG(dag_id='test_scheduler_create_dag_runs_does_not_raise_error', start_date=DEFAULT_DATE)

        DummyOperator(
            task_id='dummy',
            dag=dag,
        )

        dagbag = DagBag(
            dag_folder=os.devnull,
            include_examples=False,
            read_dags_from_db=False,
        )
        dagbag.bag_dag(dag=dag, root_dag=dag)
        # Only write to dag table and not serialized_dag table
        DAG.bulk_write_to_db(dagbag.dags.values())
        dag_model = DagModel.get_dagmodel(dag.dag_id)

        self.scheduler_job = SchedulerJob(subdir=os.devnull, executor=self.null_exec)
        self.scheduler_job.processor_agent = mock.MagicMock()

        with create_session() as session, self.assertLogs(
            'airflow.jobs.scheduler_job', level="ERROR"
        ) as log_output:
            self.scheduler_job._create_dag_runs([dag_model], session)

            assert (
                "airflow.exceptions.SerializedDagNotFound: DAG "
                "'test_scheduler_create_dag_runs_does_not_raise_error' not found in serialized_dag table"
            ) in log_output.output[0]

    def test_bulk_write_to_db_external_trigger_dont_skip_scheduled_run(self):
        """
        Test that externally triggered Dag Runs should not affect (by skipping) next
        scheduled DAG runs
        """
        dag = DAG(
            dag_id='test_bulk_write_to_db_external_trigger_dont_skip_scheduled_run',
            start_date=DEFAULT_DATE,
            schedule_interval="*/1 * * * *",
            max_active_runs=5,
            catchup=True,
        )

        DummyOperator(task_id='dummy', dag=dag, owner='airflow')

        session = settings.Session()
        dag.clear()
        dagbag = DagBag(
            dag_folder=os.path.join(settings.DAGS_FOLDER, "no_dags.py"),
            include_examples=False,
            read_dags_from_db=True,
        )
        dagbag.bag_dag(dag=dag, root_dag=dag)
        # Write to dag and serialized_dag table
        dagbag.sync_to_db(session)
        dag = dagbag.get_dag(dag.dag_id)

        # Verify that dag_model.next_dagrun is equal to next execution_date
        dag_model = session.query(DagModel).get(dag.dag_id)
        assert dag_model.next_dagrun == DEFAULT_DATE

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        self.scheduler_job.executor = MockExecutor(do_update=False)
        self.scheduler_job.processor_agent = mock.MagicMock(spec=DagFileProcessorAgent)

        # Verify a DagRun is created with the correct execution_date
        # when Scheduler._do_scheduling is run in the Scheduler Loop
        self.scheduler_job._do_scheduling(session)
        dr1 = dag.get_dagrun(DEFAULT_DATE, session)
        assert dr1 is not None
        assert dr1.state == State.RUNNING

        # Verify that dag_model.next_dagrun is set to next execution_date
        dag_model = session.query(DagModel).get(dag.dag_id)
        assert dag_model.next_dagrun == DEFAULT_DATE + timedelta(minutes=1)

        # Trigger the Dag externally
        dr = dag.create_dagrun(
            state=State.RUNNING,
            execution_date=timezone.utcnow(),
            run_type=DagRunType.MANUAL,
            session=session,
            external_trigger=True,
        )
        assert dr is not None
        # Run DAG.bulk_write_to_db -- this is run when in DagFileProcessor.process_file
        DAG.bulk_write_to_db([dag], session)

        # Test that 'dag_model.next_dagrun' has not been changed because of newly created external
        # triggered DagRun.
        dag_model = session.query(DagModel).get(dag.dag_id)
        assert dag_model.next_dagrun == DEFAULT_DATE + timedelta(minutes=1)

    def test_scheduler_create_dag_runs_check_existing_run(self):
        """
        Test that if a dag run exists, scheduler._create_dag_runs does not raise an error.
        And if a Dag Run does not exist it creates next Dag Run. In both cases the Scheduler
        sets next execution date as DagModel.next_dagrun
        """
        dag = DAG(
            dag_id='test_scheduler_create_dag_runs_check_existing_run',
            start_date=DEFAULT_DATE,
            schedule_interval=timedelta(days=1),
        )

        DummyOperator(
            task_id='dummy',
            dag=dag,
        )

        session = settings.Session()
        assert dag.get_last_dagrun(session) is None

        dagbag = DagBag(
            dag_folder=os.devnull,
            include_examples=False,
            read_dags_from_db=False,
        )
        dagbag.bag_dag(dag=dag, root_dag=dag)

        # Create DagModel
        DAG.bulk_write_to_db(dagbag.dags.values())
        dag_model = DagModel.get_dagmodel(dag.dag_id)

        # Assert dag_model.next_dagrun is set correctly
        assert dag_model.next_dagrun == DEFAULT_DATE

        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        dagrun = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=dag_model.next_dagrun,
            start_date=timezone.utcnow(),
            state=State.RUNNING,
            external_trigger=False,
            session=session,
            creating_job_id=2,
        )
        session.flush()

        assert dag.get_last_dagrun(session) == dagrun

        self.scheduler_job = SchedulerJob(subdir=os.devnull, executor=self.null_exec)
        self.scheduler_job.dagbag = dagbag
        self.scheduler_job.processor_agent = mock.MagicMock()

        # Test that this does not raise any error
        self.scheduler_job._create_dag_runs([dag_model], session)

        # Assert dag_model.next_dagrun is set correctly to next execution date
        assert dag_model.next_dagrun == DEFAULT_DATE + timedelta(days=1)
        session.rollback()

    def test_do_schedule_max_active_runs_upstream_failed(self):
        """
        Test that tasks in upstream failed don't count as actively running.

        This test can be removed when adding a queued state to DagRuns.
        """

        with DAG(
            dag_id='test_max_active_run_with_upstream_failed',
            start_date=DEFAULT_DATE,
            schedule_interval='@once',
            max_active_runs=1,
        ) as dag:
            # Cant use DummyOperator as that goes straight to success
            task1 = BashOperator(task_id='dummy1', bash_command='true')

        session = settings.Session()
        dagbag = DagBag(
            dag_folder=os.devnull,
            include_examples=False,
            read_dags_from_db=True,
        )

        dagbag.bag_dag(dag=dag, root_dag=dag)
        dagbag.sync_to_db(session=session)

        run1 = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
            session=session,
        )

        ti = run1.get_task_instance(task1.task_id, session)
        ti.state = State.UPSTREAM_FAILED

        run2 = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE + timedelta(hours=1),
            state=State.RUNNING,
            session=session,
        )

        dag.sync_to_db(session=session)  # Update the date fields

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        self.scheduler_job.executor = MockExecutor(do_update=False)
        self.scheduler_job.processor_agent = mock.MagicMock(spec=DagFileProcessorAgent)

        num_queued = self.scheduler_job._do_scheduling(session)

        assert num_queued == 1
        ti = run2.get_task_instance(task1.task_id, session)
        assert ti.state == State.QUEUED

    def test_do_schedule_max_active_runs_dag_timed_out(self):
        """Test that tasks are set to a finished state when their DAG times out"""

        dag = DAG(
            dag_id='test_max_active_run_with_dag_timed_out',
            start_date=DEFAULT_DATE,
            schedule_interval='@once',
            max_active_runs=1,
            catchup=True,
        )
        dag.dagrun_timeout = datetime.timedelta(seconds=1)

        with dag:
            task1 = BashOperator(
                task_id='task1',
                bash_command=' for((i=1;i<=600;i+=1)); do sleep "$i";  done',
            )

        session = settings.Session()
        dagbag = DagBag(
            dag_folder=os.devnull,
            include_examples=False,
            read_dags_from_db=True,
        )

        dagbag.bag_dag(dag=dag, root_dag=dag)
        dagbag.sync_to_db(session=session)

        run1 = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
            session=session,
        )
        run1_ti = run1.get_task_instance(task1.task_id, session)
        run1_ti.state = State.RUNNING

        sleep(1)

        run2 = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE + timedelta(seconds=10),
            state=State.RUNNING,
            session=session,
        )

        dag.sync_to_db(session=session)

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        self.scheduler_job.executor = MockExecutor()
        self.scheduler_job.processor_agent = mock.MagicMock(spec=DagFileProcessorAgent)

        _ = self.scheduler_job._do_scheduling(session)

        assert run1.state == State.FAILED
        assert run1_ti.state == State.SKIPPED
        assert run2.state == State.RUNNING

        _ = self.scheduler_job._do_scheduling(session)
        run2_ti = run2.get_task_instance(task1.task_id, session)
        assert run2_ti.state == State.QUEUED

    def test_do_schedule_max_active_runs_task_removed(self):
        """Test that tasks in removed state don't count as actively running."""

        with DAG(
            dag_id='test_do_schedule_max_active_runs_task_removed',
            start_date=DEFAULT_DATE,
            schedule_interval='@once',
            max_active_runs=1,
        ) as dag:
            # Cant use DummyOperator as that goes straight to success
            task1 = BashOperator(task_id='dummy1', bash_command='true')

        session = settings.Session()
        dagbag = DagBag(
            dag_folder=os.devnull,
            include_examples=False,
            read_dags_from_db=True,
        )

        dagbag.bag_dag(dag=dag, root_dag=dag)
        dagbag.sync_to_db(session=session)

        session.add(TaskInstance(task1, DEFAULT_DATE, State.REMOVED))
        session.flush()

        run1 = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE + timedelta(hours=1),
            state=State.RUNNING,
            session=session,
        )

        dag.sync_to_db(session=session)  # Update the date fields

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        self.scheduler_job.executor = MockExecutor(do_update=False)
        self.scheduler_job.processor_agent = mock.MagicMock(spec=DagFileProcessorAgent)

        num_queued = self.scheduler_job._do_scheduling(session)

        assert num_queued == 1
        ti = run1.get_task_instance(task1.task_id, session)
        assert ti.state == State.QUEUED

    def test_do_schedule_max_active_runs_and_manual_trigger(self):
        """
        Make sure that when a DAG is already at max_active_runs, that manually triggering a run doesn't cause
        the dag to "stall".
        """

        with DAG(
            dag_id='test_max_active_run_plus_manual_trigger',
            start_date=DEFAULT_DATE,
            schedule_interval='@once',
            max_active_runs=1,
        ) as dag:
            # Cant use DummyOperator as that goes straight to success
            task1 = BashOperator(task_id='dummy1', bash_command='true')
            task2 = BashOperator(task_id='dummy2', bash_command='true')

            task1 >> task2

            task3 = BashOperator(task_id='dummy3', bash_command='true')

        session = settings.Session()
        dagbag = DagBag(
            dag_folder=os.path.join(settings.DAGS_FOLDER, "no_dags.py"),
            include_examples=False,
            read_dags_from_db=True,
        )

        dagbag.bag_dag(dag=dag, root_dag=dag)
        dagbag.sync_to_db(session=session)

        dag_run = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
            session=session,
        )

        dag.sync_to_db(session=session)  # Update the date fields

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        self.scheduler_job.executor = MockExecutor(do_update=False)
        self.scheduler_job.processor_agent = mock.MagicMock(spec=DagFileProcessorAgent)

        num_queued = self.scheduler_job._do_scheduling(session)
        # Add it back in to the session so we can refresh it. (_do_scheduling does an expunge_all to reduce
        # memory)
        session.add(dag_run)
        session.refresh(dag_run)

        assert num_queued == 2
        assert dag_run.state == State.RUNNING
        ti1 = dag_run.get_task_instance(task1.task_id, session)
        assert ti1.state == State.QUEUED

        # Set task1 to success (so task2 can run) but keep task3 as "running"
        ti1.state = State.SUCCESS

        ti3 = dag_run.get_task_instance(task3.task_id, session)
        ti3.state = State.RUNNING

        session.flush()

        # At this point, ti2 and ti3 of the scheduled dag run should be running
        num_queued = self.scheduler_job._do_scheduling(session)

        assert num_queued == 1
        # Should have queued task2
        ti2 = dag_run.get_task_instance(task2.task_id, session)
        assert ti2.state == State.QUEUED

        ti2.state = None
        session.flush()

        # Now that this one is running, manually trigger a dag.

        manual_run = dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            execution_date=DEFAULT_DATE + timedelta(hours=1),
            state=State.RUNNING,
            session=session,
        )
        session.flush()

        num_queued = self.scheduler_job._do_scheduling(session)

        assert num_queued == 1
        # Should have queued task2 again.
        ti2 = dag_run.get_task_instance(task2.task_id, session)
        assert ti2.state == State.QUEUED
        # Manual run shouldn't have been started, because we're at max_active_runs with DR1
        ti1 = manual_run.get_task_instance(task1.task_id, session)
        assert ti1.state is None


@pytest.mark.xfail(reason="Work out where this goes")
def test_task_with_upstream_skip_process_task_instances():
    """
    Test if _process_task_instances puts a task instance into SKIPPED state if any of its
    upstream tasks are skipped according to TriggerRuleDep.
    """
    clear_db_runs()
    with DAG(
        dag_id='test_task_with_upstream_skip_dag', start_date=DEFAULT_DATE, schedule_interval=None
    ) as dag:
        dummy1 = DummyOperator(task_id='dummy1')
        dummy2 = DummyOperator(task_id="dummy2")
        dummy3 = DummyOperator(task_id="dummy3")
        [dummy1, dummy2] >> dummy3

    # dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
    dag.clear()
    dr = dag.create_dagrun(run_type=DagRunType.MANUAL, state=State.RUNNING, execution_date=DEFAULT_DATE)
    assert dr is not None

    with create_session() as session:
        tis = {ti.task_id: ti for ti in dr.get_task_instances(session=session)}
        # Set dummy1 to skipped and dummy2 to success. dummy3 remains as none.
        tis[dummy1.task_id].state = State.SKIPPED
        tis[dummy2.task_id].state = State.SUCCESS
        assert tis[dummy3.task_id].state == State.NONE

    # dag_runs = DagRun.find(dag_id='test_task_with_upstream_skip_dag')
    # dag_file_processor._process_task_instances(dag, dag_runs=dag_runs)

    with create_session() as session:
        tis = {ti.task_id: ti for ti in dr.get_task_instances(session=session)}
        assert tis[dummy1.task_id].state == State.SKIPPED
        assert tis[dummy2.task_id].state == State.SUCCESS
        # dummy3 should be skipped because dummy1 is skipped.
        assert tis[dummy3.task_id].state == State.SKIPPED


# TODO(potiuk): unquarantine me where we get rid of those pesky 195 -> 196 problem!
@pytest.mark.quarantined
class TestSchedulerJobQueriesCount(unittest.TestCase):
    """
    These tests are designed to detect changes in the number of queries for
    different DAG files. These tests allow easy detection when a change is
    made that affects the performance of the SchedulerJob.
    """

    @staticmethod
    def clean_db():
        clear_db_runs()
        clear_db_pools()
        clear_db_dags()
        clear_db_sla_miss()
        clear_db_import_errors()
        clear_db_jobs()
        clear_db_serialized_dags()

    def setUp(self) -> None:
        self.clean_db()

    def tearDown(self):
        if self.scheduler_job and self.scheduler_job.processor_agent:
            self.scheduler_job.processor_agent.end()
            self.scheduler_job = None
        self.clean_db()

    @parameterized.expand(
        [
            # expected, dag_count, task_count
            # One DAG with one task per DAG file
            (24, 1, 1),  # noqa
            # One DAG with five tasks per DAG  file
            (28, 1, 5),  # noqa
            # 10 DAGs with 10 tasks per DAG file
            (195, 10, 10),  # noqa
        ]
    )
    def test_execute_queries_count_with_harvested_dags(self, expected_query_count, dag_count, task_count):
        with mock.patch.dict(
            "os.environ",
            {
                "PERF_DAGS_COUNT": str(dag_count),
                "PERF_TASKS_COUNT": str(task_count),
                "PERF_START_AGO": "1d",
                "PERF_SCHEDULE_INTERVAL": "30m",
                "PERF_SHAPE": "no_structure",
            },
        ), conf_vars(
            {
                ('scheduler', 'use_job_schedule'): 'True',
                ('core', 'load_examples'): 'False',
                # For longer running tests under heavy load, the min_serialized_dag_fetch_interval
                # and min_serialized_dag_update_interval might kick-in and re-retrieve the record.
                # This will increase the count of serliazied_dag.py.get() count.
                # That's why we keep the values high
                ('core', 'min_serialized_dag_update_interval'): '100',
                ('core', 'min_serialized_dag_fetch_interval'): '100',
            }
        ):
            dagruns = []
            dagbag = DagBag(dag_folder=ELASTIC_DAG_FILE, include_examples=False, read_dags_from_db=False)
            dagbag.sync_to_db()

            dag_ids = dagbag.dag_ids
            dagbag = DagBag(read_dags_from_db=True)
            for i, dag_id in enumerate(dag_ids):
                dag = dagbag.get_dag(dag_id)
                dr = dag.create_dagrun(
                    state=State.RUNNING,
                    run_id=f"{DagRunType.MANUAL.value}__{i}",
                    dag_hash=dagbag.dags_hash[dag.dag_id],
                )
                dagruns.append(dr)
                for ti in dr.get_task_instances():
                    ti.set_state(state=State.SCHEDULED)

            mock_agent = mock.MagicMock()

            self.scheduler_job = SchedulerJob(subdir=PERF_DAGS_FOLDER, num_runs=1)
            self.scheduler_job.executor = MockExecutor(do_update=False)
            self.scheduler_job.heartbeat = mock.MagicMock()
            self.scheduler_job.processor_agent = mock_agent

            with assert_queries_count(expected_query_count):
                with mock.patch.object(DagRun, 'next_dagruns_to_examine') as mock_dagruns:
                    mock_dagruns.return_value = dagruns

                    self.scheduler_job._run_scheduler_loop()

    @parameterized.expand(
        [
            # expected, dag_count, task_count, start_ago, schedule_interval, shape
            # One DAG with one task per DAG file
            ([9, 9, 9, 9], 1, 1, "1d", "None", "no_structure"),  # noqa
            ([9, 9, 9, 9], 1, 1, "1d", "None", "linear"),  # noqa
            ([21, 12, 12, 12], 1, 1, "1d", "@once", "no_structure"),  # noqa
            ([21, 12, 12, 12], 1, 1, "1d", "@once", "linear"),  # noqa
            ([21, 22, 24, 26], 1, 1, "1d", "30m", "no_structure"),  # noqa
            ([21, 22, 24, 26], 1, 1, "1d", "30m", "linear"),  # noqa
            ([21, 22, 24, 26], 1, 1, "1d", "30m", "binary_tree"),  # noqa
            ([21, 22, 24, 26], 1, 1, "1d", "30m", "star"),  # noqa
            ([21, 22, 24, 26], 1, 1, "1d", "30m", "grid"),  # noqa
            # One DAG with five tasks per DAG  file
            ([9, 9, 9, 9], 1, 5, "1d", "None", "no_structure"),  # noqa
            ([9, 9, 9, 9], 1, 5, "1d", "None", "linear"),  # noqa
            ([21, 12, 12, 12], 1, 5, "1d", "@once", "no_structure"),  # noqa
            ([22, 13, 13, 13], 1, 5, "1d", "@once", "linear"),  # noqa
            ([21, 22, 24, 26], 1, 5, "1d", "30m", "no_structure"),  # noqa
            ([22, 24, 27, 30], 1, 5, "1d", "30m", "linear"),  # noqa
            ([22, 24, 27, 30], 1, 5, "1d", "30m", "binary_tree"),  # noqa
            ([22, 24, 27, 30], 1, 5, "1d", "30m", "star"),  # noqa
            ([22, 24, 27, 30], 1, 5, "1d", "30m", "grid"),  # noqa
            # 10 DAGs with 10 tasks per DAG file
            ([9, 9, 9, 9], 10, 10, "1d", "None", "no_structure"),  # noqa
            ([9, 9, 9, 9], 10, 10, "1d", "None", "linear"),  # noqa
            ([84, 27, 27, 27], 10, 10, "1d", "@once", "no_structure"),  # noqa
            ([94, 40, 40, 40], 10, 10, "1d", "@once", "linear"),  # noqa
            ([84, 88, 88, 88], 10, 10, "1d", "30m", "no_structure"),  # noqa
            ([94, 114, 114, 114], 10, 10, "1d", "30m", "linear"),  # noqa
            ([94, 108, 108, 108], 10, 10, "1d", "30m", "binary_tree"),  # noqa
            ([94, 108, 108, 108], 10, 10, "1d", "30m", "star"),  # noqa
            ([94, 108, 108, 108], 10, 10, "1d", "30m", "grid"),  # noqa
        ]
    )
    def test_process_dags_queries_count(
        self, expected_query_counts, dag_count, task_count, start_ago, schedule_interval, shape
    ):
        with mock.patch.dict(
            "os.environ",
            {
                "PERF_DAGS_COUNT": str(dag_count),
                "PERF_TASKS_COUNT": str(task_count),
                "PERF_START_AGO": start_ago,
                "PERF_SCHEDULE_INTERVAL": schedule_interval,
                "PERF_SHAPE": shape,
            },
        ), conf_vars(
            {
                ('scheduler', 'use_job_schedule'): 'True',
                ('core', 'store_serialized_dags'): 'True',
                # For longer running tests under heavy load, the min_serialized_dag_fetch_interval
                # and min_serialized_dag_update_interval might kick-in and re-retrieve the record.
                # This will increase the count of serliazied_dag.py.get() count.
                # That's why we keep the values high
                ('core', 'min_serialized_dag_update_interval'): '100',
                ('core', 'min_serialized_dag_fetch_interval'): '100',
            }
        ):

            dagbag = DagBag(dag_folder=ELASTIC_DAG_FILE, include_examples=False)
            dagbag.sync_to_db()

            mock_agent = mock.MagicMock()

            self.scheduler_job = SchedulerJob(subdir=PERF_DAGS_FOLDER, num_runs=1)
            self.scheduler_job.executor = MockExecutor(do_update=False)
            self.scheduler_job.heartbeat = mock.MagicMock()
            self.scheduler_job.processor_agent = mock_agent
            for expected_query_count in expected_query_counts:
                with create_session() as session:
                    with assert_queries_count(expected_query_count):
                        self.scheduler_job._do_scheduling(session)
