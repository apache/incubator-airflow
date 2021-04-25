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
import contextlib
import json
import os
import sys
import unittest
from datetime import datetime, timedelta
from unittest import mock

# leave this it is used by the test worker
import celery.contrib.testing.tasks  # noqa: F401 pylint: disable=unused-import
import pytest
from celery import Celery
from celery.backends.base import BaseBackend, BaseKeyValueStoreBackend  # noqa
from celery.backends.database import DatabaseBackend
from celery.contrib.testing.worker import start_worker
from celery.result import AsyncResult
from kombu.asynchronous import set_event_loop
from parameterized import parameterized

from airflow.configuration import conf
from airflow.exceptions import AirflowException, AirflowTaskTimeout
from airflow.executors import celery_executor
from airflow.executors.celery_executor import BulkStateFetcher
from airflow.models.baseoperator import BaseOperator
from airflow.models.dag import DAG
from airflow.models.taskinstance import SimpleTaskInstance, TaskInstance, TaskInstanceKey
from airflow.operators.bash import BashOperator
from airflow.utils import timezone
from airflow.utils.state import State
from tests.test_utils import db


def _prepare_test_bodies():
    if 'CELERY_BROKER_URLS' in os.environ:
        return [(url,) for url in os.environ['CELERY_BROKER_URLS'].split(',')]
    return [(conf.get('celery', 'BROKER_URL'))]


class FakeCeleryResult:
    @property
    def state(self):
        raise Exception()

    def task_id(self):
        return "task_id"


@contextlib.contextmanager
def _prepare_app(broker_url=None, execute=None):
    broker_url = broker_url or conf.get('celery', 'BROKER_URL')
    execute = execute or celery_executor.execute_command.__wrapped__

    test_config = dict(celery_executor.celery_configuration)
    test_config.update({'broker_url': broker_url})
    test_app = Celery(broker_url, config_source=test_config)
    test_execute = test_app.task(execute)
    patch_app = mock.patch('airflow.executors.celery_executor.app', test_app)
    patch_execute = mock.patch('airflow.executors.celery_executor.execute_command', test_execute)

    backend = test_app.backend

    if hasattr(backend, 'ResultSession'):
        # Pre-create the database tables now, otherwise SQLA vis Celery has a
        # race condition where it one of the subprocesses can die with "Table
        # already exists" error, because SQLA checks for which tables exist,
        # then issues a CREATE TABLE, rather than doing CREATE TABLE IF NOT
        # EXISTS
        session = backend.ResultSession()
        session.close()

    with patch_app, patch_execute:
        try:
            yield test_app
        finally:
            # Clear event loop to tear down each celery instance
            set_event_loop(None)


class TestCeleryExecutor(unittest.TestCase):
    def setUp(self) -> None:
        db.clear_db_runs()
        db.clear_db_jobs()

    def tearDown(self) -> None:
        db.clear_db_runs()
        db.clear_db_jobs()

    @parameterized.expand(_prepare_test_bodies())
    @pytest.mark.integration("redis")
    @pytest.mark.integration("rabbitmq")
    @pytest.mark.backend("mysql", "postgres")
    def test_celery_integration(self, broker_url):
        success_command = ['airflow', 'tasks', 'run', 'true', 'some_parameter']
        fail_command = ['airflow', 'version']

        def fake_execute_command(command):
            if command != success_command:
                raise AirflowException("fail")

        with _prepare_app(broker_url, execute=fake_execute_command) as app:
            executor = celery_executor.CeleryExecutor()
            assert executor.tasks == {}
            executor.start()

            with start_worker(app=app, logfile=sys.stdout, loglevel='info'):
                execute_date = datetime.now()

                task_tuples_to_send = [
                    (
                        ('success', 'fake_simple_ti', execute_date, 0),
                        None,
                        success_command,
                        celery_executor.celery_configuration['task_default_queue'],
                        celery_executor.execute_command,
                    ),
                    (
                        ('fail', 'fake_simple_ti', execute_date, 0),
                        None,
                        fail_command,
                        celery_executor.celery_configuration['task_default_queue'],
                        celery_executor.execute_command,
                    ),
                ]

                # "Enqueue" them. We don't have a real SimpleTaskInstance, so directly edit the dict
                for (key, simple_ti, command, queue, task) in task_tuples_to_send:  # pylint: disable=W0612
                    executor.queued_tasks[key] = (command, 1, queue, simple_ti)
                    executor.task_publish_retries[key] = 1

                executor._process_tasks(task_tuples_to_send)

                assert list(executor.tasks.keys()) == [
                    ('success', 'fake_simple_ti', execute_date, 0),
                    ('fail', 'fake_simple_ti', execute_date, 0),
                ]
                assert (
                    executor.event_buffer[('success', 'fake_simple_ti', execute_date, 0)][0] == State.QUEUED
                )
                assert executor.event_buffer[('fail', 'fake_simple_ti', execute_date, 0)][0] == State.QUEUED

                executor.end(synchronous=True)

        assert executor.event_buffer[('success', 'fake_simple_ti', execute_date, 0)][0] == State.SUCCESS
        assert executor.event_buffer[('fail', 'fake_simple_ti', execute_date, 0)][0] == State.FAILED

        assert 'success' not in executor.tasks
        assert 'fail' not in executor.tasks

        assert executor.queued_tasks == {}
        assert timedelta(0, 600) == executor.task_adoption_timeout

    @pytest.mark.integration("redis")
    @pytest.mark.integration("rabbitmq")
    @pytest.mark.backend("mysql", "postgres")
    def test_error_sending_task(self):
        def fake_execute_command():
            pass

        with _prepare_app(execute=fake_execute_command):
            # fake_execute_command takes no arguments while execute_command takes 1,
            # which will cause TypeError when calling task.apply_async()
            executor = celery_executor.CeleryExecutor()
            task = BashOperator(
                task_id="test", bash_command="true", dag=DAG(dag_id='id'), start_date=datetime.now()
            )
            when = datetime.now()
            value_tuple = (
                'command',
                1,
                None,
                SimpleTaskInstance(ti=TaskInstance(task=task, execution_date=datetime.now())),
            )
            key = ('fail', 'fake_simple_ti', when, 0)
            executor.queued_tasks[key] = value_tuple
            executor.task_publish_retries[key] = 1
            executor.heartbeat()
        assert 0 == len(executor.queued_tasks), "Task should no longer be queued"
        assert executor.event_buffer[('fail', 'fake_simple_ti', when, 0)][0] == State.FAILED

    @pytest.mark.integration("redis")
    @pytest.mark.integration("rabbitmq")
    @pytest.mark.backend("mysql", "postgres")
    def test_retry_on_error_sending_task(self):
        """Test that Airflow retries publishing tasks to Celery Broker at least 3 times"""

        with _prepare_app(), self.assertLogs(celery_executor.log) as cm, mock.patch.object(
            # Mock `with timeout()` to _instantly_ fail.
            celery_executor.timeout,
            "__enter__",
            side_effect=AirflowTaskTimeout,
        ):
            executor = celery_executor.CeleryExecutor()
            assert executor.task_publish_retries == {}
            assert executor.task_publish_max_retries == 3, "Assert Default Max Retries is 3"

            task = BashOperator(
                task_id="test", bash_command="true", dag=DAG(dag_id='id'), start_date=datetime.now()
            )
            when = datetime.now()
            value_tuple = (
                'command',
                1,
                None,
                SimpleTaskInstance(ti=TaskInstance(task=task, execution_date=datetime.now())),
            )
            key = ('fail', 'fake_simple_ti', when, 0)
            executor.queued_tasks[key] = value_tuple

            # Test that when heartbeat is called again, task is published again to Celery Queue
            executor.heartbeat()
            assert dict(executor.task_publish_retries) == {key: 2}
            assert 1 == len(executor.queued_tasks), "Task should remain in queue"
            assert executor.event_buffer == {}
            assert (
                "INFO:airflow.executors.celery_executor.CeleryExecutor:"
                f"[Try 1 of 3] Task Timeout Error for Task: ({key})." in cm.output
            )

            executor.heartbeat()
            assert dict(executor.task_publish_retries) == {key: 3}
            assert 1 == len(executor.queued_tasks), "Task should remain in queue"
            assert executor.event_buffer == {}
            assert (
                "INFO:airflow.executors.celery_executor.CeleryExecutor:"
                f"[Try 2 of 3] Task Timeout Error for Task: ({key})." in cm.output
            )

            executor.heartbeat()
            assert dict(executor.task_publish_retries) == {key: 4}
            assert 1 == len(executor.queued_tasks), "Task should remain in queue"
            assert executor.event_buffer == {}
            assert (
                "INFO:airflow.executors.celery_executor.CeleryExecutor:"
                f"[Try 3 of 3] Task Timeout Error for Task: ({key})." in cm.output
            )

            executor.heartbeat()
            assert dict(executor.task_publish_retries) == {}
            assert 0 == len(executor.queued_tasks), "Task should no longer be in queue"
            assert executor.event_buffer[('fail', 'fake_simple_ti', when, 0)][0] == State.FAILED

    @pytest.mark.quarantined
    @pytest.mark.backend("mysql", "postgres")
    def test_exception_propagation(self):

        with _prepare_app(), self.assertLogs(celery_executor.log) as cm:
            executor = celery_executor.CeleryExecutor()
            executor.tasks = {'key': FakeCeleryResult()}
            executor.bulk_state_fetcher._get_many_using_multiprocessing(executor.tasks.values())

        assert any(celery_executor.CELERY_FETCH_ERR_MSG_HEADER in line for line in cm.output)
        assert any("Exception" in line for line in cm.output)

    @mock.patch('airflow.executors.celery_executor.CeleryExecutor.sync')
    @mock.patch('airflow.executors.celery_executor.CeleryExecutor.trigger_tasks')
    @mock.patch('airflow.executors.base_executor.Stats.gauge')
    def test_gauge_executor_metrics(self, mock_stats_gauge, mock_trigger_tasks, mock_sync):
        executor = celery_executor.CeleryExecutor()
        executor.heartbeat()
        calls = [
            mock.call('executor.open_slots', mock.ANY),
            mock.call('executor.queued_tasks', mock.ANY),
            mock.call('executor.running_tasks', mock.ANY),
        ]
        mock_stats_gauge.assert_has_calls(calls)

    @parameterized.expand(
        ([['true'], ValueError], [['airflow', 'version'], ValueError], [['airflow', 'tasks', 'run'], None])
    )
    def test_command_validation(self, command, expected_exception):
        # Check that we validate _on the receiving_ side, not just sending side
        with mock.patch(
            'airflow.executors.celery_executor._execute_in_subprocess'
        ) as mock_subproc, mock.patch('airflow.executors.celery_executor._execute_in_fork') as mock_fork:
            if expected_exception:
                with pytest.raises(expected_exception):
                    celery_executor.execute_command(command)
                mock_subproc.assert_not_called()
                mock_fork.assert_not_called()
            else:
                celery_executor.execute_command(command)
                # One of these should be called.
                assert mock_subproc.call_args == ((command,),) or mock_fork.call_args == ((command,),)

    @pytest.mark.backend("mysql", "postgres")
    def test_try_adopt_task_instances_none(self):
        date = datetime.utcnow()
        start_date = datetime.utcnow() - timedelta(days=2)

        with DAG("test_try_adopt_task_instances_none"):
            task_1 = BaseOperator(task_id="task_1", start_date=start_date)

        key1 = TaskInstance(task=task_1, execution_date=date)
        tis = [key1]
        executor = celery_executor.CeleryExecutor()

        assert executor.try_adopt_task_instances(tis) == tis

    @pytest.mark.backend("mysql", "postgres")
    def test_try_adopt_task_instances(self):
        exec_date = timezone.utcnow() - timedelta(minutes=2)
        start_date = timezone.utcnow() - timedelta(days=2)
        queued_dttm = timezone.utcnow() - timedelta(minutes=1)

        try_number = 1

        with DAG("test_try_adopt_task_instances_none") as dag:
            task_1 = BaseOperator(task_id="task_1", start_date=start_date)
            task_2 = BaseOperator(task_id="task_2", start_date=start_date)

        ti1 = TaskInstance(task=task_1, execution_date=exec_date)
        ti1.external_executor_id = '231'
        ti1.queued_dttm = queued_dttm
        ti2 = TaskInstance(task=task_2, execution_date=exec_date)
        ti2.external_executor_id = '232'
        ti2.queued_dttm = queued_dttm

        tis = [ti1, ti2]
        executor = celery_executor.CeleryExecutor()
        assert executor.running == set()
        assert executor.adopted_task_timeouts == {}
        assert executor.tasks == {}

        not_adopted_tis = executor.try_adopt_task_instances(tis)

        key_1 = TaskInstanceKey(dag.dag_id, task_1.task_id, exec_date, try_number)
        key_2 = TaskInstanceKey(dag.dag_id, task_2.task_id, exec_date, try_number)
        assert executor.running == {key_1, key_2}
        assert dict(executor.adopted_task_timeouts) == {
            key_1: queued_dttm + executor.task_adoption_timeout,
            key_2: queued_dttm + executor.task_adoption_timeout,
        }
        assert executor.tasks == {key_1: AsyncResult("231"), key_2: AsyncResult("232")}
        assert not_adopted_tis == []

    @pytest.mark.backend("mysql", "postgres")
    def test_check_for_stalled_adopted_tasks(self):
        exec_date = timezone.utcnow() - timedelta(minutes=40)
        start_date = timezone.utcnow() - timedelta(days=2)
        queued_dttm = timezone.utcnow() - timedelta(minutes=30)

        try_number = 1

        with DAG("test_check_for_stalled_adopted_tasks") as dag:
            task_1 = BaseOperator(task_id="task_1", start_date=start_date)
            task_2 = BaseOperator(task_id="task_2", start_date=start_date)

        key_1 = TaskInstanceKey(dag.dag_id, task_1.task_id, exec_date, try_number)
        key_2 = TaskInstanceKey(dag.dag_id, task_2.task_id, exec_date, try_number)

        executor = celery_executor.CeleryExecutor()
        executor.adopted_task_timeouts = {
            key_1: queued_dttm + executor.task_adoption_timeout,
            key_2: queued_dttm + executor.task_adoption_timeout,
        }
        executor.tasks = {key_1: AsyncResult("231"), key_2: AsyncResult("232")}
        executor.sync()
        assert executor.event_buffer == {key_1: (State.FAILED, None), key_2: (State.FAILED, None)}
        assert executor.tasks == {}
        assert executor.adopted_task_timeouts == {}


def test_operation_timeout_config():
    assert celery_executor.OPERATION_TIMEOUT == 1


class ClassWithCustomAttributes:
    """Class for testing purpose: allows to create objects with custom attributes in one single statement."""

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __str__(self):
        return f"{ClassWithCustomAttributes.__name__}({str(self.__dict__)})"

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not self.__eq__(other)


class TestBulkStateFetcher(unittest.TestCase):
    @mock.patch(
        "celery.backends.base.BaseKeyValueStoreBackend.mget",
        return_value=[json.dumps({"status": "SUCCESS", "task_id": "123"})],
    )
    @pytest.mark.integration("redis")
    @pytest.mark.integration("rabbitmq")
    @pytest.mark.backend("mysql", "postgres")
    def test_should_support_kv_backend(self, mock_mget):
        with _prepare_app():
            mock_backend = BaseKeyValueStoreBackend(app=celery_executor.app)
            with mock.patch.object(celery_executor.app, 'backend', mock_backend):
                fetcher = BulkStateFetcher()
                result = fetcher.get_many(
                    [
                        mock.MagicMock(task_id="123"),
                        mock.MagicMock(task_id="456"),
                    ]
                )

        # Assert called - ignore order
        mget_args, _ = mock_mget.call_args
        assert set(mget_args[0]) == {b'celery-task-meta-456', b'celery-task-meta-123'}
        mock_mget.assert_called_once_with(mock.ANY)

        assert result == {'123': ('SUCCESS', None), '456': ("PENDING", None)}

    @mock.patch("celery.backends.database.DatabaseBackend.ResultSession")
    @pytest.mark.integration("redis")
    @pytest.mark.integration("rabbitmq")
    @pytest.mark.backend("mysql", "postgres")
    def test_should_support_db_backend(self, mock_session):
        with _prepare_app():
            mock_backend = DatabaseBackend(app=celery_executor.app, url="sqlite3://")

            with mock.patch.object(celery_executor.app, 'backend', mock_backend):
                mock_session = mock_backend.ResultSession.return_value  # pylint: disable=no-member
                mock_session.query.return_value.filter.return_value.all.return_value = [
                    mock.MagicMock(**{"to_dict.return_value": {"status": "SUCCESS", "task_id": "123"}})
                ]

        fetcher = BulkStateFetcher()
        result = fetcher.get_many(
            [
                mock.MagicMock(task_id="123"),
                mock.MagicMock(task_id="456"),
            ]
        )

        assert result == {'123': ('SUCCESS', None), '456': ("PENDING", None)}

    @pytest.mark.integration("redis")
    @pytest.mark.integration("rabbitmq")
    @pytest.mark.backend("mysql", "postgres")
    def test_should_support_base_backend(self):
        with _prepare_app():
            mock_backend = mock.MagicMock(autospec=BaseBackend)

            with mock.patch.object(celery_executor.app, 'backend', mock_backend):
                fetcher = BulkStateFetcher(1)
                result = fetcher.get_many(
                    [
                        ClassWithCustomAttributes(task_id="123", state='SUCCESS'),
                        ClassWithCustomAttributes(task_id="456", state="PENDING"),
                    ]
                )

        assert result == {'123': ('SUCCESS', None), '456': ("PENDING", None)}
