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
import copy
import logging.config
import pathlib
import sys
import tempfile
import unittest.mock
import urllib.parse

import pytest

from airflow import settings
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
from airflow.models import DAG, DagBag, TaskInstance
from airflow.operators.dummy import DummyOperator
from airflow.utils import timezone
from airflow.utils.log.logging_mixin import ExternalLoggingMixin
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.www.app import create_app
from tests.test_utils.config import conf_vars
from tests.test_utils.decorators import dont_initialize_flask_app_submodules
from tests.test_utils.www import client_with_login

DAG_ID = 'dag_for_testing_log_view'
DAG_ID_REMOVED = 'removed_dag_for_testing_log_view'
TASK_ID = 'task_for_testing_log_view'
DEFAULT_DATE = timezone.datetime(2017, 9, 1)
ENDPOINT = f'log?dag_id={DAG_ID}&task_id={TASK_ID}&execution_date={DEFAULT_DATE}'


@pytest.fixture(scope="module", autouse=True)
def backup_modules():
    """Make sure that the configure_logging is not cached."""
    return dict(sys.modules)


@pytest.fixture(scope="module")
def log_app(backup_modules):  # pylint: disable=unused-argument
    @dont_initialize_flask_app_submodules(
        skip_all_except=["init_appbuilder", "init_jinja_globals", "init_appbuilder_views"]
    )
    @conf_vars({('logging', 'logging_config_class'): 'airflow_local_settings.LOGGING_CONFIG'})
    def factory():
        app = create_app(testing=True)
        app.config["WTF_CSRF_ENABLED"] = False
        settings.configure_orm()
        security_manager = app.appbuilder.sm  # pylint: disable=no-member
        if not security_manager.find_user(username='test'):
            security_manager.add_user(
                username='test',
                first_name='test',
                last_name='test',
                email='test@fab.org',
                role=security_manager.find_role('Admin'),
                password='test',
            )
        return app

    # Create a custom logging configuration
    logging_config = copy.deepcopy(DEFAULT_LOGGING_CONFIG)
    logging_config['handlers']['task']['base_log_folder'] = str(
        pathlib.Path(__file__, "..", "..", "test_logs").resolve(),
    )
    logging_config['handlers']['task'][
        'filename_template'
    ] = '{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts | replace(":", ".") }}/{{ try_number }}.log'

    with tempfile.TemporaryDirectory() as settings_dir:
        local_settings = pathlib.Path(settings_dir, "airflow_local_settings.py")
        local_settings.write_text(f"LOGGING_CONFIG = {logging_config!r}")
        sys.path.append(settings_dir)
        yield factory()
        sys.path.remove(settings_dir)

    logging.config.dictConfig(DEFAULT_LOGGING_CONFIG)


@pytest.fixture(autouse=True)
def reset_modules_after_every_test(backup_modules):
    yield
    # Remove any new modules imported during the test run.
    # This lets us import the same source files for more than one test.
    for mod in [m for m in sys.modules if m not in backup_modules]:
        del sys.modules[mod]


@pytest.fixture(autouse=True)
def dags(log_app):
    dag = DAG(DAG_ID, start_date=DEFAULT_DATE)
    dag_removed = DAG(DAG_ID_REMOVED, start_date=DEFAULT_DATE)

    bag = DagBag(include_examples=False)
    bag.bag_dag(dag=dag, root_dag=dag)
    bag.bag_dag(dag=dag_removed, root_dag=dag_removed)

    # Since we don't want to store the code for the DAG defined in this file
    with unittest.mock.patch.object(settings, "STORE_DAG_CODE", False):
        dag.sync_to_db()
        dag_removed.sync_to_db()
        bag.sync_to_db()

    log_app.dag_bag = bag
    return dag, dag_removed


@pytest.fixture(autouse=True)
def tis(dags):
    dag, dag_removed = dags
    ti = TaskInstance(
        task=DummyOperator(task_id=TASK_ID, dag=dag),
        execution_date=DEFAULT_DATE,
    )
    ti.try_number = 1
    ti_removed_dag = TaskInstance(
        task=DummyOperator(task_id=TASK_ID, dag=dag_removed),
        execution_date=DEFAULT_DATE,
    )
    ti_removed_dag.try_number = 1

    with create_session() as session:
        session.merge(ti)
        session.merge(ti_removed_dag)

    yield ti, ti_removed_dag

    with create_session() as session:
        session.query(TaskInstance).delete()


@pytest.fixture()
def log_admin_client(log_app):
    return client_with_login(log_app, username="test", password="test")


@pytest.mark.parametrize(
    "state, try_number, num_logs",
    [
        (State.NONE, 0, 0),
        (State.UP_FOR_RETRY, 2, 2),
        (State.UP_FOR_RESCHEDULE, 0, 1),
        (State.UP_FOR_RESCHEDULE, 1, 2),
        (State.RUNNING, 1, 1),
        (State.SUCCESS, 1, 1),
        (State.FAILED, 3, 3),
    ],
    ids=[
        "none",
        "up-for-retry",
        "up-for-reschedule-0",
        "up-for-reschedule-1",
        "running",
        "success",
        "failed",
    ],
)
def test_get_file_task_log(log_admin_client, tis, state, try_number, num_logs):
    ti, _ = tis
    with create_session() as session:
        ti.state = state
        ti.try_number = try_number
        session.merge(ti)

    response = log_admin_client.get(
        ENDPOINT,
        data={"username": "test", "password": "test"},
        follow_redirects=True,
    )
    assert response.status_code == 200

    data = response.data.decode()
    assert 'Log by attempts' in data
    for num in range(1, num_logs + 1):
        assert f'log-group-{num}' in data
    assert 'log-group-0' not in data
    assert f'log-group-{num_logs + 1}' not in data


def test_get_logs_with_metadata_as_download_file(log_admin_client):
    url_template = (
        "get_logs_with_metadata?dag_id={}&"
        "task_id={}&execution_date={}&"
        "try_number={}&metadata={}&format=file"
    )
    try_number = 1
    url = url_template.format(
        DAG_ID,
        TASK_ID,
        urllib.parse.quote_plus(DEFAULT_DATE.isoformat()),
        try_number,
        "{}",
    )
    response = log_admin_client.get(url)

    content_disposition = response.headers['Content-Disposition']
    assert content_disposition.startswith('attachment')
    assert f'{DAG_ID}/{TASK_ID}/{DEFAULT_DATE.isoformat()}/{try_number}.log' in content_disposition
    assert 200 == response.status_code
    assert 'Log for testing.' in response.data.decode('utf-8')


@unittest.mock.patch(
    "airflow.utils.log.file_task_handler.FileTaskHandler.read",
    side_effect=[
        ([[('default_log', '1st line')]], [{}]),
        ([[('default_log', '2nd line')]], [{'end_of_log': False}]),
        ([[('default_log', '3rd line')]], [{'end_of_log': True}]),
        ([[('default_log', 'should never be read')]], [{'end_of_log': True}]),
    ],
)
def test_get_logs_with_metadata_as_download_large_file(_, log_admin_client):
    url_template = (
        "get_logs_with_metadata?dag_id={}&"
        "task_id={}&execution_date={}&"
        "try_number={}&metadata={}&format=file"
    )
    try_number = 1
    url = url_template.format(
        DAG_ID,
        TASK_ID,
        urllib.parse.quote_plus(DEFAULT_DATE.isoformat()),
        try_number,
        "{}",
    )
    response = log_admin_client.get(url)

    data = response.data.decode()
    assert '1st line' in data
    assert '2nd line' in data
    assert '3rd line' in data
    assert 'should never be read' not in data


@pytest.mark.parametrize("metadata", ["null", "{}"])
def test_get_logs_with_metadata(log_admin_client, metadata):
    url_template = "get_logs_with_metadata?dag_id={}&task_id={}&execution_date={}&try_number={}&metadata={}"
    response = log_admin_client.get(
        url_template.format(
            DAG_ID,
            TASK_ID,
            urllib.parse.quote_plus(DEFAULT_DATE.isoformat()),
            1,
            metadata,
        ),
        data={"username": "test", "password": "test"},
        follow_redirects=True,
    )
    assert 200 == response.status_code

    data = response.data.decode()
    assert '"message":' in data
    assert '"metadata":' in data
    assert 'Log for testing.' in data


@unittest.mock.patch(
    "airflow.utils.log.file_task_handler.FileTaskHandler.read",
    return_value=(['airflow log line'], [{'end_of_log': True}]),
)
def test_get_logs_with_metadata_for_removed_dag(_, log_admin_client):
    url_template = "get_logs_with_metadata?dag_id={}&task_id={}&execution_date={}&try_number={}&metadata={}"
    response = log_admin_client.get(
        url_template.format(
            DAG_ID_REMOVED,
            TASK_ID,
            urllib.parse.quote_plus(DEFAULT_DATE.isoformat()),
            1,
            "{}",
        ),
        data={"username": "test", "password": "test"},
        follow_redirects=True,
    )
    assert 200 == response.status_code

    data = response.data.decode()
    assert '"message":' in data
    assert '"metadata":' in data
    assert 'airflow log line' in data


def test_get_logs_response_with_ti_equal_to_none(log_admin_client):
    url_template = (
        "get_logs_with_metadata?dag_id={}&"
        "task_id={}&execution_date={}&"
        "try_number={}&metadata={}&format=file"
    )
    try_number = 1
    url = url_template.format(
        DAG_ID,
        'Non_Existing_ID',
        urllib.parse.quote_plus(DEFAULT_DATE.isoformat()),
        try_number,
        "{}",
    )
    response = log_admin_client.get(url)

    data = response.json
    assert 'message' in data
    assert 'error' in data
    assert "*** Task instance did not exist in the DB\n" == data['message']


def test_get_logs_with_json_response_format(log_admin_client):
    url_template = (
        "get_logs_with_metadata?dag_id={}&"
        "task_id={}&execution_date={}&"
        "try_number={}&metadata={}&format=json"
    )
    try_number = 1
    url = url_template.format(
        DAG_ID,
        TASK_ID,
        urllib.parse.quote_plus(DEFAULT_DATE.isoformat()),
        try_number,
        "{}",
    )
    response = log_admin_client.get(url)
    assert 200 == response.status_code

    assert 'message' in response.json
    assert 'metadata' in response.json
    assert 'Log for testing.' in response.json['message'][0][1]


@unittest.mock.patch("airflow.www.views.TaskLogReader")
def test_get_logs_for_handler_without_read_method(mock_reader, log_admin_client):
    type(mock_reader.return_value).supports_read = unittest.mock.PropertyMock(return_value=False)
    url_template = (
        "get_logs_with_metadata?dag_id={}&"
        "task_id={}&execution_date={}&"
        "try_number={}&metadata={}&format=json"
    )
    try_number = 1
    url = url_template.format(
        DAG_ID,
        TASK_ID,
        urllib.parse.quote_plus(DEFAULT_DATE.isoformat()),
        try_number,
        "{}",
    )
    response = log_admin_client.get(url)
    assert 200 == response.status_code

    data = response.json
    assert 'message' in data
    assert 'metadata' in data
    assert 'Task log handler does not support read logs.' in data['message']


@pytest.mark.parametrize("task_id", ['inexistent', TASK_ID])
def test_redirect_to_external_log_with_local_log_handler(log_admin_client, task_id):
    """Redirect to home if TI does not exist or if log handler is local"""
    url_template = "redirect_to_external_log?dag_id={}&task_id={}&execution_date={}&try_number={}"
    try_number = 1
    url = url_template.format(
        DAG_ID,
        task_id,
        urllib.parse.quote_plus(DEFAULT_DATE.isoformat()),
        try_number,
    )
    response = log_admin_client.get(url)
    assert 302 == response.status_code
    assert 'http://localhost/home' == response.headers['Location']


class ExternalHandler(ExternalLoggingMixin):
    EXTERNAL_URL = 'http://external-service.com'

    def get_external_log_url(self, *args, **kwargs):
        return self.EXTERNAL_URL


@unittest.mock.patch(
    'airflow.utils.log.log_reader.TaskLogReader.log_handler',
    new_callable=unittest.mock.PropertyMock,
    return_value=ExternalHandler(),
)
def test_redirect_to_external_log_with_external_log_handler(_, log_admin_client):
    url_template = "redirect_to_external_log?dag_id={}&task_id={}&execution_date={}&try_number={}"
    try_number = 1
    url = url_template.format(
        DAG_ID,
        TASK_ID,
        urllib.parse.quote_plus(DEFAULT_DATE.isoformat()),
        try_number,
    )
    response = log_admin_client.get(url)
    assert 302 == response.status_code
    assert ExternalHandler.EXTERNAL_URL == response.headers['Location']
