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
import json

import pytest

from airflow.api.common.experimental.trigger_dag import trigger_dag
from airflow.models import DagBag, DagRun
from airflow.models.serialized_dag import SerializedDagModel
from airflow.settings import Session
from airflow.www import app as application
from tests.test_utils.config import conf_vars
from tests.test_utils.decorators import dont_initialize_flask_app_submodules


@pytest.fixture(scope="module")
def app():
    @dont_initialize_flask_app_submodules(
        skip_all_except=[
            "init_api_experimental_auth",
            "init_appbuilder_views",
            "init_api_experimental",
            "init_appbuilder",
        ]
    )
    def factory():
        with conf_vars({('api', 'enable_experimental_api'): 'true'}):
            return application.create_app(testing=True)

    return factory()


class TestDagRunsEndpoint:
    @pytest.fixture(scope="class", autouse=True)
    def _setup_session(self):
        session = Session()
        session.query(DagRun).delete()
        session.commit()
        session.close()
        dagbag = DagBag(include_examples=True)
        for dag in dagbag.dags.values():
            dag.sync_to_db()
            SerializedDagModel.write_dag(dag)

    @pytest.fixture(autouse=True)
    def _reset_test_session(self, app):
        self.app = app.test_client()
        yield
        session = Session()
        session.query(DagRun).delete()
        session.commit()
        session.close()

    def test_get_dag_runs_success(self):
        url_template = '/api/experimental/dags/{}/dag_runs'
        dag_id = 'example_bash_operator'
        # Create DagRun
        dag_run = trigger_dag(dag_id=dag_id, run_id='test_get_dag_runs_success')

        response = self.app.get(url_template.format(dag_id))
        assert 200 == response.status_code
        data = json.loads(response.data.decode('utf-8'))

        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]['dag_id'] == dag_id
        assert data[0]['id'] == dag_run.id

    def test_get_dag_runs_success_with_state_parameter(self):
        url_template = '/api/experimental/dags/{}/dag_runs?state=running'
        dag_id = 'example_bash_operator'
        # Create DagRun
        dag_run = trigger_dag(dag_id=dag_id, run_id='test_get_dag_runs_success')

        response = self.app.get(url_template.format(dag_id))
        assert 200 == response.status_code
        data = json.loads(response.data.decode('utf-8'))

        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]['dag_id'] == dag_id
        assert data[0]['id'] == dag_run.id

    def test_get_dag_runs_success_with_capital_state_parameter(self):
        url_template = '/api/experimental/dags/{}/dag_runs?state=RUNNING'
        dag_id = 'example_bash_operator'
        # Create DagRun
        dag_run = trigger_dag(dag_id=dag_id, run_id='test_get_dag_runs_success')

        response = self.app.get(url_template.format(dag_id))
        assert 200 == response.status_code
        data = json.loads(response.data.decode('utf-8'))

        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]['dag_id'] == dag_id
        assert data[0]['id'] == dag_run.id

    def test_get_dag_runs_success_with_state_no_result(self):
        url_template = '/api/experimental/dags/{}/dag_runs?state=dummy'
        dag_id = 'example_bash_operator'
        # Create DagRun
        trigger_dag(dag_id=dag_id, run_id='test_get_dag_runs_success')

        response = self.app.get(url_template.format(dag_id))
        assert 200 == response.status_code
        data = json.loads(response.data.decode('utf-8'))

        assert isinstance(data, list)
        assert len(data) == 0

    def test_get_dag_runs_invalid_dag_id(self):
        url_template = '/api/experimental/dags/{}/dag_runs'
        dag_id = 'DUMMY_DAG'

        response = self.app.get(url_template.format(dag_id))
        assert 400 == response.status_code
        data = json.loads(response.data.decode('utf-8'))

        assert not isinstance(data, list)

    def test_get_dag_runs_no_runs(self):
        url_template = '/api/experimental/dags/{}/dag_runs'
        dag_id = 'example_bash_operator'

        response = self.app.get(url_template.format(dag_id))
        assert 200 == response.status_code
        data = json.loads(response.data.decode('utf-8'))

        assert isinstance(data, list)
        assert len(data) == 0
