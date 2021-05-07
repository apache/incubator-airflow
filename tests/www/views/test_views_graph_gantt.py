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
import pytest

from airflow.configuration import conf
from airflow.models import DAG
from airflow.utils import timezone
from airflow.utils.state import State

DAG_ID = "dag_for_testing_dt_nr_dr_form"
DEFAULT_DATE = timezone.datetime(2017, 9, 1)
RUNS_DATA = [
    ("dag_run_for_testing_dt_nr_dr_form_4", timezone.datetime(2018, 4, 4)),
    ("dag_run_for_testing_dt_nr_dr_form_3", timezone.datetime(2018, 3, 3)),
    ("dag_run_for_testing_dt_nr_dr_form_2", timezone.datetime(2018, 2, 2)),
    ("dag_run_for_testing_dt_nr_dr_form_1", timezone.datetime(2018, 1, 1)),
]

ENDPOINTS = [
    "/graph?dag_id=dag_for_testing_dt_nr_dr_form",
    "/gantt?dag_id=dag_for_testing_dt_nr_dr_form",
]


@pytest.fixture(scope="module")
def dag(app):
    dag = DAG(DAG_ID, start_date=DEFAULT_DATE)
    app.dag_bag.bag_dag(dag=dag, root_dag=dag)
    return dag


@pytest.fixture(scope="module")
def runs(dag):
    return [
        dag.create_dagrun(
            run_id=run_id,
            execution_date=execution_date,
            state=State.SUCCESS,
            external_trigger=True,
        )
        for run_id, execution_date, in RUNS_DATA
    ]


def _assert_run_is_in_dropdown_not_selected(run, data):
    exec_date = run.execution_date.isoformat()
    assert f'<option value="{exec_date}">{run.run_id}</option>' in data


def _assert_run_is_selected(run, data):
    exec_date = run.execution_date.isoformat()
    assert f'<option selected value="{exec_date}">{run.run_id}</option>' in data


def _assert_base_date_and_num_runs(base_date, num, data):
    assert f'name="base_date" value="{base_date}"' not in data
    assert f'<option selected="" value="{num}">{num}</option>' not in data


def _assert_run_is_not_in_dropdown(run, data):
    assert run.execution_date.isoformat() not in data
    assert run.run_id not in data


@pytest.mark.parametrize("endpoint", ENDPOINTS)
def test_with_default_parameters(admin_client, runs, endpoint):
    """
    Tests view with no URL parameter.
    Should show all dag runs in the drop down.
    Should select the latest dag run.
    Should set base date to current date (not asserted)
    """
    response = admin_client.get(
        endpoint, data={"username": "test", "password": "test"}, follow_redirects=True
    )
    assert response.status_code == 200

    data = response.data.decode()
    assert '<label class="sr-only" for="base_date">Base date</label>' in data
    assert '<label class="sr-only" for="num_runs">Number of runs</label>' in data

    _assert_run_is_selected(runs[0], data)
    _assert_run_is_in_dropdown_not_selected(runs[1], data)
    _assert_run_is_in_dropdown_not_selected(runs[2], data)
    _assert_run_is_in_dropdown_not_selected(runs[3], data)


@pytest.mark.parametrize("endpoint", ENDPOINTS)
def test_with_execution_date_parameter_only(admin_client, runs, endpoint):
    """
    Tests view with execution_date URL parameter.
    Scenario: click link from dag runs view.
    Should only show dag runs older than execution_date in the drop down.
    Should select the particular dag run.
    Should set base date to execution date.
    """
    response = admin_client.get(
        f'{endpoint}&execution_date={runs[1].execution_date.isoformat()}',
        data={"username": "test", "password": "test"},
        follow_redirects=True,
    )
    assert response.status_code == 200

    data = response.data.decode()
    _assert_base_date_and_num_runs(
        runs[1].execution_date,
        conf.getint('webserver', 'default_dag_run_display_number'),
        data,
    )
    _assert_run_is_not_in_dropdown(runs[0], data)
    _assert_run_is_selected(runs[1], data)
    _assert_run_is_in_dropdown_not_selected(runs[2], data)
    _assert_run_is_in_dropdown_not_selected(runs[3], data)


@pytest.mark.parametrize("endpoint", ENDPOINTS)
def test_with_base_date_and_num_runs_parameters_only(admin_client, runs, endpoint):
    """
    Tests view with base_date and num_runs URL parameters.
    Should only show dag runs older than base_date in the drop down,
    limited to num_runs.
    Should select the latest dag run.
    Should set base date and num runs to submitted values.
    """
    response = admin_client.get(
        f'{endpoint}&base_date={runs[1].execution_date.isoformat()}&num_runs=2',
        data={"username": "test", "password": "test"},
        follow_redirects=True,
    )
    assert response.status_code == 200

    data = response.data.decode()
    _assert_base_date_and_num_runs(runs[1].execution_date, 2, data)
    _assert_run_is_not_in_dropdown(runs[0], data)
    _assert_run_is_selected(runs[1], data)
    _assert_run_is_in_dropdown_not_selected(runs[2], data)
    _assert_run_is_not_in_dropdown(runs[3], data)


@pytest.mark.parametrize("endpoint", ENDPOINTS)
def test_with_base_date_and_num_runs_and_execution_date_outside(admin_client, runs, endpoint):
    """
    Tests view with base_date and num_runs and execution-date URL parameters.
    Scenario: change the base date and num runs and press "Go",
    the selected execution date is outside the new range.
    Should only show dag runs older than base_date in the drop down.
    Should select the latest dag run within the range.
    Should set base date and num runs to submitted values.
    """
    base_date = runs[1].execution_date.isoformat()
    exec_date = runs[0].execution_date.isoformat()
    response = admin_client.get(
        f'{endpoint}&base_date={base_date}&num_runs=42&execution_date={exec_date}',
        data={"username": "test", "password": "test"},
        follow_redirects=True,
    )
    assert response.status_code == 200

    data = response.data.decode()
    _assert_base_date_and_num_runs(runs[1].execution_date, 42, data)
    _assert_run_is_not_in_dropdown(runs[0], data)
    _assert_run_is_selected(runs[1], data)
    _assert_run_is_in_dropdown_not_selected(runs[2], data)
    _assert_run_is_in_dropdown_not_selected(runs[3], data)


@pytest.mark.parametrize("endpoint", ENDPOINTS)
def test_with_base_date_and_num_runs_and_execution_date_within(admin_client, runs, endpoint):
    """
    Tests view with base_date and num_runs and execution-date URL parameters.
    Scenario: change the base date and num runs and press "Go",
    the selected execution date is within the new range.
    Should only show dag runs older than base_date in the drop down.
    Should select the dag run with the execution date.
    Should set base date and num runs to submitted values.
    """
    base_date = runs[2].execution_date.isoformat()
    exec_date = runs[3].execution_date.isoformat()
    response = admin_client.get(
        f'{endpoint}&base_date={base_date}&num_runs=5&execution_date={exec_date}',
        data={"username": "test", "password": "test"},
        follow_redirects=True,
    )
    assert response.status_code == 200

    data = response.data.decode()
    _assert_base_date_and_num_runs(runs[2].execution_date, 5, data)
    _assert_run_is_not_in_dropdown(runs[0], data)
    _assert_run_is_not_in_dropdown(runs[1], data)
    _assert_run_is_in_dropdown_not_selected(runs[2], data)
    _assert_run_is_selected(runs[3], data)
