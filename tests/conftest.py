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

import os
import subprocess
import sys
from contextlib import ExitStack
from datetime import datetime, timedelta

import freezegun
import pytest

# We should set these before loading _any_ of the rest of airflow so that the
# unit test mode config is set as early as possible.
tests_directory = os.path.dirname(os.path.realpath(__file__))

os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = os.path.join(tests_directory, "dags")
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
os.environ["AWS_DEFAULT_REGION"] = os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"
os.environ["CREDENTIALS_DIR"] = os.environ.get('CREDENTIALS_DIR') or "/files/airflow-breeze-config/keys"

from tests.test_utils.perf.perf_kit.sqlalchemy import (  # noqa isort:skip # pylint: disable=wrong-import-position
    count_queries,
    trace_queries,
)


@pytest.fixture()
def reset_environment():
    """
    Resets env variables.
    """
    init_env = os.environ.copy()
    yield
    changed_env = os.environ
    for key in changed_env:
        if key not in init_env:
            del os.environ[key]
        else:
            os.environ[key] = init_env[key]


@pytest.fixture()
def reset_db():
    """
    Resets Airflow db.
    """

    from airflow.utils import db

    db.resetdb()
    yield


ALLOWED_TRACE_SQL_COLUMNS = ['num', 'time', 'trace', 'sql', 'parameters', 'count']


@pytest.fixture(autouse=True)
def trace_sql(request):
    """
    Displays queries from the tests to console.
    """
    trace_sql_option = request.config.getoption("trace_sql")
    if not trace_sql_option:
        yield
        return

    terminal_reporter = request.config.pluginmanager.getplugin("terminalreporter")
    # if no terminal reporter plugin is present, nothing we can do here;
    # this can happen when this function executes in a slave node
    # when using pytest-xdist, for example
    if terminal_reporter is None:
        yield
        return

    columns = [col.strip() for col in trace_sql_option.split(",")]

    def pytest_print(text):
        return terminal_reporter.write_line(text)

    with ExitStack() as exit_stack:
        if columns == ['num']:
            # It is very unlikely that the user wants to display only numbers, but probably
            # the user just wants to count the queries.
            exit_stack.enter_context(count_queries(print_fn=pytest_print))  # pylint: disable=no-member
        elif any(c for c in ['time', 'trace', 'sql', 'parameters']):
            exit_stack.enter_context(  # pylint: disable=no-member
                trace_queries(
                    display_num='num' in columns,
                    display_time='time' in columns,
                    display_trace='trace' in columns,
                    display_sql='sql' in columns,
                    display_parameters='parameters' in columns,
                    print_fn=pytest_print,
                )
            )

        yield


def pytest_addoption(parser):
    """
    Add options parser for custom plugins
    """
    group = parser.getgroup("airflow")
    group.addoption(
        "--with-db-init",
        action="store_true",
        dest="db_init",
        help="Forces database initialization before tests",
    )
    group.addoption(
        "--integration",
        action="append",
        metavar="INTEGRATIONS",
        help="only run tests matching integration specified: "
        "[cassandra,kerberos,mongo,openldap,presto,rabbitmq,redis]. ",
    )
    group.addoption(
        "--backend",
        action="store",
        metavar="BACKEND",
        help="only run tests matching the backend: [sqlite,postgres,mysql].",
    )
    group.addoption(
        "--system",
        action="append",
        metavar="SYSTEMS",
        help="only run tests matching the system specified [google.cloud, google.marketing_platform]",
    )
    group.addoption(
        "--include-long-running",
        action="store_true",
        help="Includes long running tests (marked with long_running marker). They are skipped by default.",
    )
    group.addoption(
        "--include-quarantined",
        action="store_true",
        help="Includes quarantined tests (marked with quarantined marker). They are skipped by default.",
    )
    allowed_trace_sql_columns_list = ",".join(ALLOWED_TRACE_SQL_COLUMNS)
    group.addoption(
        "--trace-sql",
        action="store",
        help=(
            "Trace SQL statements. As an argument, you must specify the columns to be "
            f"displayed as a comma-separated list. Supported values: [f{allowed_trace_sql_columns_list}]"
        ),
        metavar="COLUMNS",
    )


def initial_db_init():
    if os.environ.get("RUN_AIRFLOW_1_10") == "true":
        print("Attempting to reset the db using airflow command")
        os.system("airflow resetdb -y")
    else:
        from airflow.utils import db

        db.resetdb()


@pytest.fixture(autouse=True, scope="session")
def breeze_test_helper(request):
    """
    Helper that setups Airflow testing environment. It does the same thing
    as the old 'run-tests' script.
    """

    # fixme: this should use some other env variable ex. RUNNING_ON_K8S
    if os.environ.get("SKIP_INIT_DB"):
        print("Skipping db initialization. Tests do not require database")
        return

    from airflow import __version__

    if __version__.startswith("1.10"):
        os.environ['RUN_AIRFLOW_1_10'] = "true"

    print(" AIRFLOW ".center(60, "="))

    # Setup test environment for breeze
    home = os.path.expanduser("~")
    airflow_home = os.environ.get("AIRFLOW_HOME") or os.path.join(home, "airflow")

    print(f"Home of the user: {home}\nAirflow home {airflow_home}")

    # Initialize Airflow db if required
    lock_file = os.path.join(airflow_home, ".airflow_db_initialised")
    if request.config.option.db_init:
        print("Initializing the DB - forced with --with-db-init switch.")
        initial_db_init()
    elif not os.path.exists(lock_file):
        print(
            "Initializing the DB - first time after entering the container.\n"
            "You can force re-initialization the database by adding --with-db-init switch to run-tests."
        )
        initial_db_init()
        # Create pid file
        with open(lock_file, "w+"):
            pass
    else:
        print(
            "Skipping initializing of the DB as it was initialized already.\n"
            "You can re-initialize the database by adding --with-db-init flag when running tests."
        )
    integration_kerberos = os.environ.get("INTEGRATION_KERBEROS")
    if integration_kerberos == "true":
        # Initialize kerberos
        kerberos = os.environ.get("KRB5_KTNAME")
        if kerberos:
            subprocess.check_call(["kinit", "-kt", kerberos, 'bob@EXAMPLE.COM'])
        else:
            print("Kerberos enabled! Please setup KRB5_KTNAME environment variable")
            sys.exit(1)


def pytest_configure(config):
    config.addinivalue_line("markers", "integration(name): mark test to run with named integration")
    config.addinivalue_line("markers", "backend(name): mark test to run with named backend")
    config.addinivalue_line("markers", "system(name): mark test to run with named system")
    config.addinivalue_line("markers", "long_running: mark test that run for a long time (many minutes)")
    config.addinivalue_line(
        "markers", "quarantined: mark test that are in quarantine (i.e. flaky, need to be isolated and fixed)"
    )
    config.addinivalue_line(
        "markers", "credential_file(name): mark tests that require credential file in CREDENTIALS_DIR"
    )
    config.addinivalue_line("markers", "airflow_2: mark tests that works only on Airflow 2.0 / master")


def skip_if_not_marked_with_integration(selected_integrations, item):
    for marker in item.iter_markers(name="integration"):
        integration_name = marker.args[0]
        if integration_name in selected_integrations or "all" in selected_integrations:
            return
    pytest.skip(
        "The test is skipped because it does not have the right integration marker. "
        "Only tests marked with pytest.mark.integration(INTEGRATION) are run with INTEGRATION"
        " being one of {integration}. {item}".format(integration=selected_integrations, item=item)
    )


def skip_if_not_marked_with_backend(selected_backend, item):
    for marker in item.iter_markers(name="backend"):
        backend_names = marker.args
        if selected_backend in backend_names:
            return
    pytest.skip(
        "The test is skipped because it does not have the right backend marker "
        "Only tests marked with pytest.mark.backend('{backend}') are run"
        ": {item}".format(backend=selected_backend, item=item)
    )


def skip_if_not_marked_with_system(selected_systems, item):
    for marker in item.iter_markers(name="system"):
        systems_name = marker.args[0]
        if systems_name in selected_systems or "all" in selected_systems:
            return
    pytest.skip(
        "The test is skipped because it does not have the right system marker. "
        "Only tests marked with pytest.mark.system(SYSTEM) are run with SYSTEM"
        " being one of {systems}. {item}".format(systems=selected_systems, item=item)
    )


def skip_system_test(item):
    for marker in item.iter_markers(name="system"):
        pytest.skip(
            "The test is skipped because it has system marker. "
            "System tests are only run when --system flag "
            "with the right system ({system}) is passed to pytest. {item}".format(
                system=marker.args[0], item=item
            )
        )


def skip_long_running_test(item):
    for _ in item.iter_markers(name="long_running"):
        pytest.skip(
            "The test is skipped because it has long_running marker. "
            "And --include-long-running flag is not passed to pytest. {item}".format(item=item)
        )


def skip_quarantined_test(item):
    for _ in item.iter_markers(name="quarantined"):
        pytest.skip(
            "The test is skipped because it has quarantined marker. "
            "And --include-quarantined flag is passed to pytest. {item}".format(item=item)
        )


def skip_if_integration_disabled(marker, item):
    integration_name = marker.args[0]
    environment_variable_name = "INTEGRATION_" + integration_name.upper()
    environment_variable_value = os.environ.get(environment_variable_name)
    if not environment_variable_value or environment_variable_value != "true":
        pytest.skip(
            "The test requires {integration_name} integration started and "
            "{name} environment variable to be set to true (it is '{value}')."
            " It can be set by specifying '--integration {integration_name}' at breeze startup"
            ": {item}".format(
                name=environment_variable_name,
                value=environment_variable_value,
                integration_name=integration_name,
                item=item,
            )
        )


def skip_if_wrong_backend(marker, item):
    valid_backend_names = marker.args
    environment_variable_name = "BACKEND"
    environment_variable_value = os.environ.get(environment_variable_name)
    if not environment_variable_value or environment_variable_value not in valid_backend_names:
        pytest.skip(
            "The test requires one of {valid_backend_names} backend started and "
            "{name} environment variable to be set to 'true' (it is '{value}')."
            " It can be set by specifying backend at breeze startup"
            ": {item}".format(
                name=environment_variable_name,
                value=environment_variable_value,
                valid_backend_names=valid_backend_names,
                item=item,
            )
        )


def skip_if_credential_file_missing(item):
    for marker in item.iter_markers(name="credential_file"):
        credential_file = marker.args[0]
        credential_path = os.path.join(os.environ.get('CREDENTIALS_DIR'), credential_file)
        if not os.path.exists(credential_path):
            pytest.skip(f"The test requires credential file {credential_path}: {item}")


def skip_if_airflow_2_test(item):
    for _ in item.iter_markers(name="airflow_2"):
        if os.environ.get("RUN_AIRFLOW_1_10") == "true":
            pytest.skip("The test works only with Airflow 2.0 / master branch")


def pytest_runtest_setup(item):
    selected_integrations_list = item.config.getoption("--integration")
    selected_systems_list = item.config.getoption("--system")

    include_long_running = item.config.getoption("--include-long-running")
    include_quarantined = item.config.getoption("--include-quarantined")

    for marker in item.iter_markers(name="integration"):
        skip_if_integration_disabled(marker, item)
    if selected_integrations_list:
        skip_if_not_marked_with_integration(selected_integrations_list, item)
    if selected_systems_list:
        skip_if_not_marked_with_system(selected_systems_list, item)
    else:
        skip_system_test(item)
    for marker in item.iter_markers(name="backend"):
        skip_if_wrong_backend(marker, item)
    selected_backend = item.config.getoption("--backend")
    if selected_backend:
        skip_if_not_marked_with_backend(selected_backend, item)
    if not include_long_running:
        skip_long_running_test(item)
    if not include_quarantined:
        skip_quarantined_test(item)
    skip_if_credential_file_missing(item)
    skip_if_airflow_2_test(item)


@pytest.fixture
def frozen_sleep(monkeypatch):
    """
    Use freezegun to "stub" sleep, so that it takes no time, but that
    ``datetime.now()`` appears to move forwards

    If your module under test does ``import time`` and then ``time.sleep``::

        def test_something(frozen_sleep):
            my_mod.fn_under_test()


    If your module under test does ``from time import sleep`` then you will
    have to mock that sleep function directly::

        def test_something(frozen_sleep, monkeypatch):
            monkeypatch.setattr('my_mod.sleep', frozen_sleep)
            my_mod.fn_under_test()
    """
    freezegun_control = None

    def fake_sleep(seconds):
        nonlocal freezegun_control
        utcnow = datetime.utcnow()
        if freezegun_control is not None:
            freezegun_control.stop()
        freezegun_control = freezegun.freeze_time(utcnow + timedelta(seconds=seconds))
        freezegun_control.start()

    monkeypatch.setattr("time.sleep", fake_sleep)
    yield fake_sleep

    if freezegun_control is not None:
        freezegun_control.stop()
