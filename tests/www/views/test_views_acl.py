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
import datetime
import json
import urllib.parse

import pytest

from airflow.security import permissions
from airflow.utils import timezone
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from tests.test_utils.api_connexion_utils import create_user
from tests.test_utils.db import clear_db_runs
from tests.test_utils.www import check_content_in_response, check_content_not_in_response, client_with_login

NEXT_YEAR = datetime.datetime.now().year + 1
DEFAULT_DATE = timezone.datetime(NEXT_YEAR, 6, 1)
USER_DATA = {
    "dag_tester": (
        "dag_acl_tester",
        {
            "first_name": 'dag_test',
            "last_name": 'dag_test',
            "email": 'dag_test@fab.org',
            "password": 'dag_test',
        },
    ),
    "dag_faker": (  # User without permission.
        "dag_acl_faker",
        {
            "first_name": 'dag_faker',
            "last_name": 'dag_faker',
            "email": 'dag_fake@fab.org',
            "password": 'dag_faker',
        },
    ),
    "dag_read_only": (  # User with only read permission.
        "dag_acl_read_only",
        {
            "first_name": 'dag_read_only',
            "last_name": 'dag_read_only',
            "email": 'dag_read_only@fab.org',
            "password": 'dag_read_only',
        },
    ),
    "all_dag_user": (  # User has all dag access.
        "all_dag_role",
        {
            "first_name": 'all_dag_user',
            "last_name": 'all_dag_user',
            "email": 'all_dag_user@fab.org',
            "password": 'all_dag_user',
        },
    ),
}


@pytest.fixture(scope="module")
def acl_app(app):
    security_manager = app.appbuilder.sm
    for username, (role_name, kwargs) in USER_DATA.items():
        if not security_manager.find_user(username=username):
            role = security_manager.add_role(role_name)
            security_manager.add_user(
                role=role,
                username=username,
                **kwargs,  # pylint: disable=not-a-mapping
            )

    # FIXME: Clean up this block of code.....

    website_permission = security_manager.find_permission_view_menu(
        permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE
    )

    dag_tester_role = security_manager.find_role('dag_acl_tester')
    edit_perm_on_dag = security_manager.find_permission_view_menu(
        permissions.ACTION_CAN_EDIT, 'DAG:example_bash_operator'
    )
    security_manager.add_permission_role(dag_tester_role, edit_perm_on_dag)
    read_perm_on_dag = security_manager.find_permission_view_menu(
        permissions.ACTION_CAN_READ, 'DAG:example_bash_operator'
    )
    security_manager.add_permission_role(dag_tester_role, read_perm_on_dag)
    security_manager.add_permission_role(dag_tester_role, website_permission)

    all_dag_role = security_manager.find_role('all_dag_role')
    edit_perm_on_all_dag = security_manager.find_permission_view_menu(
        permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG
    )
    security_manager.add_permission_role(all_dag_role, edit_perm_on_all_dag)
    read_perm_on_all_dag = security_manager.find_permission_view_menu(
        permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG
    )
    security_manager.add_permission_role(all_dag_role, read_perm_on_all_dag)
    read_perm_on_task_instance = security_manager.find_permission_view_menu(
        permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE
    )
    security_manager.add_permission_role(all_dag_role, read_perm_on_task_instance)
    security_manager.add_permission_role(all_dag_role, website_permission)

    role_user = security_manager.find_role('User')
    security_manager.add_permission_role(role_user, read_perm_on_all_dag)
    security_manager.add_permission_role(role_user, edit_perm_on_all_dag)
    security_manager.add_permission_role(role_user, website_permission)

    read_only_perm_on_dag = security_manager.find_permission_view_menu(
        permissions.ACTION_CAN_READ, 'DAG:example_bash_operator'
    )
    dag_read_only_role = security_manager.find_role('dag_acl_read_only')
    security_manager.add_permission_role(dag_read_only_role, read_only_perm_on_dag)
    security_manager.add_permission_role(dag_read_only_role, website_permission)

    dag_acl_faker_role = security_manager.find_role('dag_acl_faker')
    security_manager.add_permission_role(dag_acl_faker_role, website_permission)

    yield app

    for username, _ in USER_DATA.items():
        user = security_manager.find_user(username=username)
        if user:
            security_manager.del_register_user(user)


@pytest.fixture(scope="module")
def reset_dagruns():
    """Clean up stray garbage from other tests."""
    clear_db_runs()


@pytest.fixture(autouse=True)
def init_dagruns(acl_app, reset_dagruns):  # pylint: disable=unused-argument
    acl_app.dag_bag.get_dag("example_bash_operator").create_dagrun(
        run_type=DagRunType.SCHEDULED,
        execution_date=DEFAULT_DATE,
        start_date=timezone.utcnow(),
        state=State.RUNNING,
    )
    acl_app.dag_bag.get_dag("example_subdag_operator").create_dagrun(
        run_type=DagRunType.SCHEDULED,
        execution_date=DEFAULT_DATE,
        start_date=timezone.utcnow(),
        state=State.RUNNING,
    )
    yield
    clear_db_runs()


@pytest.fixture()
def dag_test_client(acl_app):
    return client_with_login(acl_app, username="dag_test", password="dag_test")


@pytest.fixture()
def dag_faker_client(acl_app):
    return client_with_login(acl_app, username="dag_faker", password="dag_faker")


@pytest.fixture()
def all_dag_user_client(acl_app):
    return client_with_login(
        acl_app,
        username="all_dag_user",
        password="all_dag_user",
    )


@pytest.fixture(scope="module")
def user_edit_one_dag(acl_app):
    return create_user(
        acl_app,
        username="user_edit_one_dag",
        role_name="role_edit_one_dag",
        permissions=[
            (permissions.ACTION_CAN_READ, 'DAG:example_bash_operator'),
            (permissions.ACTION_CAN_EDIT, 'DAG:example_bash_operator'),
        ],
    )


@pytest.mark.usefixtures("user_edit_one_dag")
def test_permission_exist(acl_app):
    perms_views = acl_app.appbuilder.sm.find_permissions_view_menu(
        acl_app.appbuilder.sm.find_view_menu('DAG:example_bash_operator'),
    )
    assert len(perms_views) == 2

    perms = {str(perm) for perm in perms_views}
    assert "can read on DAG:example_bash_operator" in perms
    assert "can edit on DAG:example_bash_operator" in perms


@pytest.mark.usefixtures("user_edit_one_dag")
def test_role_permission_associate(acl_app):
    test_role = acl_app.appbuilder.sm.find_role('role_edit_one_dag')
    perms = {str(perm) for perm in test_role.permissions}
    assert 'can edit on DAG:example_bash_operator' in perms
    assert 'can read on DAG:example_bash_operator' in perms


@pytest.fixture(scope="module")
def user_all_dags(acl_app):
    return create_user(
        acl_app,
        username="user_all_dags",
        role_name="role_all_dags",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
        ],
    )


@pytest.fixture()
def client_all_dags(acl_app, user_all_dags):  # pylint: disable=unused-argument
    return client_with_login(
        acl_app,
        username="user_all_dags",
        password="user_all_dags",
    )


def test_index_for_all_dag_user(client_all_dags):
    # The all dag user can access/view all dags.
    resp = client_all_dags.get('/', follow_redirects=True)
    check_content_in_response('example_subdag_operator', resp)
    check_content_in_response('example_bash_operator', resp)


def test_index_failure(dag_test_client):
    # This user can only access/view example_bash_operator dag.
    resp = dag_test_client.get('/', follow_redirects=True)
    check_content_not_in_response('example_subdag_operator', resp)


def test_dag_autocomplete_success(client_all_dags):
    resp = client_all_dags.get(
        'dagmodel/autocomplete?query=example_bash',
        follow_redirects=False,
    )
    check_content_in_response('example_bash_operator', resp)
    check_content_not_in_response('example_subdag_operator', resp)


@pytest.fixture(scope="module")
def user_all_dags_dagruns(acl_app):
    return create_user(
        acl_app,
        username="user_all_dags_dagruns",
        role_name="role_all_dags_dagruns",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
        ],
    )


@pytest.fixture()
def client_all_dags_dagruns(acl_app, user_all_dags_dagruns):  # pylint: disable=unused-argument
    return client_with_login(
        acl_app,
        username="user_all_dags_dagruns",
        password="user_all_dags_dagruns",
    )


def test_dag_stats_success(client_all_dags_dagruns):
    resp = client_all_dags_dagruns.post('dag_stats', follow_redirects=True)
    check_content_in_response('example_bash_operator', resp)
    assert set(list(resp.json.items())[0][1][0].keys()) == {'state', 'count'}


def test_task_stats_failure(dag_test_client):
    resp = dag_test_client.post('task_stats', follow_redirects=True)
    check_content_not_in_response('example_subdag_operator', resp)


def test_dag_stats_success_for_all_dag_user(client_all_dags_dagruns):
    resp = client_all_dags_dagruns.post('dag_stats', follow_redirects=True)
    check_content_in_response('example_subdag_operator', resp)
    check_content_in_response('example_bash_operator', resp)


@pytest.fixture(scope="module")
def user_all_dags_dagruns_tis(acl_app):
    return create_user(
        acl_app,
        username="user_all_dags_dagruns_tis",
        role_name="role_all_dags_dagruns_tis",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
        ],
    )


@pytest.fixture()
def client_all_dags_dagruns_tis(acl_app, user_all_dags_dagruns_tis):  # pylint: disable=unused-argument
    return client_with_login(
        acl_app,
        username="user_all_dags_dagruns_tis",
        password="user_all_dags_dagruns_tis",
    )


def test_task_stats_empty_success(client_all_dags_dagruns_tis):
    resp = client_all_dags_dagruns_tis.post('task_stats', follow_redirects=True)
    check_content_in_response('example_bash_operator', resp)
    check_content_in_response('example_subdag_operator', resp)


@pytest.mark.parametrize(
    "dags_to_run, unexpected_dag_ids",
    [
        (
            ["example_subdag_operator"],
            ["example_bash_operator", "example_xcom"],
        ),
        (
            ["example_subdag_operator", "example_bash_operator"],
            ["example_xcom"],
        ),
    ],
    ids=["single", "multi"],
)
def test_task_stats_success(
    client_all_dags_dagruns_tis,
    dags_to_run,
    unexpected_dag_ids,
):
    resp = client_all_dags_dagruns_tis.post(
        'task_stats', data={'dag_ids': dags_to_run}, follow_redirects=True
    )
    assert resp.status_code == 200
    for dag_id in unexpected_dag_ids:
        check_content_not_in_response(dag_id, resp)
    stats = json.loads(resp.data.decode())
    for dag_id in dags_to_run:
        assert dag_id in stats


@pytest.fixture(scope="module")
def user_all_dags_codes(acl_app):
    return create_user(
        acl_app,
        username="user_all_dags_codes",
        role_name="role_all_dags_codes",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_CODE),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
        ],
    )


@pytest.fixture()
def client_all_dags_codes(acl_app, user_all_dags_codes):  # pylint: disable=unused-argument
    return client_with_login(
        acl_app,
        username="user_all_dags_codes",
        password="user_all_dags_codes",
    )


def test_code_success(client_all_dags_codes):
    url = 'code?dag_id=example_bash_operator'
    resp = client_all_dags_codes.get(url, follow_redirects=True)
    check_content_in_response('example_bash_operator', resp)


def test_code_failure(dag_test_client):
    url = 'code?dag_id=example_bash_operator'
    resp = dag_test_client.get(url, follow_redirects=True)
    check_content_not_in_response('example_bash_operator', resp)


@pytest.mark.parametrize(
    "dag_id",
    ["example_bash_operator", "example_subdag_operator"],
)
def test_code_success_for_all_dag_user(client_all_dags_codes, dag_id):
    url = f'code?dag_id={dag_id}'
    resp = client_all_dags_codes.get(url, follow_redirects=True)
    check_content_in_response(dag_id, resp)


def test_dag_details_success(client_all_dags_dagruns):
    """User without RESOURCE_DAG_CODE can see the page, just not the ID."""
    url = 'dag_details?dag_id=example_bash_operator'
    resp = client_all_dags_dagruns.get(url, follow_redirects=True)
    check_content_in_response('DAG Details', resp)


def test_dag_details_failure(dag_faker_client):
    url = 'dag_details?dag_id=example_bash_operator'
    resp = dag_faker_client.get(url, follow_redirects=True)
    check_content_not_in_response('DAG Details', resp)


@pytest.mark.parametrize(
    "dag_id",
    ["example_bash_operator", "example_subdag_operator"],
)
def test_dag_details_success_for_all_dag_user(client_all_dags_dagruns, dag_id):
    url = f'dag_details?dag_id={dag_id}'
    resp = client_all_dags_dagruns.get(url, follow_redirects=True)
    check_content_in_response(dag_id, resp)


@pytest.fixture(scope="module")
def user_all_dags_tis(acl_app):
    return create_user(
        acl_app,
        username="user_all_dags_tis",
        role_name="role_all_dags_tis",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
        ],
    )


@pytest.fixture()
def client_all_dags_tis(acl_app, user_all_dags_tis):  # pylint: disable=unused-argument
    return client_with_login(
        acl_app,
        username="user_all_dags_tis",
        password="user_all_dags_tis",
    )


@pytest.fixture(scope="module")
def user_all_dags_tis_xcom(acl_app):
    return create_user(
        acl_app,
        username="user_all_dags_tis_xcom",
        role_name="role_all_dags_tis_xcom",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_XCOM),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
        ],
    )


@pytest.fixture()
def client_all_dags_tis_xcom(acl_app, user_all_dags_tis_xcom):  # pylint: disable=unused-argument
    return client_with_login(
        acl_app,
        username="user_all_dags_tis_xcom",
        password="user_all_dags_tis_xcom",
    )


@pytest.fixture(scope="module")
def user_dags_tis_logs(acl_app):
    return create_user(
        acl_app,
        username="user_dags_tis_logs",
        role_name="role_dags_tis_logs",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_LOG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
        ],
    )


@pytest.fixture()
def client_dags_tis_logs(acl_app, user_dags_tis_logs):  # pylint: disable=unused-argument
    return client_with_login(
        acl_app,
        username="user_dags_tis_logs",
        password="user_dags_tis_logs",
    )


RENDERED_TEMPLATES_URL = (
    f'rendered-templates?task_id=runme_0&dag_id=example_bash_operator&'
    f'execution_date={urllib.parse.quote_plus(str(DEFAULT_DATE))}'
)
TASK_URL = (
    f'task?task_id=runme_0&dag_id=example_bash_operator&'
    f'execution_date={urllib.parse.quote_plus(str(DEFAULT_DATE))}'
)
XCOM_URL = (
    f'xcom?task_id=runme_0&dag_id=example_bash_operator&'
    f'execution_date={urllib.parse.quote_plus(str(DEFAULT_DATE))}'
)
DURATION_URL = "duration?days=30&dag_id=example_bash_operator"
TRIES_URL = "tries?days=30&dag_id=example_bash_operator"
LANDING_TIMES_URL = "landing_times?days=30&dag_id=example_bash_operator"
GANTT_URL = "gantt?dag_id=example_bash_operator"
TREE_URL = "tree?dag_id=example_bash_operator"
LOG_URL = (
    f"log?task_id=runme_0&dag_id=example_bash_operator&"
    f"execution_date={urllib.parse.quote_plus(str(DEFAULT_DATE))}"
)


@pytest.mark.parametrize(
    "client, url, expected_content",
    [
        ("client_all_dags_tis", RENDERED_TEMPLATES_URL, "Rendered Template"),
        ("all_dag_user_client", RENDERED_TEMPLATES_URL, "Rendered Template"),
        ("client_all_dags_tis", TASK_URL, "Task Instance Details"),
        ("client_all_dags_tis_xcom", XCOM_URL, "XCom"),
        ("client_all_dags_tis", DURATION_URL, "example_bash_operator"),
        ("client_all_dags_tis", TRIES_URL, "example_bash_operator"),
        ("client_all_dags_tis", LANDING_TIMES_URL, "example_bash_operator"),
        ("client_all_dags_tis", GANTT_URL, "example_bash_operator"),
        ("client_dags_tis_logs", TREE_URL, "runme_1"),
        ("viewer_client", TREE_URL, "runme_1"),
        ("client_dags_tis_logs", LOG_URL, "Log by attempts"),
        ("user_client", LOG_URL, "Log by attempts"),
    ],
    ids=[
        "rendered-templates",
        "rendered-templates-all-dag-user",
        "task",
        "xcom",
        "duration",
        "tries",
        "landing-times",
        "gantt",
        "tree-for-readonly-role",
        "tree-for-viewer",
        "log",
        "log-for-user",
    ],
)
def test_success(request, client, url, expected_content):
    resp = request.getfixturevalue(client).get(url, follow_redirects=True)
    check_content_in_response(expected_content, resp)


@pytest.mark.parametrize(
    "url, unexpected_content",
    [
        (RENDERED_TEMPLATES_URL, "Rendered Template"),
        (TASK_URL, "Task Instance Details"),
        (XCOM_URL, "XCom"),
        (DURATION_URL, "example_bash_operator"),
        (TRIES_URL, "example_bash_operator"),
        (LANDING_TIMES_URL, "example_bash_operator"),
        (GANTT_URL, "example_bash_operator"),
        (LOG_URL, "Log by attempts"),
    ],
    ids=[
        "rendered-templates",
        "task",
        "xcom",
        "duration",
        "tries",
        "landing-times",
        "gantt",
        "log",
    ],
)
def test_failure(dag_faker_client, url, unexpected_content):
    resp = dag_faker_client.get(url, follow_redirects=True)
    check_content_not_in_response(unexpected_content, resp)


@pytest.mark.parametrize("client", ["dag_test_client", "all_dag_user_client"])
def test_run_success(request, client):
    form = dict(
        task_id="runme_0",
        dag_id="example_bash_operator",
        ignore_all_deps="false",
        ignore_ti_state="true",
        execution_date=DEFAULT_DATE,
    )
    resp = request.getfixturevalue(client).post('run', data=form)
    assert resp.status_code == 302


def test_blocked_success(client_all_dags_dagruns):
    resp = client_all_dags_dagruns.post('blocked', follow_redirects=True)
    check_content_in_response('example_bash_operator', resp)


def test_blocked_success_for_all_dag_user(all_dag_user_client):
    resp = all_dag_user_client.post('blocked', follow_redirects=True)
    check_content_in_response('example_bash_operator', resp)
    check_content_in_response('example_subdag_operator', resp)


@pytest.mark.parametrize(
    "dags_to_block, unexpected_dag_ids",
    [
        (
            ["example_subdag_operator"],
            ["example_bash_operator", "example_xcom"],
        ),
        (
            ["example_subdag_operator", "example_bash_operator"],
            ["example_xcom"],
        ),
    ],
    ids=["single", "multi"],
)
def test_blocked_success_when_selecting_dags(
    admin_client,
    dags_to_block,
    unexpected_dag_ids,
):
    resp = admin_client.post(
        'blocked',
        data={'dag_ids': dags_to_block},
        follow_redirects=True,
    )
    assert resp.status_code == 200
    for dag_id in unexpected_dag_ids:
        check_content_not_in_response(dag_id, resp)
    blocked_dags = {blocked['dag_id'] for blocked in json.loads(resp.data.decode())}
    for dag_id in dags_to_block:
        assert dag_id in blocked_dags


@pytest.fixture(scope="module")
def user_all_dags_edit_tis(acl_app):
    return create_user(
        acl_app,
        username="user_all_dags_edit_tis",
        role_name="role_all_dags_edit_tis",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_TASK_INSTANCE),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
        ],
    )


@pytest.fixture()
def client_all_dags_edit_tis(acl_app, user_all_dags_edit_tis):  # pylint: disable=unused-argument
    return client_with_login(
        acl_app,
        username="user_all_dags_edit_tis",
        password="user_all_dags_edit_tis",
    )


def test_failed_success(client_all_dags_edit_tis):
    form = dict(
        task_id="run_this_last",
        dag_id="example_bash_operator",
        execution_date=DEFAULT_DATE,
        upstream="false",
        downstream="false",
        future="false",
        past="false",
    )
    resp = client_all_dags_edit_tis.post('failed', data=form)
    check_content_in_response('example_bash_operator', resp)


@pytest.mark.parametrize(
    "url, expected_content",
    [
        ("paused?dag_id=example_bash_operator&is_paused=false", "OK"),
        ("refresh?dag_id=example_bash_operator", ""),
    ],
    ids=[
        "paused",
        "refresh",
    ],
)
def test_post_success(dag_test_client, url, expected_content):
    # post request failure won't test
    resp = dag_test_client.post(url, follow_redirects=True)
    check_content_in_response(expected_content, resp)


@pytest.fixture(scope="module")
def user_only_dags_tis(acl_app):
    return create_user(
        acl_app,
        username="user_only_dags_tis",
        role_name="role_only_dags_tis",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
        ],
    )


@pytest.fixture()
def client_only_dags_tis(acl_app, user_only_dags_tis):  # pylint: disable=unused-argument
    return client_with_login(
        acl_app,
        username="user_only_dags_tis",
        password="user_only_dags_tis",
    )


def test_success_fail_for_read_only_task_instance_access(client_only_dags_tis):
    form = dict(
        task_id="run_this_last",
        dag_id="example_bash_operator",
        execution_date=DEFAULT_DATE,
        upstream="false",
        downstream="false",
        future="false",
        past="false",
    )
    resp = client_only_dags_tis.post('success', data=form)
    check_content_not_in_response('Wait a minute', resp, resp_code=302)


GET_LOGS_WITH_METADATA_URL = (
    f"get_logs_with_metadata?task_id=runme_0&dag_id=example_bash_operator&"
    f"execution_date={urllib.parse.quote_plus(str(DEFAULT_DATE))}&"
    f"try_number=1&metadata=null"
)


@pytest.mark.parametrize("client", ["client_dags_tis_logs", "user_client"])
def test_get_logs_with_metadata_success(request, client):
    resp = request.getfixturevalue(client).get(
        GET_LOGS_WITH_METADATA_URL,
        follow_redirects=True,
    )
    check_content_in_response('"message":', resp)
    check_content_in_response('"metadata":', resp)


def test_get_logs_with_metadata_failure(dag_faker_client):
    resp = dag_faker_client.get(
        GET_LOGS_WITH_METADATA_URL,
        follow_redirects=True,
    )
    check_content_not_in_response('"message":', resp)
    check_content_not_in_response('"metadata":', resp)


def test_refresh_failure_for_viewer(viewer_client):
    # viewer role can't refresh
    resp = viewer_client.post('refresh?dag_id=example_bash_operator')
    check_content_in_response('Redirecting', resp, resp_code=302)
