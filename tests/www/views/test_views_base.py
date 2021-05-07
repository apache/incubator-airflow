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

import flask
import pytest

from airflow import version
from airflow.jobs.base_job import BaseJob
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.www.views import FILTER_STATUS_COOKIE, FILTER_TAGS_COOKIE
from tests.test_utils.asserts import assert_queries_count
from tests.test_utils.config import conf_vars
from tests.test_utils.www import check_content_in_response, check_content_not_in_response


def test_index(admin_client):
    with assert_queries_count(43):
        resp = admin_client.get('/', follow_redirects=True)
    check_content_in_response('DAGs', resp)


def test_doc_urls(admin_client):
    resp = admin_client.get('/', follow_redirects=True)
    if "dev" in version.version:
        airflow_doc_site = (
            "http://apache-airflow-docs.s3-website.eu-central-1.amazonaws.com/docs/apache-airflow/"
        )
    else:
        airflow_doc_site = f'https://airflow.apache.org/docs/apache-airflow/{version.version}'

    check_content_in_response(airflow_doc_site, resp)
    check_content_in_response("/api/v1/ui", resp)


def test_home(capture_templates, admin_client):
    with capture_templates() as templates:
        resp = admin_client.get('home', follow_redirects=True)
        check_content_in_response('DAGs', resp)
        val_state_color_mapping = (
            'const STATE_COLOR = {"failed": "red", '
            '"null": "lightblue", "queued": "gray", '
            '"removed": "lightgrey", "running": "lime", '
            '"scheduled": "tan", "sensing": "lightseagreen", '
            '"shutdown": "blue", "skipped": "pink", '
            '"success": "green", "up_for_reschedule": "turquoise", '
            '"up_for_retry": "gold", "upstream_failed": "orange"};'
        )
        check_content_in_response(val_state_color_mapping, resp)

    assert len(templates) == 1
    assert templates[0].name == 'airflow/dags.html'
    state_color_mapping = State.state_color.copy()
    state_color_mapping["null"] = state_color_mapping.pop(None)
    assert templates[0].local_context['state_color'] == state_color_mapping


@pytest.fixture()
def heartbeat_healthy():
    # case-1: healthy scheduler status
    last_heartbeat = timezone.utcnow()
    job = BaseJob(
        job_type='SchedulerJob',
        state='running',
        latest_heartbeat=last_heartbeat,
    )
    with create_session() as session:
        session.add(job)
    yield 'healthy', last_heartbeat.isoformat()
    with create_session() as session:
        session.query(BaseJob).filter(
            BaseJob.job_type == 'SchedulerJob',
            BaseJob.state == 'running',
            BaseJob.latest_heartbeat == last_heartbeat,
        ).delete()


@pytest.fixture()
def heartbeat_too_slow():
    # case-2: unhealthy scheduler status - scenario 1 (SchedulerJob is running too slowly)
    last_heartbeat = timezone.utcnow() - datetime.timedelta(minutes=1)
    job = BaseJob(
        job_type='SchedulerJob',
        state='running',
        latest_heartbeat=last_heartbeat,
    )
    with create_session() as session:
        session.query(BaseJob).filter(
            BaseJob.job_type == 'SchedulerJob',
        ).update({'latest_heartbeat': last_heartbeat - datetime.timedelta(seconds=1)})
        session.add(job)
    yield 'unhealthy', last_heartbeat.isoformat()
    with create_session() as session:
        session.query(BaseJob).filter(
            BaseJob.job_type == 'SchedulerJob',
            BaseJob.state == 'running',
            BaseJob.latest_heartbeat == last_heartbeat,
        ).delete()


@pytest.fixture()
def heartbeat_not_running():
    # case-3: unhealthy scheduler status - scenario 2 (no running SchedulerJob)
    with create_session() as session:
        session.query(BaseJob).filter(
            BaseJob.job_type == 'SchedulerJob',
            BaseJob.state == 'running',
        ).delete()
    yield 'unhealthy', None


@pytest.mark.parametrize(
    "heartbeat",
    ["heartbeat_healthy", "heartbeat_too_slow", "heartbeat_not_running"],
)
def test_health(request, admin_client, heartbeat):
    # Load the corresponding fixture by name.
    scheduler_status, last_scheduler_heartbeat = request.getfixturevalue(heartbeat)
    resp = admin_client.get('health', follow_redirects=True)
    resp_json = json.loads(resp.data.decode('utf-8'))
    assert 'healthy' == resp_json['metadatabase']['status']
    assert scheduler_status == resp_json['scheduler']['status']
    assert last_scheduler_heartbeat == resp_json['scheduler']['latest_scheduler_heartbeat']


def test_users_list(admin_client):
    resp = admin_client.get('users/list', follow_redirects=True)
    check_content_in_response('List Users', resp)


@pytest.mark.parametrize(
    "path, body_content",
    [("roles/list", "List Roles"), ("roles/show/1", "Show Role")],
)
def test_roles_read(admin_client, path, body_content):
    resp = admin_client.get(path, follow_redirects=True)
    check_content_in_response(body_content, resp)


def test_roles_read_unauthorized(viewer_client):
    resp = viewer_client.get("roles/list", follow_redirects=True)
    check_content_in_response('Access is Denied', resp)


@pytest.fixture(scope="module")
def delete_role_if_exists(app):
    def func(role_name):
        if app.appbuilder.sm.find_role(role_name):
            app.appbuilder.sm.delete_role(role_name)

    return func


@pytest.fixture()
def non_exist_role_name(delete_role_if_exists):
    role_name = "test_roles_create_role"
    delete_role_if_exists(role_name)
    yield role_name
    delete_role_if_exists(role_name)


@pytest.fixture()
def exist_role_name(app, delete_role_if_exists):
    role_name = "test_roles_create_role_new"
    app.appbuilder.sm.add_role(role_name)
    yield role_name
    delete_role_if_exists(role_name)


@pytest.fixture()
def exist_role(app, exist_role_name):
    return app.appbuilder.sm.find_role(exist_role_name)


def test_roles_create(app, admin_client, non_exist_role_name):
    admin_client.post("roles/add", data={'name': non_exist_role_name}, follow_redirects=True)
    assert app.appbuilder.sm.find_role(non_exist_role_name) is not None


def test_roles_create_unauthorized(app, viewer_client, non_exist_role_name):
    resp = viewer_client.post("roles/add", data={'name': non_exist_role_name}, follow_redirects=True)
    check_content_in_response('Access is Denied', resp)
    assert app.appbuilder.sm.find_role(non_exist_role_name) is None


def test_roles_edit(app, admin_client, non_exist_role_name, exist_role):
    admin_client.post(
        f"roles/edit/{exist_role.id}", data={'name': non_exist_role_name}, follow_redirects=True
    )
    updated_role = app.appbuilder.sm.find_role(non_exist_role_name)
    assert exist_role.id == updated_role.id


def test_roles_edit_unauthorized(app, viewer_client, non_exist_role_name, exist_role_name, exist_role):
    resp = viewer_client.post(
        f"roles/edit/{exist_role.id}", data={'name': non_exist_role_name}, follow_redirects=True
    )
    check_content_in_response('Access is Denied', resp)
    assert app.appbuilder.sm.find_role(exist_role_name)
    assert app.appbuilder.sm.find_role(non_exist_role_name) is None


def test_roles_delete(app, admin_client, exist_role_name, exist_role):
    admin_client.post(f"roles/delete/{exist_role.id}", follow_redirects=True)
    assert app.appbuilder.sm.find_role(exist_role_name) is None


def test_roles_delete_unauthorized(app, viewer_client, exist_role, exist_role_name):
    resp = viewer_client.post(f"roles/delete/{exist_role.id}", follow_redirects=True)
    check_content_in_response('Access is Denied', resp)
    assert app.appbuilder.sm.find_role(exist_role_name)


@pytest.mark.parametrize(
    "url, client, content",
    [
        ("userstatschartview/chart/", "admin_client", "User Statistics"),
        ("userstatschartview/chart/", "viewer_client", "Access is Denied"),
        ("permissions/list", "admin_client", "List Base Permissions"),
        ("permissions/list", "viewer_client", "Access is Denied"),
        ("viewmenus/list/", "admin_client", "List View Menus"),
        ("viewmenus/list/", "viewer_client", "Access is Denied"),
        ("permissionviews/list/", "admin_client", "List Permissions on Views/Menus"),
        ("permissionviews/list/", "viewer_client", "Access is Denied"),
        ("resetpassword/form?pk=1", "admin_client", "Reset Password Form"),
        ("resetpassword/form?pk=1", "viewer_client", "Access is Denied"),
        ("users/list", "admin_client", "List Users"),
        ("users/list", "viewer_client", "Access is Denied"),
    ],
    ids=[
        "userstatschertview-admin",
        "userstatschertview-viewer",
        "permissions-admin",
        "permissions-viewer",
        "viewmenus-admin",
        "viewmenus-viewer",
        "permissionviews-admin",
        "permissionviews-viewer",
        "resetpassword-admin",
        "resetpassword-viewer",
        "users-admin",
        "users-viewer",
    ],
)
def test_views_get(request, url, client, content):
    resp = request.getfixturevalue(client).get(url, follow_redirects=True)
    check_content_in_response(content, resp)


def _check_task_stats_json(resp):
    return set(list(resp.json.items())[0][1][0].keys()) == {'state', 'count'}


@pytest.mark.parametrize(
    "url, check_response",
    [
        ("blocked", None),
        ("dag_stats", None),
        ("task_stats", _check_task_stats_json),
    ],
)
def test_views_post(admin_client, url, check_response):
    resp = admin_client.post(url, follow_redirects=True)
    assert resp.status_code == 200
    if check_response:
        assert check_response(resp)


@pytest.mark.parametrize(
    "url, client, content",
    [
        ("resetmypassword/form", "viewer_client", "Password Changed"),
        ("resetpassword/form?pk=1", "admin_client", "Password Changed"),
        ("resetpassword/form?pk=1", "viewer_client", "Access is Denied"),
    ],
    ids=["my-viewer", "pk-admin", "pk-viewer"],
)
def test_resetmypasswordview_edit(request, url, client, content):
    resp = request.getfixturevalue(client).post(
        url, data={'password': 'blah', 'conf_password': 'blah'}, follow_redirects=True
    )
    check_content_in_response(content, resp)


def test_resetmypasswordview_read(viewer_client):
    # Tests with viewer as all roles should have access.
    resp = viewer_client.get('resetmypassword/form', follow_redirects=True)
    check_content_in_response('Reset Password Form', resp)


def test_get_myuserinfo(admin_client):
    resp = admin_client.get("users/userinfo/", follow_redirects=True)
    check_content_in_response('Your user information', resp)


def test_edit_myuserinfo(admin_client):
    resp = admin_client.post(
        "userinfoeditview/form",
        data={'first_name': 'new_first_name', 'last_name': 'new_last_name'},
        follow_redirects=True,
    )
    check_content_in_response("User information changed", resp)


@pytest.mark.parametrize(
    "url",
    ["users/add", "users/edit/1", "users/delete/1"],
    ids=["add-user", "edit-user", "delete-user"],
)
def test_views_post_access_denied(viewer_client, url):
    resp = viewer_client.get(url, follow_redirects=True)
    check_content_in_response("Access is Denied", resp)


@pytest.fixture()
def non_exist_username(app):
    username = "fake_username"
    if app.appbuilder.sm.find_user(username):
        app.appbuilder.sm.del_register_user(username)
    yield username
    if app.appbuilder.sm.find_user(username):
        app.appbuilder.sm.del_register_user(username)


def test_create_user(app, admin_client, non_exist_username):
    resp = admin_client.post(
        "users/add",
        data={
            'first_name': 'fake_first_name',
            'last_name': 'fake_last_name',
            'username': non_exist_username,
            'email': 'fake_email@email.com',
            'password': 'test',
            'conf_password': 'test',
        },
        follow_redirects=True,
    )
    check_content_in_response("Added Row", resp)
    assert app.appbuilder.sm.find_user(non_exist_username)


@pytest.fixture()
def exist_username(app, exist_role):
    username = "test_edit_user_user"
    app.appbuilder.sm.add_user(
        username,
        "first_name",
        "last_name",
        "email@email.com",
        exist_role,
        password="password",
    )
    yield username
    if app.appbuilder.sm.find_user(username):
        app.appbuilder.sm.del_register_user(username)


def test_edit_user(app, admin_client, exist_username):
    user = app.appbuilder.sm.find_user(exist_username)
    resp = admin_client.post(
        f"users/edit/{user.id}",
        data={"first_name": "new_first_name"},
        follow_redirects=True,
    )
    check_content_in_response("new_first_name", resp)


def test_delete_user(app, admin_client, exist_username):
    user = app.appbuilder.sm.find_user(exist_username)
    resp = admin_client.post(
        f"users/delete/{user.id}",
        follow_redirects=True,
    )
    check_content_in_response("Deleted Row", resp)


def test_home_filter_tags(admin_client):
    with admin_client:
        admin_client.get('home?tags=example&tags=data', follow_redirects=True)
        assert 'example,data' == flask.session[FILTER_TAGS_COOKIE]

        admin_client.get('home?reset_tags', follow_redirects=True)
        assert flask.session[FILTER_TAGS_COOKIE] is None


def test_home_status_filter_cookie(admin_client):
    with admin_client:
        admin_client.get('home', follow_redirects=True)
        assert 'all' == flask.session[FILTER_STATUS_COOKIE]

        admin_client.get('home?status=active', follow_redirects=True)
        assert 'active' == flask.session[FILTER_STATUS_COOKIE]

        admin_client.get('home?status=paused', follow_redirects=True)
        assert 'paused' == flask.session[FILTER_STATUS_COOKIE]

        admin_client.get('home?status=all', follow_redirects=True)
        assert 'all' == flask.session[FILTER_STATUS_COOKIE]


@conf_vars({("webserver", "show_recent_stats_for_completed_runs"): "False"})
def test_task_stats_only_noncompleted(admin_client):
    resp = admin_client.post('task_stats', follow_redirects=True)
    assert resp.status_code == 200


@conf_vars({('webserver', 'instance_name'): 'Site Title Test'})
def test_page_instance_name(admin_client):
    resp = admin_client.get('home', follow_redirects=True)
    check_content_in_response('Site Title Test', resp)


def test_page_instance_name_xss_prevention(admin_client):
    xss_string = "<script>alert('Give me your credit card number')</script>"
    with conf_vars({('webserver', 'instance_name'): xss_string}):
        resp = admin_client.get('home', follow_redirects=True)
        escaped_xss_string = "&lt;script&gt;alert(&#39;Give me your credit card number&#39;)&lt;/script&gt;"
        check_content_in_response(escaped_xss_string, resp)
        check_content_not_in_response(xss_string, resp)
