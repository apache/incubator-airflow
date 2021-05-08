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
import html
import json
import logging.config
import os
import re
import shutil
import sys
import tempfile
import unittest
import urllib
from contextlib import contextmanager
from datetime import datetime as dt, timedelta
from typing import Any, Dict, Generator, List, NamedTuple
from unittest import mock
from unittest.mock import PropertyMock
from urllib.parse import quote_plus

import jinja2
from flask import session as flask_session, template_rendered
from parameterized import parameterized

from airflow import models, settings, version
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
from airflow.configuration import conf
from airflow.executors.celery_executor import CeleryExecutor
from airflow.jobs.base_job import BaseJob
from airflow.models import DAG, DagRun, TaskInstance
from airflow.operators.dummy import DummyOperator
from airflow.security import permissions
from airflow.ti_deps.dependencies_states import QUEUEABLE_STATES, RUNNABLE_STATES
from airflow.utils import dates, timezone
from airflow.utils.log.logging_mixin import ExternalLoggingMixin
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.timezone import datetime
from airflow.utils.types import DagRunType
from airflow.www import app as application
from airflow.www.extensions import init_views
from airflow.www.extensions.init_appbuilder_links import init_appbuilder_links
from tests.test_utils import api_connexion_utils
from tests.test_utils.asserts import assert_queries_count
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_runs
from tests.test_utils.decorators import dont_initialize_flask_app_submodules


class TemplateWithContext(NamedTuple):
    template: jinja2.environment.Template
    context: Dict[str, Any]

    @property
    def name(self):
        return self.template.name

    @property
    def local_context(self):
        """Returns context without global arguments"""
        result = copy.copy(self.context)
        keys_to_delete = [
            # flask.templating._default_template_ctx_processor
            'g',
            'request',
            'session',
            # flask_wtf.csrf.CSRFProtect.init_app
            'csrf_token',
            # flask_login.utils._user_context_processor
            'current_user',
            # flask_appbuilder.baseviews.BaseView.render_template
            'appbuilder',
            'base_template',
            # airflow.www.app.py.create_app (inner method - jinja_globals)
            'server_timezone',
            'default_ui_timezone',
            'hostname',
            'navbar_color',
            'log_fetch_delay_sec',
            'log_auto_tailing_offset',
            'log_animation_speed',
            'state_color_mapping',
            'airflow_version',
            'git_version',
            'k8s_or_k8scelery_executor',
            # airflow.www.static_config.configure_manifest_files
            'url_for_asset',
            # airflow.www.views.AirflowBaseView.render_template
            'scheduler_job',
            # airflow.www.views.AirflowBaseView.extra_args
            'macros',
        ]
        for key in keys_to_delete:
            del result[key]

        return result


class TestBase(unittest.TestCase):
    @classmethod
    @dont_initialize_flask_app_submodules(
        skip_all_except=[
            "init_appbuilder",
            "init_appbuilder_views",
            "init_flash_views",
            "init_jinja_globals",
        ]
    )
    def setUpClass(cls):
        settings.configure_orm()
        cls.session = settings.Session
        cls.app = application.create_app(testing=True)
        cls.appbuilder = cls.app.appbuilder  # pylint: disable=no-member
        cls.app.config['WTF_CSRF_ENABLED'] = False
        cls.app.jinja_env.undefined = jinja2.StrictUndefined

    @classmethod
    def tearDownClass(cls) -> None:
        clear_db_runs()
        api_connexion_utils.delete_roles(cls.app)

    def setUp(self):
        self.client = self.app.test_client()
        self.login()

    def login(self, username='test', password='test'):
        with mock.patch('flask_appbuilder.security.manager.check_password_hash') as set_mock:
            set_mock.return_value = True
            if username == 'test' and not self.appbuilder.sm.find_user(username='test'):
                self.appbuilder.sm.add_user(
                    username='test',
                    first_name='test',
                    last_name='test',
                    email='test@fab.org',
                    role=self.appbuilder.sm.find_role('Admin'),
                    password='test',
                )
            if username == 'test_user' and not self.appbuilder.sm.find_user(username='test_user'):
                self.appbuilder.sm.add_user(
                    username='test_user',
                    first_name='test_user',
                    last_name='test_user',
                    email='test_user@fab.org',
                    role=self.appbuilder.sm.find_role('User'),
                    password='test_user',
                )

            if username == 'test_viewer' and not self.appbuilder.sm.find_user(username='test_viewer'):
                self.appbuilder.sm.add_user(
                    username='test_viewer',
                    first_name='test_viewer',
                    last_name='test_viewer',
                    email='test_viewer@fab.org',
                    role=self.appbuilder.sm.find_role('Viewer'),
                    password='test_viewer',
                )

            return self.client.post('/login/', data={"username": username, "password": password})

    def logout(self):
        return self.client.get('/logout/')

    @contextmanager
    def capture_templates(self) -> Generator[List[TemplateWithContext], None, None]:
        recorded = []

        def record(sender, template, context, **extra):  # pylint: disable=unused-argument
            recorded.append(TemplateWithContext(template, context))

        template_rendered.connect(record, self.app)  # type: ignore
        try:
            yield recorded
        finally:
            template_rendered.disconnect(record, self.app)  # type: ignore

        assert recorded, "Failed to catch the templates"

    @classmethod
    def clear_table(cls, model):
        with create_session() as session:
            session.query(model).delete()

    def check_content_in_response(self, text, resp, resp_code=200):
        resp_html = resp.data.decode('utf-8')
        assert resp_code == resp.status_code
        if isinstance(text, list):
            for line in text:
                assert line in resp_html
        else:
            assert text in resp_html

    def check_content_not_in_response(self, text, resp, resp_code=200):
        resp_html = resp.data.decode('utf-8')
        assert resp_code == resp.status_code
        if isinstance(text, list):
            for line in text:
                assert line not in resp_html
        else:
            assert text not in resp_html

    @staticmethod
    def percent_encode(obj):
        return urllib.parse.quote_plus(str(obj))

    def create_user_and_login(self, username, role_name, perms):
        self.logout()
        api_connexion_utils.create_user(
            self.app,
            username=username,
            role_name=role_name,
            permissions=perms,
        )
        self.login(username=username, password=username)


class TestAirflowBaseViews(TestBase):
    EXAMPLE_DAG_DEFAULT_DATE = dates.days_ago(2)

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        models.DagBag(include_examples=True).sync_to_db()
        cls.dagbag = models.DagBag(include_examples=True, read_dags_from_db=True)
        cls.app.dag_bag = cls.dagbag
        with cls.app.app_context():
            init_views.init_api_connexion(cls.app)
            init_views.init_plugins(cls.app)
            init_appbuilder_links(cls.app)

    def setUp(self):
        super().setUp()
        self.logout()
        self.login()
        clear_db_runs()
        self.prepare_dagruns()

    def _delete_role_if_exists(self, role_name):
        if self.appbuilder.sm.find_role(role_name):
            self.appbuilder.sm.delete_role(role_name)

    def prepare_dagruns(self):
        self.bash_dag = self.dagbag.get_dag('example_bash_operator')
        self.sub_dag = self.dagbag.get_dag('example_subdag_operator')
        self.xcom_dag = self.dagbag.get_dag('example_xcom')

        self.bash_dagrun = self.bash_dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            start_date=timezone.utcnow(),
            state=State.RUNNING,
        )

        self.sub_dagrun = self.sub_dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            start_date=timezone.utcnow(),
            state=State.RUNNING,
        )

        self.xcom_dagrun = self.xcom_dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            start_date=timezone.utcnow(),
            state=State.RUNNING,
        )

    def test_index(self):
        with assert_queries_count(44):
            resp = self.client.get('/', follow_redirects=True)
        self.check_content_in_response('DAGs', resp)

    def test_doc_urls(self):
        resp = self.client.get('/', follow_redirects=True)
        if "dev" in version.version:
            airflow_doc_site = (
                "http://apache-airflow-docs.s3-website.eu-central-1.amazonaws.com/docs/apache-airflow/"
            )
        else:
            airflow_doc_site = f'https://airflow.apache.org/docs/apache-airflow/{version.version}'

        self.check_content_in_response(airflow_doc_site, resp)
        self.check_content_in_response("/api/v1/ui", resp)

    def test_health(self):

        # case-1: healthy scheduler status
        last_scheduler_heartbeat_for_testing_1 = timezone.utcnow()
        self.session.add(
            BaseJob(
                job_type='SchedulerJob',
                state='running',
                latest_heartbeat=last_scheduler_heartbeat_for_testing_1,
            )
        )
        self.session.commit()

        resp_json = json.loads(self.client.get('health', follow_redirects=True).data.decode('utf-8'))

        assert 'healthy' == resp_json['metadatabase']['status']
        assert 'healthy' == resp_json['scheduler']['status']
        assert (
            last_scheduler_heartbeat_for_testing_1.isoformat()
            == resp_json['scheduler']['latest_scheduler_heartbeat']
        )

        self.session.query(BaseJob).filter(
            BaseJob.job_type == 'SchedulerJob',
            BaseJob.state == 'running',
            BaseJob.latest_heartbeat == last_scheduler_heartbeat_for_testing_1,
        ).delete()
        self.session.commit()

        # case-2: unhealthy scheduler status - scenario 1 (SchedulerJob is running too slowly)
        last_scheduler_heartbeat_for_testing_2 = timezone.utcnow() - timedelta(minutes=1)
        (
            self.session.query(BaseJob)
            .filter(BaseJob.job_type == 'SchedulerJob')
            .update({'latest_heartbeat': last_scheduler_heartbeat_for_testing_2 - timedelta(seconds=1)})
        )
        self.session.add(
            BaseJob(
                job_type='SchedulerJob',
                state='running',
                latest_heartbeat=last_scheduler_heartbeat_for_testing_2,
            )
        )
        self.session.commit()

        resp_json = json.loads(self.client.get('health', follow_redirects=True).data.decode('utf-8'))

        assert 'healthy' == resp_json['metadatabase']['status']
        assert 'unhealthy' == resp_json['scheduler']['status']
        assert (
            last_scheduler_heartbeat_for_testing_2.isoformat()
            == resp_json['scheduler']['latest_scheduler_heartbeat']
        )

        self.session.query(BaseJob).filter(
            BaseJob.job_type == 'SchedulerJob',
            BaseJob.state == 'running',
            BaseJob.latest_heartbeat == last_scheduler_heartbeat_for_testing_2,
        ).delete()
        self.session.commit()

        # case-3: unhealthy scheduler status - scenario 2 (no running SchedulerJob)
        self.session.query(BaseJob).filter(
            BaseJob.job_type == 'SchedulerJob', BaseJob.state == 'running'
        ).delete()
        self.session.commit()

        resp_json = json.loads(self.client.get('health', follow_redirects=True).data.decode('utf-8'))

        assert 'healthy' == resp_json['metadatabase']['status']
        assert 'unhealthy' == resp_json['scheduler']['status']
        assert resp_json['scheduler']['latest_scheduler_heartbeat'] is None

    def test_home(self):
        with self.capture_templates() as templates:
            resp = self.client.get('home', follow_redirects=True)
            self.check_content_in_response('DAGs', resp)
            val_state_color_mapping = (
                'const STATE_COLOR = {"failed": "red", '
                '"null": "lightblue", "queued": "gray", '
                '"removed": "lightgrey", "running": "lime", '
                '"scheduled": "tan", "sensing": "lightseagreen", '
                '"shutdown": "blue", "skipped": "pink", '
                '"success": "green", "up_for_reschedule": "turquoise", '
                '"up_for_retry": "gold", "upstream_failed": "orange"};'
            )
            self.check_content_in_response(val_state_color_mapping, resp)

        assert len(templates) == 1
        assert templates[0].name == 'airflow/dags.html'
        state_color_mapping = State.state_color.copy()
        state_color_mapping["null"] = state_color_mapping.pop(None)
        assert templates[0].local_context['state_color'] == state_color_mapping

    def test_users_list(self):
        resp = self.client.get('users/list', follow_redirects=True)
        self.check_content_in_response('List Users', resp)

    @parameterized.expand(
        [
            ("roles/list", "List Roles"),
            ("roles/show/1", "Show Role"),
        ]
    )
    def test_roles_read(self, path, body_content):
        resp = self.client.get(path, follow_redirects=True)
        self.check_content_in_response(body_content, resp)

    def test_roles_read_unauthorized(self):
        self.logout()
        self.login(username='test_viewer', password='test_viewer')
        resp = self.client.get("roles/list", follow_redirects=True)
        self.check_content_in_response('Access is Denied', resp)

    def test_roles_create(self):
        role_name = "test_roles_create_role"
        self._delete_role_if_exists(role_name)
        if self.appbuilder.sm.find_role(role_name):
            self.appbuilder.sm.delete_role(role_name)
        self.client.post("roles/add", data={'name': role_name}, follow_redirects=True)
        assert self.appbuilder.sm.find_role(role_name)
        self._delete_role_if_exists(role_name)

    def test_roles_create_unauthorized(self):
        self.logout()
        self.login(username='test_viewer', password='test_viewer')
        role_name = "test_roles_create_role"
        resp = self.client.post("roles/add", data={'name': role_name}, follow_redirects=True)
        self.check_content_in_response('Access is Denied', resp)
        assert self.appbuilder.sm.find_role(role_name) is None

    def test_roles_edit(self):
        role_name = "test_roles_create_role"
        role = self.appbuilder.sm.add_role(role_name)
        updated_role_name = "test_roles_create_role_new"
        self._delete_role_if_exists(updated_role_name)
        self.client.post(f"roles/edit/{role.id}", data={'name': updated_role_name}, follow_redirects=True)
        updated_role = self.appbuilder.sm.find_role(updated_role_name)
        assert role.id == updated_role.id

        self._delete_role_if_exists(updated_role_name)

    def test_roles_edit_unauthorized(self):
        self.logout()
        self.login(username='test_viewer', password='test_viewer')

        role_name = "test_roles_create_role"
        role = self.appbuilder.sm.add_role(role_name)

        updated_role_name = "test_roles_create_role_new"
        resp = self.client.post(
            f"roles/edit/{role.id}", data={'name': updated_role_name}, follow_redirects=True
        )

        self.check_content_in_response('Access is Denied', resp)
        assert self.appbuilder.sm.find_role(role_name)
        assert self.appbuilder.sm.find_role(updated_role_name) is None

        self._delete_role_if_exists(role_name)

    def test_roles_delete(self):
        role_name = "test_roles_create_role"
        role = self.appbuilder.sm.add_role(role_name)

        self.client.post(f"roles/delete/{role.id}", follow_redirects=True)
        assert self.appbuilder.sm.find_role(role_name) is None
        self._delete_role_if_exists(role_name)

    def test_roles_delete_unauthorized(self):
        self.logout()
        self.login(username='test_viewer', password='test_viewer')

        role_name = "test_roles_create_role"
        role = self.appbuilder.sm.add_role(role_name)

        resp = self.client.post(f"roles/delete/{role.id}", follow_redirects=True)
        self.check_content_in_response('Access is Denied', resp)
        assert self.appbuilder.sm.find_role(role_name)

        self._delete_role_if_exists(role_name)

    def test_userstatschart_view(self):
        resp = self.client.get('userstatschartview/chart/', follow_redirects=True)
        self.check_content_in_response('User Statistics', resp)

    def test_userstatschart_view_unauthorized(self):
        self.logout()
        self.login(username='test_viewer', password='test_viewer')
        resp = self.client.get('userstatschartview/chart/', follow_redirects=True)
        self.check_content_in_response('Access is Denied', resp)

    def test_permissions_list(self):
        resp = self.client.get('permissions/list/', follow_redirects=True)
        self.check_content_in_response('List Base Permissions', resp)

    def test_permissions_list_unauthorized(self):
        self.logout()
        self.login(username='test_viewer', password='test_viewer')
        resp = self.client.get('permissions/list/', follow_redirects=True)
        self.check_content_in_response('Access is Denied', resp)

    def test_viewmenus_list(self):
        resp = self.client.get('viewmenus/list/', follow_redirects=True)
        self.check_content_in_response('List View Menus', resp)

    def test_viewmenus_list_unauthorized(self):
        self.logout()
        self.login(username='test_viewer', password='test_viewer')
        resp = self.client.get('viewmenus/list/', follow_redirects=True)
        self.check_content_in_response('Access is Denied', resp)

    def test_permissionsviews_list(self):
        resp = self.client.get('permissionviews/list/', follow_redirects=True)
        self.check_content_in_response('List Permissions on Views/Menus', resp)

    def test_permissionsviews_list_unauthorized(self):
        self.logout()
        self.login(username='test_viewer', password='test_viewer')
        resp = self.client.get('permissionviews/list/', follow_redirects=True)
        self.check_content_in_response('Access is Denied', resp)

    def test_resetmypasswordview_read(self):
        self.logout()
        self.login(username='test_viewer', password='test_viewer')
        # Tests with viewer as all roles should have access.
        resp = self.client.get('resetmypassword/form', follow_redirects=True)
        self.check_content_in_response('Reset Password Form', resp)

    def test_resetmypasswordview_edit(self):
        self.logout()
        self.login(username='test_viewer', password='test_viewer')
        # Tests with viewer as all roles should have access.
        resp = self.client.post(
            'resetmypassword/form', data={'password': 'blah', 'conf_password': 'blah'}, follow_redirects=True
        )
        self.check_content_in_response('Password Changed', resp)

    def test_resetpasswordview_read(self):
        resp = self.client.get('resetpassword/form?pk=1', follow_redirects=True)
        self.check_content_in_response('Reset Password Form', resp)

    def test_resetpasswordview_read_unauthorized(self):
        self.logout()
        self.login(username='test_viewer', password='test_viewer')
        resp = self.client.get('resetpassword/form?pk=1', follow_redirects=True)
        self.check_content_in_response('Access is Denied', resp)

    def test_resetpasswordview_edit(self):
        resp = self.client.post(
            'resetpassword/form?pk=1',
            data={'password': 'blah', 'conf_password': 'blah'},
            follow_redirects=True,
        )
        self.check_content_in_response('Password Changed', resp)

    def test_resetpasswordview_edit_unauthorized(self):
        self.logout()
        self.login(username='test_viewer', password='test_viewer')
        # Tests with viewer as all roles should have access.
        resp = self.client.post(
            'resetpassword/form?pk=1',
            data={'password': 'blah', 'conf_password': 'blah'},
            follow_redirects=True,
        )
        self.check_content_in_response('Access is Denied', resp)

    def test_get_myuserinfo(self):
        resp = self.client.get("users/userinfo/", follow_redirects=True)
        self.check_content_in_response('Your user information', resp)

    def test_edit_myuserinfo(self):
        resp = self.client.post(
            "userinfoeditview/form",
            data={'first_name': 'new_first_name', 'last_name': 'new_last_name'},
            follow_redirects=True,
        )
        self.check_content_in_response("User information changed", resp)

    def test_create_user(self):
        resp = self.client.post(
            "users/add",
            data={
                'first_name': 'fake_first_name',
                'last_name': 'fake_last_name',
                'username': 'fake_username',
                'email': 'fake_email@email.com',
                'password': 'test',
                'conf_password': 'test',
            },
            follow_redirects=True,
        )
        self.check_content_in_response("Added Row", resp)
        new_user = self.appbuilder.sm.find_user("fake_username")
        self.appbuilder.sm.del_register_user(new_user)

    def test_create_user_unauthorized(self):
        self.logout()
        self.login(username='test_viewer', password='test_viewer')
        resp = self.client.post("users/add", follow_redirects=True)
        self.check_content_in_response("Access is Denied", resp)

    def test_read_users(self):
        resp = self.client.get("users/list/", follow_redirects=True)
        self.check_content_in_response("List Users", resp)

    def test_read_users_unauthorized(self):
        self.logout()
        self.login(username='test_viewer', password='test_viewer')
        resp = self.client.get("users/list/", follow_redirects=True)
        self.check_content_in_response("Access is Denied", resp)

    def test_edit_user(self):
        username = "test_edit_user_user"
        self._delete_user_if_exists(username)
        dag_tester_role = self.appbuilder.sm.add_role('dag_acl_tester')
        new_user = self.appbuilder.sm.add_user(
            "test_edit_user_user",
            "first_name",
            "last_name",
            "email@email.com",
            dag_tester_role,
            password="password",
        )
        resp = self.client.post(
            f"users/edit/{new_user.id}",
            data={"first_name": "new_first_name"},
            follow_redirects=True,
        )
        self.check_content_in_response("new_first_name", resp)
        self._delete_user_if_exists(username)

    def test_edit_users_unauthorized(self):
        self.logout()
        self.login(username='test_viewer', password='test_viewer')
        resp = self.client.post("users/edit/1", follow_redirects=True)
        self.check_content_in_response("Access is Denied", resp)

    def _delete_user_if_exists(self, username):
        user = self.appbuilder.sm.find_user(username)
        if user:
            self.appbuilder.sm.del_register_user(user)

    def test_delete_user(self):
        username = "test_edit_user_user"
        self._delete_user_if_exists(username)
        dag_tester_role = self.appbuilder.sm.add_role('dag_acl_tester')
        new_user = self.appbuilder.sm.add_user(
            "test_edit_user_user",
            "first_name",
            "last_name",
            "email@email.com",
            dag_tester_role,
            password="password",
        )
        resp = self.client.post(
            f"users/delete/{new_user.id}",
            follow_redirects=True,
        )
        self.check_content_in_response("Deleted Row", resp)
        self._delete_user_if_exists(username)

    def test_delete_users_unauthorized(self):
        self.logout()
        self.login(username='test_viewer', password='test_viewer')
        dag_tester_role = self.appbuilder.sm.add_role('dag_acl_tester')
        resp = self.client.post(f"users/delete/{dag_tester_role.id}", follow_redirects=True)
        self.check_content_in_response("Access is Denied", resp)

    def test_home_filter_tags(self):
        from airflow.www.views import FILTER_TAGS_COOKIE

        with self.client:
            self.client.get('home?tags=example&tags=data', follow_redirects=True)
            assert 'example,data' == flask_session[FILTER_TAGS_COOKIE]

            self.client.get('home?reset_tags', follow_redirects=True)
            assert flask_session[FILTER_TAGS_COOKIE] is None

    def test_home_status_filter_cookie(self):
        from airflow.www.views import FILTER_STATUS_COOKIE

        with self.client:
            self.client.get('home', follow_redirects=True)
            assert 'all' == flask_session[FILTER_STATUS_COOKIE]

            self.client.get('home?status=active', follow_redirects=True)
            assert 'active' == flask_session[FILTER_STATUS_COOKIE]

            self.client.get('home?status=paused', follow_redirects=True)
            assert 'paused' == flask_session[FILTER_STATUS_COOKIE]

            self.client.get('home?status=all', follow_redirects=True)
            assert 'all' == flask_session[FILTER_STATUS_COOKIE]

    def test_task(self):
        url = 'task?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'.format(
            self.percent_encode(self.EXAMPLE_DAG_DEFAULT_DATE)
        )
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('Task Instance Details', resp)

    def test_xcom(self):
        url = 'xcom?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'.format(
            self.percent_encode(self.EXAMPLE_DAG_DEFAULT_DATE)
        )
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('XCom', resp)

    def test_xcom_list_view_title(self):
        resp = self.client.get('xcom/list', follow_redirects=True)
        self.check_content_in_response('List XComs', resp)

    def test_rendered_template(self):
        url = 'rendered-templates?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'.format(
            self.percent_encode(self.EXAMPLE_DAG_DEFAULT_DATE)
        )
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('Rendered Template', resp)

    def test_rendered_k8s(self):
        url = 'rendered-k8s?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'.format(
            self.percent_encode(self.EXAMPLE_DAG_DEFAULT_DATE)
        )
        with mock.patch.object(settings, "IS_K8S_OR_K8SCELERY_EXECUTOR", True):
            resp = self.client.get(url, follow_redirects=True)
            self.check_content_in_response('K8s Pod Spec', resp)

    @conf_vars({('core', 'executor'): 'LocalExecutor'})
    def test_rendered_k8s_without_k8s(self):
        url = 'rendered-k8s?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'.format(
            self.percent_encode(self.EXAMPLE_DAG_DEFAULT_DATE)
        )
        resp = self.client.get(url, follow_redirects=True)
        assert 404 == resp.status_code

    def test_blocked(self):
        url = 'blocked'
        resp = self.client.post(url, follow_redirects=True)
        assert 200 == resp.status_code

    def test_dag_stats(self):
        resp = self.client.post('dag_stats', follow_redirects=True)
        assert resp.status_code == 200

    def test_task_stats(self):
        resp = self.client.post('task_stats', follow_redirects=True)
        assert resp.status_code == 200
        assert set(list(resp.json.items())[0][1][0].keys()) == {'state', 'count'}

    @conf_vars({("webserver", "show_recent_stats_for_completed_runs"): "False"})
    def test_task_stats_only_noncompleted(self):
        resp = self.client.post('task_stats', follow_redirects=True)
        assert resp.status_code == 200

    def test_dag_details(self):
        url = 'dag_details?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('DAG Details', resp)

    @parameterized.expand(["graph", "tree", "calendar", "dag_details"])
    def test_view_uses_existing_dagbag(self, endpoint):
        """
        Test that Graph, Tree, Calendar & Dag Details View uses the DagBag already created in views.py
        instead of creating a new one.
        """
        url = f'{endpoint}?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    @parameterized.expand(
        [
            ("hello\nworld", r'\"conf\":{\"abc\":\"hello\\nworld\"}'),
            ("hello'world", r'\"conf\":{\"abc\":\"hello\\u0027world\"}'),
            ("<script>", r'\"conf\":{\"abc\":\"\\u003cscript\\u003e\"}'),
            ("\"", r'\"conf\":{\"abc\":\"\\\"\"}'),
        ]
    )
    def test_escape_in_tree_view(self, test_str, expected_text):
        dag = self.dagbag.get_dag('test_tree_view')
        dag.create_dagrun(
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            start_date=timezone.utcnow(),
            run_type=DagRunType.MANUAL,
            state=State.RUNNING,
            conf={"abc": test_str},
        )

        url = 'tree?dag_id=test_tree_view'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response(expected_text, resp)

    def test_dag_details_trigger_origin_tree_view(self):
        dag = self.dagbag.get_dag('test_tree_view')
        dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            start_date=timezone.utcnow(),
            state=State.RUNNING,
        )

        url = 'dag_details?dag_id=test_tree_view'
        resp = self.client.get(url, follow_redirects=True)
        params = {'dag_id': 'test_tree_view', 'origin': '/tree?dag_id=test_tree_view'}
        href = f"/trigger?{html.escape(urllib.parse.urlencode(params))}"
        self.check_content_in_response(href, resp)

    def test_dag_details_trigger_origin_graph_view(self):
        dag = self.dagbag.get_dag('test_graph_view')
        dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            start_date=timezone.utcnow(),
            state=State.RUNNING,
        )

        url = 'dag_details?dag_id=test_graph_view'
        resp = self.client.get(url, follow_redirects=True)
        params = {'dag_id': 'test_graph_view', 'origin': '/graph?dag_id=test_graph_view'}
        href = f"/trigger?{html.escape(urllib.parse.urlencode(params))}"
        self.check_content_in_response(href, resp)

    def test_dag_details_subdag(self):
        url = 'dag_details?dag_id=example_subdag_operator.section-1'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('DAG Details', resp)

    def test_graph(self):
        url = 'graph?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('runme_1', resp)

    def test_dag_dependencies(self):
        url = 'dag-dependencies'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('child_task1', resp)
        self.check_content_in_response('test_trigger_dagrun', resp)

    def test_last_dagruns(self):
        resp = self.client.post('last_dagruns', follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_last_dagruns_success_when_selecting_dags(self):
        resp = self.client.post(
            'last_dagruns', data={'dag_ids': ['example_subdag_operator']}, follow_redirects=True
        )
        assert resp.status_code == 200
        stats = json.loads(resp.data.decode('utf-8'))
        assert 'example_bash_operator' not in stats
        assert 'example_subdag_operator' in stats

        # Multiple
        resp = self.client.post(
            'last_dagruns',
            data={'dag_ids': ['example_subdag_operator', 'example_bash_operator']},
            follow_redirects=True,
        )
        stats = json.loads(resp.data.decode('utf-8'))
        assert 'example_bash_operator' in stats
        assert 'example_subdag_operator' in stats
        self.check_content_not_in_response('example_xcom', resp)

    def test_tree(self):
        url = 'tree?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('runme_1', resp)

    def test_tree_subdag(self):
        url = 'tree?dag_id=example_subdag_operator.section-1'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('section-1-task-1', resp)

    def test_calendar(self):
        url = 'calendar?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        dag_run_date = self.bash_dagrun.execution_date.date().isoformat()
        expected_data = [{'date': dag_run_date, 'state': State.RUNNING, 'count': 1}]
        expected_data_json_escaped = json.dumps(expected_data).replace('"', '\\"').replace(' ', '')
        self.check_content_in_response(expected_data_json_escaped, resp)

    def test_duration(self):
        url = 'duration?days=30&dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_duration_missing(self):
        url = 'duration?days=30&dag_id=missing_dag'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('seems to be missing', resp)

    def test_tries(self):
        url = 'tries?days=30&dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_landing_times(self):
        url = 'landing_times?days=30&dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_gantt(self):
        url = 'gantt?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_code(self):
        url = 'code?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_not_in_response('Failed to load file', resp)
        self.check_content_in_response('example_bash_operator', resp)

    def test_code_no_file(self):
        url = 'code?dag_id=example_bash_operator'
        mock_open_patch = mock.mock_open(read_data='')
        mock_open_patch.side_effect = FileNotFoundError
        with mock.patch('builtins.open', mock_open_patch), mock.patch(
            "airflow.models.dagcode.STORE_DAG_CODE", False
        ):
            resp = self.client.get(url, follow_redirects=True)
            self.check_content_in_response('Failed to load file', resp)
            self.check_content_in_response('example_bash_operator', resp)

    @conf_vars({("core", "store_dag_code"): "True"})
    def test_code_from_db(self):
        from airflow.models.dagcode import DagCode

        dag = models.DagBag(include_examples=True).get_dag("example_bash_operator")
        DagCode(dag.fileloc, DagCode._get_code_from_file(dag.fileloc)).sync_to_db()
        url = 'code?dag_id=example_bash_operator'
        resp = self.client.get(url)
        self.check_content_not_in_response('Failed to load file', resp)
        self.check_content_in_response('example_bash_operator', resp)

    @conf_vars({("core", "store_dag_code"): "True"})
    def test_code_from_db_all_example_dags(self):
        from airflow.models.dagcode import DagCode

        dagbag = models.DagBag(include_examples=True)
        for dag in dagbag.dags.values():
            DagCode(dag.fileloc, DagCode._get_code_from_file(dag.fileloc)).sync_to_db()
        url = 'code?dag_id=example_bash_operator'
        resp = self.client.get(url)
        self.check_content_not_in_response('Failed to load file', resp)
        self.check_content_in_response('example_bash_operator', resp)

    def test_paused(self):
        url = 'paused?dag_id=example_bash_operator&is_paused=false'
        resp = self.client.post(url, follow_redirects=True)
        self.check_content_in_response('OK', resp)

    def test_failed(self):
        form = dict(
            task_id="run_this_last",
            dag_id="example_bash_operator",
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            upstream="false",
            downstream="false",
            future="false",
            past="false",
        )
        resp = self.client.post("failed", data=form)
        self.check_content_in_response('Wait a minute', resp)

    def test_failed_dag_never_run(self):
        endpoint = "failed"
        dag_id = "example_bash_operator"
        form = dict(
            task_id="run_this_last",
            dag_id=dag_id,
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            upstream="false",
            downstream="false",
            future="false",
            past="false",
            origin="/graph?dag_id=example_bash_operator",
        )
        clear_db_runs()
        resp = self.client.post(endpoint, data=form, follow_redirects=True)
        self.check_content_in_response(f"Cannot make {endpoint}, seem that dag {dag_id} has never run", resp)

    def test_failed_flash_hint(self):
        form = dict(
            task_id="run_this_last",
            dag_id="example_bash_operator",
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            confirmed="true",
            upstream="false",
            downstream="false",
            future="false",
            past="false",
            origin="/graph?dag_id=example_bash_operator",
        )
        resp = self.client.post("failed", data=form, follow_redirects=True)
        self.check_content_in_response("Marked failed on 1 task instances", resp)

    def test_success(self):
        form = dict(
            task_id="run_this_last",
            dag_id="example_bash_operator",
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            upstream="false",
            downstream="false",
            future="false",
            past="false",
        )
        resp = self.client.post('success', data=form)
        self.check_content_in_response('Wait a minute', resp)

    def test_success_dag_never_run(self):
        endpoint = "success"
        dag_id = "example_bash_operator"
        form = dict(
            task_id="run_this_last",
            dag_id=dag_id,
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            upstream="false",
            downstream="false",
            future="false",
            past="false",
            origin="/graph?dag_id=example_bash_operator",
        )
        clear_db_runs()
        resp = self.client.post('success', data=form, follow_redirects=True)
        self.check_content_in_response(f"Cannot make {endpoint}, seem that dag {dag_id} has never run", resp)

    def test_success_flash_hint(self):
        form = dict(
            task_id="run_this_last",
            dag_id="example_bash_operator",
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            confirmed="true",
            upstream="false",
            downstream="false",
            future="false",
            past="false",
            origin="/graph?dag_id=example_bash_operator",
        )
        resp = self.client.post("success", data=form, follow_redirects=True)
        self.check_content_in_response("Marked success on 1 task instances", resp)

    def test_clear(self):
        form = dict(
            task_id="runme_1",
            dag_id="example_bash_operator",
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            upstream="false",
            downstream="false",
            future="false",
            past="false",
            only_failed="false",
        )
        resp = self.client.post("clear", data=form)
        self.check_content_in_response(['example_bash_operator', 'Wait a minute'], resp)

    def test_run(self):
        form = dict(
            task_id="runme_0",
            dag_id="example_bash_operator",
            ignore_all_deps="false",
            ignore_ti_state="true",
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
        )
        resp = self.client.post('run', data=form)
        self.check_content_in_response('', resp, resp_code=302)

    @mock.patch('airflow.executors.executor_loader.ExecutorLoader.get_default_executor')
    def test_run_with_runnable_states(self, get_default_executor_function):
        executor = CeleryExecutor()
        executor.heartbeat = lambda: True
        get_default_executor_function.return_value = executor

        task_id = 'runme_0'

        for state in RUNNABLE_STATES:
            self.session.query(models.TaskInstance).filter(models.TaskInstance.task_id == task_id).update(
                {'state': state, 'end_date': timezone.utcnow()}
            )
            self.session.commit()

            form = dict(
                task_id=task_id,
                dag_id="example_bash_operator",
                ignore_all_deps="false",
                ignore_ti_state="false",
                execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
                origin='/home',
            )
            resp = self.client.post('run', data=form, follow_redirects=True)

            self.check_content_in_response('', resp)

            msg = (
                f"Task is in the &#39;{state}&#39; state which is not a valid state for execution. "
                + "The task must be cleared in order to be run"
            )
            assert not re.search(msg, resp.get_data(as_text=True))

    @mock.patch('airflow.executors.executor_loader.ExecutorLoader.get_default_executor')
    def test_run_with_not_runnable_states(self, get_default_executor_function):
        get_default_executor_function.return_value = CeleryExecutor()

        task_id = 'runme_0'

        for state in QUEUEABLE_STATES:
            assert state not in RUNNABLE_STATES

            self.session.query(models.TaskInstance).filter(models.TaskInstance.task_id == task_id).update(
                {'state': state, 'end_date': timezone.utcnow()}
            )
            self.session.commit()

            form = dict(
                task_id=task_id,
                dag_id="example_bash_operator",
                ignore_all_deps="false",
                ignore_ti_state="false",
                execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
                origin='/home',
            )
            resp = self.client.post('run', data=form, follow_redirects=True)

            self.check_content_in_response('', resp)

            msg = (
                f"Task is in the &#39;{state}&#39; state which is not a valid state for execution. "
                + "The task must be cleared in order to be run"
            )
            assert re.search(msg, resp.get_data(as_text=True))

    def test_refresh(self):
        resp = self.client.post('refresh?dag_id=example_bash_operator')
        self.check_content_in_response('', resp, resp_code=302)

    def test_refresh_all(self):
        with mock.patch.object(self.app.dag_bag, 'collect_dags_from_db') as collect_dags_from_db:
            resp = self.client.post("/refresh_all", follow_redirects=True)
            self.check_content_in_response('', resp)
            collect_dags_from_db.assert_called_once_with()

    def test_delete_dag_button_normal(self):
        resp = self.client.get('/', follow_redirects=True)
        self.check_content_in_response('/delete?dag_id=example_bash_operator', resp)
        self.check_content_in_response("return confirmDeleteDag(this, 'example_bash_operator')", resp)

    def test_delete_dag_button_for_dag_on_scheduler_only(self):
        # Test for JIRA AIRFLOW-3233 (PR 4069):
        # The delete-dag URL should be generated correctly for DAGs
        # that exist on the scheduler (DB) but not the webserver DagBag

        dag_id = 'example_bash_operator'
        test_dag_id = "non_existent_dag"

        DM = models.DagModel  # pylint: disable=invalid-name
        dag_query = self.session.query(DM).filter(DM.dag_id == dag_id)
        dag_query.first().tags = []  # To avoid "FOREIGN KEY constraint" error
        self.session.commit()

        dag_query.update({'dag_id': test_dag_id})
        self.session.commit()

        resp = self.client.get('/', follow_redirects=True)
        self.check_content_in_response(f'/delete?dag_id={test_dag_id}', resp)
        self.check_content_in_response(f"return confirmDeleteDag(this, '{test_dag_id}')", resp)

        self.session.query(DM).filter(DM.dag_id == test_dag_id).update({'dag_id': dag_id})
        self.session.commit()

    @parameterized.expand(["graph", "tree"])
    def test_show_external_log_redirect_link_with_local_log_handler(self, endpoint):
        """Do not show external links if log handler is local."""
        url = f'{endpoint}?dag_id=example_bash_operator'
        with self.capture_templates() as templates:
            self.client.get(url, follow_redirects=True)
            ctx = templates[0].local_context
            assert not ctx['show_external_log_redirect']
            assert ctx['external_log_name'] is None

    @parameterized.expand(["graph", "tree"])
    @mock.patch('airflow.utils.log.log_reader.TaskLogReader.log_handler', new_callable=PropertyMock)
    def test_show_external_log_redirect_link_with_external_log_handler(self, endpoint, mock_log_handler):
        """Show external links if log handler is external."""

        class ExternalHandler(ExternalLoggingMixin):
            LOG_NAME = 'ExternalLog'

            @property
            def log_name(self):
                return self.LOG_NAME

        mock_log_handler.return_value = ExternalHandler()

        url = f'{endpoint}?dag_id=example_bash_operator'
        with self.capture_templates() as templates:
            self.client.get(url, follow_redirects=True)
            ctx = templates[0].local_context
            assert ctx['show_external_log_redirect']
            assert ctx['external_log_name'] == ExternalHandler.LOG_NAME

    def test_page_instance_name(self):
        with conf_vars({('webserver', 'instance_name'): 'Site Title Test'}):
            resp = self.client.get('home', follow_redirects=True)
            self.check_content_in_response('Site Title Test', resp)

    def test_page_instance_name_xss_prevention(self):
        xss_string = "<script>alert('Give me your credit card number')</script>"
        with conf_vars({('webserver', 'instance_name'): xss_string}):
            resp = self.client.get('home', follow_redirects=True)
            escaped_xss_string = (
                "&lt;script&gt;alert(&#39;Give me your credit card number&#39;)&lt;/script&gt;"
            )
            self.check_content_in_response(escaped_xss_string, resp)
            self.check_content_not_in_response(xss_string, resp)


class TestLogView(TestBase):
    DAG_ID = 'dag_for_testing_log_view'
    DAG_ID_REMOVED = 'removed_dag_for_testing_log_view'
    TASK_ID = 'task_for_testing_log_view'
    DEFAULT_DATE = timezone.datetime(2017, 9, 1)
    ENDPOINT = f'log?dag_id={DAG_ID}&task_id={TASK_ID}&execution_date={DEFAULT_DATE}'

    @classmethod
    @dont_initialize_flask_app_submodules(
        skip_all_except=["init_appbuilder", "init_jinja_globals", "init_appbuilder_views"]
    )
    def setUpClass(cls):
        # Make sure that the configure_logging is not cached
        cls.old_modules = dict(sys.modules)

        # Create a custom logging configuration
        logging_config = copy.deepcopy(DEFAULT_LOGGING_CONFIG)
        current_dir = os.path.dirname(os.path.abspath(__file__))
        logging_config['handlers']['task']['base_log_folder'] = os.path.normpath(
            os.path.join(current_dir, 'test_logs')
        )

        logging_config['handlers']['task'][
            'filename_template'
        ] = '{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts | replace(":", ".") }}/{{ try_number }}.log'

        # Write the custom logging configuration to a file
        cls.settings_folder = tempfile.mkdtemp()
        settings_file = os.path.join(cls.settings_folder, "airflow_local_settings.py")
        new_logging_file = f"LOGGING_CONFIG = {logging_config}"
        with open(settings_file, 'w') as handle:
            handle.writelines(new_logging_file)
        sys.path.append(cls.settings_folder)

        with conf_vars({('logging', 'logging_config_class'): 'airflow_local_settings.LOGGING_CONFIG'}):
            cls.app = application.create_app(testing=True)

    def setUp(self):
        with conf_vars({('logging', 'logging_config_class'): 'airflow_local_settings.LOGGING_CONFIG'}):
            self.appbuilder = self.app.appbuilder  # pylint: disable=no-member
            self.app.config['WTF_CSRF_ENABLED'] = False
            self.client = self.app.test_client()
            settings.configure_orm()
            self.login()

            dagbag = self.app.dag_bag
            dagbag.dags.pop(self.DAG_ID, None)
            dagbag.dags.pop(self.DAG_ID_REMOVED, None)
            dag = DAG(self.DAG_ID, start_date=self.DEFAULT_DATE)
            dag_removed = DAG(self.DAG_ID_REMOVED, start_date=self.DEFAULT_DATE)
            dagbag.bag_dag(dag=dag, root_dag=dag)
            dagbag.bag_dag(dag=dag_removed, root_dag=dag_removed)

            # Since we don't want to store the code for the DAG defined in this file
            with mock.patch.object(settings, "STORE_DAG_CODE", False):
                dag.sync_to_db()
                dag_removed.sync_to_db()
                dagbag.sync_to_db()

            with create_session() as session:
                self.ti = TaskInstance(
                    task=DummyOperator(task_id=self.TASK_ID, dag=dag), execution_date=self.DEFAULT_DATE
                )
                self.ti.try_number = 1
                self.ti_removed_dag = TaskInstance(
                    task=DummyOperator(task_id=self.TASK_ID, dag=dag_removed),
                    execution_date=self.DEFAULT_DATE,
                )
                self.ti_removed_dag.try_number = 1

                session.merge(self.ti)
                session.merge(self.ti_removed_dag)

    def tearDown(self):
        self.clear_table(TaskInstance)

        # Remove any new modules imported during the test run. This lets us
        # import the same source files for more than one test.
        for mod in [m for m in sys.modules if m not in self.old_modules]:
            del sys.modules[mod]

        self.logout()
        super().tearDown()

    @classmethod
    def tearDownClass(cls):
        logging.config.dictConfig(DEFAULT_LOGGING_CONFIG)
        sys.path.remove(cls.settings_folder)
        shutil.rmtree(cls.settings_folder)

    @parameterized.expand(
        [
            [State.NONE, 0, 0],
            [State.UP_FOR_RETRY, 2, 2],
            [State.UP_FOR_RESCHEDULE, 0, 1],
            [State.UP_FOR_RESCHEDULE, 1, 2],
            [State.RUNNING, 1, 1],
            [State.SUCCESS, 1, 1],
            [State.FAILED, 3, 3],
        ]
    )
    def test_get_file_task_log(self, state, try_number, expected_num_logs_visible):
        with create_session() as session:
            self.ti.state = state
            self.ti.try_number = try_number
            session.merge(self.ti)

        response = self.client.get(
            TestLogView.ENDPOINT, data=dict(username='test', password='test'), follow_redirects=True
        )

        assert response.status_code == 200
        assert 'Log by attempts' in response.data.decode('utf-8')
        for num in range(1, expected_num_logs_visible + 1):
            assert f'log-group-{num}' in response.data.decode('utf-8')
        assert 'log-group-0' not in response.data.decode('utf-8')
        assert f'log-group-{expected_num_logs_visible + 1}' not in response.data.decode('utf-8')

    def test_get_logs_with_metadata_as_download_file(self):
        url_template = (
            "get_logs_with_metadata?dag_id={}&"
            "task_id={}&execution_date={}&"
            "try_number={}&metadata={}&format=file"
        )
        try_number = 1
        url = url_template.format(
            self.DAG_ID, self.TASK_ID, quote_plus(self.DEFAULT_DATE.isoformat()), try_number, json.dumps({})
        )
        response = self.client.get(url)
        expected_filename = '{}/{}/{}/{}.log'.format(
            self.DAG_ID, self.TASK_ID, self.DEFAULT_DATE.isoformat(), try_number
        )

        content_disposition = response.headers.get('Content-Disposition')
        assert content_disposition.startswith('attachment')
        assert expected_filename in content_disposition
        assert 200 == response.status_code
        assert 'Log for testing.' in response.data.decode('utf-8')

    def test_get_logs_with_metadata_as_download_large_file(self):
        with mock.patch("airflow.utils.log.file_task_handler.FileTaskHandler.read") as read_mock:
            first_return = ([[('default_log', '1st line')]], [{}])
            second_return = ([[('default_log', '2nd line')]], [{'end_of_log': False}])
            third_return = ([[('default_log', '3rd line')]], [{'end_of_log': True}])
            fourth_return = ([[('default_log', 'should never be read')]], [{'end_of_log': True}])
            read_mock.side_effect = [first_return, second_return, third_return, fourth_return]
            url_template = (
                "get_logs_with_metadata?dag_id={}&"
                "task_id={}&execution_date={}&"
                "try_number={}&metadata={}&format=file"
            )
            try_number = 1
            url = url_template.format(
                self.DAG_ID,
                self.TASK_ID,
                quote_plus(self.DEFAULT_DATE.isoformat()),
                try_number,
                json.dumps({}),
            )
            response = self.client.get(url)

            assert '1st line' in response.data.decode('utf-8')
            assert '2nd line' in response.data.decode('utf-8')
            assert '3rd line' in response.data.decode('utf-8')
            assert 'should never be read' not in response.data.decode('utf-8')

    def test_get_logs_with_metadata(self):
        url_template = (
            "get_logs_with_metadata?dag_id={}&task_id={}&execution_date={}&try_number={}&metadata={}"
        )
        response = self.client.get(
            url_template.format(
                self.DAG_ID, self.TASK_ID, quote_plus(self.DEFAULT_DATE.isoformat()), 1, json.dumps({})
            ),
            data=dict(username='test', password='test'),
            follow_redirects=True,
        )

        assert '"message":' in response.data.decode('utf-8')
        assert '"metadata":' in response.data.decode('utf-8')
        assert 'Log for testing.' in response.data.decode('utf-8')
        assert 200 == response.status_code

    def test_get_logs_with_null_metadata(self):
        url_template = (
            "get_logs_with_metadata?dag_id={}&task_id={}&execution_date={}&try_number={}&metadata=null"
        )
        response = self.client.get(
            url_template.format(self.DAG_ID, self.TASK_ID, quote_plus(self.DEFAULT_DATE.isoformat()), 1),
            data=dict(username='test', password='test'),
            follow_redirects=True,
        )

        assert '"message":' in response.data.decode('utf-8')
        assert '"metadata":' in response.data.decode('utf-8')
        assert 'Log for testing.' in response.data.decode('utf-8')
        assert 200 == response.status_code

    @mock.patch("airflow.utils.log.file_task_handler.FileTaskHandler.read")
    def test_get_logs_with_metadata_for_removed_dag(self, mock_read):
        mock_read.return_value = (['airflow log line'], [{'end_of_log': True}])
        url_template = (
            "get_logs_with_metadata?dag_id={}&task_id={}&execution_date={}&try_number={}&metadata={}"
        )
        url = url_template.format(
            self.DAG_ID_REMOVED, self.TASK_ID, quote_plus(self.DEFAULT_DATE.isoformat()), 1, json.dumps({})
        )
        response = self.client.get(url, data=dict(username='test', password='test'), follow_redirects=True)

        assert '"message":' in response.data.decode('utf-8')
        assert '"metadata":' in response.data.decode('utf-8')
        assert 'airflow log line' in response.data.decode('utf-8')
        assert 200 == response.status_code

    def test_get_logs_response_with_ti_equal_to_none(self):
        url_template = (
            "get_logs_with_metadata?dag_id={}&"
            "task_id={}&execution_date={}&"
            "try_number={}&metadata={}&format=file"
        )
        try_number = 1
        url = url_template.format(
            self.DAG_ID,
            'Non_Existing_ID',
            quote_plus(self.DEFAULT_DATE.isoformat()),
            try_number,
            json.dumps({}),
        )
        response = self.client.get(url)
        assert 'message' in response.json
        assert 'error' in response.json
        assert "*** Task instance did not exist in the DB\n" == response.json['message']

    def test_get_logs_with_json_response_format(self):
        url_template = (
            "get_logs_with_metadata?dag_id={}&"
            "task_id={}&execution_date={}&"
            "try_number={}&metadata={}&format=json"
        )
        try_number = 1
        url = url_template.format(
            self.DAG_ID, self.TASK_ID, quote_plus(self.DEFAULT_DATE.isoformat()), try_number, json.dumps({})
        )
        response = self.client.get(url)
        assert 'message' in response.json
        assert 'metadata' in response.json
        assert 'Log for testing.' in response.json['message'][0][1]
        assert 200 == response.status_code

    @mock.patch("airflow.www.views.TaskLogReader")
    def test_get_logs_for_handler_without_read_method(self, mock_log_reader):
        type(mock_log_reader.return_value).supports_read = PropertyMock(return_value=False)

        url_template = (
            "get_logs_with_metadata?dag_id={}&"
            "task_id={}&execution_date={}&"
            "try_number={}&metadata={}&format=json"
        )
        try_number = 1
        url = url_template.format(
            self.DAG_ID, self.TASK_ID, quote_plus(self.DEFAULT_DATE.isoformat()), try_number, json.dumps({})
        )
        response = self.client.get(url)
        assert 200 == response.status_code
        assert 'message' in response.json
        assert 'metadata' in response.json
        assert 'Task log handler does not support read logs.' in response.json['message']

    @parameterized.expand(
        [
            ('inexistent',),
            (TASK_ID,),
        ]
    )
    def test_redirect_to_external_log_with_local_log_handler(self, task_id):
        """Redirect to home if TI does not exist or if log handler is local"""
        url_template = "redirect_to_external_log?dag_id={}&task_id={}&execution_date={}&try_number={}"
        try_number = 1
        url = url_template.format(self.DAG_ID, task_id, quote_plus(self.DEFAULT_DATE.isoformat()), try_number)
        response = self.client.get(url)

        assert 302 == response.status_code
        assert 'http://localhost/home' == response.headers['Location']

    @mock.patch('airflow.utils.log.log_reader.TaskLogReader.log_handler', new_callable=PropertyMock)
    def test_redirect_to_external_log_with_external_log_handler(self, mock_log_handler):
        class ExternalHandler(ExternalLoggingMixin):
            EXTERNAL_URL = 'http://external-service.com'

            def get_external_log_url(self, *args, **kwargs):
                return self.EXTERNAL_URL

        mock_log_handler.return_value = ExternalHandler()

        url_template = "redirect_to_external_log?dag_id={}&task_id={}&execution_date={}&try_number={}"
        try_number = 1
        url = url_template.format(
            self.DAG_ID, self.TASK_ID, quote_plus(self.DEFAULT_DATE.isoformat()), try_number
        )
        response = self.client.get(url)

        assert 302 == response.status_code
        assert ExternalHandler.EXTERNAL_URL == response.headers['Location']


class ViewWithDateTimeAndNumRunsAndDagRunsFormTester:
    DAG_ID = 'dag_for_testing_dt_nr_dr_form'
    DEFAULT_DATE = datetime(2017, 9, 1)
    RUNS_DATA = [
        ('dag_run_for_testing_dt_nr_dr_form_4', datetime(2018, 4, 4)),
        ('dag_run_for_testing_dt_nr_dr_form_3', datetime(2018, 3, 3)),
        ('dag_run_for_testing_dt_nr_dr_form_2', datetime(2018, 2, 2)),
        ('dag_run_for_testing_dt_nr_dr_form_1', datetime(2018, 1, 1)),
    ]

    def __init__(self, test, endpoint):
        self.test = test
        self.endpoint = endpoint
        self.runs = []

    def setup(self):
        dagbag = self.test.app.dag_bag
        dagbag.dags.pop(self.DAG_ID, None)
        dag = DAG(self.DAG_ID, start_date=self.DEFAULT_DATE)
        dagbag.bag_dag(dag=dag, root_dag=dag)
        for run_data in self.RUNS_DATA:
            run = dag.create_dagrun(
                run_id=run_data[0], execution_date=run_data[1], state=State.SUCCESS, external_trigger=True
            )
            self.runs.append(run)

    def teardown(self):
        self.test.session.query(DagRun).filter(DagRun.dag_id == self.DAG_ID).delete()
        self.test.session.commit()
        self.test.session.close()

    def assert_base_date_and_num_runs(self, base_date, num_runs, data):
        self.test.assertNotIn(f'name="base_date" value="{base_date}"', data)
        self.test.assertNotIn('<option selected="" value="{num}">{num}</option>'.format(num=num_runs), data)

    def assert_run_is_not_in_dropdown(self, run, data):
        self.test.assertNotIn(run.execution_date.isoformat(), data)
        self.test.assertNotIn(run.run_id, data)

    def assert_run_is_in_dropdown_not_selected(self, run, data):
        self.test.assertIn(f'<option value="{run.execution_date.isoformat()}">{run.run_id}</option>', data)

    def assert_run_is_selected(self, run, data):
        self.test.assertIn(
            f'<option selected value="{run.execution_date.isoformat()}">{run.run_id}</option>', data
        )

    def test_with_default_parameters(self):
        """
        Tests view with no URL parameter.
        Should show all dag runs in the drop down.
        Should select the latest dag run.
        Should set base date to current date (not asserted)
        """
        response = self.test.client.get(
            self.endpoint, data=dict(username='test', password='test'), follow_redirects=True
        )
        self.test.assertEqual(response.status_code, 200)
        data = response.data.decode('utf-8')
        self.test.assertIn('<label class="sr-only" for="base_date">Base date</label>', data)
        self.test.assertIn('<label class="sr-only" for="num_runs">Number of runs</label>', data)
        self.assert_run_is_selected(self.runs[0], data)
        self.assert_run_is_in_dropdown_not_selected(self.runs[1], data)
        self.assert_run_is_in_dropdown_not_selected(self.runs[2], data)
        self.assert_run_is_in_dropdown_not_selected(self.runs[3], data)

    def test_with_execution_date_parameter_only(self):
        """
        Tests view with execution_date URL parameter.
        Scenario: click link from dag runs view.
        Should only show dag runs older than execution_date in the drop down.
        Should select the particular dag run.
        Should set base date to execution date.
        """
        response = self.test.client.get(
            self.endpoint + f'&execution_date={self.runs[1].execution_date.isoformat()}',
            data=dict(username='test', password='test'),
            follow_redirects=True,
        )
        self.test.assertEqual(response.status_code, 200)
        data = response.data.decode('utf-8')
        self.assert_base_date_and_num_runs(
            self.runs[1].execution_date, conf.getint('webserver', 'default_dag_run_display_number'), data
        )
        self.assert_run_is_not_in_dropdown(self.runs[0], data)
        self.assert_run_is_selected(self.runs[1], data)
        self.assert_run_is_in_dropdown_not_selected(self.runs[2], data)
        self.assert_run_is_in_dropdown_not_selected(self.runs[3], data)

    def test_with_base_date_and_num_runs_parameters_only(self):
        """
        Tests view with base_date and num_runs URL parameters.
        Should only show dag runs older than base_date in the drop down,
        limited to num_runs.
        Should select the latest dag run.
        Should set base date and num runs to submitted values.
        """
        response = self.test.client.get(
            self.endpoint + f'&base_date={self.runs[1].execution_date.isoformat()}&num_runs=2',
            data=dict(username='test', password='test'),
            follow_redirects=True,
        )
        self.test.assertEqual(response.status_code, 200)
        data = response.data.decode('utf-8')
        self.assert_base_date_and_num_runs(self.runs[1].execution_date, 2, data)
        self.assert_run_is_not_in_dropdown(self.runs[0], data)
        self.assert_run_is_selected(self.runs[1], data)
        self.assert_run_is_in_dropdown_not_selected(self.runs[2], data)
        self.assert_run_is_not_in_dropdown(self.runs[3], data)

    def test_with_base_date_and_num_runs_and_execution_date_outside(self):
        """
        Tests view with base_date and num_runs and execution-date URL parameters.
        Scenario: change the base date and num runs and press "Go",
        the selected execution date is outside the new range.
        Should only show dag runs older than base_date in the drop down.
        Should select the latest dag run within the range.
        Should set base date and num runs to submitted values.
        """
        response = self.test.client.get(
            self.endpoint
            + '&base_date={}&num_runs=42&execution_date={}'.format(
                self.runs[1].execution_date.isoformat(), self.runs[0].execution_date.isoformat()
            ),
            data=dict(username='test', password='test'),
            follow_redirects=True,
        )
        self.test.assertEqual(response.status_code, 200)
        data = response.data.decode('utf-8')
        self.assert_base_date_and_num_runs(self.runs[1].execution_date, 42, data)
        self.assert_run_is_not_in_dropdown(self.runs[0], data)
        self.assert_run_is_selected(self.runs[1], data)
        self.assert_run_is_in_dropdown_not_selected(self.runs[2], data)
        self.assert_run_is_in_dropdown_not_selected(self.runs[3], data)

    def test_with_base_date_and_num_runs_and_execution_date_within(self):
        """
        Tests view with base_date and num_runs and execution-date URL parameters.
        Scenario: change the base date and num runs and press "Go",
        the selected execution date is within the new range.
        Should only show dag runs older than base_date in the drop down.
        Should select the dag run with the execution date.
        Should set base date and num runs to submitted values.
        """
        response = self.test.client.get(
            self.endpoint
            + '&base_date={}&num_runs=5&execution_date={}'.format(
                self.runs[2].execution_date.isoformat(), self.runs[3].execution_date.isoformat()
            ),
            data=dict(username='test', password='test'),
            follow_redirects=True,
        )
        self.test.assertEqual(response.status_code, 200)
        data = response.data.decode('utf-8')
        self.assert_base_date_and_num_runs(self.runs[2].execution_date, 5, data)
        self.assert_run_is_not_in_dropdown(self.runs[0], data)
        self.assert_run_is_not_in_dropdown(self.runs[1], data)
        self.assert_run_is_in_dropdown_not_selected(self.runs[2], data)
        self.assert_run_is_selected(self.runs[3], data)


class TestGraphView(TestBase):
    GRAPH_ENDPOINT = '/graph?dag_id={dag_id}'.format(
        dag_id=ViewWithDateTimeAndNumRunsAndDagRunsFormTester.DAG_ID
    )

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

    def setUp(self):
        super().setUp()
        self.tester = ViewWithDateTimeAndNumRunsAndDagRunsFormTester(self, self.GRAPH_ENDPOINT)
        self.tester.setup()

    def tearDown(self):
        self.tester.teardown()
        super().tearDown()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

    def test_dt_nr_dr_form_default_parameters(self):
        self.tester.test_with_default_parameters()

    def test_dt_nr_dr_form_with_execution_date_parameter_only(self):
        self.tester.test_with_execution_date_parameter_only()

    def test_dt_nr_dr_form_with_base_date_and_num_runs_parameters_only(self):
        self.tester.test_with_base_date_and_num_runs_parameters_only()

    def test_dt_nr_dr_form_with_base_date_and_num_runs_and_execution_date_outside(self):
        self.tester.test_with_base_date_and_num_runs_and_execution_date_outside()

    def test_dt_nr_dr_form_with_base_date_and_num_runs_and_execution_date_within(self):
        self.tester.test_with_base_date_and_num_runs_and_execution_date_within()


class TestGanttView(TestBase):
    GANTT_ENDPOINT = '/gantt?dag_id={dag_id}'.format(
        dag_id=ViewWithDateTimeAndNumRunsAndDagRunsFormTester.DAG_ID
    )

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

    def setUp(self):
        super().setUp()
        self.tester = ViewWithDateTimeAndNumRunsAndDagRunsFormTester(self, self.GANTT_ENDPOINT)
        self.tester.setup()

    def tearDown(self):
        self.tester.teardown()
        super().tearDown()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

    def test_dt_nr_dr_form_default_parameters(self):
        self.tester.test_with_default_parameters()

    def test_dt_nr_dr_form_with_execution_date_parameter_only(self):
        self.tester.test_with_execution_date_parameter_only()

    def test_dt_nr_dr_form_with_base_date_and_num_runs_parameters_only(self):
        self.tester.test_with_base_date_and_num_runs_parameters_only()

    def test_dt_nr_dr_form_with_base_date_and_num_runs_and_execution_date_outside(self):
        self.tester.test_with_base_date_and_num_runs_and_execution_date_outside()

    def test_dt_nr_dr_form_with_base_date_and_num_runs_and_execution_date_within(self):
        self.tester.test_with_base_date_and_num_runs_and_execution_date_within()


class TestDagACLView(TestBase):
    """
    Test Airflow DAG acl
    """

    next_year = dt.now().year + 1
    default_date = timezone.datetime(next_year, 6, 1)

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        dagbag = models.DagBag(include_examples=True)
        DAG.bulk_write_to_db(dagbag.dags.values())
        for username in ['all_dag_user', 'dag_read_only', 'dag_faker', 'dag_tester']:
            user = cls.appbuilder.sm.find_user(username=username)
            if user:
                cls.appbuilder.sm.del_register_user(user)

    def prepare_dagruns(self):
        dagbag = models.DagBag(include_examples=True, read_dags_from_db=True)
        self.bash_dag = dagbag.get_dag("example_bash_operator")
        self.sub_dag = dagbag.get_dag("example_subdag_operator")

        self.bash_dagrun = self.bash_dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=self.default_date,
            start_date=timezone.utcnow(),
            state=State.RUNNING,
        )

        self.sub_dagrun = self.sub_dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=self.default_date,
            start_date=timezone.utcnow(),
            state=State.RUNNING,
        )

    def setUp(self):
        super().setUp()
        clear_db_runs()
        self.prepare_dagruns()
        self.logout()

    def login(self, username='dag_tester', password='dag_test'):
        dag_tester_role = self.appbuilder.sm.add_role('dag_acl_tester')
        if username == 'dag_tester' and not self.appbuilder.sm.find_user(username='dag_tester'):
            self.appbuilder.sm.add_user(
                username='dag_tester',
                first_name='dag_test',
                last_name='dag_test',
                email='dag_test@fab.org',
                role=dag_tester_role,
                password='dag_test',
            )

        # create a user without permissions
        dag_no_role = self.appbuilder.sm.add_role('dag_acl_faker')
        if username == 'dag_faker' and not self.appbuilder.sm.find_user(username='dag_faker'):
            self.appbuilder.sm.add_user(
                username='dag_faker',
                first_name='dag_faker',
                last_name='dag_faker',
                email='dag_fake@fab.org',
                role=dag_no_role,
                password='dag_faker',
            )

        # create a user with only read access
        dag_read_only_role = self.appbuilder.sm.add_role('dag_acl_read_only')
        if username == 'dag_read_only' and not self.appbuilder.sm.find_user(username='dag_read_only'):
            self.appbuilder.sm.add_user(
                username='dag_read_only',
                first_name='dag_read_only',
                last_name='dag_read_only',
                email='dag_read_only@fab.org',
                role=dag_read_only_role,
                password='dag_read_only',
            )

        # create a user that has all dag access
        all_dag_role = self.appbuilder.sm.add_role('all_dag_role')
        if username == 'all_dag_user' and not self.appbuilder.sm.find_user(username='all_dag_user'):
            self.appbuilder.sm.add_user(
                username='all_dag_user',
                first_name='all_dag_user',
                last_name='all_dag_user',
                email='all_dag_user@fab.org',
                role=all_dag_role,
                password='all_dag_user',
            )

        return super().login(username, password)

    def add_permission_for_role(self):
        self.logout()
        self.login(username='test', password='test')
        website_permission = self.appbuilder.sm.get_permission(
            permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE
        )

        dag_tester_role = self.appbuilder.sm.find_role('dag_acl_tester')
        edit_perm_on_dag = self.appbuilder.sm.get_permission(
            permissions.ACTION_CAN_EDIT, 'DAG:example_bash_operator'
        )
        self.appbuilder.sm.add_permission_to_role(dag_tester_role, edit_perm_on_dag)
        read_perm_on_dag = self.appbuilder.sm.get_permission(
            permissions.ACTION_CAN_READ, 'DAG:example_bash_operator'
        )
        self.appbuilder.sm.add_permission_to_role(dag_tester_role, read_perm_on_dag)
        self.appbuilder.sm.add_permission_to_role(dag_tester_role, website_permission)

        all_dag_role = self.appbuilder.sm.find_role('all_dag_role')
        edit_perm_on_all_dag = self.appbuilder.sm.get_permission(
            permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG
        )
        self.appbuilder.sm.add_permission_to_role(all_dag_role, edit_perm_on_all_dag)
        read_perm_on_all_dag = self.appbuilder.sm.get_permission(
            permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG
        )
        self.appbuilder.sm.add_permission_to_role(all_dag_role, read_perm_on_all_dag)
        read_perm_on_task_instance = self.appbuilder.sm.get_permission(
            permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE
        )
        self.appbuilder.sm.add_permission_to_role(all_dag_role, read_perm_on_task_instance)
        self.appbuilder.sm.add_permission_to_role(all_dag_role, website_permission)

        role_user = self.appbuilder.sm.find_role('User')
        self.appbuilder.sm.add_permission_to_role(role_user, read_perm_on_all_dag)
        self.appbuilder.sm.add_permission_to_role(role_user, edit_perm_on_all_dag)
        self.appbuilder.sm.add_permission_to_role(role_user, website_permission)

        read_only_perm_on_dag = self.appbuilder.sm.get_permission(
            permissions.ACTION_CAN_READ, 'DAG:example_bash_operator'
        )
        dag_read_only_role = self.appbuilder.sm.find_role('dag_acl_read_only')
        self.appbuilder.sm.add_permission_to_role(dag_read_only_role, read_only_perm_on_dag)
        self.appbuilder.sm.add_permission_to_role(dag_read_only_role, website_permission)

        dag_acl_faker_role = self.appbuilder.sm.find_role('dag_acl_faker')
        self.appbuilder.sm.add_permission_to_role(dag_acl_faker_role, website_permission)

    def test_permission_exist(self):
        self.create_user_and_login(
            username='permission_exist_user',
            role_name='permission_exist_role',
            perms=[
                (permissions.ACTION_CAN_READ, 'DAG:example_bash_operator'),
                (permissions.ACTION_CAN_EDIT, 'DAG:example_bash_operator'),
            ],
        )

        resource = self.appbuilder.sm.get_resource('DAG:example_bash_operator')
        perms = self.appbuilder.sm.get_resource_permissions(resource)
        assert len(perms) == 2

        perms = [str(perm) for perm in perms]
        expected_perms = [
            'can read on DAG:example_bash_operator',
            'can edit on DAG:example_bash_operator',
        ]
        for perm in expected_perms:
            assert perm in perms

    def test_role_permission_associate(self):
        self.create_user_and_login(
            username='role_permission_associate_user',
            role_name='role_permission_associate_role',
            perms=[
                (permissions.ACTION_CAN_EDIT, 'DAG:example_bash_operator'),
                (permissions.ACTION_CAN_READ, 'DAG:example_bash_operator'),
            ],
        )

        test_role = self.appbuilder.sm.find_role('role_permission_associate_role')
        perms = {str(perm) for perm in test_role.permissions}
        assert 'can edit on DAG:example_bash_operator' in perms
        assert 'can read on DAG:example_bash_operator' in perms

    def test_index_success(self):
        self.create_user_and_login(
            username='index_success_user',
            role_name='index_success_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        resp = self.client.get('/', follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_index_failure(self):
        self.logout()
        self.login()
        resp = self.client.get('/', follow_redirects=True)
        # The user can only access/view example_bash_operator dag.
        self.check_content_not_in_response('example_subdag_operator', resp)

    def test_index_for_all_dag_user(self):
        self.create_user_and_login(
            username='index_for_all_dag_user',
            role_name='index_for_all_dag_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        resp = self.client.get('/', follow_redirects=True)
        # The all dag user can access/view all dags.
        self.check_content_in_response('example_subdag_operator', resp)
        self.check_content_in_response('example_bash_operator', resp)

    def test_dag_autocomplete_success(self):
        self.login(username='all_dag_user', password='all_dag_user')
        resp = self.client.get('dagmodel/autocomplete?query=example_bash', follow_redirects=False)
        self.check_content_in_response('example_bash_operator', resp)
        self.check_content_not_in_response('example_subdag_operator', resp)

    def test_dag_stats_success(self):
        self.create_user_and_login(
            username='dag_stats_success_user',
            role_name='dag_stats_success_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        resp = self.client.post('dag_stats', follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)
        assert set(list(resp.json.items())[0][1][0].keys()) == {'state', 'count'}

    def test_dag_stats_failure(self):
        self.logout()
        self.login()
        resp = self.client.post('dag_stats', follow_redirects=True)
        self.check_content_not_in_response('example_subdag_operator', resp)

    def test_dag_stats_success_for_all_dag_user(self):
        self.create_user_and_login(
            username='dag_stats_success_for_all_dag_user',
            role_name='dag_stats_success_for_all_dag_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        resp = self.client.post('dag_stats', follow_redirects=True)
        self.check_content_in_response('example_subdag_operator', resp)
        self.check_content_in_response('example_bash_operator', resp)

    def test_dag_stats_success_when_selecting_dags(self):
        self.add_permission_for_role()
        resp = self.client.post(
            'dag_stats', data={'dag_ids': ['example_subdag_operator']}, follow_redirects=True
        )
        assert resp.status_code == 200
        stats = json.loads(resp.data.decode('utf-8'))
        assert 'example_bash_operator' not in stats
        assert 'example_subdag_operator' in stats

        # Multiple
        resp = self.client.post(
            'dag_stats',
            data={'dag_ids': ['example_subdag_operator', 'example_bash_operator']},
            follow_redirects=True,
        )
        stats = json.loads(resp.data.decode('utf-8'))
        assert 'example_bash_operator' in stats
        assert 'example_subdag_operator' in stats
        self.check_content_not_in_response('example_xcom', resp)

    def test_task_stats_success(self):
        self.create_user_and_login(
            username='task_stats_success_user',
            role_name='task_stats_success_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        resp = self.client.post('task_stats', follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_task_stats_failure(self):
        self.logout()
        self.login()
        resp = self.client.post('task_stats', follow_redirects=True)
        self.check_content_not_in_response('example_subdag_operator', resp)

    def test_task_stats_success_for_all_dag_user(self):
        self.create_user_and_login(
            username='task_stats_success_for_all_dag_user',
            role_name='task_stats_success_for_all_dag_user_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        resp = self.client.post('task_stats', follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)
        self.check_content_in_response('example_subdag_operator', resp)

    def test_task_stats_success_when_selecting_dags(self):
        self.logout()
        username = 'task_stats_success_when_selecting_dags_user'
        self.create_user_and_login(
            username=username,
            role_name='task_stats_success_when_selecting_dags_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )
        self.login(username=username, password=username)

        resp = self.client.post(
            'task_stats', data={'dag_ids': ['example_subdag_operator']}, follow_redirects=True
        )
        assert resp.status_code == 200
        stats = json.loads(resp.data.decode('utf-8'))
        assert 'example_bash_operator' not in stats
        assert 'example_subdag_operator' in stats

        # Multiple
        resp = self.client.post(
            'task_stats',
            data={'dag_ids': ['example_subdag_operator', 'example_bash_operator']},
            follow_redirects=True,
        )
        stats = json.loads(resp.data.decode('utf-8'))
        assert 'example_bash_operator' in stats
        assert 'example_subdag_operator' in stats
        self.check_content_not_in_response('example_xcom', resp)

    def test_code_success(self):
        self.create_user_and_login(
            username='code_success_user',
            role_name='code_success_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_CODE),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        url = 'code?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_code_failure(self):
        self.create_user_and_login(
            username='code_failure_user',
            role_name='code_failure_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        url = 'code?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_not_in_response('example_bash_operator', resp)

    def test_code_success_for_all_dag_user(self):
        self.create_user_and_login(
            username='code_success_for_all_dag_user',
            role_name='code_success_for_all_dag_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_CODE),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        url = 'code?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

        url = 'code?dag_id=example_subdag_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_subdag_operator', resp)

    def test_dag_details_success(self):
        self.create_user_and_login(
            username='dag_details_success_user',
            role_name='dag_details_success_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        url = 'dag_details?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('DAG Details', resp)

    def test_dag_details_failure(self):
        self.logout()
        self.login(username='dag_faker', password='dag_faker')
        url = 'dag_details?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_not_in_response('DAG Details', resp)

    def test_dag_details_success_for_all_dag_user(self):
        self.create_user_and_login(
            username='dag_details_success_for_all_dag_user',
            role_name='dag_details_success_for_all_dag_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        url = 'dag_details?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

        url = 'dag_details?dag_id=example_subdag_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_subdag_operator', resp)

    def test_rendered_template_success(self):
        self.logout()
        username = 'rendered_success_user'
        self.create_user_and_login(
            username=username,
            role_name='rendered_success_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )
        self.login(username=username, password=username)
        url = 'rendered-templates?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'.format(
            self.percent_encode(self.default_date)
        )
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('Rendered Template', resp)

    def test_rendered_template_failure(self):
        self.logout()
        self.login(username='dag_faker', password='dag_faker')
        url = 'rendered-templates?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'.format(
            self.percent_encode(self.default_date)
        )
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_not_in_response('Rendered Template', resp)

    def test_rendered_template_success_for_all_dag_user(self):
        self.logout()
        self.login(username='all_dag_user', password='all_dag_user')
        url = 'rendered-templates?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'.format(
            self.percent_encode(self.default_date)
        )
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('Rendered Template', resp)

    def test_task_success(self):
        self.create_user_and_login(
            username='task_success_user',
            role_name='task_success_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        url = 'task?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'.format(
            self.percent_encode(self.default_date)
        )
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('Task Instance Details', resp)

    def test_task_failure(self):
        self.logout()
        self.login(username='dag_faker', password='dag_faker')
        url = 'task?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'.format(
            self.percent_encode(self.default_date)
        )
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_not_in_response('Task Instance Details', resp)

    def test_task_success_for_all_dag_user(self):
        self.create_user_and_login(
            username='task_success_for_all_dag_user',
            role_name='task_success_for_all_dag_user_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        url = 'task?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'.format(
            self.percent_encode(self.default_date)
        )
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('Task Instance Details', resp)

    def test_xcom_success(self):
        self.logout()
        username = 'xcom_success_user'
        self.create_user_and_login(
            username=username,
            role_name='xcom_success_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_XCOM),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )
        self.login(username=username, password=username)

        url = 'xcom?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'.format(
            self.percent_encode(self.default_date)
        )
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('XCom', resp)

    def test_xcom_failure(self):
        self.logout()
        self.login(username='dag_faker', password='dag_faker')
        url = 'xcom?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'.format(
            self.percent_encode(self.default_date)
        )
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_not_in_response('XCom', resp)

    def test_xcom_success_for_all_dag_user(self):
        self.create_user_and_login(
            username='xcom_success_for_all_dag_user_user',
            role_name='xcom_success_for_all_dag_user_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_XCOM),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        url = 'xcom?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'.format(
            self.percent_encode(self.default_date)
        )
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('XCom', resp)

    def test_run_success(self):
        self.logout()
        self.login()
        form = dict(
            task_id="runme_0",
            dag_id="example_bash_operator",
            ignore_all_deps="false",
            ignore_ti_state="true",
            execution_date=self.default_date,
        )
        resp = self.client.post('run', data=form)
        self.check_content_in_response('', resp, resp_code=302)

    def test_run_success_for_all_dag_user(self):
        self.logout()
        self.login(username='all_dag_user', password='all_dag_user')
        form = dict(
            task_id="runme_0",
            dag_id="example_bash_operator",
            ignore_all_deps="false",
            ignore_ti_state="true",
            execution_date=self.default_date,
        )
        resp = self.client.post('run', data=form)
        self.check_content_in_response('', resp, resp_code=302)

    def test_blocked_success(self):
        self.create_user_and_login(
            username='blocked_success_user',
            role_name='blocked_success_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )
        url = 'blocked'

        resp = self.client.post(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_blocked_success_for_all_dag_user(self):
        self.create_user_and_login(
            username='block_success_user',
            role_name='block_success_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        url = 'blocked'
        resp = self.client.post(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)
        self.check_content_in_response('example_subdag_operator', resp)

    def test_blocked_success_when_selecting_dags(self):
        self.add_permission_for_role()
        resp = self.client.post(
            'blocked', data={'dag_ids': ['example_subdag_operator']}, follow_redirects=True
        )
        assert resp.status_code == 200
        blocked_dags = {blocked['dag_id'] for blocked in json.loads(resp.data.decode('utf-8'))}
        assert 'example_bash_operator' not in blocked_dags
        assert 'example_subdag_operator' in blocked_dags

        # Multiple
        resp = self.client.post(
            'blocked',
            data={'dag_ids': ['example_subdag_operator', 'example_bash_operator']},
            follow_redirects=True,
        )

        blocked_dags = {blocked['dag_id'] for blocked in json.loads(resp.data.decode('utf-8'))}
        assert 'example_bash_operator' in blocked_dags
        assert 'example_subdag_operator' in blocked_dags
        self.check_content_not_in_response('example_xcom', resp)

    def test_failed_success(self):
        self.create_user_and_login(
            username='failed_success_user',
            role_name='failed_success_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_TASK_INSTANCE),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        form = dict(
            task_id="run_this_last",
            dag_id="example_bash_operator",
            execution_date=self.default_date,
            upstream="false",
            downstream="false",
            future="false",
            past="false",
        )
        resp = self.client.post('failed', data=form)
        self.check_content_in_response('example_bash_operator', resp)

    def test_duration_success(self):
        url = 'duration?days=30&dag_id=example_bash_operator'
        self.create_user_and_login(
            username='duration_success_user',
            role_name='duration_success_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_duration_failure(self):
        url = 'duration?days=30&dag_id=example_bash_operator'
        self.logout()
        # login as a user without permissions
        self.login(username='dag_faker', password='dag_faker')
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_not_in_response('example_bash_operator', resp)

    def test_tries_success(self):
        url = 'tries?days=30&dag_id=example_bash_operator'
        self.create_user_and_login(
            username='tries_success_user',
            role_name='tries_success_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_tries_failure(self):
        url = 'tries?days=30&dag_id=example_bash_operator'
        self.logout()
        # login as a user without permissions
        self.login(username='dag_faker', password='dag_faker')
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_not_in_response('example_bash_operator', resp)

    def test_landing_times_success(self):
        self.create_user_and_login(
            username='landing_times_success_user',
            role_name='landing_times_success_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        url = 'landing_times?days=30&dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_landing_times_failure(self):
        url = 'landing_times?days=30&dag_id=example_bash_operator'
        self.logout()
        self.login(username='dag_faker', password='dag_faker')
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_not_in_response('example_bash_operator', resp)

    def test_paused_success(self):
        # post request failure won't test
        url = 'paused?dag_id=example_bash_operator&is_paused=false'
        self.logout()
        self.login()
        resp = self.client.post(url, follow_redirects=True)
        self.check_content_in_response('OK', resp)

    def test_refresh_success(self):
        self.logout()
        self.login()
        resp = self.client.post('refresh?dag_id=example_bash_operator')
        self.check_content_in_response('', resp, resp_code=302)

    def test_gantt_success(self):
        url = 'gantt?dag_id=example_bash_operator'
        self.create_user_and_login(
            username='gantt_success_user',
            role_name='gantt_success_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_gantt_failure(self):
        url = 'gantt?dag_id=example_bash_operator'
        self.logout()
        self.login(username='dag_faker', password='dag_faker')
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_not_in_response('example_bash_operator', resp)

    def test_success_fail_for_read_only_task_instance_access(self):
        # success endpoint need can_edit, which read only role can not access
        self.create_user_and_login(
            username='task_instance_read_user',
            role_name='task_instance_read_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
            ],
        )

        form = dict(
            task_id="run_this_last",
            dag_id="example_bash_operator",
            execution_date=self.default_date,
            upstream="false",
            downstream="false",
            future="false",
            past="false",
        )
        resp = self.client.post('success', data=form)
        self.check_content_not_in_response('Wait a minute', resp, resp_code=302)

    def test_tree_success_for_read_only_role(self):
        # tree view only allows can_read, which read only role could access
        self.create_user_and_login(
            username='tree_success_for_read_only_role_user',
            role_name='tree_success_for_read_only_role_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_LOG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        url = 'tree?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('runme_1', resp)

    def test_log_success(self):
        self.create_user_and_login(
            username='log_success_user',
            role_name='log_success_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_LOG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        url = 'log?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'.format(
            self.percent_encode(self.default_date)
        )
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('Log by attempts', resp)
        url = (
            'get_logs_with_metadata?task_id=runme_0&dag_id=example_bash_operator&'
            'execution_date={}&try_number=1&metadata=null'.format(self.percent_encode(self.default_date))
        )
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('"message":', resp)
        self.check_content_in_response('"metadata":', resp)

    def test_log_failure(self):
        self.logout()
        self.login(username='dag_faker', password='dag_faker')
        url = 'log?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'.format(
            self.percent_encode(self.default_date)
        )
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_not_in_response('Log by attempts', resp)
        url = (
            'get_logs_with_metadata?task_id=runme_0&dag_id=example_bash_operator&'
            'execution_date={}&try_number=1&metadata=null'.format(self.percent_encode(self.default_date))
        )
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_not_in_response('"message":', resp)
        self.check_content_not_in_response('"metadata":', resp)

    def test_log_success_for_user(self):
        self.logout()
        self.login(username='test_user', password='test_user')
        url = 'log?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'.format(
            self.percent_encode(self.default_date)
        )

        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('Log by attempts', resp)
        url = (
            'get_logs_with_metadata?task_id=runme_0&dag_id=example_bash_operator&'
            'execution_date={}&try_number=1&metadata=null'.format(self.percent_encode(self.default_date))
        )
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('"message":', resp)
        self.check_content_in_response('"metadata":', resp)

    def test_tree_view_for_viewer(self):
        self.logout()
        self.login(username='test_viewer', password='test_viewer')
        url = 'tree?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('runme_1', resp)

    def test_refresh_failure_for_viewer(self):
        # viewer role can't refresh
        self.logout()
        self.login(username='test_viewer', password='test_viewer')
        resp = self.client.post('refresh?dag_id=example_bash_operator')
        self.check_content_in_response('Redirecting', resp, resp_code=302)
