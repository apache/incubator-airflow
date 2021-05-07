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
from contextlib import contextmanager
from typing import Any, Dict, Generator, List, NamedTuple

import flask
import jinja2
import pytest

from airflow import settings
from airflow.models import DagBag
from airflow.www.app import create_app
from tests.test_utils.api_connexion_utils import create_user, delete_roles
from tests.test_utils.decorators import dont_initialize_flask_app_submodules
from tests.test_utils.www import client_with_login


@pytest.fixture(autouse=True, scope="module")
def session():
    settings.configure_orm()
    yield settings.Session


@pytest.fixture(autouse=True, scope="module")
def examples_dag_bag(session):
    DagBag(include_examples=True).sync_to_db()
    dag_bag = DagBag(include_examples=True, read_dags_from_db=True)
    session.commit()
    yield dag_bag


@pytest.fixture(scope="module")
def app(examples_dag_bag):
    @dont_initialize_flask_app_submodules(
        skip_all_except=[
            "init_api_connexion",
            "init_appbuilder",
            "init_appbuilder_links",
            "init_appbuilder_views",
            "init_flash_views",
            "init_jinja_globals",
            "init_plugins",
        ]
    )
    def factory():
        return create_app(testing=True)

    app = factory()
    app.config["WTF_CSRF_ENABLED"] = False
    app.dag_bag = examples_dag_bag
    app.jinja_env.undefined = jinja2.StrictUndefined

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
    if not security_manager.find_user(username='test_user'):
        security_manager.add_user(
            username='test_user',
            first_name='test_user',
            last_name='test_user',
            email='test_user@fab.org',
            role=security_manager.find_role('User'),
            password='test_user',
        )
    if not security_manager.find_user(username='test_viewer'):
        security_manager.add_user(
            username='test_viewer',
            first_name='test_viewer',
            last_name='test_viewer',
            email='test_viewer@fab.org',
            role=security_manager.find_role('Viewer'),
            password='test_viewer',
        )

    yield app

    delete_roles(app)


@pytest.fixture()
def admin_client(app):
    return client_with_login(app, username="test", password="test")


@pytest.fixture()
def viewer_client(app):
    return client_with_login(app, username="test_viewer", password="test_viewer")


@pytest.fixture()
def user_client(app):
    return client_with_login(app, username="test_user", password="test_user")


@pytest.fixture(scope="module")
def client_factory(app):
    def factory(name, role_name, permissions):
        create_user(app, name, role_name, permissions)
        client = app.test_client()
        resp = client.post("/login/", data={"username": name, "password": name})
        assert resp.status_code == 302
        return client

    return factory


class _TemplateWithContext(NamedTuple):
    template: jinja2.environment.Template
    context: Dict[str, Any]

    @property
    def name(self):
        return self.template.name

    @property
    def local_context(self):
        """Returns context without global arguments"""
        result = self.context.copy()
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


@pytest.fixture(scope="module")
def capture_templates(app):
    @contextmanager
    def manager() -> Generator[List[_TemplateWithContext], None, None]:
        recorded = []

        def record(sender, template, context, **extra):  # pylint: disable=unused-argument
            recorded.append(_TemplateWithContext(template, context))

        flask.template_rendered.connect(record, app)  # type: ignore
        try:
            yield recorded
        finally:
            flask.template_rendered.disconnect(record, app)  # type: ignore

        assert recorded, "Failed to catch the templates"

    return manager
