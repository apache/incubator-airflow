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

import logging
import warnings
from os import path

import connexion
from connexion import ProblemException
from flask import Flask, request

from airflow.api_connexion.exceptions import common_error_handler
from airflow.configuration import conf
from airflow.security import permissions
from airflow.www.views import lazy_add_provider_discovered_options_to_connection_form

log = logging.getLogger(__name__)

# airflow/www/extensions/init_views.py => airflow/
ROOT_APP_DIR = path.abspath(path.join(path.dirname(__file__), path.pardir, path.pardir))


def init_flash_views(app):
    """Init main app view - redirect to FAB"""
    from airflow.www.blueprints import routes

    app.register_blueprint(routes)


def init_appbuilder_views(app):
    """Initialize Web UI views"""
    appbuilder = app.appbuilder
    from airflow.www import views

    # Remove the session from scoped_session registry to avoid
    # reusing a session with a disconnected connection
    appbuilder.session.remove()
    appbuilder.add_view_no_menu(views.DagModelView())
    appbuilder.add_view_no_menu(views.Airflow())
    appbuilder.add_view(
        views.DagRunModelView,
        permissions.RESOURCE_DAG_RUN,
        category=permissions.RESOURCE_BROWSE_MENU,
        category_icon="fa-globe",
    )
    appbuilder.add_view(
        views.JobModelView, permissions.RESOURCE_JOB, category=permissions.RESOURCE_BROWSE_MENU
    )
    appbuilder.add_view(
        views.LogModelView, permissions.RESOURCE_AUDIT_LOG, category=permissions.RESOURCE_BROWSE_MENU
    )
    appbuilder.add_view(
        views.VariableModelView, permissions.RESOURCE_VARIABLE, category=permissions.RESOURCE_ADMIN_MENU
    )
    appbuilder.add_view(
        views.TaskInstanceModelView,
        permissions.RESOURCE_TASK_INSTANCE,
        category=permissions.RESOURCE_BROWSE_MENU,
    )
    appbuilder.add_view(
        views.TaskRescheduleModelView,
        permissions.RESOURCE_TASK_RESCHEDULE,
        category=permissions.RESOURCE_BROWSE_MENU,
    )
    appbuilder.add_view(
        views.ConfigurationView,
        permissions.RESOURCE_CONFIG,
        category=permissions.RESOURCE_ADMIN_MENU,
        category_icon="fa-user",
    )
    appbuilder.add_view(
        views.ConnectionModelView, permissions.RESOURCE_CONNECTION, category=permissions.RESOURCE_ADMIN_MENU
    )
    appbuilder.add_view(
        views.SlaMissModelView, permissions.RESOURCE_SLA_MISS, category=permissions.RESOURCE_BROWSE_MENU
    )
    appbuilder.add_view(
        views.PluginView, permissions.RESOURCE_PLUGIN, category=permissions.RESOURCE_ADMIN_MENU
    )
    appbuilder.add_view(
        views.PoolModelView, permissions.RESOURCE_POOL, category=permissions.RESOURCE_ADMIN_MENU
    )
    appbuilder.add_view(
        views.XComModelView, permissions.RESOURCE_XCOM, category=permissions.RESOURCE_ADMIN_MENU
    )
    # add_view_no_menu to change item position.
    # I added link in extensions.init_appbuilder_links.init_appbuilder_links
    appbuilder.add_view_no_menu(views.RedocView)


def init_plugins(app):
    """Integrate Flask and FAB with plugins"""
    from airflow import plugins_manager

    plugins_manager.initialize_web_ui_plugins()

    appbuilder = app.appbuilder

    for view in plugins_manager.flask_appbuilder_views:
        name = view.get('name')
        if name:
            log.debug("Adding view %s with menu", name)
            appbuilder.add_view(view["view"], name, category=view["category"])
        else:
            # if 'name' key is missing, intent is to add view without menu
            log.debug("Adding view %s without menu", str(type(view["view"])))
            appbuilder.add_view_no_menu(view["view"])

    for menu_link in sorted(plugins_manager.flask_appbuilder_menu_links, key=lambda x: x["name"]):
        log.debug("Adding menu link %s to %s", menu_link["name"], menu_link["href"])
        appbuilder.add_link(**menu_link)

    for blue_print in plugins_manager.flask_blueprints:
        log.debug("Adding blueprint %s:%s", blue_print["name"], blue_print["blueprint"].import_name)
        app.register_blueprint(blue_print["blueprint"])


def init_connection_form():
    """Initializes connection form"""
    lazy_add_provider_discovered_options_to_connection_form()


def init_error_handlers(app: Flask):
    """Add custom errors handlers"""
    from airflow.www import views

    app.register_error_handler(500, views.show_traceback)
    app.register_error_handler(404, views.circles)


def set_cors_headers_on_response(response):
    """Add response headers"""
    allow_headers = conf.get('api', 'access_control_allow_headers')
    allow_methods = conf.get('api', 'access_control_allow_methods')
    allow_origin = conf.get('api', 'access_control_allow_origin')
    if allow_headers is not None:
        response.headers['Access-Control-Allow-Headers'] = allow_headers
    if allow_methods is not None:
        response.headers['Access-Control-Allow-Methods'] = allow_methods
    if allow_origin is not None:
        response.headers['Access-Control-Allow-Origin'] = allow_origin
    return response


def init_api_connexion(app: Flask) -> None:
    """Initialize Stable API"""
    base_path = '/api/v1'

    from airflow.www import views

    @app.errorhandler(404)
    @app.errorhandler(405)
    def _handle_api_error(ex):
        if request.path.startswith(base_path):
            # 404 errors are never handled on the blueprint level
            # unless raised from a view func so actual 404 errors,
            # i.e. "no route for it" defined, need to be handled
            # here on the application level
            return common_error_handler(ex)
        else:
            return views.circles(ex)

    spec_dir = path.join(ROOT_APP_DIR, 'api_connexion', 'openapi')
    connexion_app = connexion.App(__name__, specification_dir=spec_dir, skip_error_handlers=True)
    connexion_app.app = app
    api_bp = connexion_app.add_api(
        specification='v1.yaml', base_path=base_path, validate_responses=True, strict_validation=True
    ).blueprint
    # Like "api_bp.after_request", but the BP is already registered, so we have
    # to register it in the app directly.
    app.after_request_funcs.setdefault(api_bp.name, []).append(set_cors_headers_on_response)
    app.register_error_handler(ProblemException, common_error_handler)
    app.extensions['csrf'].exempt(api_bp)


def init_api_experimental(app):
    """Initialize Experimental API"""
    if not conf.getboolean('api', 'enable_experimental_api', fallback=False):
        return
    from airflow.www.api.experimental import endpoints

    warnings.warn(
        "The experimental REST API is deprecated. Please migrate to the stable REST API. "
        "Please note that the experimental API do not have access control. "
        "The authenticated user has full access.",
        DeprecationWarning,
    )
    app.register_blueprint(endpoints.api_experimental, url_prefix='/api/experimental')
    app.extensions['csrf'].exempt(endpoints.api_experimental)
