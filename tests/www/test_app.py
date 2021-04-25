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
import runpy
import sys
import unittest
from datetime import timedelta
from unittest import mock

import pytest
from werkzeug.routing import Rule
from werkzeug.test import create_environ
from werkzeug.wrappers import Response

from airflow.www import app as application
from tests.test_utils.config import conf_vars
from tests.test_utils.decorators import dont_initialize_flask_app_submodules


class TestApp(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from airflow import settings

        settings.configure_orm()

    @conf_vars(
        {
            ('webserver', 'enable_proxy_fix'): 'True',
            ('webserver', 'proxy_fix_x_for'): '1',
            ('webserver', 'proxy_fix_x_proto'): '1',
            ('webserver', 'proxy_fix_x_host'): '1',
            ('webserver', 'proxy_fix_x_port'): '1',
            ('webserver', 'proxy_fix_x_prefix'): '1',
        }
    )
    @dont_initialize_flask_app_submodules
    def test_should_respect_proxy_fix(self):
        app = application.cached_app(testing=True)
        app.url_map.add(Rule("/debug", endpoint="debug"))

        def debug_view():
            from flask import request

            # Should respect HTTP_X_FORWARDED_FOR
            assert request.remote_addr == '192.168.0.1'
            # Should respect HTTP_X_FORWARDED_PROTO, HTTP_X_FORWARDED_HOST, HTTP_X_FORWARDED_PORT,
            # HTTP_X_FORWARDED_PREFIX
            assert request.url == 'https://valid:445/proxy-prefix/debug'

            return Response("success")

        app.view_functions['debug'] = debug_view

        new_environ = {
            "PATH_INFO": "/debug",
            "REMOTE_ADDR": "192.168.0.2",
            "HTTP_HOST": "invalid:9000",
            "HTTP_X_FORWARDED_FOR": "192.168.0.1",
            "HTTP_X_FORWARDED_PROTO": "https",
            "HTTP_X_FORWARDED_HOST": "valid",
            "HTTP_X_FORWARDED_PORT": "445",
            "HTTP_X_FORWARDED_PREFIX": "/proxy-prefix",
        }
        environ = create_environ(environ_overrides=new_environ)

        response = Response.from_app(app, environ)

        assert b"success" == response.get_data()
        assert response.status_code == 200

    @conf_vars(
        {
            ('webserver', 'base_url'): 'http://localhost:8080/internal-client',
        }
    )
    @dont_initialize_flask_app_submodules
    def test_should_respect_base_url_ignore_proxy_headers(self):
        app = application.cached_app(testing=True)
        app.url_map.add(Rule("/debug", endpoint="debug"))

        def debug_view():
            from flask import request

            # Should ignore HTTP_X_FORWARDED_FOR
            assert request.remote_addr == '192.168.0.2'
            # Should ignore HTTP_X_FORWARDED_PROTO, HTTP_X_FORWARDED_HOST, HTTP_X_FORWARDED_PORT,
            # HTTP_X_FORWARDED_PREFIX
            assert request.url == 'http://invalid:9000/internal-client/debug'

            return Response("success")

        app.view_functions['debug'] = debug_view

        new_environ = {
            "PATH_INFO": "/internal-client/debug",
            "REMOTE_ADDR": "192.168.0.2",
            "HTTP_HOST": "invalid:9000",
            "HTTP_X_FORWARDED_FOR": "192.168.0.1",
            "HTTP_X_FORWARDED_PROTO": "https",
            "HTTP_X_FORWARDED_HOST": "valid",
            "HTTP_X_FORWARDED_PORT": "445",
            "HTTP_X_FORWARDED_PREFIX": "/proxy-prefix",
        }
        environ = create_environ(environ_overrides=new_environ)

        response = Response.from_app(app, environ)

        assert b"success" == response.get_data()
        assert response.status_code == 200

    @conf_vars(
        {
            ('webserver', 'base_url'): 'http://localhost:8080/internal-client',
            ('webserver', 'enable_proxy_fix'): 'True',
            ('webserver', 'proxy_fix_x_for'): '1',
            ('webserver', 'proxy_fix_x_proto'): '1',
            ('webserver', 'proxy_fix_x_host'): '1',
            ('webserver', 'proxy_fix_x_port'): '1',
            ('webserver', 'proxy_fix_x_prefix'): '1',
        }
    )
    @dont_initialize_flask_app_submodules
    def test_should_respect_base_url_when_proxy_fix_and_base_url_is_set_up_but_headers_missing(self):
        app = application.cached_app(testing=True)
        app.url_map.add(Rule("/debug", endpoint="debug"))

        def debug_view():
            from flask import request

            # Should use original REMOTE_ADDR
            assert request.remote_addr == '192.168.0.1'
            # Should respect base_url
            assert request.url == "http://invalid:9000/internal-client/debug"

            return Response("success")

        app.view_functions['debug'] = debug_view

        new_environ = {
            "PATH_INFO": "/internal-client/debug",
            "REMOTE_ADDR": "192.168.0.1",
            "HTTP_HOST": "invalid:9000",
        }
        environ = create_environ(environ_overrides=new_environ)

        response = Response.from_app(app, environ)

        assert b"success" == response.get_data()
        assert response.status_code == 200

    @conf_vars(
        {
            ('webserver', 'base_url'): 'http://localhost:8080/internal-client',
            ('webserver', 'enable_proxy_fix'): 'True',
            ('webserver', 'proxy_fix_x_for'): '1',
            ('webserver', 'proxy_fix_x_proto'): '1',
            ('webserver', 'proxy_fix_x_host'): '1',
            ('webserver', 'proxy_fix_x_port'): '1',
            ('webserver', 'proxy_fix_x_prefix'): '1',
        }
    )
    @dont_initialize_flask_app_submodules
    def test_should_respect_base_url_and_proxy_when_proxy_fix_and_base_url_is_set_up(self):
        app = application.cached_app(testing=True)
        app.url_map.add(Rule("/debug", endpoint="debug"))

        def debug_view():
            from flask import request

            # Should respect HTTP_X_FORWARDED_FOR
            assert request.remote_addr == '192.168.0.1'
            # Should respect HTTP_X_FORWARDED_PROTO, HTTP_X_FORWARDED_HOST, HTTP_X_FORWARDED_PORT,
            # HTTP_X_FORWARDED_PREFIX and use base_url
            assert request.url == "https://valid:445/proxy-prefix/internal-client/debug"

            return Response("success")

        app.view_functions['debug'] = debug_view

        new_environ = {
            "PATH_INFO": "/internal-client/debug",
            "REMOTE_ADDR": "192.168.0.2",
            "HTTP_HOST": "invalid:9000",
            "HTTP_X_FORWARDED_FOR": "192.168.0.1",
            "HTTP_X_FORWARDED_PROTO": "https",
            "HTTP_X_FORWARDED_HOST": "valid",
            "HTTP_X_FORWARDED_PORT": "445",
            "HTTP_X_FORWARDED_PREFIX": "/proxy-prefix",
        }
        environ = create_environ(environ_overrides=new_environ)

        response = Response.from_app(app, environ)

        assert b"success" == response.get_data()
        assert response.status_code == 200

    @conf_vars(
        {
            ('core', 'sql_alchemy_pool_enabled'): 'True',
            ('core', 'sql_alchemy_pool_size'): '3',
            ('core', 'sql_alchemy_max_overflow'): '5',
            ('core', 'sql_alchemy_pool_recycle'): '120',
            ('core', 'sql_alchemy_pool_pre_ping'): 'True',
        }
    )
    @dont_initialize_flask_app_submodules
    @pytest.mark.backend("mysql", "postgres")
    def test_should_set_sqlalchemy_engine_options(self):
        app = application.cached_app(testing=True)
        engine_params = {'pool_size': 3, 'pool_recycle': 120, 'pool_pre_ping': True, 'max_overflow': 5}
        assert app.config['SQLALCHEMY_ENGINE_OPTIONS'] == engine_params

    @conf_vars(
        {
            ('webserver', 'session_lifetime_minutes'): '3600',
        }
    )
    @dont_initialize_flask_app_submodules
    def test_should_set_permanent_session_timeout(self):
        app = application.cached_app(testing=True)
        assert app.config['PERMANENT_SESSION_LIFETIME'] == timedelta(minutes=3600)

    @conf_vars({('webserver', 'cookie_samesite'): ''})
    @dont_initialize_flask_app_submodules
    def test_correct_default_is_set_for_cookie_samesite(self):
        app = application.cached_app(testing=True)
        assert app.config['SESSION_COOKIE_SAMESITE'] == 'Lax'


class TestFlaskCli:
    @dont_initialize_flask_app_submodules(skip_all_except=['init_appbuilder'])
    def test_flask_cli_should_display_routes(self, capsys):
        with mock.patch.dict("os.environ", FLASK_APP="airflow.www.app:cached_app"), mock.patch.object(
            sys, 'argv', ['flask', 'routes']
        ), pytest.raises(SystemExit):
            runpy.run_module('flask', run_name='__main__')

        output = capsys.readouterr()
        assert "/login/" in output.out
