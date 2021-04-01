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

import functools
from unittest.mock import patch

from airflow.www.app import purge_cached_app


def dont_initialize_flask_app_submodules(_func=None, *, skip_all_except=None):
    if not skip_all_except:
        skip_all_except = []

    def decorator_dont_initialize_flask_app_submodules(f):
        def no_op(*args, **kwargs):
            pass

        methods = [
            "init_api_experimental_auth",
            "init_flash_views",
            "init_appbuilder_links",
            "init_appbuilder_views",
            "init_plugins",
            "init_connection_form",
            "init_error_handlers",
            "init_api_connexion",
            "init_api_experimental",
            "sync_appbuilder_roles",
            "init_jinja_globals",
            "init_xframe_protection",
            "init_permanent_session",
            "init_appbuilder",
        ]

        @functools.wraps(f)
        def func(*args, **kwargs):

            for method in methods:
                if method not in skip_all_except:
                    patcher = patch(f"airflow.www.app.{method}", no_op)
                    patcher.start()
            purge_cached_app()
            result = f(*args, **kwargs)
            patch.stopall()
            purge_cached_app()

            return result

        return func

    if _func is None:
        return decorator_dont_initialize_flask_app_submodules
    else:
        return decorator_dont_initialize_flask_app_submodules(_func)
