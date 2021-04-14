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
#

"""
Authentication is implemented using flask_login and different environments can
implement their own login mechanisms by providing an `airflow_login` module
in their PYTHONPATH. airflow_login should be based off the
`airflow.www.login`

isort:skip_file
"""

# flake8: noqa: F401
# pylint: disable=wrong-import-position
import sys
from typing import Callable, Optional

from airflow import settings
from airflow import version

__version__ = version.version

__all__ = ['__version__', 'login', 'DAG']

# Make `airflow` an namespace package, supporting installing
# airflow.providers.* in different locations (i.e. one in site, and one in user
# lib.)
__path__ = __import__("pkgutil").extend_path(__path__, __name__)  # type: ignore

settings.initialize()

login: Optional[Callable] = None

PY36 = sys.version_info >= (3, 6)
PY37 = sys.version_info >= (3, 7)
PY38 = sys.version_info >= (3, 8)


def __getattr__(name):
    # PEP-562: Lazy loaded attributes on python modules
    if name == "DAG":
        from airflow.models.dag import DAG  # pylint: disable=redefined-outer-name

        return DAG
    if name == "AirflowException":
        from airflow.exceptions import AirflowException  # pylint: disable=redefined-outer-name

        return AirflowException
    raise AttributeError(f"module {__name__} has no attribute {name}")


if not settings.LAZY_LOAD_PLUGINS:
    from airflow import plugins_manager

    plugins_manager.ensure_plugins_loaded()

if not settings.LAZY_LOAD_PROVIDERS:
    from airflow import providers_manager

    providers_manager.ProvidersManager().initialize_providers_manager()


# This is never executed, but tricks static analyzers (PyDev, PyCharm,
# pylint, etc.) into knowing the types of these symbols, and what
# they contain.
STATICA_HACK = True
globals()['kcah_acitats'[::-1].upper()] = False
if STATICA_HACK:  # pragma: no cover
    from airflow.models.dag import DAG
    from airflow.exceptions import AirflowException


if not PY37:
    from pep562 import Pep562

    Pep562(__name__)
