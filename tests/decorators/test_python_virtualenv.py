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
import sys
from datetime import timedelta
from subprocess import CalledProcessError

import pytest

from airflow.decorators import task
from airflow.utils import timezone

from .test_python import TestPythonBase

DEFAULT_DATE = timezone.datetime(2016, 1, 1)
END_DATE = timezone.datetime(2016, 1, 2)
INTERVAL = timedelta(hours=12)
FROZEN_NOW = timezone.datetime(2016, 1, 2, 12, 1, 1)

TI_CONTEXT_ENV_VARS = [
    'AIRFLOW_CTX_DAG_ID',
    'AIRFLOW_CTX_TASK_ID',
    'AIRFLOW_CTX_EXECUTION_DATE',
    'AIRFLOW_CTX_DAG_RUN_ID',
]


PYTHON_VERSION = sys.version_info[0]


class TestPythonVirtualenvDecorator(TestPythonBase):
    def test_add_dill(self):
        @task.virtualenv(use_dill=True, system_site_packages=False)
        def f():
            pass

        with self.dag:
            ret = f()

        assert 'dill' in ret.operator.requirements

    def test_no_requirements(self):
        """Tests that the python callable is invoked on task run."""

        @task.virtualenv()
        def f():
            pass

        with self.dag:
            ret = f()

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_no_system_site_packages(self):
        @task.virtualenv(system_site_packages=False, python_version=PYTHON_VERSION, use_dill=True)
        def f():
            try:
                import funcsigs  # noqa: F401  # pylint: disable=redefined-outer-name,reimported,unused-import
            except ImportError:
                return True
            raise Exception

        with self.dag:
            ret = f()

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)  # pylint: disable=no-member

    def test_system_site_packages(self):
        @task.virtualenv(
            system_site_packages=False,
            requirements=['funcsigs'],
            python_version=PYTHON_VERSION,
            use_dill=True,
        )
        def f():
            import funcsigs  # noqa: F401  # pylint: disable=redefined-outer-name,reimported,unused-import

        with self.dag:
            ret = f()

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_with_requirements_pinned(self):
        @task.virtualenv(
            system_site_packages=False,
            requirements=['funcsigs==0.4'],
            python_version=PYTHON_VERSION,
            use_dill=True,
        )
        def f():
            import funcsigs  # noqa: F401  # pylint: disable=redefined-outer-name,reimported

            if funcsigs.__version__ != '0.4':
                raise Exception

        with self.dag:
            ret = f()

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_unpinned_requirements(self):
        @task.virtualenv(
            system_site_packages=False,
            requirements=['funcsigs', 'dill'],
            python_version=PYTHON_VERSION,
            use_dill=True,
        )
        def f():
            import funcsigs  # noqa: F401  # pylint: disable=redefined-outer-name,reimported,unused-import

        with self.dag:
            ret = f()

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_fail(self):
        @task.virtualenv()
        def f():
            raise Exception

        with self.dag:
            ret = f()

        with pytest.raises(CalledProcessError):
            ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_python_2(self):
        @task.virtualenv(python_version=2, requirements=['dill'])
        def f():
            {}.iteritems()  # pylint: disable=no-member

        with self.dag:
            ret = f()

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_python_2_7(self):
        @task.virtualenv(python_version='2.7', requirements=['dill'])
        def f():
            {}.iteritems()  # pylint: disable=no-member
            return True

        with self.dag:
            ret = f()

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)  # pylint: disable=no-member

    def test_python_3(self):
        @task.virtualenv(python_version=3, use_dill=False, requirements=['dill'])
        def f():
            import sys  # pylint: disable=reimported,unused-import,redefined-outer-name

            print(sys.version)
            try:
                {}.iteritems()  # pylint: disable=no-member
            except AttributeError:
                return
            raise Exception

        with self.dag:
            ret = f()

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    @staticmethod
    def _invert_python_major_version():
        if sys.version_info[0] == 2:
            return 3
        else:
            return 2

    def test_string_args(self):
        @task.virtualenv(python_version=self._invert_python_major_version(), string_args=[1, 2, 1])
        def f():
            global virtualenv_string_args  # pylint: disable=global-statement,global-variable-not-assigned
            print(virtualenv_string_args)  # pylint: disable=undefined-variable
            if virtualenv_string_args[0] != virtualenv_string_args[2]:  # pylint: disable=undefined-variable
                raise Exception

        with self.dag:
            ret = f()

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_with_args(self):
        @task.virtualenv
        def f(a, b, c=False, d=False):
            if a == 0 and b == 1 and c and not d:
                return True
            else:
                raise Exception

        with self.dag:
            ret = f(0, 1, c=True)

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)  # pylint: disable=no-member

    def test_return_none(self):
        @task.virtualenv
        def f():
            return None

        with self.dag:
            ret = f()

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_nonimported_as_arg(self):
        @task.virtualenv
        def f(_):
            return None

        with self.dag:
            ret = f(datetime.datetime.utcnow())

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
