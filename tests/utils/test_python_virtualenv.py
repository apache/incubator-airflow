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
import unittest
from unittest import mock

from airflow.utils.python_virtualenv import prepare_virtualenv, remove_task_decorator


class TestPrepareVirtualenv(unittest.TestCase):
    @mock.patch('airflow.utils.python_virtualenv.execute_in_subprocess')
    def test_should_create_virtualenv(self, mock_execute_in_subprocess):
        python_bin = prepare_virtualenv(
            venv_directory="/VENV", python_bin="pythonVER", system_site_packages=False, requirements=[]
        )
        assert "/VENV/bin/python" == python_bin
        mock_execute_in_subprocess.assert_called_once_with(['virtualenv', '/VENV', '--python=pythonVER'])

    @mock.patch('airflow.utils.python_virtualenv.execute_in_subprocess')
    def test_should_create_virtualenv_with_system_packages(self, mock_execute_in_subprocess):
        python_bin = prepare_virtualenv(
            venv_directory="/VENV", python_bin="pythonVER", system_site_packages=True, requirements=[]
        )
        assert "/VENV/bin/python" == python_bin
        mock_execute_in_subprocess.assert_called_once_with(
            ['virtualenv', '/VENV', '--system-site-packages', '--python=pythonVER']
        )

    @mock.patch('airflow.utils.python_virtualenv.execute_in_subprocess')
    def test_should_create_virtualenv_with_extra_packages(self, mock_execute_in_subprocess):
        python_bin = prepare_virtualenv(
            venv_directory="/VENV",
            python_bin="pythonVER",
            system_site_packages=False,
            requirements=['apache-beam[gcp]'],
        )
        assert "/VENV/bin/python" == python_bin

        mock_execute_in_subprocess.assert_any_call(['virtualenv', '/VENV', '--python=pythonVER'])

        mock_execute_in_subprocess.assert_called_with(['/VENV/bin/pip', 'install', 'apache-beam[gcp]'])

    def test_remove_task_decorator(self):

        py_source = "@task.virtualenv(use_dill=True)\ndef f():\nimport funcsigs"
        res = remove_task_decorator(python_source=py_source, task_decorator_name="@task.virtualenv")
        assert res == "def f():\nimport funcsigs"

    def test_remove_decorator_no_parens(self):

        py_source = "@task.virtualenv\ndef f():\nimport funcsigs"
        res = remove_task_decorator(python_source=py_source, task_decorator_name="@task.virtualenv")
        assert res == "def f():\nimport funcsigs"

    def test_remove_decorator_nested(self):

        py_source = "@foo\n@task.virtualenv\n@bar\ndef f():\nimport funcsigs"
        res = remove_task_decorator(python_source=py_source, task_decorator_name="@task.virtualenv")
        assert res == "@foo\n@bar\ndef f():\nimport funcsigs"

        py_source = "@foo\n@task.virtualenv()\n@bar\ndef f():\nimport funcsigs"
        res = remove_task_decorator(python_source=py_source, task_decorator_name="@task.virtualenv")
        assert res == "@foo\n@bar\ndef f():\nimport funcsigs"
