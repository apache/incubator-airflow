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

import importlib
import sys
from inspect import isabstract
from unittest import TestCase, mock

import pytest
from parameterized import parameterized

from airflow.models.baseoperator import BaseOperator
from tests.deprecated_classes import ALL, RENAMED_ALL


class TestMovingCoreToContrib(TestCase):
    @staticmethod
    def assert_warning(msg: str, warnings):
        error = f"Text '{msg}' not in warnings"
        assert any(msg in str(w) for w in warnings), error

    def assert_is_subclass(self, clazz, other):
        assert issubclass(clazz, other), f"{clazz} is not subclass of {other}"

    def assert_proper_import(self, old_resource, new_resource):
        new_path, _, _ = new_resource.rpartition(".")
        old_path, _, _ = old_resource.rpartition(".")
        with pytest.warns(DeprecationWarning) as warnings:
            # Reload to see deprecation warning each time
            importlib.reload(importlib.import_module(old_path))
            self.assert_warning(new_path, warnings)

    def skip_test_with_mssql_in_py38(self, path_a="", path_b=""):
        py_38 = sys.version_info >= (3, 8)
        if py_38:
            if "mssql" in path_a or "mssql" in path_b:
                raise self.skipTest("Mssql package not available when Python >= 3.8.")

    @staticmethod
    def get_class_from_path(path_to_class, parent=False):
        """
        :param parent indicates if "path_to_class" arg is super class
        """

        path, _, class_name = path_to_class.rpartition(".")
        module = importlib.import_module(path)
        class_ = getattr(module, class_name)

        if isabstract(class_) and not parent:
            class_name = f"Mock({class_.__name__})"

            attributes = {a: mock.MagicMock() for a in class_.__abstractmethods__}

            new_class = type(class_name, (class_,), attributes)
            return new_class
        return class_

    @parameterized.expand(RENAMED_ALL)
    def test_is_class_deprecated(self, new_module, old_module):
        self.skip_test_with_mssql_in_py38(new_module, old_module)
        deprecation_warning_msg = "This class is deprecated."
        old_module_class = self.get_class_from_path(old_module)
        with pytest.warns(DeprecationWarning, match=deprecation_warning_msg) as warnings:
            with mock.patch(f"{new_module}.__init__") as init_mock:
                init_mock.return_value = None
                klass = old_module_class()
                if isinstance(klass, BaseOperator):
                    # In case of operators we are validating that proper stacklevel
                    # is used (=3 or =4 if @apply_defaults)
                    assert len(warnings) >= 1
                    assert any(warning.filename == __file__ for warning in warnings)
                init_mock.assert_called_once()

    @parameterized.expand(ALL)
    def test_is_subclass(self, parent_class_path, sub_class_path):
        self.skip_test_with_mssql_in_py38(parent_class_path, sub_class_path)
        with mock.patch(f"{parent_class_path}.__init__"):
            parent_class_path = self.get_class_from_path(parent_class_path, parent=True)
            sub_class_path = self.get_class_from_path(sub_class_path)
            self.assert_is_subclass(sub_class_path, parent_class_path)

    @parameterized.expand(ALL)
    def test_warning_on_import(self, new_path, old_path):
        self.skip_test_with_mssql_in_py38(new_path, old_path)
        self.assert_proper_import(old_path, new_path)

    def test_no_redirect_to_deprecated_classes(self):
        """
        When we have the following items:
        new_A, old_B
        old_B, old_C

        This will tell us to use new_A instead of old_B.
        """
        all_classes_by_old = {old: new for new, old in ALL}

        for new, old in ALL:
            # Using if statement allows us to create a developer-friendly message only when we need it.
            # Otherwise, it wouldn't always be possible - KeyError
            if new in all_classes_by_old:
                raise AssertionError(
                    f'Deprecation "{old}" to "{new}" is incorrect. '
                    f'Please use \"{all_classes_by_old[new]}\" instead of "{old}".'
                )
