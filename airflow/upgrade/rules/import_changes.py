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

import itertools
from typing import NamedTuple, Optional, List
import os
from cached_property import cached_property
from packaging.version import Version

from airflow import conf
from airflow.upgrade.rules.base_rule import BaseRule
from airflow.upgrade.rules.renamed_classes import ALL
from airflow.utils.dag_processing import list_py_file_paths, correct_maybe_zipped

try:
    from importlib_metadata import PackageNotFoundError, distribution
except ImportError:
    from importlib.metadata import PackageNotFoundError, distribution


class ImportChange(
    NamedTuple(
        "ImportChange",
        [("old_path", str), ("new_path", str), ("providers_package", Optional[str])],
    )
):
    def info(self, file_path=None):
        msg = "Using `{}` should be replaced by `{}`".format(
            self.old_path, self.new_path
        )
        if file_path:
            msg += ". Affected file: {}".format(file_path)
        return msg

    @cached_property
    def old_path_without_classname(self):
        part = self.old_path.split(".")
        part.pop()
        return ".".join(part)

    @cached_property
    def old_class(self):
        return self.old_path.split(".")[-1]

    @cached_property
    def new_class(self):
        return self.new_path.split(".")[-1]

    @classmethod
    def provider_stub_from_module(cls, module):
        if "providers" not in module:
            return None

        # [2:] strips off the airflow.providers. part
        parts = module.split(".")[2:]
        if parts[0] in ('apache', 'cncf', 'microsoft'):
            return '-'.join(parts[:2])
        return parts[0]

    @classmethod
    def from_new_old_paths(cls, new_path, old_path):
        providers_package = cls.provider_stub_from_module(new_path)
        return cls(
            old_path=old_path, new_path=new_path, providers_package=providers_package
        )


class ImportChangesRule(BaseRule):
    title = "Changes in import paths of hooks, operators, sensors and others"
    description = (
        "Many hooks, operators and other classes has been renamed and moved. Those changes were part of "
        "unifying names and imports paths as described in AIP-21.\nThe `contrib` folder has been replaced "
        "by `providers` directory and packages:\n"
        "https://github.com/apache/airflow#backport-packages"
    )

    current_airflow_version = Version(__import__("airflow").__version__)

    if current_airflow_version < Version("2.0.0"):

        def _filter_incompatible_renames(arg):
            new_path = arg[0]
            return (
                not new_path.startswith("airflow.operators")
                and not new_path.startswith("airflow.sensors")
                and not new_path.startswith("airflow.hooks")
            )

    else:
        # Everything allowed on 2.0.0+
        def _filter_incompatible_renames(arg):
            return True

    ALL_CHANGES = [
        ImportChange.from_new_old_paths(*args)
        for args in filter(_filter_incompatible_renames, ALL)
    ]  # type: List[ImportChange]

    del _filter_incompatible_renames

    @staticmethod
    def _check_file(file_path):
        problems = []
        providers = set()
        with open(file_path, "r") as file:
            try:
                content = file.read()
                for change in ImportChangesRule.ALL_CHANGES:
                    if change.old_path_without_classname in content and change.old_class in content:
                        problems.append(change.info(file_path))
                        if change.providers_package:
                            providers.add(change.providers_package)
            except UnicodeDecodeError:
                problems.append("Unable to read python file {}".format(file_path))
        return problems, providers

    @staticmethod
    def _check_missing_providers(providers):

        current_airflow_version = Version(__import__("airflow").__version__)
        if current_airflow_version >= Version("2.0.0"):
            prefix = "apache-airflow-providers-"
        else:
            prefix = "apache-airflow-backport-providers-"

        for provider in providers:
            dist_name = prefix + provider
            try:
                distribution(dist_name)
            except PackageNotFoundError:
                yield "Please install `{}`".format(dist_name)

    def check(self):
        dags_folder = conf.get("core", "dags_folder")
        dags_folder = correct_maybe_zipped(dags_folder)
        files = list_py_file_paths(directory=dags_folder, include_examples=False)
        files = [file for file in files if os.path.splitext(file)[1] == ".py"]
        problems = []
        providers = set()
        # Split in to two groups - install backports first, then make changes
        for file in files:
            new_problems, new_providers = self._check_file(file)
            problems.extend(new_problems)
            providers |= new_providers

        return itertools.chain(
            self._check_missing_providers(sorted(providers)),
            problems,
        )
