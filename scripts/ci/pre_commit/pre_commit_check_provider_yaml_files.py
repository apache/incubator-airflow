#!/usr/bin/env python3
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

import json
import os
import sys
import textwrap
from collections import Counter
from glob import glob
from itertools import chain, product
from typing import Any, Dict, Iterable, List

import jsonschema
import yaml
from tabulate import tabulate

try:
    from yaml import CSafeLoader as SafeLoader
except ImportError:
    from yaml import SafeLoader  # type: ignore[no-redef]

if __name__ != "__main__":
    raise Exception(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
    )

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, os.pardir))
DOCS_DIR = os.path.join(ROOT_DIR, 'docs')
PROVIDER_DATA_SCHEMA_PATH = os.path.join(ROOT_DIR, "airflow", "provider.yaml.schema.json")
CORE_INTEGRATIONS = ["SQL", "Local"]

errors = []


def _filepath_to_module(filepath: str):
    filepath = os.path.relpath(os.path.abspath(filepath), ROOT_DIR)
    if filepath.endswith(".py"):
        filepath = filepath[: -(len(".py"))]
    return filepath.replace("/", ".")


def _load_schema() -> Dict[str, Any]:
    with open(PROVIDER_DATA_SCHEMA_PATH) as schema_file:
        content = json.load(schema_file)
    return content


def _load_package_data(package_paths: Iterable[str]):
    schema = _load_schema()
    result = {}
    for provider_yaml_path in package_paths:
        with open(provider_yaml_path) as yaml_file:
            provider = yaml.load(yaml_file, SafeLoader)
        rel_path = os.path.relpath(provider_yaml_path, ROOT_DIR)
        try:
            jsonschema.validate(provider, schema=schema)
        except jsonschema.ValidationError:
            raise Exception(f"Unable to parse: {rel_path}.")
        result[rel_path] = provider
    return result


def get_all_integration_names(yaml_files):
    all_integrations = [
        i['integration-name'] for f in yaml_files.values() if 'integrations' in f for i in f["integrations"]
    ]
    all_integrations += ["SQL", "Local"]
    return all_integrations


def check_integration_duplicates(yaml_files: Dict[str, Dict]):
    """Integration names must be globally unique."""
    print("Checking integration duplicates")
    all_integrations = get_all_integration_names(yaml_files)

    duplicates = [(k, v) for (k, v) in Counter(all_integrations).items() if v > 1]

    if duplicates:
        print(
            "Duplicate integration names found. Integration names must be globally unique. "
            "Please delete duplicates."
        )
        print(tabulate(duplicates, headers=["Integration name", "Number of occurrences"]))
        sys.exit(0)


def assert_sets_equal(set1, set2):
    try:
        difference1 = set1.difference(set2)
    except TypeError as e:
        raise AssertionError(f'invalid type when attempting set difference: {e}')
    except AttributeError as e:
        raise AssertionError(f'first argument does not support set difference: {e}')

    try:
        difference2 = set2.difference(set1)
    except TypeError as e:
        raise AssertionError(f'invalid type when attempting set difference: {e}')
    except AttributeError as e:
        raise AssertionError(f'second argument does not support set difference: {e}')

    if not (difference1 or difference2):
        return

    lines = []
    if difference1:
        lines.append('Items in the first set but not the second:')
        for item in sorted(difference1):
            lines.append(repr(item))
    if difference2:
        lines.append('Items in the second set but not the first:')
        for item in sorted(difference2):
            lines.append(repr(item))

    standard_msg = '\n'.join(lines)
    raise AssertionError(standard_msg)


def check_if_objects_belongs_to_package(
    object_names: List[str], provider_package: str, yaml_file_path: str, resource_type: str
):
    for object_name in object_names:
        if not object_name.startswith(provider_package):
            errors.append(
                f"The `{object_name}` object in {resource_type} list in {yaml_file_path} does not start"
                f" with the expected {provider_package}."
            )


def parse_module_data(provider_data, resource_type, yaml_file_path):
    package_dir = ROOT_DIR + "/" + os.path.dirname(yaml_file_path)
    provider_package = os.path.dirname(yaml_file_path).replace(os.sep, ".")
    py_files = chain(
        glob(f"{package_dir}/**/{resource_type}/*.py"), glob(f"{package_dir}/{resource_type}/*.py")
    )
    expected_modules = {_filepath_to_module(f) for f in py_files if not f.endswith("/__init__.py")}
    resource_data = provider_data.get(resource_type, [])
    return expected_modules, provider_package, resource_data


def check_completeness_of_list_of_hooks_sensors_hooks(yaml_files: Dict[str, Dict]):
    print("Checking completeness of list of {sensors, hooks, operators}")
    for (yaml_file_path, provider_data), resource_type in product(
        yaml_files.items(), ["sensors", "operators", "hooks"]
    ):
        expected_modules, provider_package, resource_data = parse_module_data(
            provider_data, resource_type, yaml_file_path
        )

        current_modules = {str(i) for r in resource_data for i in r.get('python-modules', [])}
        check_if_objects_belongs_to_package(current_modules, provider_package, yaml_file_path, resource_type)
        try:
            assert_sets_equal(set(expected_modules), set(current_modules))
        except AssertionError as ex:
            nested_error = textwrap.indent(str(ex), '  ')
            errors.append(
                f"Incorrect content of key '{resource_type}/python-modules' "
                f"in file: {yaml_file_path}\n{nested_error}"
            )


def check_duplicates_in_integrations_names_of_hooks_sensors_operators(yaml_files: Dict[str, Dict]):
    print("Checking for duplicates in list of {sensors, hooks, operators}")
    for (yaml_file_path, provider_data), resource_type in product(
        yaml_files.items(), ["sensors", "operators", "hooks"]
    ):
        resource_data = provider_data.get(resource_type, [])
        current_integrations = [r.get("integration-name", "") for r in resource_data]
        if len(current_integrations) != len(set(current_integrations)):
            for integration in current_integrations:
                if current_integrations.count(integration) > 1:
                    errors.append(
                        f"Duplicated content of '{resource_type}/integration-name/{integration}' "
                        f"in file: {yaml_file_path}"
                    )


def check_completeness_of_list_of_transfers(yaml_files: Dict[str, Dict]):
    print("Checking completeness of list of transfers")
    resource_type = 'transfers'
    for yaml_file_path, provider_data in yaml_files.items():
        expected_modules, provider_package, resource_data = parse_module_data(
            provider_data, resource_type, yaml_file_path
        )

        current_modules = {r.get('python-module') for r in resource_data}
        check_if_objects_belongs_to_package(current_modules, provider_package, yaml_file_path, resource_type)
        try:
            assert_sets_equal(set(expected_modules), set(current_modules))
        except AssertionError as ex:
            nested_error = textwrap.indent(str(ex), '  ')
            errors.append(
                f"Incorrect content of key '{resource_type}/python-module' "
                f"in file: {yaml_file_path}\n{nested_error}"
            )


def check_hook_classes(yaml_files: Dict[str, Dict]):
    print("Checking connection classes belong to package")
    resource_type = 'hook-class-names'
    for yaml_file_path, provider_data in yaml_files.items():
        provider_package = os.path.dirname(yaml_file_path).replace(os.sep, ".")
        hook_class_names = provider_data.get(resource_type)
        if hook_class_names:
            check_if_objects_belongs_to_package(
                hook_class_names, provider_package, yaml_file_path, resource_type
            )


def check_duplicates_in_list_of_transfers(yaml_files: Dict[str, Dict]):
    print("Checking for duplicates in list of transfers")
    errors = []
    resource_type = "transfers"
    for yaml_file_path, provider_data in yaml_files.items():
        resource_data = provider_data.get(resource_type, [])

        source_target_integrations = [
            (r.get("source-integration-name", ""), r.get("target-integration-name", ""))
            for r in resource_data
        ]
        if len(source_target_integrations) != len(set(source_target_integrations)):
            for integration_couple in source_target_integrations:
                if source_target_integrations.count(integration_couple) > 1:
                    errors.append(
                        f"Duplicated content of \n"
                        f" '{resource_type}/source-integration-name/{integration_couple[0]}' "
                        f" '{resource_type}/target-integration-name/{integration_couple[1]}' "
                        f"in file: {yaml_file_path}"
                    )


def check_invalid_integration(yaml_files: Dict[str, Dict]):
    print("Detect unregistered integrations")
    all_integration_names = set(get_all_integration_names(yaml_files))

    for (yaml_file_path, provider_data), resource_type in product(
        yaml_files.items(), ["sensors", "operators", "hooks"]
    ):
        resource_data = provider_data.get(resource_type, [])
        current_names = {r['integration-name'] for r in resource_data}
        invalid_names = current_names - all_integration_names
        if invalid_names:
            errors.append(
                f"Incorrect content of key '{resource_type}/integration-name' in file: {yaml_file_path}. "
                f"Invalid values: {invalid_names}"
            )

    for (yaml_file_path, provider_data), key in product(
        yaml_files.items(), ['source-integration-name', 'target-integration-name']
    ):
        resource_data = provider_data.get('transfers', [])
        current_names = {r[key] for r in resource_data}
        invalid_names = current_names - all_integration_names
        if invalid_names:
            errors.append(
                f"Incorrect content of key 'transfers/{key}' in file: {yaml_file_path}. "
                f"Invalid values: {invalid_names}"
            )


def check_doc_files(yaml_files: Dict[str, Dict]):
    print("Checking doc files")
    current_doc_urls = []
    current_logo_urls = []
    for provider in yaml_files.values():
        if 'integrations' in provider:
            current_doc_urls.extend(
                guide
                for guides in provider['integrations']
                if 'how-to-guide' in guides
                for guide in guides['how-to-guide']
            )
            current_logo_urls.extend(
                integration['logo'] for integration in provider['integrations'] if 'logo' in integration
            )
        if 'transfers' in provider:
            current_doc_urls.extend(
                op['how-to-guide'] for op in provider['transfers'] if 'how-to-guide' in op
            )

    expected_doc_urls = {
        "/docs/" + os.path.relpath(f, start=DOCS_DIR)
        for f in glob(f"{DOCS_DIR}/apache-airflow-providers-*/operators/**/*.rst", recursive=True)
        if not f.endswith("/index.rst") and '/_partials' not in f
    }
    expected_doc_urls |= {
        "/docs/" + os.path.relpath(f, start=DOCS_DIR)
        for f in glob(f"{DOCS_DIR}/apache-airflow-providers-*/operators.rst", recursive=True)
    }
    expected_logo_urls = {
        "/" + os.path.relpath(f, start=DOCS_DIR)
        for f in glob(f"{DOCS_DIR}/integration-logos/**/*", recursive=True)
        if os.path.isfile(f)
    }

    try:
        assert_sets_equal(set(expected_doc_urls), set(current_doc_urls))
        assert_sets_equal(set(expected_logo_urls), set(current_logo_urls))
    except AssertionError as ex:
        print(ex)
        sys.exit(1)


def check_unique_provider_name(yaml_files: Dict[str, Dict]):
    provider_names = [d['name'] for d in yaml_files.values()]
    duplicates = {x for x in provider_names if provider_names.count(x) > 1}
    if duplicates:
        errors.append(f"Provider name must be unique. Duplicates: {duplicates}")


if __name__ == '__main__':
    all_provider_files = sorted(glob(f"{ROOT_DIR}/airflow/providers/**/provider.yaml", recursive=True))
    if len(sys.argv) > 1:
        paths = sorted(sys.argv[1:])
    else:
        paths = all_provider_files

    all_parsed_yaml_files: Dict[str, Dict] = _load_package_data(paths)

    all_files_loaded = len(all_provider_files) == len(paths)
    check_integration_duplicates(all_parsed_yaml_files)

    check_completeness_of_list_of_hooks_sensors_hooks(all_parsed_yaml_files)
    check_duplicates_in_integrations_names_of_hooks_sensors_operators(all_parsed_yaml_files)

    check_completeness_of_list_of_transfers(all_parsed_yaml_files)
    check_duplicates_in_list_of_transfers(all_parsed_yaml_files)
    check_hook_classes(all_parsed_yaml_files)
    check_unique_provider_name(all_parsed_yaml_files)

    if all_files_loaded:
        # Only check those if all provider files are loaded
        check_doc_files(all_parsed_yaml_files)
        check_invalid_integration(all_parsed_yaml_files)

    if errors:
        print(f"Found {len(errors)} errors")
        for error in errors:
            print(error)
            print()
        sys.exit(1)
