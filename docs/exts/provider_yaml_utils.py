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
from glob import glob
from typing import Any, Dict, List

import jsonschema
import yaml

try:
    from yaml import CSafeLoader as SafeLoader
except ImportError:
    from yaml import SafeLoader  # type: ignore[misc]


ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))
PROVIDER_DATA_SCHEMA_PATH = os.path.join(ROOT_DIR, "airflow", "provider.yaml.schema.json")


def _load_schema() -> Dict[str, Any]:
    with open(PROVIDER_DATA_SCHEMA_PATH) as schema_file:
        content = json.load(schema_file)
    return content


def _filepath_to_module(filepath: str):
    filepath = os.path.relpath(os.path.abspath(filepath), ROOT_DIR)
    return filepath.replace("/", ".")


def get_provider_yaml_paths():
    """Returns list of provider.yaml files"""
    return sorted(glob(f"{ROOT_DIR}/airflow/providers/**/provider.yaml", recursive=True))


def load_package_data() -> List[Dict[str, Any]]:
    """
    Load all data from providers files

    :return: A list containing the contents of all provider.yaml files.
    """
    schema = _load_schema()
    result = []
    for provider_yaml_path in get_provider_yaml_paths():
        with open(provider_yaml_path) as yaml_file:
            provider = yaml.load(yaml_file, SafeLoader)
        try:
            jsonschema.validate(provider, schema=schema)
        except jsonschema.ValidationError:
            raise Exception(f"Unable to parse: {provider_yaml_path}.")
        provider['python-module'] = _filepath_to_module(os.path.dirname(provider_yaml_path))
        provider['package-dir'] = os.path.dirname(provider_yaml_path)
        result.append(provider)
    return result
