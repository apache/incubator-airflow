#!/usr/bin/env python3
# pylint: disable=wrong-import-order
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
"""Setup.py for the Provider packages of Airflow project."""
import collections
import glob
import importlib
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timedelta
from enum import Enum
from functools import lru_cache
from os.path import dirname
from shutil import copyfile
from typing import Any, Dict, Iterable, List, NamedTuple, Optional, Set, Tuple, Type

import click
import jsonpath_ng
import jsonschema
import yaml
from packaging.version import Version
from rich import print
from rich.console import Console
from rich.syntax import Syntax

try:
    from yaml import CSafeLoader as SafeLoader
except ImportError:
    from yaml import SafeLoader  # noqa

INITIAL_CHANGELOG_CONTENT = """


 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.


Changelog
---------

1.0.0
.....

Initial version of the provider.
"""

HTTPS_REMOTE = "apache-https-for-providers"
HEAD_OF_HTTPS_REMOTE = f"{HTTPS_REMOTE}/master"

PROVIDER_TEMPLATE_PREFIX = "PROVIDER_"

MY_DIR_PATH = os.path.dirname(__file__)
SOURCE_DIR_PATH = os.path.abspath(os.path.join(MY_DIR_PATH, os.pardir, os.pardir))
AIRFLOW_PATH = os.path.join(SOURCE_DIR_PATH, "airflow")
PROVIDERS_PATH = os.path.join(AIRFLOW_PATH, "providers")
DOCUMENTATION_PATH = os.path.join(SOURCE_DIR_PATH, "docs")
TARGET_PROVIDER_PACKAGES_PATH = os.path.join(SOURCE_DIR_PATH, "provider_packages")
GENERATED_AIRFLOW_PATH = os.path.join(TARGET_PROVIDER_PACKAGES_PATH, "airflow")
GENERATED_PROVIDERS_PATH = os.path.join(GENERATED_AIRFLOW_PATH, "providers")

PROVIDER_2_0_0_DATA_SCHEMA_PATH = os.path.join(
    SOURCE_DIR_PATH, "airflow", "deprecated_schemas", "provider-2.0.0.yaml.schema.json"
)

PROVIDER_RUNTIME_DATA_SCHEMA_PATH = os.path.join(SOURCE_DIR_PATH, "airflow", "provider_info.schema.json")

sys.path.insert(0, SOURCE_DIR_PATH)

# those imports need to come after the above sys.path.insert to make sure that Airflow
# sources are importable without having to add the airflow sources to the PYTHONPATH before
# running the script
import tests.deprecated_classes  # noqa # isort:skip
from dev.import_all_classes import import_all_classes  # noqa # isort:skip
from setup import PROVIDERS_REQUIREMENTS, PREINSTALLED_PROVIDERS  # noqa # isort:skip

# Note - we do not test protocols as they are not really part of the official API of
# Apache Airflow

logger = logging.getLogger(__name__)  # noqa

PY3 = sys.version_info[0] == 3


@click.group(context_settings={'help_option_names': ['-h', '--help'], 'max_content_width': 500})
def cli():
    ...


@cli.resultcallback()
def process_result(result):
    # This is special case - when the command executed returns false, it means that we are skipping
    # the package
    if result is False:
        raise click.exceptions.Exit(64)
    return result


option_git_update = click.option(
    '--git-update/--no-git-update',
    default=True,
    is_flag=True,
    help=f"If the git remote {HTTPS_REMOTE} already exists, don't try to update it",
)
option_version_suffix = click.option(
    "--version-suffix",
    metavar="suffix",
    help=textwrap.dedent(
        """
        adds version suffix to version of the packages.
        only useful when generating rc candidates for pypi."""
    ),
)
option_verbose = click.option(
    "--verbose",
    is_flag=True,
    help="Print verbose information about performed steps",
)
argument_package_id = click.argument('package_id')


@contextmanager
def with_group(title):
    """
    If used in GitHub Action, creates an expandable group in the GitHub Action log.
    Otherwise, display simple text groups.

    For more information, see:
    https://docs.github.com/en/free-pro-team@latest/actions/reference/workflow-commands-for-github-actions#grouping-log-lines
    """
    if os.environ.get('GITHUB_ACTIONS', 'false') != "true":
        print("[blue]" + "#" * 10 + ' ' + title + ' ' + "#" * 10 + "[/]")
        yield
        return
    print(f"::group::{title}")
    yield
    print("::endgroup::")


class EntityType(Enum):
    Operators = "Operators"
    Transfers = "Transfers"
    Sensors = "Sensors"
    Hooks = "Hooks"
    Secrets = "Secrets"


class EntityTypeSummary(NamedTuple):
    entities: Set[str]
    new_entities: List[str]
    moved_entities: Dict[str, str]
    new_entities_table: str
    moved_entities_table: str
    wrong_entities: List[Tuple[type, str]]


class VerifiedEntities(NamedTuple):
    all_entities: Set[str]
    wrong_entities: List[Tuple[type, str]]


class ProviderPackageDetails(NamedTuple):
    provider_package_id: str
    full_package_name: str
    source_provider_package_path: str
    documentation_provider_package_path: str
    provider_description: str
    versions: List[str]


ENTITY_NAMES = {
    EntityType.Operators: "Operators",
    EntityType.Transfers: "Transfer Operators",
    EntityType.Sensors: "Sensors",
    EntityType.Hooks: "Hooks",
    EntityType.Secrets: "Secrets",
}

TOTALS: Dict[EntityType, List[int]] = {
    EntityType.Operators: [0, 0],
    EntityType.Hooks: [0, 0],
    EntityType.Sensors: [0, 0],
    EntityType.Transfers: [0, 0],
    EntityType.Secrets: [0, 0],
}

OPERATORS_PATTERN = r".*Operator$"
SENSORS_PATTERN = r".*Sensor$"
HOOKS_PATTERN = r".*Hook$"
SECRETS_PATTERN = r".*Backend$"
TRANSFERS_PATTERN = r".*To[A-Z0-9].*Operator$"
WRONG_TRANSFERS_PATTERN = r".*Transfer$|.*TransferOperator$"

ALL_PATTERNS = {
    OPERATORS_PATTERN,
    SENSORS_PATTERN,
    HOOKS_PATTERN,
    SECRETS_PATTERN,
    TRANSFERS_PATTERN,
    WRONG_TRANSFERS_PATTERN,
}

EXPECTED_SUFFIXES: Dict[EntityType, str] = {
    EntityType.Operators: "Operator",
    EntityType.Hooks: "Hook",
    EntityType.Sensors: "Sensor",
    EntityType.Secrets: "Backend",
    EntityType.Transfers: "Operator",
}


def get_source_airflow_folder() -> str:
    """
    Returns source directory for whole airflow (from the main airflow project).

    :return: the folder path
    """
    return os.path.abspath(SOURCE_DIR_PATH)


def get_source_providers_folder() -> str:
    """
    Returns source directory for providers (from the main airflow project).

    :return: the folder path
    """
    return os.path.join(get_source_airflow_folder(), "airflow", "providers")


def get_target_folder() -> str:
    """
    Returns target directory for providers (in the provider_packages folder)

    :return: the folder path
    """
    return os.path.abspath(os.path.join(dirname(__file__), os.pardir, os.pardir, "provider_packages"))


def get_target_providers_folder() -> str:
    """
    Returns target directory for providers (in the provider_packages folder)

    :return: the folder path
    """
    return os.path.abspath(os.path.join(get_target_folder(), "airflow", "providers"))


def get_target_providers_package_folder(provider_package_id: str) -> str:
    """
    Returns target package folder based on package_id

    :return: the folder path
    """
    return os.path.join(get_target_providers_folder(), *provider_package_id.split("."))


DEPENDENCIES_JSON_FILE = os.path.join(PROVIDERS_PATH, "dependencies.json")

MOVED_ENTITIES: Dict[EntityType, Dict[str, str]] = {
    EntityType.Operators: {value[0]: value[1] for value in tests.deprecated_classes.OPERATORS},
    EntityType.Sensors: {value[0]: value[1] for value in tests.deprecated_classes.SENSORS},
    EntityType.Hooks: {value[0]: value[1] for value in tests.deprecated_classes.HOOKS},
    EntityType.Secrets: {value[0]: value[1] for value in tests.deprecated_classes.SECRETS},
    EntityType.Transfers: {value[0]: value[1] for value in tests.deprecated_classes.TRANSFERS},
}


def get_pip_package_name(provider_package_id: str) -> str:
    """
    Returns PIP package name for the package id.

    :param provider_package_id: id of the package
    :return: the name of pip package
    """
    return "apache-airflow-providers-" + provider_package_id.replace(".", "-")


def get_long_description(provider_package_id: str) -> str:
    """
    Gets long description of the package.

    :param provider_package_id: package id
    :return: content of the description: README file
    """
    package_folder = get_target_providers_package_folder(provider_package_id)
    readme_file = os.path.join(package_folder, "README.md")
    if not os.path.exists(readme_file):
        return ""
    with open(readme_file, encoding='utf-8', mode="r") as file:
        readme_contents = file.read()
    copying = True
    long_description = ""
    for line in readme_contents.splitlines(keepends=True):
        if line.startswith("**Table of contents**"):
            copying = False
            continue
        header_line = "## Provider package"
        if line.startswith(header_line):
            copying = True
        if copying:
            long_description += line
    return long_description


def get_install_requirements(provider_package_id: str) -> List[str]:
    """
    Returns install requirements for the package.

    :param provider_package_id: id of the provider package

    :return: install requirements of the package
    """
    dependencies = PROVIDERS_REQUIREMENTS[provider_package_id]
    airflow_dependency = 'apache-airflow>=2.0.0'
    # Avoid circular dependency for the preinstalled packages
    install_requires = [airflow_dependency] if provider_package_id not in PREINSTALLED_PROVIDERS else []
    install_requires.extend(dependencies)
    return install_requires


def get_setup_requirements() -> List[str]:
    """
    Returns setup requirements (common for all package for now).
    :return: setup requirements
    """
    return ['setuptools', 'wheel']


def get_package_extras(provider_package_id: str) -> Dict[str, List[str]]:
    """
    Finds extras for the package specified.

    :param provider_package_id: id of the package
    """
    if provider_package_id == 'providers':
        return {}
    with open(DEPENDENCIES_JSON_FILE) as dependencies_file:
        cross_provider_dependencies: Dict[str, List[str]] = json.load(dependencies_file)
    extras_dict = (
        {
            module: [get_pip_package_name(module)]
            for module in cross_provider_dependencies[provider_package_id]
        }
        if cross_provider_dependencies.get(provider_package_id)
        else {}
    )
    return extras_dict


def get_provider_packages() -> List[str]:
    """
    Returns all provider packages.

    """
    return list(PROVIDERS_REQUIREMENTS.keys())


def is_imported_from_same_module(the_class: str, imported_name: str) -> bool:
    """
    Is the class imported from another module?

    :param the_class: the class object itself
    :param imported_name: name of the imported class
    :return: true if the class was imported from another module
    """
    return ".".join(imported_name.split(".")[:-1]) == the_class.__module__


def is_example_dag(imported_name: str) -> bool:
    """
    Is the class an example_dag class?

    :param imported_name: name where the class is imported from
    :return: true if it is an example_dags class
    """
    return ".example_dags." in imported_name


def is_from_the_expected_base_package(the_class: Type, expected_package: str) -> bool:
    """
    Returns true if the class is from the package expected.
    :param the_class: the class object
    :param expected_package: package expected for the class
    :return:
    """
    return the_class.__module__.startswith(expected_package)


def inherits_from(the_class: Type, expected_ancestor: Optional[Type] = None) -> bool:
    """
    Returns true if the class inherits (directly or indirectly) from the class specified.
    :param the_class: The class to check
    :param expected_ancestor: expected class to inherit from
    :return: true is the class inherits from the class expected
    """
    if expected_ancestor is None:
        return False
    import inspect

    mro = inspect.getmro(the_class)
    return the_class is not expected_ancestor and expected_ancestor in mro


def is_class(the_class: Type) -> bool:
    """
    Returns true if the object passed is a class
    :param the_class: the class to pass
    :return: true if it is a class
    """
    import inspect

    return inspect.isclass(the_class)


def package_name_matches(the_class: Type, expected_pattern: Optional[str] = None) -> bool:
    """
    In case expected_pattern is set, it checks if the package name matches the pattern.
    .
    :param the_class: imported class
    :param expected_pattern: the pattern that should match the package
    :return: true if the expected_pattern is None or the pattern matches the package
    """
    return expected_pattern is None or re.match(expected_pattern, the_class.__module__) is not None


def find_all_entities(
    imported_classes: List[str],
    base_package: str,
    ancestor_match: Type,
    sub_package_pattern_match: str,
    expected_class_name_pattern: str,
    unexpected_class_name_patterns: Set[str],
    exclude_class_type: Optional[Type] = None,
    false_positive_class_names: Optional[Set[str]] = None,
) -> VerifiedEntities:
    """
    Returns set of entities containing all subclasses in package specified.

    :param imported_classes: entities imported from providers
    :param base_package: base package name where to start looking for the entities
    :param sub_package_pattern_match: this string is expected to appear in the sub-package name
    :param ancestor_match: type of the object the method looks for
    :param expected_class_name_pattern: regexp of class name pattern to expect
    :param unexpected_class_name_patterns: set of regexp of class name pattern that are not expected
    :param exclude_class_type: exclude class of this type (Sensor are also Operators so
           they should be excluded from the list)
    :param false_positive_class_names: set of class names that are wrongly recognised as badly named
    """
    found_entities: Set[str] = set()
    wrong_entities: List[Tuple[type, str]] = []
    for imported_name in imported_classes:
        module, class_name = imported_name.rsplit(".", maxsplit=1)
        the_class = getattr(importlib.import_module(module), class_name)
        if (
            is_class(the_class=the_class)
            and not is_example_dag(imported_name=imported_name)
            and is_from_the_expected_base_package(the_class=the_class, expected_package=base_package)
            and is_imported_from_same_module(the_class=the_class, imported_name=imported_name)
            and inherits_from(the_class=the_class, expected_ancestor=ancestor_match)
            and not inherits_from(the_class=the_class, expected_ancestor=exclude_class_type)
            and package_name_matches(the_class=the_class, expected_pattern=sub_package_pattern_match)
        ):

            if not false_positive_class_names or class_name not in false_positive_class_names:
                if not re.match(expected_class_name_pattern, class_name):
                    wrong_entities.append(
                        (
                            the_class,
                            f"The class name {class_name} is wrong. "
                            f"It should match {expected_class_name_pattern}",
                        )
                    )
                    continue
                if unexpected_class_name_patterns:
                    for unexpected_class_name_pattern in unexpected_class_name_patterns:
                        if re.match(unexpected_class_name_pattern, class_name):
                            wrong_entities.append(
                                (
                                    the_class,
                                    f"The class name {class_name} is wrong. "
                                    f"It should not match {unexpected_class_name_pattern}",
                                )
                            )
                        continue
            found_entities.add(imported_name)
    return VerifiedEntities(all_entities=found_entities, wrong_entities=wrong_entities)


def convert_new_classes_to_table(
    entity_type: EntityType, new_entities: List[str], full_package_name: str
) -> str:
    """
    Converts new entities tp a markdown table.

    :param entity_type: list of entities to convert to markup
    :param new_entities: list of new entities
    :param full_package_name: name of the provider package
    :return: table of new classes
    """
    from tabulate import tabulate

    headers = [f"New Airflow 2.0 {entity_type.value.lower()}: `{full_package_name}` package"]
    table = [(get_class_code_link(full_package_name, class_name, "master"),) for class_name in new_entities]
    return tabulate(table, headers=headers, tablefmt="pipe")


def convert_moved_classes_to_table(
    entity_type: EntityType,
    moved_entities: Dict[str, str],
    full_package_name: str,
) -> str:
    """
    Converts moved entities to a markdown table
    :param entity_type: type of entities -> operators, sensors etc.
    :param moved_entities: dictionary of moved entities `to -> from`
    :param full_package_name: name of the provider package
    :return: table of moved classes
    """
    from tabulate import tabulate

    headers = [
        f"Airflow 2.0 {entity_type.value.lower()}: `{full_package_name}` package",
        "Airflow 1.10.* previous location (usually `airflow.contrib`)",
    ]
    table = [
        (
            get_class_code_link(full_package_name, to_class, "master"),
            get_class_code_link("airflow", moved_entities[to_class], "v1-10-stable"),
        )
        for to_class in sorted(moved_entities.keys())
    ]
    return tabulate(table, headers=headers, tablefmt="pipe")


def get_details_about_classes(
    entity_type: EntityType,
    entities: Set[str],
    wrong_entities: List[Tuple[type, str]],
    full_package_name: str,
) -> EntityTypeSummary:
    """
    Splits the set of entities into new and moved, depending on their presence in the dict of objects
    retrieved from the test_contrib_to_core. Updates all_entities with the split class.

    :param entity_type: type of entity (Operators, Hooks etc.)
    :param entities: set of entities found
    :param wrong_entities: wrong entities found for that type
    :param full_package_name: full package name
    :return:
    """
    dict_of_moved_classes = MOVED_ENTITIES[entity_type]
    new_entities = []
    moved_entities = {}
    for obj in entities:
        if obj in dict_of_moved_classes:
            moved_entities[obj] = dict_of_moved_classes[obj]
            del dict_of_moved_classes[obj]
        else:
            new_entities.append(obj)
    new_entities.sort()
    TOTALS[entity_type][0] += len(new_entities)
    TOTALS[entity_type][1] += len(moved_entities)
    return EntityTypeSummary(
        entities=entities,
        new_entities=new_entities,
        moved_entities=moved_entities,
        new_entities_table=convert_new_classes_to_table(
            entity_type=entity_type,
            new_entities=new_entities,
            full_package_name=full_package_name,
        ),
        moved_entities_table=convert_moved_classes_to_table(
            entity_type=entity_type,
            moved_entities=moved_entities,
            full_package_name=full_package_name,
        ),
        wrong_entities=wrong_entities,
    )


def strip_package_from_class(base_package: str, class_name: str) -> str:
    """
    Strips base package name from the class (if it starts with the package name).
    """
    if class_name.startswith(base_package):
        return class_name[len(base_package) + 1 :]
    else:
        return class_name


def convert_class_name_to_url(base_url: str, class_name) -> str:
    """
    Converts the class name to URL that the class can be reached

    :param base_url: base URL to use
    :param class_name: name of the class
    :return: URL to the class
    """
    return base_url + os.path.sep.join(class_name.split(".")[:-1]) + ".py"


def get_class_code_link(base_package: str, class_name: str, git_tag: str) -> str:
    """
    Provides markdown link for the class passed as parameter.

    :param base_package: base package to strip from most names
    :param class_name: name of the class
    :param git_tag: tag to use for the URL link
    :return: URL to the class
    """
    url_prefix = f'https://github.com/apache/airflow/blob/{git_tag}/'
    return (
        f'[{strip_package_from_class(base_package, class_name)}]'
        f'({convert_class_name_to_url(url_prefix, class_name)})'
    )


def print_wrong_naming(entity_type: EntityType, wrong_classes: List[Tuple[type, str]]):
    """
    Prints wrong entities of a given entity type if there are any
    :param entity_type: type of the class to print
    :param wrong_classes: list of wrong entities
    """
    if wrong_classes:
        print(f"\n[red]There are wrongly named entities of type {entity_type}:[/]\n", file=sys.stderr)
        for wrong_entity_type, message in wrong_classes:
            print(f"{wrong_entity_type}: {message}", file=sys.stderr)


def get_package_class_summary(
    full_package_name: str, imported_classes: List[str]
) -> Dict[EntityType, EntityTypeSummary]:
    """
    Gets summary of the package in the form of dictionary containing all types of entities
    :param full_package_name: full package name
    :param imported_classes: entities imported_from providers
    :return: dictionary of objects usable as context for JINJA2 templates - or None if there are some errors
    """
    from airflow.hooks.base import BaseHook
    from airflow.models.baseoperator import BaseOperator
    from airflow.secrets import BaseSecretsBackend
    from airflow.sensors.base import BaseSensorOperator

    all_verified_entities: Dict[EntityType, VerifiedEntities] = {
        EntityType.Operators: find_all_entities(
            imported_classes=imported_classes,
            base_package=full_package_name,
            sub_package_pattern_match=r".*\.operators\..*",
            ancestor_match=BaseOperator,
            expected_class_name_pattern=OPERATORS_PATTERN,
            unexpected_class_name_patterns=ALL_PATTERNS - {OPERATORS_PATTERN},
            exclude_class_type=BaseSensorOperator,
            false_positive_class_names={
                'CloudVisionAddProductToProductSetOperator',
                'CloudDataTransferServiceGCSToGCSOperator',
                'CloudDataTransferServiceS3ToGCSOperator',
                'BigQueryCreateDataTransferOperator',
                'CloudTextToSpeechSynthesizeOperator',
                'CloudSpeechToTextRecognizeSpeechOperator',
            },
        ),
        EntityType.Sensors: find_all_entities(
            imported_classes=imported_classes,
            base_package=full_package_name,
            sub_package_pattern_match=r".*\.sensors\..*",
            ancestor_match=BaseSensorOperator,
            expected_class_name_pattern=SENSORS_PATTERN,
            unexpected_class_name_patterns=ALL_PATTERNS - {OPERATORS_PATTERN, SENSORS_PATTERN},
        ),
        EntityType.Hooks: find_all_entities(
            imported_classes=imported_classes,
            base_package=full_package_name,
            sub_package_pattern_match=r".*\.hooks\..*",
            ancestor_match=BaseHook,
            expected_class_name_pattern=HOOKS_PATTERN,
            unexpected_class_name_patterns=ALL_PATTERNS - {HOOKS_PATTERN},
        ),
        EntityType.Secrets: find_all_entities(
            imported_classes=imported_classes,
            sub_package_pattern_match=r".*\.secrets\..*",
            base_package=full_package_name,
            ancestor_match=BaseSecretsBackend,
            expected_class_name_pattern=SECRETS_PATTERN,
            unexpected_class_name_patterns=ALL_PATTERNS - {SECRETS_PATTERN},
        ),
        EntityType.Transfers: find_all_entities(
            imported_classes=imported_classes,
            base_package=full_package_name,
            sub_package_pattern_match=r".*\.transfers\..*",
            ancestor_match=BaseOperator,
            expected_class_name_pattern=TRANSFERS_PATTERN,
            unexpected_class_name_patterns=ALL_PATTERNS - {OPERATORS_PATTERN, TRANSFERS_PATTERN},
        ),
    }
    for entity in EntityType:
        print_wrong_naming(entity, all_verified_entities[entity].wrong_entities)

    entities_summary: Dict[EntityType, EntityTypeSummary] = {}  # noqa

    for entity_type in EntityType:
        entities_summary[entity_type] = get_details_about_classes(
            entity_type,
            all_verified_entities[entity_type].all_entities,
            all_verified_entities[entity_type].wrong_entities,
            full_package_name,
        )

    return entities_summary


def render_template(
    template_name: str,
    context: Dict[str, Any],
    extension: str,
    autoescape: bool = True,
    keep_trailing_newline: bool = False,
) -> str:
    """
    Renders template based on it's name. Reads the template from <name>_TEMPLATE.md.jinja2 in current dir.
    :param template_name: name of the template to use
    :param context: Jinja2 context
    :param extension: Target file extension
    :param autoescape: Whether to autoescape HTML
    :param keep_trailing_newline: Whether to keep the newline in rendered output
    :return: rendered template
    """
    import jinja2

    template_loader = jinja2.FileSystemLoader(searchpath=MY_DIR_PATH)
    template_env = jinja2.Environment(
        loader=template_loader,
        undefined=jinja2.StrictUndefined,
        autoescape=autoescape,
        keep_trailing_newline=keep_trailing_newline,
    )
    template = template_env.get_template(f"{template_name}_TEMPLATE{extension}.jinja2")
    content: str = template.render(context)
    return content


def convert_git_changes_to_table(
    print_version: Optional[str], changes: str, base_url: str, markdown: bool = True
) -> str:
    """
    Converts list of changes from it's string form to markdown table.

    The changes are in the form of multiple lines where each line consists of:
    FULL_COMMIT_HASH SHORT_COMMIT_HASH COMMIT_DATE COMMIT_SUBJECT

    The subject can contain spaces but one of the preceding values can, so we can make split
    3 times on spaces to break it up.
    :param print_version: Version to print
    :param changes: list of changes in a form of multiple-line string
    :param base_url: base url for the commit URL
    :param markdown: if True, markdown format is used else rst
    :return: formatted table
    """
    from tabulate import tabulate

    lines = changes.split("\n")
    headers = ["Commit", "Committed", "Subject"]
    table_data = []
    for line in lines:
        if line == "":
            continue
        full_hash, short_hash, date, message = line.split(" ", maxsplit=3)
        message_without_backticks = message.replace("`", "'")
        table_data.append(
            (
                f"[{short_hash}]({base_url}{full_hash})"
                if markdown
                else f"`{short_hash} <{base_url}{full_hash}>`_",
                date,
                f"`{message_without_backticks}`" if markdown else f"``{message_without_backticks}``",
            )
        )
    header = ""
    if not table_data:
        return header
    table = tabulate(table_data, headers=headers, tablefmt="pipe" if markdown else "rst")
    if not markdown:
        header += f"\n\n{print_version}\n" + "." * (len(print_version) if print_version else 0) + "\n\n"
        release_date = table_data[0][1]
        header += f"Latest change: {release_date}\n\n"
    return header + table


def convert_pip_requirements_to_table(requirements: Iterable[str], markdown: bool = True) -> str:
    """
    Converts PIP requirement list to a markdown table.
    :param requirements: requirements list
    :param markdown: if True, markdown format is used else rst
    :return: formatted table
    """
    from tabulate import tabulate

    headers = ["PIP package", "Version required"]
    table_data = []
    for dependency in requirements:
        found = re.match(r"(^[^<=>~]*)([^<=>~]?.*)$", dependency)
        if found:
            package = found.group(1)
            version_required = found.group(2)
            if version_required != "":
                version_required = f"`{version_required}`" if markdown else f'``{version_required}``'
            table_data.append((f"`{package}`" if markdown else f"``{package}``", version_required))
        else:
            table_data.append((dependency, ""))
    return tabulate(table_data, headers=headers, tablefmt="pipe" if markdown else "rst")


def convert_cross_package_dependencies_to_table(
    cross_package_dependencies: List[str],
    markdown: bool = True,
) -> str:
    """
    Converts cross-package dependencies to a markdown table
    :param cross_package_dependencies: list of cross-package dependencies
    :param markdown: if True, markdown format is used else rst
    :return: formatted table
    """
    from tabulate import tabulate

    headers = ["Dependent package", "Extra"]
    table_data = []
    prefix = "apache-airflow-providers-"
    base_url = "https://airflow.apache.org/docs/"
    for dependency in cross_package_dependencies:
        pip_package_name = f"{prefix}{dependency.replace('.','-')}"
        url_suffix = f"{dependency.replace('.','-')}"
        if markdown:
            url = f"[{pip_package_name}]({base_url}{url_suffix})"
        else:
            url = f"`{pip_package_name} <{base_url}{prefix}{url_suffix}>`_"
        table_data.append((url, f"`{dependency}`" if markdown else f"``{dependency}``"))
    return tabulate(table_data, headers=headers, tablefmt="pipe" if markdown else "rst")


LICENCE = """<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->
"""

LICENCE_RST = """
.. Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.
"""

"""
Keeps information about historical releases.
"""
ReleaseInfo = collections.namedtuple(
    "ReleaseInfo", "release_version release_version_no_leading_zeros last_commit_hash content file_name"
)


def strip_leading_zeros(version: str) -> str:
    """
    Strips leading zeros from version number.

    This converts 1974.04.03 to 1974.4.3 as the format with leading month and day zeros is not accepted
    by PIP versioning.

    :param version: version number in CALVER format (potentially with leading 0s in date and month)
    :return: string with leading 0s after dot replaced.
    """
    return ".".join(str(int(i)) for i in version.split("."))


def get_previous_release_info(
    previous_release_version: Optional[str], past_releases: List[ReleaseInfo], current_release_version: str
) -> Optional[str]:
    """
    Find previous release. In case we are re-running current release we assume that last release was
    the previous one. This is needed so that we can generate list of changes since the previous release.
    :param previous_release_version: known last release version
    :param past_releases: list of past releases
    :param current_release_version: release that we are working on currently
    :return:
    """
    previous_release = None
    if previous_release_version == current_release_version:
        # Re-running for current release - use previous release as base for git log
        if len(past_releases) > 1:
            previous_release = past_releases[1].last_commit_hash
    else:
        previous_release = past_releases[0].last_commit_hash if past_releases else None
    return previous_release


def check_if_release_version_ok(
    past_releases: List[ReleaseInfo],
    current_release_version: str,
) -> Tuple[str, Optional[str]]:
    """
    Check if the release version passed is not later than the last release version
    :param past_releases: all past releases (if there are any)
    :param current_release_version: release version to check
    :return: Tuple of current/previous_release (previous might be None if there are no releases)
    """
    previous_release_version = past_releases[0].release_version if past_releases else None
    if current_release_version == '':
        if previous_release_version:
            current_release_version = previous_release_version
        else:
            current_release_version = (datetime.today() + timedelta(days=5)).strftime('%Y.%m.%d')
    if previous_release_version:
        if Version(current_release_version) < Version(previous_release_version):
            print(
                f"[red]The release {current_release_version} must be not less than "
                f"{previous_release_version} - last release for the package[/]",
                file=sys.stderr,
            )
            raise Exception("Bad release version")
    return current_release_version, previous_release_version


def get_cross_provider_dependent_packages(provider_package_id: str) -> List[str]:
    """
    Returns cross-provider dependencies for the package.
    :param provider_package_id: package id
    :return: list of cross-provider dependencies
    """
    with open(os.path.join(PROVIDERS_PATH, "dependencies.json")) as dependencies_file:
        dependent_packages = json.load(dependencies_file).get(provider_package_id) or []
    return dependent_packages


def make_sure_remote_apache_exists_and_fetch(git_update: bool, verbose: bool):
    """
    Make sure that apache remote exist in git. We need to take a log from the apache
    repository - not locally.

    Also the local repo might be shallow so we need to unshallow it.

    This will:
    * check if the remote exists and add if it does not
    * check if the local repo is shallow, mark it to be unshallowed in this case
    * fetch from the remote including all tags and overriding local tags in case they are set differently

    :param git_update: If the git remote already exists, should we try to update it
    :param verbose: print verbose messages while fetching
    """
    try:
        check_remote_command = ["git", "remote", "get-url", HTTPS_REMOTE]
        if verbose:
            print(f"Running command: '{' '.join(check_remote_command)}'")
        subprocess.check_call(
            check_remote_command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        # Remote already exists, don't update it again!
        if not git_update:
            return
    except subprocess.CalledProcessError as ex:
        if ex.returncode == 128:
            remote_add_command = [
                "git",
                "remote",
                "add",
                HTTPS_REMOTE,
                "https://github.com/apache/airflow.git",
            ]
            if verbose:
                print(f"Running command: '{' '.join(remote_add_command)}'")
            try:
                subprocess.check_output(
                    remote_add_command,
                    stderr=subprocess.STDOUT,
                )
            except subprocess.CalledProcessError as ex:
                print("[red]Error: when adding remote:[/]", ex)
        else:
            raise
    if verbose:
        print("Fetching full history and tags from remote. ")
        print("This might override your local tags!")
    is_shallow_repo = (
        subprocess.check_output(["git", "rev-parse", "--is-shallow-repository"], stderr=subprocess.DEVNULL)
        == 'true'
    )
    fetch_command = ["git", "fetch", "--tags", "--force", HTTPS_REMOTE]
    if is_shallow_repo:
        if verbose:
            print(
                "This will also unshallow the repository, "
                "making all history available and increasing storage!"
            )
        fetch_command.append("--unshallow")
    if verbose:
        print(f"Running command: '{' '.join(fetch_command)}'")
    subprocess.check_call(
        fetch_command,
        stderr=subprocess.DEVNULL,
    )


def get_git_log_command(
    verbose: bool, from_commit: Optional[str] = None, to_commit: Optional[str] = None
) -> List[str]:
    """
    Get git command to run for the current repo from the current folder (which is the package folder).
    :param verbose: whether to print verbose info while getting the command
    :param from_commit: if present - base commit from which to start the log from
    :param to_commit: if present - final commit which should be the start of the log
    :return: git command to run
    """
    git_cmd = [
        "git",
        "log",
        "--pretty=format:%H %h %cd %s",
        "--date=short",
    ]
    if from_commit and to_commit:
        git_cmd.append(f"{from_commit}...{to_commit}")
    elif from_commit:
        git_cmd.append(from_commit)
    git_cmd.extend(['--', '.'])
    if verbose:
        print(f"Command to run: '{' '.join(git_cmd)}'")
    return git_cmd


def get_git_tag_check_command(tag: str) -> List[str]:
    """
    Get git command to check if tag exits.
    :param tag: Tag to check
    :return: git command to run
    """
    return [
        "git",
        "rev-parse",
        tag,
    ]


def get_source_package_path(provider_package_id: str) -> str:
    """
    Retrieves source package path from package id.
    :param provider_package_id: id of the package
    :return: path of the providers folder
    """
    return os.path.join(PROVIDERS_PATH, *provider_package_id.split("."))


def get_documentation_package_path(provider_package_id: str) -> str:
    """
    Retrieves documentation package path from package id.
    :param provider_package_id: id of the package
    :return: path of the documentation folder
    """
    return os.path.join(
        DOCUMENTATION_PATH, f"apache-airflow-providers-{provider_package_id.replace('.','-')}"
    )


def get_generated_package_path(provider_package_id: str) -> str:
    """
    Retrieves generated package path from package id.
    :param provider_package_id: id of the package
    :return: path of the providers folder
    """
    provider_package_path = os.path.join(GENERATED_PROVIDERS_PATH, *provider_package_id.split("."))
    return provider_package_path


def get_additional_package_info(provider_package_path: str) -> str:
    """
    Returns additional info for the package.

    :param provider_package_path: path for the package
    :return: additional information for the path (empty string if missing)
    """
    additional_info_file_path = os.path.join(provider_package_path, "ADDITIONAL_INFO.md")
    if os.path.isfile(additional_info_file_path):
        with open(additional_info_file_path) as additional_info_file:
            additional_info = additional_info_file.read()

        additional_info_lines = additional_info.splitlines(keepends=True)
        result = ""
        skip_comment = True
        for line in additional_info_lines:
            if line.startswith(" -->"):
                skip_comment = False
                continue
            if not skip_comment:
                result += line
        return result
    return ""


def get_changelog_for_package(provider_package_path: str) -> str:
    """
    Returns changelog_for the package.

    :param provider_package_path: path for the package
    :return: additional information for the path (empty string if missing)
    """
    changelog_path = os.path.join(provider_package_path, "CHANGELOG.rst")
    if os.path.isfile(changelog_path):
        with open(changelog_path) as changelog_file:
            return changelog_file.read()
    else:
        print(f"[red]ERROR: Missing ${changelog_path}[/]")
        print("Please add the file with initial content:")
        print()
        syntax = Syntax(
            INITIAL_CHANGELOG_CONTENT,
            "rst",
            theme="ansi_dark",
        )
    console = Console(width=100)
    console.print(syntax)
    print()
    raise Exception(f"Missing {changelog_path}")


def is_camel_case_with_acronyms(s: str):
    """
    Checks if the string passed is Camel Case (with capitalised acronyms allowed).
    :param s: string to check
    :return: true if the name looks cool as Class name.
    """
    return s != s.lower() and s != s.upper() and "_" not in s and s[0].upper() == s[0]


def check_if_classes_are_properly_named(
    entity_summary: Dict[EntityType, EntityTypeSummary]
) -> Tuple[int, int]:
    """
    Check if all entities in the dictionary are named properly. It prints names at the output
    and returns the status of class names.

    :param entity_summary: dictionary of class names to check, grouped by types.
    :return: Tuple of 2 ints = total number of entities and number of badly named entities
    """
    total_class_number = 0
    badly_named_class_number = 0
    for entity_type, class_suffix in EXPECTED_SUFFIXES.items():
        for class_full_name in entity_summary[entity_type].entities:
            _, class_name = class_full_name.rsplit(".", maxsplit=1)
            error_encountered = False
            if not is_camel_case_with_acronyms(class_name):
                print(
                    f"[red]The class {class_full_name} is wrongly named. The "
                    f"class name should be CamelCaseWithACRONYMS ![/]"
                )
                error_encountered = True
            if not class_name.endswith(class_suffix):
                print(
                    f"[red]The class {class_full_name} is wrongly named. It is one of the {entity_type.value}"
                    f" so it should end with {class_suffix}[/]"
                )
                error_encountered = True
            total_class_number += 1
            if error_encountered:
                badly_named_class_number += 1
    return total_class_number, badly_named_class_number


def get_package_pip_name(provider_package_id: str):
    return f"apache-airflow-providers-{provider_package_id.replace('.', '-')}"


def validate_provider_info_with_2_0_0_schema(provider_info: Dict[str, Any]) -> None:
    """
    Validates provider info against 2.0.0 schema. We need to run this validation until we make Airflow
    2.0.0 yank and add apache-airflow>=2.0.1 (possibly) to provider dependencies.

    :param provider_info: provider info to validate
    """

    with open(PROVIDER_2_0_0_DATA_SCHEMA_PATH) as schema_file:
        schema = json.load(schema_file)
    try:
        jsonschema.validate(provider_info, schema=schema)
    except jsonschema.ValidationError as ex:
        print("[red]Provider info validated not against 2.0.0 schema[/]")
        raise Exception(
            "Error when validating schema. The schema must be Airflow 2.0.0 compatible. "
            "If you added any fields please remove them via 'convert_to_provider_info' method.",
            ex,
        )


def validate_provider_info_with_runtime_schema(provider_info: Dict[str, Any]) -> None:
    """
    Validates provider info against the runtime schema. This way we check if the provider info in the
    packages is future-compatible. The Runtime Schema should only change when there is a major version
    change.

    :param provider_info: provider info to validate
    """

    with open(PROVIDER_RUNTIME_DATA_SCHEMA_PATH) as schema_file:
        schema = json.load(schema_file)
    try:
        jsonschema.validate(provider_info, schema=schema)
    except jsonschema.ValidationError as ex:
        print("[red]Provider info not validated against runtime schema[/]")
        raise Exception(
            "Error when validating schema. The schema must be compatible with "
            + "airflow/provider_info.schema.json. "
            + "If you added any fields please remove them via 'convert_to_provider_info' method.",
            ex,
        )


def convert_to_provider_info(provider_info: Dict[str, Any]) -> Dict[str, Any]:
    """
    In Airflow 2.0.0 we set 'additionalProperties" to 'false' in provider's schema, which makes the schema
    non future-compatible.

    While we changed tho additionalProperties to 'true' in 2.0.1, we have to
    make sure that the returned provider_info when preparing package is compatible with the older version
    of the schema and remove all the newly added fields until we deprecate (possibly even yank) 2.0.0
    and make provider packages depend on Airflow >=2.0.1.

    Currently we have two provider schemas:
    * provider.yaml.schema.json that is used to verify the schema while it is developed (it has, for example
      additionalProperties set to false, to avoid typos in field names). This is the full set of
      fields that are used for both: runtime information and documentation building.
    * provider_info.schema.json that is used to verify the schema at runtime - it only contains
      fields from provider.yaml that are necessary for runtime provider discovery.

      This method converts the full provider.yaml schema into the limited version needed at runtime.
    """
    updated_provider_info = deepcopy(provider_info)
    expression = jsonpath_ng.parse("[hooks,operators,integrations,sensors,transfers]")
    return expression.filter(lambda x: True, updated_provider_info)


def get_provider_info_from_provider_yaml(provider_package_id: str) -> Dict[str, Any]:
    """
    Retrieves provider info from the provider yaml file. The provider yaml file contains more information
    than provider_info that is used at runtime. This method converts the full provider yaml file into
    stripped-down provider info and validates it against deprecated 2.0.0 schema and runtime schema.
    :param provider_package_id: package id to retrieve provider.yaml from
    :return: provider_info dictionary
    """
    provider_yaml_file_name = os.path.join(get_source_package_path(provider_package_id), "provider.yaml")
    if not os.path.exists(provider_yaml_file_name):
        raise Exception(f"The provider.yaml file is missing: {provider_yaml_file_name}")
    with open(provider_yaml_file_name) as provider_file:
        provider_yaml_dict = yaml.load(provider_file, SafeLoader)  # noqa
    provider_info = convert_to_provider_info(provider_yaml_dict)
    validate_provider_info_with_2_0_0_schema(provider_info)
    validate_provider_info_with_runtime_schema(provider_info)
    return provider_info


def get_version_tag(version: str, provider_package_id: str, version_suffix: str = ''):
    if version_suffix is None:
        version_suffix = ''
    return f"providers-{provider_package_id.replace('.','-')}/{version}{version_suffix}"


def print_changes_table(changes_table):
    syntax = Syntax(changes_table, "rst", theme="ansi_dark")
    console = Console(width=200)
    console.print(syntax)


def get_all_changes_for_regular_packages(
    versions: List[str],
    provider_package_id: str,
    source_provider_package_path: str,
    verbose: bool,
) -> Tuple[bool, str]:
    current_version = versions[0]
    current_tag_no_suffix = get_version_tag(current_version, provider_package_id)
    if verbose:
        print(f"Checking if tag '{current_tag_no_suffix}' exist.")
    if not subprocess.call(
        get_git_tag_check_command(current_tag_no_suffix),
        cwd=source_provider_package_path,
        stderr=subprocess.DEVNULL,
    ):
        if verbose:
            print(f"The tag {current_tag_no_suffix} exists.")
        # The tag already exists
        changes = subprocess.check_output(
            get_git_log_command(verbose, HEAD_OF_HTTPS_REMOTE, current_tag_no_suffix),
            cwd=source_provider_package_path,
            universal_newlines=True,
        )
        if changes:
            print(
                f"[yellow]The provider {provider_package_id} has changes"
                f" since last release but version is not updated[/]"
            )
            print()
            print(
                "[yellow]Please update version in "
                f"'airflow/providers/{provider_package_id.replace('-','/')}/'"
                "provider.yaml' to prepare release.[/]\n"
            )
            changes_table = convert_git_changes_to_table(
                "UNKNOWN", changes, base_url="https://github.com/apache/airflow/commit/", markdown=False
            )
            print_changes_table(changes_table)
            return False, changes_table
        else:
            print(f"No changes for {provider_package_id}")
            return False, ""
    if verbose:
        print("The tag does not exist. ")
    if len(versions) == 1:
        print(f"The provider '{provider_package_id}' has never been released but it is ready to release!\n")
    else:
        print(f"New version of the '{provider_package_id}' package is ready to be released!\n")
    next_version_tag = HEAD_OF_HTTPS_REMOTE
    changes_table = ''
    print_version = versions[0]
    for version in versions[1:]:
        version_tag = get_version_tag(version, provider_package_id)
        changes = subprocess.check_output(
            get_git_log_command(verbose, next_version_tag, version_tag),
            cwd=source_provider_package_path,
            universal_newlines=True,
        )
        changes_table += convert_git_changes_to_table(
            print_version, changes, base_url="https://github.com/apache/airflow/commit/", markdown=False
        )
        next_version_tag = version_tag
        print_version = version
    changes = subprocess.check_output(
        get_git_log_command(verbose, next_version_tag),
        cwd=source_provider_package_path,
        universal_newlines=True,
    )
    changes_table += convert_git_changes_to_table(
        print_version, changes, base_url="https://github.com/apache/airflow/commit/", markdown=False
    )
    if verbose:
        print_changes_table(changes_table)
    return True, changes_table


def get_provider_details(provider_package_id: str) -> ProviderPackageDetails:
    provider_info = get_provider_info_from_provider_yaml(provider_package_id)
    return ProviderPackageDetails(
        provider_package_id=provider_package_id,
        full_package_name=f"airflow.providers.{provider_package_id}",
        source_provider_package_path=get_source_package_path(provider_package_id),
        documentation_provider_package_path=get_documentation_package_path(provider_package_id),
        provider_description=provider_info['description'],
        versions=provider_info['versions'],
    )


def get_provider_jinja_context(
    provider_details: ProviderPackageDetails,
    current_release_version: str,
    version_suffix: str,
):
    verify_provider_package(provider_details.provider_package_id)
    cross_providers_dependencies = get_cross_provider_dependent_packages(
        provider_package_id=provider_details.provider_package_id
    )
    release_version_no_leading_zeros = strip_leading_zeros(current_release_version)
    pip_requirements_table = convert_pip_requirements_to_table(
        PROVIDERS_REQUIREMENTS[provider_details.provider_package_id]
    )
    pip_requirements_table_rst = convert_pip_requirements_to_table(
        PROVIDERS_REQUIREMENTS[provider_details.provider_package_id], markdown=False
    )
    cross_providers_dependencies_table = convert_cross_package_dependencies_to_table(
        cross_providers_dependencies
    )
    cross_providers_dependencies_table_rst = convert_cross_package_dependencies_to_table(
        cross_providers_dependencies, markdown=False
    )
    context: Dict[str, Any] = {
        "ENTITY_TYPES": list(EntityType),
        "README_FILE": "README.rst",
        "PROVIDER_PACKAGE_ID": provider_details.provider_package_id,
        "PACKAGE_PIP_NAME": get_pip_package_name(provider_details.provider_package_id),
        "FULL_PACKAGE_NAME": provider_details.full_package_name,
        "PROVIDER_PATH": provider_details.full_package_name.replace(".", "/"),
        "RELEASE": current_release_version,
        "RELEASE_NO_LEADING_ZEROS": release_version_no_leading_zeros,
        "VERSION_SUFFIX": version_suffix or '',
        "ADDITIONAL_INFO": get_additional_package_info(
            provider_package_path=provider_details.source_provider_package_path
        ),
        "CHANGELOG": get_changelog_for_package(
            provider_package_path=provider_details.source_provider_package_path
        ),
        "CROSS_PROVIDERS_DEPENDENCIES": cross_providers_dependencies,
        "PIP_REQUIREMENTS": PROVIDERS_REQUIREMENTS[provider_details.provider_package_id],
        "PROVIDER_TYPE": "Provider",
        "PROVIDERS_FOLDER": "providers",
        "PROVIDER_DESCRIPTION": provider_details.provider_description,
        "INSTALL_REQUIREMENTS": get_install_requirements(
            provider_package_id=provider_details.provider_package_id
        ),
        "SETUP_REQUIREMENTS": get_setup_requirements(),
        "EXTRAS_REQUIREMENTS": get_package_extras(provider_package_id=provider_details.provider_package_id),
        "CROSS_PROVIDERS_DEPENDENCIES_TABLE": cross_providers_dependencies_table,
        "CROSS_PROVIDERS_DEPENDENCIES_TABLE_RST": cross_providers_dependencies_table_rst,
        "PIP_REQUIREMENTS_TABLE": pip_requirements_table,
        "PIP_REQUIREMENTS_TABLE_RST": pip_requirements_table_rst,
    }
    return context


def prepare_readme_file(context):
    readme_content = LICENCE_RST
    readme_template_name = PROVIDER_TEMPLATE_PREFIX + "README"
    readme_content += render_template(template_name=readme_template_name, context=context, extension=".rst")
    readme_file_path = os.path.join(TARGET_PROVIDER_PACKAGES_PATH, "README.rst")
    with open(readme_file_path, "wt") as readme_file:
        readme_file.write(readme_content)


def update_generated_files_for_regular_package(
    provider_package_id: str,
    version_suffix: str,
    update_release_notes: bool,
    update_setup: bool,
    verbose: bool,
) -> bool:
    """
    Updates generated files (readme, changes and/or setup.cfg/setup.py/manifest.in/provider_info)

    :param provider_package_id: id of the package
    :param version_suffix: version suffix corresponding to the version in the code
    :param update_release_notes: whether to update release notes
    :param update_setup: whether to update setup files
    :param verbose: whether to print verbose messages
    :returns False if the package should be skipped, Tre if everything generated properly
    """
    verify_provider_package(provider_package_id)
    provider_details = get_provider_details(provider_package_id)
    provider_info = get_provider_info_from_provider_yaml(provider_package_id)
    current_release_version = provider_details.versions[0]
    jinja_context = get_provider_jinja_context(
        provider_details=provider_details,
        current_release_version=current_release_version,
        version_suffix=version_suffix,
    )
    jinja_context["PROVIDER_INFO"] = provider_info
    if update_release_notes:
        proceed, changes = get_all_changes_for_regular_packages(
            provider_details.versions,
            provider_package_id,
            provider_details.source_provider_package_path,
            verbose,
        )
        if not proceed:
            print()
            print(
                f"[yellow]Provider: {provider_package_id} - skipping documentation generation. No changes![/]"
            )
            print()
            return False
        jinja_context["DETAILED_CHANGES_RST"] = changes
        jinja_context["DETAILED_CHANGES_PRESENT"] = len(changes) > 0
        print()
        print(f"Update index.rst for {provider_package_id}")
        print()
        update_index_rst_for_regular_providers(
            jinja_context, provider_package_id, provider_details.documentation_provider_package_path
        )
        update_commits_rst_for_regular_providers(
            jinja_context, provider_package_id, provider_details.documentation_provider_package_path
        )
    if update_setup:
        print()
        print(f"Generating setup files for {provider_package_id}")
        print()
        prepare_setup_py_file(jinja_context)
        prepare_setup_cfg_file(jinja_context)
        prepare_get_provider_info_py_file(jinja_context, provider_package_id)
        prepare_manifest_in_file(jinja_context)
        prepare_readme_file(jinja_context)
    return True


def replace_content(file_path, old_text, new_text, provider_package_id):
    if new_text != old_text:
        _, temp_file_path = tempfile.mkstemp()
        try:
            if os.path.isfile(file_path):
                copyfile(file_path, temp_file_path)
            with open(file_path, "wt") as readme_file:
                readme_file.write(new_text)
            print()
            print(f"Generated {file_path} file for the {provider_package_id} provider")
            print()
            if old_text != "":
                subprocess.call(["diff", "--color=always", temp_file_path, file_path])
        finally:
            os.remove(temp_file_path)


AUTOMATICALLY_GENERATED_CONTENT = (
    ".. THE REMINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!"
)


def update_index_rst_for_regular_providers(
    context,
    provider_package_id,
    target_path,
):
    index_template_name = PROVIDER_TEMPLATE_PREFIX + "INDEX"
    index_update = render_template(
        template_name=index_template_name, context=context, extension='.rst', keep_trailing_newline=False
    )
    index_file_path = os.path.join(target_path, "index.rst")
    old_text = ""
    if os.path.isfile(index_file_path):
        with open(index_file_path) as readme_file_read:
            old_text = readme_file_read.read()
    new_text = deepcopy(old_text)
    lines = old_text.splitlines(keepends=False)
    for index, line in enumerate(lines):
        if line == AUTOMATICALLY_GENERATED_CONTENT:
            new_text = "\n".join(lines[:index])
    new_text += "\n" + AUTOMATICALLY_GENERATED_CONTENT + "\n"
    new_text += index_update
    replace_content(index_file_path, old_text, new_text, provider_package_id)


def update_commits_rst_for_regular_providers(
    context,
    provider_package_id,
    target_path,
):
    commits_template_name = PROVIDER_TEMPLATE_PREFIX + "COMMITS"
    new_text = render_template(
        template_name=commits_template_name, context=context, extension='.rst', keep_trailing_newline=True
    )
    index_file_path = os.path.join(target_path, "commits.rst")
    old_text = ""
    if os.path.isfile(index_file_path):
        with open(index_file_path) as readme_file_read:
            old_text = readme_file_read.read()
    replace_content(index_file_path, old_text, new_text, provider_package_id)


@lru_cache(maxsize=None)
def black_mode():
    from black import Mode, parse_pyproject_toml, target_version_option_callback

    config = parse_pyproject_toml(os.path.join(SOURCE_DIR_PATH, "pyproject.toml"))

    target_versions = set(
        target_version_option_callback(None, None, config.get('target_version', [])),  # noqa
    )

    return Mode(
        target_versions=target_versions,
        line_length=config.get('line_length', Mode.line_length),
        is_pyi=config.get('is_pyi', Mode.is_pyi),
        string_normalization=not config.get('skip_string_normalization', not Mode.string_normalization),
        experimental_string_processing=config.get(
            'experimental_string_processing', Mode.experimental_string_processing
        ),
    )


def black_format(content) -> str:
    from black import format_str

    return format_str(content, mode=black_mode())


def prepare_setup_py_file(context):
    setup_py_template_name = "SETUP"
    setup_py_file_path = os.path.abspath(os.path.join(get_target_folder(), "setup.py"))
    setup_py_content = render_template(
        template_name=setup_py_template_name, context=context, extension='.py', autoescape=False
    )
    with open(setup_py_file_path, "wt") as setup_py_file:
        setup_py_file.write(black_format(setup_py_content))


def prepare_setup_cfg_file(context):
    setup_cfg_template_name = "SETUP"
    setup_cfg_file_path = os.path.abspath(os.path.join(get_target_folder(), "setup.cfg"))
    setup_cfg_content = render_template(
        template_name=setup_cfg_template_name,
        context=context,
        extension='.cfg',
        autoescape=False,
        keep_trailing_newline=True,
    )
    with open(setup_cfg_file_path, "wt") as setup_cfg_file:
        setup_cfg_file.write(setup_cfg_content)


def prepare_get_provider_info_py_file(context, provider_package_id: str):
    get_provider_template_name = "get_provider_info"
    get_provider_file_path = os.path.abspath(
        os.path.join(
            get_target_providers_package_folder(provider_package_id),
            "get_provider_info.py",
        )
    )
    get_provider_content = render_template(
        template_name=get_provider_template_name,
        context=context,
        extension='.py',
        autoescape=False,
        keep_trailing_newline=True,
    )
    with open(get_provider_file_path, "wt") as get_provider_file:
        get_provider_file.write(black_format(get_provider_content))


def prepare_manifest_in_file(context):
    target = os.path.abspath(os.path.join(get_target_folder(), "MANIFEST.in"))
    content = render_template(
        template_name="MANIFEST",
        context=context,
        extension='.in',
        autoescape=False,
        keep_trailing_newline=True,
    )
    with open(target, "wt") as fh:
        fh.write(content)


def get_all_providers() -> List[str]:
    """
    Returns all providers for regular packages.
    :return: list of providers that are considered for provider packages
    """
    return list(PROVIDERS_REQUIREMENTS.keys())


def verify_provider_package(package: str) -> None:
    """
    Verifies if the provider package is good.
    :param package: package id to verify
    :return: None
    """
    if package not in get_provider_packages():
        print(f"[red]Wrong package name: {package}[/]")
        print("Use one of:")
        print(get_provider_packages())
        raise Exception(f"The package {package} is not a provider package.")


@cli.command()
def list_providers_packages() -> bool:
    """List all provider packages."""
    providers = get_all_providers()
    for provider in providers:
        print(provider)
    return True


@cli.command()
@option_version_suffix
@option_git_update
@argument_package_id
@option_verbose
def update_package_documentation(
    version_suffix: str,
    git_update: bool,
    package_id: str,
    verbose: bool,
):
    """
    Updates package documentation.

    See `list-providers-packages` subcommand for the possible PACKAGE_ID values
    """
    provider_package_id = package_id
    verify_provider_package(provider_package_id)
    with with_group(f"Update generated files for package '{provider_package_id}' "):
        print("Updating documentation for the latest release version.")
        make_sure_remote_apache_exists_and_fetch(git_update, verbose)
        return update_generated_files_for_regular_package(
            provider_package_id,
            version_suffix,
            update_release_notes=True,
            update_setup=False,
            verbose=verbose,
        )


def tag_exists_for_version(provider_package_id: str, current_tag: str, verbose: bool):
    provider_details = get_provider_details(provider_package_id)
    if verbose:
        print(f"Checking if tag `{current_tag}` exists.")
    if not subprocess.call(
        get_git_tag_check_command(current_tag),
        cwd=provider_details.source_provider_package_path,
        stderr=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
    ):
        if verbose:
            print(f"Tag `{current_tag}` exists.")
        return True
    if verbose:
        print(f"Tag `{current_tag}` does not exist.")
    return False


@cli.command()
@option_version_suffix
@option_git_update
@argument_package_id
@option_verbose
def generate_setup_files(version_suffix: str, git_update: bool, package_id: str, verbose: bool):
    """
    Generates setup files for the package.

    See `list-providers-packages` subcommand for the possible PACKAGE_ID values
    """
    provider_package_id = package_id
    package_ok = True
    with with_group(f"Generate setup files for '{provider_package_id}'"):
        current_tag = get_current_tag(provider_package_id, version_suffix, git_update, verbose)
        if tag_exists_for_version(provider_package_id, current_tag, verbose):
            print(f"[yellow]The tag {current_tag} exists. Not preparing the package.[/]")
            package_ok = False
        else:
            if update_generated_files_for_regular_package(
                provider_package_id,
                version_suffix,
                update_release_notes=False,
                update_setup=True,
                verbose=verbose,
            ):
                print(f"[green]Generated regular package setup files for {provider_package_id}[/]")
            else:
                package_ok = False
    return package_ok


def get_current_tag(provider_package_id: str, suffix: str, git_update: bool, verbose: bool):
    verify_provider_package(provider_package_id)
    make_sure_remote_apache_exists_and_fetch(git_update, verbose)
    provider_info = get_provider_info_from_provider_yaml(provider_package_id)
    versions: List[str] = provider_info['versions']
    current_version = versions[0]
    current_tag = get_version_tag(current_version, provider_package_id, suffix)
    return current_tag


def cleanup_remnants(verbose: bool):
    if verbose:
        print("Cleaning remnants (*.egginfo)")
    files = glob.glob("*.egg-info")
    for file in files:
        shutil.rmtree(file, ignore_errors=True)


def verify_setup_py_prepared(provider_package):
    with open("setup.py") as f:
        setup_content = f.read()
    search_for = f"providers-{provider_package.replace('.','-')} for Apache Airflow"
    if search_for not in setup_content:
        print(
            f"[red]The setup.py is probably prepared for another package. "
            f"It does not contain [bold]{search_for}[/bold]![/]"
        )
        print(
            f"\nRun:\n\n[bold]./dev/provider_packages/prepare_provider_packages.py "
            f"generate-setup-files {provider_package}[/bold]\n"
        )
        raise Exception("Wrong setup!")


@cli.command()
@click.option(
    '--package-format',
    type=click.Choice(['sdist', 'wheel', 'both']),
    default='wheel',
    help='Optional format - only used in case of building packages (default: wheel)',
)
@option_git_update
@option_version_suffix
@argument_package_id
@option_verbose
def build_provider_packages(
    package_format: str,
    git_update: bool,
    version_suffix: str,
    package_id: str,
    verbose: bool,
) -> bool:
    """
    Builds provider package.

    See `list-providers-packages` subcommand for the possible PACKAGE_ID values
    """

    import tempfile

    # we cannot use context managers because if the directory gets deleted (which bdist_wheel does),
    # the context manager will throw an exception when trying to delete it again
    tmp_build_dir = tempfile.TemporaryDirectory().name
    tmp_dist_dir = tempfile.TemporaryDirectory().name
    try:
        provider_package_id = package_id
        with with_group(f"Prepare provider package for '{provider_package_id}'"):
            current_tag = get_current_tag(provider_package_id, version_suffix, git_update, verbose)
            if tag_exists_for_version(provider_package_id, current_tag, verbose):
                print(f"[yellow]The tag {current_tag} exists. Skipping the package.[/]")
                return False
            print(f"Changing directory to ${TARGET_PROVIDER_PACKAGES_PATH}")
            os.chdir(TARGET_PROVIDER_PACKAGES_PATH)
            cleanup_remnants(verbose)
            provider_package = package_id
            verify_setup_py_prepared(provider_package)

            print(f"Building provider package: {provider_package} in format {package_format}")
            command = ["python3", "setup.py", "build", "--build-temp", tmp_build_dir]
            if version_suffix is not None:
                command.extend(['egg_info', '--tag-build', version_suffix])
            if package_format in ['sdist', 'both']:
                command.append("sdist")
            if package_format in ['wheel', 'both']:
                command.extend(["bdist_wheel", "--bdist-dir", tmp_dist_dir])
            print(f"Executing command: '{' '.join(command)}'")
            try:
                subprocess.check_call(command, stdout=subprocess.DEVNULL)
            except subprocess.CalledProcessError as ex:
                print(ex.output.decode())
                raise Exception("The command returned an error %s", command)
            print(f"[green]Prepared provider package {provider_package} in format {package_format}[/]")
    finally:
        shutil.rmtree(tmp_build_dir, ignore_errors=True)
        shutil.rmtree(tmp_dist_dir, ignore_errors=True)

    return True


def verify_provider_classes_for_single_provider(imported_classes: List[str], provider_package_id: str):
    """Verify naming of provider classes for single provider."""
    full_package_name = f"airflow.providers.{provider_package_id}"
    entity_summaries = get_package_class_summary(full_package_name, imported_classes)
    total, bad = check_if_classes_are_properly_named(entity_summaries)
    bad += sum([len(entity_summary.wrong_entities) for entity_summary in entity_summaries.values()])
    if bad != 0:
        print()
        print(f"[red]There are {bad} errors of {total} entities for {provider_package_id}[/]")
        print()
    return total, bad


def summarise_total_vs_bad(total: int, bad: int):
    """Summarises Bad/Good class names for providers"""
    if bad == 0:
        print()
        print(f"[green]All good! All {total} entities are properly named[/]")
        print()
        print("Totals:")
        print()
        print("New:")
        print()
        for entity in EntityType:
            print(f"{entity.value}: {TOTALS[entity][0]}")
        print()
        print("Moved:")
        print()
        for entity in EntityType:
            print(f"{entity.value}: {TOTALS[entity][1]}")
        print()
    else:
        print()
        print(f"[red]There are in total: {bad} entities badly named out of {total} entities[/]")
        print()
        raise Exception("Badly names entities")


@cli.command()
def verify_provider_classes() -> bool:
    """Verifies if all classes in all providers are correctly named."""
    with with_group("Verifies names for all provider classes"):
        provider_ids = get_all_providers()
        imported_classes = import_all_classes(
            provider_ids=provider_ids,
            print_imports=False,
            paths=[PROVIDERS_PATH],
            prefix="airflow.providers.",
        )
        total = 0
        bad = 0
        for provider_package_id in provider_ids:
            inc_total, inc_bad = verify_provider_classes_for_single_provider(
                imported_classes, provider_package_id
            )
            total += inc_total
            bad += inc_bad
        summarise_total_vs_bad(total, bad)
    return True


if __name__ == "__main__":
    cli()
