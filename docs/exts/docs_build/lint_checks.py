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

import ast
import os
import re
from glob import glob
from itertools import chain
from typing import Iterable, List, Optional, Set

import yaml

try:
    from yaml import CSafeLoader as SafeLoader
except ImportError:
    from yaml import SafeLoader  # type: ignore[misc]

import airflow
from docs.exts.docs_build.docs_builder import ALL_PROVIDER_YAMLS  # pylint: disable=no-name-in-module
from docs.exts.docs_build.errors import DocBuildError  # pylint: disable=no-name-in-module

ROOT_PROJECT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, os.pardir, os.pardir)
)
ROOT_PACKAGE_DIR = os.path.join(ROOT_PROJECT_DIR, "airflow")
DOCS_DIR = os.path.join(ROOT_PROJECT_DIR, "docs")


def find_existing_guide_operator_names(src_dir_pattern: str) -> Set[str]:
    """
    Find names of existing operators.
    :return names of existing operators.
    """
    operator_names = set()

    paths = glob(src_dir_pattern, recursive=True)
    for path in paths:
        with open(path) as f:
            operator_names |= set(re.findall(".. _howto/operator:(.+?):", f.read()))

    return operator_names


def extract_ast_class_def_by_name(ast_tree, class_name):
    """
    Extracts class definition by name

    :param ast_tree: AST tree
    :param class_name: name of the class.
    :return: class node found
    """
    for node in ast.walk(ast_tree):
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            return node

    return None


def _generate_missing_guide_error(path, line_no, operator_name):
    return DocBuildError(
        file_path=path,
        line_no=line_no,
        message=(
            f"Link to the guide is missing in operator's description: {operator_name}.\n"
            f"Please add link to the guide to the description in the following form:\n"
            f"\n"
            f".. seealso::\n"
            f"    For more information on how to use this operator, take a look at the guide:\n"
            f"    :ref:`howto/operator:{operator_name}`\n"
        ),
    )


def check_guide_links_in_operator_descriptions() -> List[DocBuildError]:
    """Check if there are links to guides in operator's descriptions."""
    build_errors = []

    build_errors.extend(
        _check_missing_guide_references(
            operator_names=find_existing_guide_operator_names(
                f"{DOCS_DIR}/apache-airflow/howto/operator/**/*.rst"
            ),
            python_module_paths=chain(
                glob(f"{ROOT_PACKAGE_DIR}/operators/*.py"),
                glob(f"{ROOT_PACKAGE_DIR}/sensors/*.py"),
            ),
        )
    )

    for provider in ALL_PROVIDER_YAMLS:
        operator_names = {
            *find_existing_guide_operator_names(f"{DOCS_DIR}/{provider['package-name']}/operators/**/*.rst"),
            *find_existing_guide_operator_names(f"{DOCS_DIR}/{provider['package-name']}/operators.rst"),
        }

        # Extract all potential python modules that can contain operators
        python_module_paths = chain(
            glob(f"{provider['package-dir']}/**/operators/*.py", recursive=True),
            glob(f"{provider['package-dir']}/**/sensors/*.py", recursive=True),
            glob(f"{provider['package-dir']}/**/transfers/*.py", recursive=True),
        )

        build_errors.extend(
            _check_missing_guide_references(
                operator_names=operator_names, python_module_paths=python_module_paths
            )
        )

    return build_errors


def _check_missing_guide_references(operator_names, python_module_paths) -> List[DocBuildError]:
    build_errors = []

    for py_module_path in python_module_paths:
        with open(py_module_path) as f:
            py_content = f.read()

        if "This module is deprecated" in py_content:
            continue
        for existing_operator in operator_names:
            if f"class {existing_operator}" not in py_content:
                continue
            # This is a potential file with necessary class definition.
            # To make sure it's a real Python class definition, we build AST tree
            ast_tree = ast.parse(py_content)
            class_def = extract_ast_class_def_by_name(ast_tree, existing_operator)

            if class_def is None:
                continue

            docstring = ast.get_docstring(class_def)
            if "This class is deprecated." in docstring:
                continue

            if f":ref:`howto/operator:{existing_operator}`" in ast.get_docstring(class_def):
                continue

            build_errors.append(
                _generate_missing_guide_error(py_module_path, class_def.lineno, existing_operator)
            )
    return build_errors


def assert_file_not_contains(
    *, file_path: str, pattern: str, message: Optional[str] = None
) -> Optional[DocBuildError]:
    """
    Asserts that file does not contain the pattern. Return message error if it does.

    :param file_path: file
    :param pattern: pattern
    :param message: message to return
    """
    return _extract_file_content(file_path, message, pattern, False)


def assert_file_contains(
    *, file_path: str, pattern: str, message: Optional[str] = None
) -> Optional[DocBuildError]:
    """
    Asserts that file does contain the pattern. Return message error if it does not.

    :param file_path: file
    :param pattern: pattern
    :param message: message to return
    """
    return _extract_file_content(file_path, message, pattern, True)


def _extract_file_content(file_path: str, message: Optional[str], pattern: str, expected_contain: bool):
    if not message:
        message = f"Pattern '{pattern}' could not be found in '{file_path}' file."
    with open(file_path, "rb", 0) as doc_file:
        pattern_compiled = re.compile(pattern)
        found = False
        for num, line in enumerate(doc_file, 1):
            line_decode = line.decode()
            result = re.search(pattern_compiled, line_decode)
            if not expected_contain and result:
                return DocBuildError(file_path=file_path, line_no=num, message=message)
            elif expected_contain and result:
                found = True

        if expected_contain and not found:
            return DocBuildError(file_path=file_path, line_no=None, message=message)
    return None


def filter_file_list_by_pattern(file_paths: Iterable[str], pattern: str) -> List[str]:
    """
    Filters file list to those tha content matches the pattern
    :param file_paths: file paths to check
    :param pattern: pattern to match
    :return: list of files matching the pattern
    """
    output_paths = []
    pattern_compiled = re.compile(pattern)
    for file_path in file_paths:
        with open(file_path, "rb", 0) as text_file:
            text_file_content = text_file.read().decode()
            if re.findall(pattern_compiled, text_file_content):
                output_paths.append(file_path)
    return output_paths


def find_modules(deprecated_only: bool = False) -> Set[str]:
    """
    Finds all modules.
    :param deprecated_only: whether only deprecated modules should be found.
    :return: set of all modules found
    """
    file_paths = glob(f"{ROOT_PACKAGE_DIR}/**/*.py", recursive=True)
    # Exclude __init__.py
    file_paths = [f for f in file_paths if not f.endswith("__init__.py")]
    if deprecated_only:
        file_paths = filter_file_list_by_pattern(file_paths, r"This module is deprecated.")
    # Make path relative
    file_paths = [os.path.relpath(f, ROOT_PROJECT_DIR) for f in file_paths]
    # Convert filename to module
    modules_names = {file_path.rpartition(".")[0].replace("/", ".") for file_path in file_paths}
    return modules_names


def check_exampleinclude_for_example_dags() -> List[DocBuildError]:
    """Checks all exampleincludes for  example dags."""
    all_docs_files = glob(f"${DOCS_DIR}/**/*.rst", recursive=True)
    build_errors = []
    for doc_file in all_docs_files:
        build_error = assert_file_not_contains(
            file_path=doc_file,
            pattern=r"literalinclude::.+example_dags",
            message=(
                "literalinclude directive is prohibited for example DAGs. \n"
                "You should use the exampleinclude directive to include example DAGs."
            ),
        )
        if build_error:
            build_errors.append(build_error)
    return build_errors


def check_enforce_code_block() -> List[DocBuildError]:
    """Checks all code:: blocks."""
    all_docs_files = glob(f"{DOCS_DIR}/**/*.rst", recursive=True)
    build_errors = []
    for doc_file in all_docs_files:
        build_error = assert_file_not_contains(
            file_path=doc_file,
            pattern=r"^.. code::",
            message=(
                "We recommend using the code-block directive instead of the code directive. "
                "The code-block directive is more feature-full."
            ),
        )
        if build_error:
            build_errors.append(build_error)
    return build_errors


def check_example_dags_in_provider_tocs() -> List[DocBuildError]:
    """Checks that each documentation for provider packages has a link to example DAGs in the TOC."""
    build_errors = []

    for provider in ALL_PROVIDER_YAMLS:
        example_dags_dirs = list(glob(f"{provider['package-dir']}/**/example_dags", recursive=True))
        if not example_dags_dirs:
            continue
        doc_file_path = f"{DOCS_DIR}/{provider['package-name']}/index.rst"

        if len(example_dags_dirs) == 1:
            package_rel_path = os.path.relpath(example_dags_dirs[0], start=ROOT_PROJECT_DIR)
            github_url = f"https://github.com/apache/airflow/tree/master/{package_rel_path}"
            expected_text = f"Example DAGs <{github_url}>"
        else:
            expected_text = "Example DAGs <example-dags>"

        build_error = assert_file_contains(
            file_path=doc_file_path,
            pattern=re.escape(expected_text),
            message=(
                f"A link to the example DAGs in table of contents is missing. Can you add it?\n\n"
                f"    {expected_text}"
            ),
        )
        if build_error:
            build_errors.append(build_error)

    return build_errors


def check_pypi_repository_in_provider_tocs() -> List[DocBuildError]:
    """Checks that each documentation for provider packages has a link to PyPI files in the TOC."""
    build_errors = []
    for provider in ALL_PROVIDER_YAMLS:
        doc_file_path = f"{DOCS_DIR}/{provider['package-name']}/index.rst"
        expected_text = f"PyPI Repository <https://pypi.org/project/{provider['package-name']}/>"
        build_error = assert_file_contains(
            file_path=doc_file_path,
            pattern=re.escape(expected_text),
            message=(
                f"A link to the PyPI in table of contents is missing. Can you add it?\n\n"
                f"    {expected_text}"
            ),
        )
        if build_error:
            build_errors.append(build_error)

    return build_errors


def check_docker_image_tag_in_quick_start_guide() -> List[DocBuildError]:
    """Check that a good docker image is used in the quick start guide for Docker."""
    build_errors = []

    compose_file_path = f"{DOCS_DIR}/apache-airflow/start/docker-compose.yaml"
    expected_tag = 'master-python3.8' if "dev" in airflow.__version__ else airflow.__version__
    # master tag is little outdated.
    expected_image = f'apache/airflow:{expected_tag}'
    with open(compose_file_path) as yaml_file:
        content = yaml.load(yaml_file, SafeLoader)
        current_image_expression = content['x-airflow-common']['image']
        if expected_image not in current_image_expression:
            build_errors.append(
                DocBuildError(
                    file_path=compose_file_path,
                    line_no=None,
                    message=(
                        f"Invalid image in docker - compose.yaml\n"
                        f"Current image expression: {current_image_expression}\n"
                        f"Expected image: {expected_image}\n"
                        f"Please check the value of x-airflow-common.image key"
                    ),
                )
            )
    build_error = assert_file_contains(
        file_path=f"{DOCS_DIR}/apache-airflow/start/docker.rst",
        pattern=re.escape(f'{expected_image}   "/usr/bin/dumb-init'),
    )
    if build_error:
        build_errors.append(build_error)

    return build_errors


def check_airflow_versions_in_quick_start_guide() -> List[DocBuildError]:
    """Check that a airflow version is presented in example in the quick start guide for Docker."""
    build_errors = []

    build_error = assert_file_contains(
        file_path=f"{DOCS_DIR}/apache-airflow/start/docker.rst",
        pattern=re.escape(f"airflow-init_1       | {airflow.__version__}"),
    )
    if build_error:
        build_errors.append(build_error)

    return build_errors


def run_all_check() -> List[DocBuildError]:
    """Run all checks from this module"""
    general_errors = []
    general_errors.extend(check_guide_links_in_operator_descriptions())
    general_errors.extend(check_enforce_code_block())
    general_errors.extend(check_exampleinclude_for_example_dags())
    general_errors.extend(check_example_dags_in_provider_tocs())
    general_errors.extend(check_pypi_repository_in_provider_tocs())
    general_errors.extend(check_docker_image_tag_in_quick_start_guide())
    general_errors.extend(check_airflow_versions_in_quick_start_guide())

    return general_errors
