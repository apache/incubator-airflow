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
import os
from functools import total_ordering
from typing import Dict, List, NamedTuple, Optional

from rich.console import Console

from airflow.utils.code_utils import prepare_code_snippet
from docs.exts.docs_build.code_utils import CONSOLE_WIDTH, remap_from_docker

CURRENT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__)))
DOCS_DIR = os.path.abspath(os.path.join(CURRENT_DIR, os.pardir, os.pardir))

console = Console(force_terminal=True, color_system="standard", width=CONSOLE_WIDTH)


@total_ordering
class DocBuildError(NamedTuple):
    """Errors found in docs build."""

    file_path: Optional[str]
    line_no: Optional[int]
    message: str

    def __eq__(self, other):
        left = (self.file_path, self.line_no, self.message)
        right = (other.file_path, other.line_no, other.message)
        return left == right

    def __ne__(self, other):
        return not self == other

    def __lt__(self, right):
        file_path_a = self.file_path or ''
        file_path_b = right.file_path or ''
        line_no_a = self.line_no or 0
        line_no_b = right.line_no or 0
        left = (file_path_a, line_no_a, self.message)
        right = (file_path_b, line_no_b, right.message)
        return left < right


def display_errors_summary(build_errors: Dict[str, List[DocBuildError]]) -> None:
    """Displays summary of errors"""
    console.print()
    console.print("[red]" + "#" * 30 + " Start docs build errors summary " + "#" * 30 + "[/]")
    console.print()
    for package_name, errors in build_errors.items():
        if package_name:
            console.print("=" * 30 + f" [blue]{package_name}[/] " + "=" * 30)
        else:
            console.print("=" * 30, " [blue]General[/] ", "=" * 30)
        for warning_no, error in enumerate(sorted(errors), 1):
            console.print("-" * 30, f"[red]Error {warning_no:3}[/]", "-" * 20)
            console.print(error.message)
            console.print()
            if error.file_path and error.file_path != "<unknown>" and error.line_no:
                console.print(
                    f"File path: {os.path.relpath(error.file_path, start=DOCS_DIR)} ({error.line_no})"
                )
                console.print()
                console.print(prepare_code_snippet(error.file_path, error.line_no))
            elif error.file_path:
                console.print(f"File path: {error.file_path}")
    console.print()
    console.print("[red]" + "#" * 30 + " End docs build errors summary " + "#" * 30 + "[/]")
    console.print()


def parse_sphinx_warnings(warning_text: str, docs_dir: str, dockerized: bool) -> List[DocBuildError]:
    """
    Parses warnings from Sphinx.

    :param warning_text: warning to parse
    :return: list of DocBuildErrors.
    """
    sphinx_build_errors = []
    for sphinx_warning in warning_text.split("\n"):
        if not sphinx_warning:
            continue
        warning_parts = sphinx_warning.split(":", 2)
        if len(warning_parts) == 3:
            try:
                sphinx_build_errors.append(
                    DocBuildError(
                        file_path=remap_from_docker(os.path.join(docs_dir, warning_parts[0]), dockerized),
                        line_no=int(warning_parts[1]),
                        message=warning_parts[2],
                    )
                )
            except Exception:  # noqa pylint: disable=broad-except
                # If an exception occurred while parsing the warning message, display the raw warning message.
                sphinx_build_errors.append(
                    DocBuildError(file_path=None, line_no=None, message=sphinx_warning)
                )
        else:
            sphinx_build_errors.append(DocBuildError(file_path=None, line_no=None, message=sphinx_warning))
    return sphinx_build_errors
