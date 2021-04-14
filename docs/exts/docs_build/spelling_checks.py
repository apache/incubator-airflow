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
import re
from functools import total_ordering
from typing import Dict, List, NamedTuple, Optional

from airflow.utils.code_utils import prepare_code_snippet

CURRENT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__)))
DOCS_DIR = os.path.abspath(os.path.join(CURRENT_DIR, os.pardir, os.pardir))


@total_ordering
class SpellingError(NamedTuple):
    """Spelling errors found when building docs."""

    file_path: Optional[str]
    line_no: Optional[int]
    spelling: Optional[str]
    suggestion: Optional[str]
    context_line: Optional[str]
    message: str

    def __eq__(self, other):
        left = (
            self.file_path,
            self.line_no,
            self.spelling,
            self.context_line,
            self.message,
        )
        right = (
            other.file_path,
            other.line_no,
            other.spelling,
            other.context_line,
            other.message,
        )
        return left == right

    def __ne__(self, other):
        return not self == other

    def __lt__(self, other):
        file_path_a = self.file_path or ''
        file_path_b = other.file_path or ''
        line_no_a = self.line_no or 0
        line_no_b = other.line_no or 0
        context_line_a = self.context_line or ''
        context_line_b = other.context_line or ''
        left = (file_path_a, line_no_a, context_line_a, self.spelling, self.message)
        right = (
            file_path_b,
            line_no_b,
            context_line_b,
            other.spelling,
            other.message,
        )
        return left < right


def parse_spelling_warnings(warning_text: str, docs_dir) -> List[SpellingError]:
    """
    Parses warnings from Sphinx.

    :param warning_text: warning to parse
    :return: list of SpellingError.
    """
    sphinx_spelling_errors = []
    for sphinx_warning in warning_text.split("\n"):
        if not sphinx_warning:
            continue
        warning_parts = None
        match = re.search(r"(.*):(\w*):\s\((\w*)\)\s?(\w*)\s?(.*)", sphinx_warning)
        if match:
            warning_parts = match.groups()
        if warning_parts and len(warning_parts) == 5:
            try:
                sphinx_spelling_errors.append(
                    SpellingError(
                        file_path=os.path.join(docs_dir, warning_parts[0]),
                        line_no=int(warning_parts[1]) if warning_parts[1] not in ('None', '') else None,
                        spelling=warning_parts[2],
                        suggestion=warning_parts[3] if warning_parts[3] else None,
                        context_line=warning_parts[4],
                        message=sphinx_warning,
                    )
                )
            except Exception:  # noqa pylint: disable=broad-except
                # If an exception occurred while parsing the warning message, display the raw warning message.
                sphinx_spelling_errors.append(
                    SpellingError(
                        file_path=None,
                        line_no=None,
                        spelling=None,
                        suggestion=None,
                        context_line=None,
                        message=sphinx_warning,
                    )
                )
        else:
            sphinx_spelling_errors.append(
                SpellingError(
                    file_path=None,
                    line_no=None,
                    spelling=None,
                    suggestion=None,
                    context_line=None,
                    message=sphinx_warning,
                )
            )
    return sphinx_spelling_errors


def display_spelling_error_summary(spelling_errors: Dict[str, List[SpellingError]]) -> None:
    """Displays summary of Spelling errors"""
    print("#" * 20, "Spelling errors summary", "#" * 20)

    for package_name, errors in sorted(spelling_errors.items()):
        if package_name:
            print("=" * 20, package_name, "=" * 20)
        else:
            print("=" * 20, "General", "=" * 20)

        for warning_no, error in enumerate(sorted(errors), 1):
            print("-" * 20, f"Error {warning_no:3}", "-" * 20)

            _display_error(error)

    print("=" * 50)
    print()
    msg = """
If there are spelling errors in the summary above, and the spelling is
correct, add the spelling to docs/spelling_wordlist.txt or use the
spelling directive.
Check https://sphinxcontrib-spelling.readthedocs.io/en/latest/customize.html#private-dictionaries
for more details.

If there are no spelling errors in the summary above, there might be an
issue unrelated to spelling. Please review the traceback.
    """
    print(msg)
    print()
    print("#" * 50)


def _display_error(error: SpellingError):
    print(error.message)
    print()
    if error.file_path:
        print(f"File path: {os.path.relpath(error.file_path, start=DOCS_DIR)}")
        if error.spelling:
            print(f"Incorrect Spelling: '{error.spelling}'")
        if error.suggestion:
            print(f"Suggested Spelling: '{error.suggestion}'")
        if error.context_line:
            print(f"Line with Error: '{error.context_line}'")
        if error.line_no:
            print(f"Line Number: {error.line_no}")
            print(prepare_code_snippet(error.file_path, error.line_no))
