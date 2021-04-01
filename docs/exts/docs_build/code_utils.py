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
from contextlib import suppress

from docs.exts.provider_yaml_utils import load_package_data

ROOT_PROJECT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, os.pardir, os.pardir)
)
DOCS_DIR = os.path.join(ROOT_PROJECT_DIR, "docs")
AIRFLOW_DIR = os.path.join(ROOT_PROJECT_DIR, "airflow")

DOCKER_PROJECT_DIR = "/opt/airflow"
DOCKER_DOCS_DIR = os.path.join(DOCKER_PROJECT_DIR, "docs")
DOCKER_AIRFLOW_DIR = os.path.join(DOCKER_PROJECT_DIR, "/airflow")
ALL_PROVIDER_YAMLS = load_package_data()
AIRFLOW_SITE_DIR = os.environ.get('AIRFLOW_SITE_DIRECTORY')
PROCESS_TIMEOUT = 4 * 60

TEXT_RED = '\033[31m'
TEXT_RESET = '\033[0m'

CONSOLE_WIDTH = 180


def remap_from_docker(file_name: str, dockerized: bool):
    """
    Remaps filename from Docker to Host.
    :param file_name: name of file
    :param dockerized: whether builds were running in docker environment.
    :return:
    """
    if dockerized and file_name.startswith(DOCKER_PROJECT_DIR):
        return file_name.replace(DOCKER_PROJECT_DIR, ROOT_PROJECT_DIR)
    return file_name


def prepare_code_snippet(file_path: str, line_no: int, context_lines_count: int = 5) -> str:
    """
    Prepares code snippet.
    :param file_path: file path
    :param line_no: line number
    :param context_lines_count: number of lines of context.
    :return:
    """

    def guess_lexer_for_filename(filename):
        from pygments.lexers import get_lexer_for_filename
        from pygments.util import ClassNotFound

        try:
            lexer = get_lexer_for_filename(filename)
        except ClassNotFound:
            from pygments.lexers.special import TextLexer

            lexer = TextLexer()
        return lexer

    with open(file_path) as text_file:
        # Highlight code
        code = text_file.read()
        with suppress(ImportError):
            import pygments
            from pygments.formatters.terminal import TerminalFormatter

            code = pygments.highlight(
                code=code, formatter=TerminalFormatter(), lexer=guess_lexer_for_filename(file_path)
            )

        code_lines = code.split("\n")
        # Prepend line number
        code_lines = [f"{line_no:4} | {line}" for line_no, line in enumerate(code_lines, 1)]
        # # Cut out the snippet
        start_line_no = max(0, line_no - context_lines_count)
        end_line_no = line_no + context_lines_count
        code_lines = code_lines[start_line_no:end_line_no]
        # Join lines
        code = "\n".join(code_lines)
    return code


def pretty_format_path(path: str, start: str) -> str:
    """Formats the path by marking the important part in bold."""
    end = '\033[0m'
    bold = '\033[1m'

    relpath = os.path.relpath(path, start)
    if relpath == path:
        return f"{bold}path{end}"
    return f"{start}/{bold}{relpath}{end}"
