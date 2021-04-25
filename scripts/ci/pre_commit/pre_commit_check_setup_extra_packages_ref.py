#!/usr/bin/env python
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
"""
Checks if all the libraries in setup.py are listed in installation.rst file
"""

import os
import re
import sys
from os.path import dirname
from typing import Dict, List, Set

from rich import print
from rich.console import Console
from rich.table import Table

AIRFLOW_SOURCES_DIR = os.path.join(dirname(__file__), os.pardir, os.pardir, os.pardir)
SETUP_PY_FILE = 'setup.py'
DOCS_FILE = os.path.join('docs', 'apache-airflow', 'extra-packages-ref.rst')
PY_IDENTIFIER = r'[a-zA-Z_][a-zA-Z0-9_\.]*'

sys.path.insert(0, AIRFLOW_SOURCES_DIR)

from setup import (  # noqa # isort:skip
    add_all_provider_packages,
    EXTRAS_DEPRECATED_ALIASES,
    EXTRAS_REQUIREMENTS,
    PROVIDERS_REQUIREMENTS,
    PREINSTALLED_PROVIDERS,
)


def get_file_content(*path_elements: str) -> str:
    file_path = os.path.join(AIRFLOW_SOURCES_DIR, *path_elements)
    with open(file_path) as file_to_read:
        return file_to_read.read()


def get_extras_from_setup() -> Set[str]:
    """Returns a set of regular (non-deprecated) extras from setup."""
    return set(EXTRAS_REQUIREMENTS.keys()) - set(EXTRAS_DEPRECATED_ALIASES.keys())


def get_extras_from_docs() -> Set[str]:
    """
    Returns a list of extras from docs.
    """
    docs_content = get_file_content(DOCS_FILE)
    extras_section_regex = re.compile(
        rf'\|[^|]+\|.*pip install .apache-airflow\[({PY_IDENTIFIER})][^|]+\|[^|]+\|',
        re.MULTILINE,
    )
    doc_extra_set: Set[str] = set()
    for doc_extra in extras_section_regex.findall(docs_content):
        doc_extra_set.add(doc_extra)
    return doc_extra_set


def get_preinstalled_providers_from_docs() -> List[str]:
    """
    Returns list of pre-installed providers from the doc.
    """
    docs_content = get_file_content(DOCS_FILE)
    preinstalled_section_regex = re.compile(
        rf'\|\s*({PY_IDENTIFIER})\s*\|[^|]+pip install[^|]+\|[^|]+\|\s+\*\s+\|$',
        re.MULTILINE,
    )
    return preinstalled_section_regex.findall(docs_content)


def get_deprecated_extras_from_docs() -> Dict[str, str]:
    """
    Returns dict of deprecated extras from docs (alias -> target extra)
    """
    deprecated_extras = {}
    docs_content = get_file_content(DOCS_FILE)

    deprecated_extras_section_regex = re.compile(
        r'\| Deprecated extra    \| Extra to be used instead    \|\n(.*)\n', re.DOTALL  # noqa
    )
    deprecated_extras_content = deprecated_extras_section_regex.findall(docs_content)[0]

    deprecated_extras_regexp = re.compile(r'\|\s(\S+)\s+\|\s(\S*)\s+\|$', re.MULTILINE)
    for extras in deprecated_extras_regexp.findall(deprecated_extras_content):
        deprecated_extras[extras[0]] = extras[1]
    return deprecated_extras


def check_extras(console: Console) -> bool:
    """
    Checks if non-deprecated extras match setup vs. doc.
    :param console: print table there in case of errors
    :return: True if all ok, False otherwise
    """
    extras_table = Table()
    extras_table.add_column("NAME", justify="right", style="cyan")
    extras_table.add_column("SETUP", justify="center", style="magenta")
    extras_table.add_column("DOCS", justify="center", style="yellow")
    non_deprecated_setup_extras = get_extras_from_setup()
    non_deprecated_docs_extras = get_extras_from_docs()
    for extra in non_deprecated_setup_extras:
        if extra not in non_deprecated_docs_extras:
            extras_table.add_row(extra, "V", "")
    for extra in non_deprecated_docs_extras:
        if extra not in non_deprecated_setup_extras:
            extras_table.add_row(extra, "", "V")
    if extras_table.row_count != 0:
        print(
            f"""\
[red bold]ERROR!![/red bold]

The "[bold]CORE_EXTRAS_REQUIREMENTS[/bold]", "[bold]ADDITIONAL_PROVIDERS_REQUIREMENTS[/bold]", and
    "[bold]PROVIDERS_REQUIREMENTS[/bold]"
sections in the setup file: [bold yellow]{SETUP_PY_FILE}[/bold yellow]
should be synchronized with the "Extra Packages Reference"
in the documentation file: [bold yellow]{DOCS_FILE}[/bold yellow].

Below is the list of extras that:

  * are used but are not documented,
  * are documented but not used,

[bold]Please synchronize setup/documentation files![/bold]

"""
        )
        console.print(extras_table)
        return False
    return True


def check_deprecated_extras(console: Console) -> bool:
    """
    Checks if deprecated extras match setup vs. doc.
    :param console: print table there in case of errors
    :return: True if all ok, False otherwise
    """
    deprecated_setup_extras = EXTRAS_DEPRECATED_ALIASES
    deprecated_docs_extras = get_deprecated_extras_from_docs()

    deprecated_extras_table = Table()
    deprecated_extras_table.add_column("DEPRECATED_IN_SETUP", justify="right", style="cyan")
    deprecated_extras_table.add_column("TARGET_IN_SETUP", justify="center", style="magenta")
    deprecated_extras_table.add_column("DEPRECATED_IN_DOCS", justify="right", style="cyan")
    deprecated_extras_table.add_column("TARGET_IN_DOCS", justify="center", style="magenta")

    for extra in deprecated_setup_extras.keys():
        if extra not in deprecated_docs_extras:
            deprecated_extras_table.add_row(extra, deprecated_setup_extras[extra], "", "")
        elif deprecated_docs_extras[extra] != deprecated_setup_extras[extra]:
            deprecated_extras_table.add_row(
                extra, deprecated_setup_extras[extra], extra, deprecated_docs_extras[extra]
            )

    for extra in deprecated_docs_extras.keys():
        if extra not in deprecated_setup_extras:
            deprecated_extras_table.add_row("", "", extra, deprecated_docs_extras[extra])

    if deprecated_extras_table.row_count != 0:
        print(
            f"""\
[red bold]ERROR!![/red bold]

The "[bold]EXTRAS_DEPRECATED_ALIASES[/bold]" section in the setup file:\
[bold yellow]{SETUP_PY_FILE}[/bold yellow]
should be synchronized with the "Extra Packages Reference"
in the documentation file: [bold yellow]{DOCS_FILE}[/bold yellow].

Below is the list of deprecated extras that:

  * are used but are not documented,
  * are documented but not used,
  * or have different target extra specified in the documentation or setup.

[bold]Please synchronize setup/documentation files![/bold]

"""
        )
        console.print(deprecated_extras_table)
        return False
    return True


def check_preinstalled_extras(console: Console) -> bool:
    """
    Checks if preinstalled extras match setup vs. doc.
    :param console: print table there in case of errors
    :return: True if all ok, False otherwise
    """
    preinstalled_providers_from_docs = get_preinstalled_providers_from_docs()
    preinstalled_providers_from_setup = PREINSTALLED_PROVIDERS

    preinstalled_providers_table = Table()
    preinstalled_providers_table.add_column("PREINSTALLED_IN_SETUP", justify="right", style="cyan")
    preinstalled_providers_table.add_column("PREINSTALLED_IN_DOCS", justify="center", style="magenta")

    for provider in preinstalled_providers_from_setup:
        if provider not in preinstalled_providers_from_docs:
            preinstalled_providers_table.add_row(provider, "")

    for provider in preinstalled_providers_from_docs:
        if provider not in preinstalled_providers_from_setup:
            preinstalled_providers_table.add_row("", provider)

    if preinstalled_providers_table.row_count != 0:
        print(
            f"""\
[red bold]ERROR!![/red bold]

The "[bold]PREINSTALLED_PROVIDERS[/bold]" section in the setup file:\
[bold yellow]{SETUP_PY_FILE}[/bold yellow]
should be synchronized with the "Extra Packages Reference"
in the documentation file: [bold yellow]{DOCS_FILE}[/bold yellow].

Below is the list of preinstalled providers that:
  * are used but are not documented,
  * or are documented but not used.

[bold]Please synchronize setup/documentation files![/bold]

"""
        )
        console.print(preinstalled_providers_table)
        return False
    return True


if __name__ == '__main__':
    status: List[bool] = []
    # force adding all provider package dependencies, to check providers status
    add_all_provider_packages()
    main_console = Console()
    status.append(check_extras(main_console))
    status.append(check_deprecated_extras(main_console))
    status.append(check_preinstalled_extras(main_console))

    if all(status):
        print("All extras are synchronized: [green]OK[/]")
        sys.exit(0)
    sys.exit(1)
