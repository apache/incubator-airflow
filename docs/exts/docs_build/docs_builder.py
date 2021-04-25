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
import shlex
import shutil
from glob import glob
from subprocess import run
from typing import List

from rich.console import Console

from docs.exts.docs_build.code_utils import (
    AIRFLOW_SITE_DIR,
    ALL_PROVIDER_YAMLS,
    CONSOLE_WIDTH,
    DOCKER_DOCS_DIR,
    DOCS_DIR,
    PROCESS_TIMEOUT,
    pretty_format_path,
)
from docs.exts.docs_build.errors import DocBuildError, parse_sphinx_warnings

# pylint: disable=no-name-in-module
from docs.exts.docs_build.spelling_checks import SpellingError, parse_spelling_warnings

# pylint: enable=no-name-in-module

console = Console(force_terminal=True, color_system="standard", width=CONSOLE_WIDTH)


class AirflowDocsBuilder:
    """Documentation builder for Airflow."""

    def __init__(self, package_name: str, for_production: bool):
        self.package_name = package_name
        self.for_production = for_production

    @property
    def _doctree_dir(self) -> str:
        return f"{DOCS_DIR}/_doctrees/docs/{self.package_name}"

    @property
    def _docker_doctree_dir(self) -> str:
        return f"{DOCKER_DOCS_DIR}/_doctrees/docs/{self.package_name}"

    @property
    def _inventory_cache_dir(self) -> str:
        return f"{DOCS_DIR}/_inventory_cache"

    @property
    def _docker_inventory_cache_dir(self) -> str:
        return f"{DOCKER_DOCS_DIR}/_inventory_cache"

    @property
    def is_versioned(self):
        """Is current documentation package versioned?"""
        # Disable versioning. This documentation does not apply to any released product and we can update
        # it as needed, i.e. with each new package of providers.
        return self.package_name not in ('apache-airflow-providers', 'docker-stack')

    @property
    def _build_dir(self) -> str:
        if self.is_versioned:
            version = "stable" if self.for_production else "latest"
            return f"{DOCS_DIR}/_build/docs/{self.package_name}/{version}"
        else:
            return f"{DOCS_DIR}/_build/docs/{self.package_name}"

    @property
    def log_spelling_filename(self) -> str:
        """Log from spelling job."""
        return os.path.join(self._build_dir, f"output-spelling-{self.package_name}.log")

    @property
    def docker_log_spelling_filename(self) -> str:
        """Log from spelling job in docker."""
        return os.path.join(self._docker_build_dir, f"output-spelling-{self.package_name}.log")

    @property
    def log_spelling_output_dir(self) -> str:
        """Results from spelling job."""
        return os.path.join(self._build_dir, f"output-spelling-results-{self.package_name}")

    @property
    def docker_log_spelling_output_dir(self) -> str:
        """Results from spelling job in docker."""
        return os.path.join(self._docker_build_dir, f"output-spelling-results-{self.package_name}")

    @property
    def log_build_filename(self) -> str:
        """Log from build job."""
        return os.path.join(self._build_dir, f"output-build-{self.package_name}.log")

    @property
    def docker_log_build_filename(self) -> str:
        """Log from build job in docker."""
        return os.path.join(self._docker_build_dir, f"output-build-{self.package_name}.log")

    @property
    def log_build_warning_filename(self) -> str:
        """Warnings from build job."""
        return os.path.join(self._build_dir, f"warning-build-{self.package_name}.log")

    @property
    def docker_log_warning_filename(self) -> str:
        """Warnings from build job in docker."""
        return os.path.join(self._docker_build_dir, f"warning-build-{self.package_name}.log")

    @property
    def _docker_build_dir(self) -> str:
        if self.is_versioned:
            version = "stable" if self.for_production else "latest"
            return f"{DOCKER_DOCS_DIR}/_build/docs/{self.package_name}/{version}"
        else:
            return f"{DOCKER_DOCS_DIR}/_build/docs/{self.package_name}"

    @property
    def _current_version(self):
        if not self.is_versioned:
            raise Exception("This documentation package is not versioned")
        if self.package_name == 'apache-airflow':
            from airflow.version import version as airflow_version

            return airflow_version
        if self.package_name.startswith('apache-airflow-providers-'):
            provider = next(p for p in ALL_PROVIDER_YAMLS if p['package-name'] == self.package_name)
            return provider['versions'][0]
        return Exception(f"Unsupported package: {self.package_name}")

    @property
    def _publish_dir(self) -> str:
        if self.is_versioned:
            return f"docs-archive/{self.package_name}/{self._current_version}"
        else:
            return f"docs-archive/{self.package_name}"

    @property
    def _src_dir(self) -> str:
        return f"{DOCS_DIR}/{self.package_name}"

    @property
    def _docker_src_dir(self) -> str:
        return f"{DOCKER_DOCS_DIR}/{self.package_name}"

    def clean_files(self) -> None:
        """Cleanup all artifacts generated by previous builds."""
        api_dir = os.path.join(self._src_dir, "_api")

        shutil.rmtree(api_dir, ignore_errors=True)
        shutil.rmtree(self._build_dir, ignore_errors=True)
        os.makedirs(api_dir, exist_ok=True)
        os.makedirs(self._build_dir, exist_ok=True)

    def check_spelling(self, verbose: bool, dockerized: bool) -> List[SpellingError]:
        """
        Checks spelling

        :param verbose: whether to show output while running
        :param dockerized: whether to run dockerized build (required for paralllel processing on CI)
        :return: list of errors
        """
        spelling_errors = []
        os.makedirs(self._build_dir, exist_ok=True)
        shutil.rmtree(self.log_spelling_output_dir, ignore_errors=True)
        os.makedirs(self.log_spelling_output_dir, exist_ok=True)
        if dockerized:
            python_version = os.getenv('PYTHON_MAJOR_MINOR_VERSION', "3.6")
            build_cmd = [
                "docker",
                "run",
                "--rm",
                "-e",
                "AIRFLOW_FOR_PRODUCTION",
                "-e",
                "AIRFLOW_PACKAGE_NAME",
                "-v",
                f"{self._build_dir}:{self._docker_build_dir}",
                "-v",
                f"{self._inventory_cache_dir}:{self._docker_inventory_cache_dir}",
                "-w",
                DOCKER_DOCS_DIR,
                f"apache/airflow:master-python{python_version}-ci",
                "/opt/airflow/scripts/in_container/run_anything.sh",
            ]
        else:
            build_cmd = []

        build_cmd.extend(
            [
                "sphinx-build",
                "-W",  # turn warnings into errors
                "--color",  # do emit colored output
                "-T",  # show full traceback on exception
                "-b",  # builder to use
                "spelling",
                "-c",
                DOCS_DIR if not dockerized else DOCKER_DOCS_DIR,
                "-d",  # path for the cached environment and doctree files
                self._doctree_dir if not dockerized else self._docker_doctree_dir,
                self._src_dir
                if not dockerized
                else self._docker_src_dir,  # path to documentation source files
                self.log_spelling_output_dir if not dockerized else self.docker_log_spelling_output_dir,
            ]
        )
        env = os.environ.copy()
        env['AIRFLOW_PACKAGE_NAME'] = self.package_name
        if self.for_production:
            env['AIRFLOW_FOR_PRODUCTION'] = 'true'
        if verbose:
            console.print(
                f"[blue]{self.package_name:60}:[/] Executing cmd: ",
                " ".join([shlex.quote(c) for c in build_cmd]),
            )
            console.print(f"[blue]{self.package_name:60}:[/] The output is hidden until an error occurs.")
        with open(self.log_spelling_filename, "wt") as output:
            completed_proc = run(  # pylint: disable=subprocess-run-check
                build_cmd,
                cwd=self._src_dir,
                env=env,
                stdout=output if not verbose else None,
                stderr=output if not verbose else None,
                timeout=PROCESS_TIMEOUT,
            )
        if completed_proc.returncode != 0:
            spelling_errors.append(
                SpellingError(
                    file_path=None,
                    line_no=None,
                    spelling=None,
                    suggestion=None,
                    context_line=None,
                    message=(
                        f"Sphinx spellcheck returned non-zero exit status: " f"{completed_proc.returncode}."
                    ),
                )
            )
            warning_text = ""
            for filepath in glob(f"{self.log_spelling_output_dir}/**/*.spelling", recursive=True):
                with open(filepath) as spelling_file:
                    warning_text += spelling_file.read()

            spelling_errors.extend(parse_spelling_warnings(warning_text, self._src_dir, dockerized))
            console.print(f"[blue]{self.package_name:60}:[/] [red]Finished spell-checking with errors[/]")
        else:
            if spelling_errors:
                console.print(
                    f"[blue]{self.package_name:60}:[/] [yellow]Finished spell-checking " f"with warnings[/]"
                )
            else:
                console.print(
                    f"[blue]{self.package_name:60}:[/] [green]Finished spell-checking " f"successfully[/]"
                )
        return spelling_errors

    def build_sphinx_docs(self, verbose: bool, dockerized: bool) -> List[DocBuildError]:
        """
        Build Sphinx documentation.

        :param verbose: whether to show output while running
        :param dockerized: whether to run dockerized build (required for paralllel processing on CI)
        :return: list of errors
        """
        build_errors = []
        os.makedirs(self._build_dir, exist_ok=True)
        if dockerized:
            python_version = os.getenv('PYTHON_MAJOR_MINOR_VERSION', "3.6")
            build_cmd = [
                "docker",
                "run",
                "--rm",
                "-e",
                "AIRFLOW_FOR_PRODUCTION",
                "-e",
                "AIRFLOW_PACKAGE_NAME",
                "-v",
                f"{self._build_dir}:{self._docker_build_dir}",
                "-v",
                f"{self._inventory_cache_dir}:{self._docker_inventory_cache_dir}",
                "-w",
                DOCKER_DOCS_DIR,
                f"apache/airflow:master-python{python_version}-ci",
                "/opt/airflow/scripts/in_container/run_anything.sh",
            ]
        else:
            build_cmd = []
        build_cmd.extend(
            [
                "sphinx-build",
                "-T",  # show full traceback on exception
                "--color",  # do emit colored output
                "-b",  # builder to use
                "html",
                "-d",  # path for the cached environment and doctree files
                self._doctree_dir if not dockerized else self._docker_doctree_dir,
                "-c",
                DOCS_DIR if not dockerized else DOCKER_DOCS_DIR,
                "-w",  # write warnings (and errors) to given file
                self.log_build_warning_filename if not dockerized else self.docker_log_warning_filename,
                self._src_dir
                if not dockerized
                else self._docker_src_dir,  # path to documentation source files
                self._build_dir if not dockerized else self._docker_build_dir,  # path to output directory
            ]
        )
        env = os.environ.copy()
        env['AIRFLOW_PACKAGE_NAME'] = self.package_name
        if self.for_production:
            env['AIRFLOW_FOR_PRODUCTION'] = 'true'
        if verbose:
            console.print(
                f"[blue]{self.package_name:60}:[/] Executing cmd: ",
                " ".join([shlex.quote(c) for c in build_cmd]),
            )
        else:
            console.print(
                f"[blue]{self.package_name:60}:[/] Running sphinx. "
                f"The output is hidden until an error occurs."
            )
        with open(self.log_build_filename, "wt") as output:
            completed_proc = run(  # pylint: disable=subprocess-run-check
                build_cmd,
                cwd=self._src_dir,
                env=env,
                stdout=output if not verbose else None,
                stderr=output if not verbose else None,
                timeout=PROCESS_TIMEOUT,
            )
        if completed_proc.returncode != 0:
            build_errors.append(
                DocBuildError(
                    file_path=None,
                    line_no=None,
                    message=f"Sphinx returned non-zero exit status: {completed_proc.returncode}.",
                )
            )
        if os.path.isfile(self.log_build_warning_filename):
            with open(self.log_build_warning_filename) as warning_file:
                warning_text = warning_file.read()
            # Remove 7-bit C1 ANSI escape sequences
            warning_text = re.sub(r"\x1B[@-_][0-?]*[ -/]*[@-~]", "", warning_text)
            build_errors.extend(parse_sphinx_warnings(warning_text, self._src_dir, dockerized))
        if build_errors:
            console.print(f"[blue]{self.package_name:60}:[/] [red]Finished docs building with errors[/]")
        else:
            console.print(f"[blue]{self.package_name:60}:[/] [green]Finished docs building successfully[/]")
        return build_errors

    def publish(self):
        """Copy documentation packages files to airflow-site repository."""
        console.print(f"Publishing docs for {self.package_name}")
        output_dir = os.path.join(AIRFLOW_SITE_DIR, self._publish_dir)
        pretty_source = pretty_format_path(self._build_dir, os.getcwd())
        pretty_target = pretty_format_path(output_dir, AIRFLOW_SITE_DIR)
        console.print(f"Copy directory: {pretty_source} => {pretty_target}")
        if os.path.exists(output_dir):
            if self.is_versioned:
                console.print(
                    f"Skipping previously existing {output_dir}! "
                    f"Delete it manually if you want to regenerate it!"
                )
                console.print()
                return
            else:
                shutil.rmtree(output_dir)
        shutil.copytree(self._build_dir, output_dir)
        if self.is_versioned:
            with open(os.path.join(output_dir, "..", "stable.txt"), "w") as stable_file:
                stable_file.write(self._current_version)
        console.print()


def get_available_providers_packages():
    """Get list of all available providers packages to build."""
    return [provider['package-name'] for provider in ALL_PROVIDER_YAMLS]


def get_available_packages():
    """Get list of all available packages to build."""
    provider_package_names = get_available_providers_packages()
    return [
        "apache-airflow",
        *provider_package_names,
        "apache-airflow-providers",
        "helm-chart",
        "docker-stack",
    ]
