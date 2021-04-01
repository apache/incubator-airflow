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
from typing import List

import click as click
from rich import print

PROVIDERS_DOCKER = """\
FROM apache/airflow:latest

# Install providers
{}
"""

AIRFLOW_DOCKER = """\
FROM apache/airflow:{}

# Upgrade
RUN pip install "apache-airflow=={}"

"""

DOCKER_UPGRADE = """\
FROM apache/airflow:1.10.15

# Install upgrade-check
RUN pip install "apache-airflow-upgrade-check=={}"

"""


DOCKER_CMD = """
docker build -t local/airflow .
docker local/airflow info
"""


AIRFLOW = "AIRFLOW"
PROVIDERS = "PROVIDERS"
UPGRADE_CHECK = "UPGRADE_CHECK"

ASC = re.compile(r".*\.asc$")
SHA = re.compile(r".*\.sha512$")
NORM = re.compile(r".*\.(whl|gz)$")


def get_packages() -> List[str]:
    with open("packages.txt") as file:
        content = file.read()

    if not content:
        raise SystemExit("List of packages to check is empty. Please add packages to `packages.txt`")

    packages = [p.replace("* ", "").strip() for p in content.split("\n") if p]
    return packages


def create_docker(txt: str):
    # Generate docker
    with open("Dockerfile.pmc", "w+") as f:
        f.write(txt)

    print("\n[bold]To check installation run:[/bold]")
    print(
        """\
        docker build -f Dockerfile.pmc -t local/airflow .
        docker run local/airflow info
        """
    )


def check_all_present(prefix: str, files: List[str]):
    all_present = True
    for ext in [ASC, SHA, NORM]:
        if any(re.match(ext, f) for f in files):
            print(f"    - {prefix} {ext.pattern}: [green]OK[/green]")
        else:
            print(f"    - {prefix} {ext.pattern}: [red]MISSING[/red]")
            all_present = False
    return all_present


def filter_files(files: List[str], prefix: str):
    return [f for f in files if f.startswith(prefix)]


def check_providers(files: List[str], version: str):
    name_tpl = "apache_airflow_providers_{}-{}"
    pip_packages = []
    for p in get_packages():
        print(p)

        name = name_tpl.format(p.replace(".", "_"), version)
        # Check sources
        check_all_present("sources", filter_files(files, name))

        # Check wheels
        name = name.replace("_", "-")
        if check_all_present("wheel", filter_files(files, name)):
            pip_packages.append(f"{name.rpartition('-')[0]}=={version}")

    return pip_packages


def check_release(files: List[str], version: str):
    print(f"apache_airflow-{version}")

    # Check bin
    name = f"apache-airflow-{version}-bin"
    check_all_present("binaries", filter_files(files, name))

    # Check sources
    name = f"apache-airflow-{version}-source"
    check_all_present("sources", filter_files(files, name))

    # Check wheels
    name = f"apache_airflow-{version}-py"
    check_all_present("wheel", filter_files(files, name))


def check_upgrade_check(files: List[str], version: str):
    print(f"apache_airflow-upgrade-check-{version}")

    name = f"apache-airflow-upgrade-check-{version}-bin"
    check_all_present("binaries", filter_files(files, name))

    name = f"apache-airflow-upgrade-check-{version}-source"
    check_all_present("sources", filter_files(files, name))

    name = f"apache_airflow_upgrade_check-{version}-py"
    check_all_present("wheel", filter_files(files, name))


@click.command()
@click.option(
    "--type",
    "-t",
    "check_type",
    prompt="providers, airflow, upgrade_check",
    type=str,
    help="Type of the check to perform. One of: providers, airflow, upgrade_check",
)
@click.option(
    "--version",
    "-v",
    prompt="Version",
    type=str,
    help="Version of package to verify. For example 1.10.15.rc1, 2021.3.17rc1",
)
@click.option(
    "--path",
    "-p",
    prompt="Path to files",
    type=str,
    help="Path to directory where are sources",
)
def main(check_type: str, path: str, version: str):
    """
    Use this tool to verify that all expected packages are present in Apache Airflow svn.
    In case of providers, it will generate Dockerfile.pmc that you can use
    to verify that all packages are installable.

    In case of providers, you should update `packages.txt` file with list of packages
    that you expect to find (copy-paste the list from VOTE thread).

    Example usages:
    python check_files.py -v 1.10.15rc1 -t airflow -p ~/code/airflow_svn
    python check_files.py -v 1.3.0rc2 -t upgrade_check -p ~/code/airflow_svn
    python check_files.py -v 1.0.3rc1 -t providers -p ~/code/airflow_svn
    """

    if check_type.upper() == PROVIDERS:
        files = os.listdir(os.path.join(path, "providers"))
        pips = check_providers(files, version)
        create_docker(PROVIDERS_DOCKER.format("\n".join([f"RUN pip install '{p}'" for p in pips])))
        return

    if check_type.upper() == AIRFLOW:
        files = os.listdir(os.path.join(path, version))
        check_release(files, version)

        base_version = version.split("rc")[0]
        prev_version = base_version[:-1] + str(int(base_version[-1]) - 1)
        create_docker(AIRFLOW_DOCKER.format(prev_version, version))
        return

    if check_type.upper() == UPGRADE_CHECK:
        files = os.listdir(os.path.join(path, "upgrade-check", version))
        check_upgrade_check(files, version)

        create_docker(DOCKER_UPGRADE.format(version))
        return

    raise SystemExit(f"Unknown check type: {check_type}")


if __name__ == "__main__":
    main()
