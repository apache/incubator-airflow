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
"""Config sub-commands"""
import getpass
import locale
import logging
import os
import platform
import subprocess
import sys
from typing import List, Optional
from urllib.parse import urlsplit, urlunsplit

import requests
import tenacity

from airflow import configuration
from airflow.cli.simple_table import AirflowConsole
from airflow.providers_manager import ProvidersManager
from airflow.typing_compat import Protocol
from airflow.utils.cli import suppress_logs_and_warning
from airflow.version import version as airflow_version

log = logging.getLogger(__name__)


class Anonymizer(Protocol):
    """Anonymizer protocol."""

    def process_path(self, value) -> str:
        """Remove pii from paths"""

    def process_username(self, value) -> str:
        """Remove pii from username"""

    def process_url(self, value) -> str:
        """Remove pii from URL"""


class NullAnonymizer(Anonymizer):
    """Do nothing."""

    def _identity(self, value):
        return value

    process_path = process_username = process_url = _identity

    del _identity


class PiiAnonymizer(Anonymizer):
    """Remove personally identifiable info from path."""

    def __init__(self):
        home_path = os.path.expanduser("~")
        username = getpass.getuser()
        self._path_replacements = {home_path: "${HOME}", username: "${USER}"}

    def process_path(self, value):
        if not value:
            return value
        for src, target in self._path_replacements.items():
            value = value.replace(src, target)
        return value

    def process_username(self, value):
        if not value:
            return value
        return value[0] + "..." + value[-1]

    def process_url(self, value):
        if not value:
            return value

        url_parts = urlsplit(value)
        netloc = None
        if url_parts.netloc:
            # unpack
            userinfo = None
            username = None
            password = None

            if "@" in url_parts.netloc:
                userinfo, _, host = url_parts.netloc.partition("@")
            else:
                host = url_parts.netloc
            if userinfo:
                if ":" in userinfo:
                    username, _, password = userinfo.partition(":")
                else:
                    username = userinfo

            # anonymize
            username = self.process_username(username) if username else None
            password = "PASSWORD" if password else None

            # pack
            if username and password and host:
                netloc = username + ":" + password + "@" + host
            elif username and host:
                netloc = username + "@" + host
            elif password and host:
                netloc = ":" + password + "@" + host
            elif host:
                netloc = host
            else:
                netloc = ""

        return urlunsplit((url_parts.scheme, netloc, url_parts.path, url_parts.query, url_parts.fragment))


class OperatingSystem:
    """Operating system"""

    WINDOWS = "Windows"
    LINUX = "Linux"
    MACOSX = "Mac OS"
    CYGWIN = "Cygwin"

    @staticmethod
    def get_current() -> Optional[str]:
        """Get current operating system"""
        if os.name == "nt":
            return OperatingSystem.WINDOWS
        elif "linux" in sys.platform:
            return OperatingSystem.LINUX
        elif "darwin" in sys.platform:
            return OperatingSystem.MACOSX
        elif "cygwin" in sys.platform:
            return OperatingSystem.CYGWIN
        return None


class Architecture:
    """Compute architecture"""

    X86_64 = "x86_64"
    X86 = "x86"
    PPC = "ppc"
    ARM = "arm"

    @staticmethod
    def get_current():
        """Get architecture"""
        return _MACHINE_TO_ARCHITECTURE.get(platform.machine().lower())


_MACHINE_TO_ARCHITECTURE = {
    "amd64": Architecture.X86_64,
    "x86_64": Architecture.X86_64,
    "i686-64": Architecture.X86_64,
    "i386": Architecture.X86,
    "i686": Architecture.X86,
    "x86": Architecture.X86,
    "ia64": Architecture.X86,  # Itanium is different x64 arch, treat it as the common x86.
    "powerpc": Architecture.PPC,
    "power macintosh": Architecture.PPC,
    "ppc64": Architecture.PPC,
    "armv6": Architecture.ARM,
    "armv6l": Architecture.ARM,
    "arm64": Architecture.ARM,
    "armv7": Architecture.ARM,
    "armv7l": Architecture.ARM,
}


class AirflowInfo:
    """Renders information about Airflow instance"""

    def __init__(self, anonymizer):
        self.anonymizer = anonymizer

    @staticmethod
    def _get_version(cmd: List[str], grep: Optional[bytes] = None):
        """Return tools version."""
        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        except OSError:
            return "NOT AVAILABLE"
        stdoutdata, _ = proc.communicate()
        data = [f for f in stdoutdata.split(b"\n") if f]
        if grep:
            data = [line for line in data if grep in line]
        if len(data) != 1:
            return "NOT AVAILABLE"
        else:
            return data[0].decode()

    @staticmethod
    def _task_logging_handler():
        """Returns task logging handler."""

        def get_fullname(o):
            module = o.__class__.__module__
            if module is None or module == str.__class__.__module__:
                return o.__class__.__name__  # Avoid reporting __builtin__
            else:
                return module + '.' + o.__class__.__name__

        try:
            handler_names = [get_fullname(handler) for handler in logging.getLogger('airflow.task').handlers]
            return ", ".join(handler_names)
        except Exception:  # noqa pylint: disable=broad-except
            return "NOT AVAILABLE"

    @property
    def _airflow_info(self):
        executor = configuration.conf.get("core", "executor")
        sql_alchemy_conn = self.anonymizer.process_url(
            configuration.conf.get("core", "SQL_ALCHEMY_CONN", fallback="NOT AVAILABLE")
        )
        dags_folder = self.anonymizer.process_path(
            configuration.conf.get("core", "dags_folder", fallback="NOT AVAILABLE")
        )
        plugins_folder = self.anonymizer.process_path(
            configuration.conf.get("core", "plugins_folder", fallback="NOT AVAILABLE")
        )
        base_log_folder = self.anonymizer.process_path(
            configuration.conf.get("logging", "base_log_folder", fallback="NOT AVAILABLE")
        )
        remote_base_log_folder = self.anonymizer.process_path(
            configuration.conf.get("logging", "remote_base_log_folder", fallback="NOT AVAILABLE")
        )

        return [
            ("version", airflow_version),
            ("executor", executor),
            ("task_logging_handler", self._task_logging_handler()),
            ("sql_alchemy_conn", sql_alchemy_conn),
            ("dags_folder", dags_folder),
            ("plugins_folder", plugins_folder),
            ("base_log_folder", base_log_folder),
            ("remote_base_log_folder", remote_base_log_folder),
        ]

    @property
    def _system_info(self):
        operating_system = OperatingSystem.get_current()
        arch = Architecture.get_current()
        uname = platform.uname()
        _locale = locale.getdefaultlocale()
        python_location = self.anonymizer.process_path(sys.executable)
        python_version = sys.version.replace("\n", " ")

        return [
            ("OS", operating_system or "NOT AVAILABLE"),
            ("architecture", arch or "NOT AVAILABLE"),
            ("uname", str(uname)),
            ("locale", str(_locale)),
            ("python_version", python_version),
            ("python_location", python_location),
        ]

    @property
    def _tools_info(self):
        git_version = self._get_version(["git", "--version"])
        ssh_version = self._get_version(["ssh", "-V"])
        kubectl_version = self._get_version(["kubectl", "version", "--short=True", "--client=True"])
        gcloud_version = self._get_version(["gcloud", "version"], grep=b"Google Cloud SDK")
        cloud_sql_proxy_version = self._get_version(["cloud_sql_proxy", "--version"])
        mysql_version = self._get_version(["mysql", "--version"])
        sqlite3_version = self._get_version(["sqlite3", "--version"])
        psql_version = self._get_version(["psql", "--version"])

        return [
            ("git", git_version),
            ("ssh", ssh_version),
            ("kubectl", kubectl_version),
            ("gcloud", gcloud_version),
            ("cloud_sql_proxy", cloud_sql_proxy_version),
            ("mysql", mysql_version),
            ("sqlite3", sqlite3_version),
            ("psql", psql_version),
        ]

    @property
    def _paths_info(self):
        system_path = os.environ.get("PATH", "").split(os.pathsep)
        airflow_home = self.anonymizer.process_path(configuration.get_airflow_home())
        system_path = [self.anonymizer.process_path(p) for p in system_path]
        python_path = [self.anonymizer.process_path(p) for p in sys.path]
        airflow_on_path = any(os.path.exists(os.path.join(path_elem, "airflow")) for path_elem in system_path)

        return [
            ("airflow_home", airflow_home),
            ("system_path", os.pathsep.join(system_path)),
            ("python_path", os.pathsep.join(python_path)),
            ("airflow_on_path", str(airflow_on_path)),
        ]

    @property
    def _providers_info(self):
        return [(p.provider_info['package-name'], p.version) for p in ProvidersManager().providers.values()]

    def show(self, output: str, console: Optional[AirflowConsole] = None) -> None:
        """Shows information about Airflow instance"""
        all_info = {
            "Apache Airflow": self._airflow_info,
            "System info": self._system_info,
            "Tools info": self._tools_info,
            "Paths info": self._paths_info,
            "Providers info": self._providers_info,
        }

        console = console or AirflowConsole(show_header=False)
        if output in ("table", "plain"):
            # Show each info as table with key, value column
            for key, info in all_info.items():
                console.print(f"\n[bold][green]{key}[/bold][/green]", highlight=False)
                console.print_as(data=[{"key": k, "value": v} for k, v in info], output=output)
        else:
            # Render info in given format, change keys to snake_case
            console.print_as(
                data=[{k.lower().replace(" ", "_"): dict(v)} for k, v in all_info.items()], output=output
            )

    def render_text(self, output: str) -> str:
        """Exports the info to string"""
        console = AirflowConsole(record=True)
        with console.capture():
            self.show(output=output, console=console)
        return console.export_text()


class FileIoException(Exception):
    """Raises when error happens in FileIo.io integration"""


@tenacity.retry(
    stop=tenacity.stop_after_attempt(5),
    wait=tenacity.wait_exponential(multiplier=1, max=10),
    retry=tenacity.retry_if_exception_type(FileIoException),
    before=tenacity.before_log(log, logging.DEBUG),
    after=tenacity.after_log(log, logging.DEBUG),
)
def _upload_text_to_fileio(content):
    """Upload text file to File.io service and return lnk"""
    resp = requests.post("https://file.io", data={"text": content})
    if not resp.ok:
        print(resp.json())
        raise FileIoException("Failed to send report to file.io service.")
    try:
        return resp.json()["link"]
    except ValueError as e:
        log.debug(e)
        raise FileIoException("Failed to send report to file.io service.")


def _send_report_to_fileio(info):
    print("Uploading report to file.io service.")
    try:
        link = _upload_text_to_fileio(str(info))
        print("Report uploaded.")
        print(link)
        print()
    except FileIoException as ex:
        print(str(ex))


@suppress_logs_and_warning
def show_info(args):
    """Show information related to Airflow, system and other."""
    # Enforce anonymization, when file_io upload is tuned on.
    anonymizer = PiiAnonymizer() if args.anonymize or args.file_io else NullAnonymizer()
    info = AirflowInfo(anonymizer)
    if args.file_io:
        _send_report_to_fileio(info.render_text(args.output))
    else:
        info.show(args.output)
