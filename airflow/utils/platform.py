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

"""Platform and system specific function."""
import getpass
import logging
import os
import pkgutil
import sys

log = logging.getLogger(__name__)


def is_tty():
    """
    Checks if the standard output is connected (is associated with a terminal device) to a tty(-like)
    device.
    """
    if not hasattr(sys.stdout, "isatty"):
        return False
    return sys.stdout.isatty()


def is_terminal_support_colors() -> bool:
    """Try to determine if the current terminal supports colors."""
    if sys.platform == "win32":
        return False
    if not is_tty():
        return False
    if "COLORTERM" in os.environ:
        return True
    term = os.environ.get("TERM", "dumb").lower()
    if term in ("xterm", "linux") or "color" in term:
        return True
    return False


def get_airflow_git_version():
    """Returns the git commit hash representing the current version of the application."""
    git_version = None
    try:
        git_version = str(pkgutil.get_data('airflow', 'git_version'), encoding="UTF-8")
    except Exception as e:  # pylint: disable=broad-except
        log.debug(e)

    return git_version


def getuser() -> str:
    """
    Gets the username associated with the current user, or error with a nice
    error message if there's no current user.

    We don't want to fall back to os.getuid() because not having a username
    probably means the rest of the user environment is wrong (e.g. no $HOME).
    Explicit failure is better than silently trying to work badly.
    """
    try:
        return getpass.getuser()
    except KeyError:
        # Inner import to avoid circular import
        from airflow.exceptions import AirflowConfigException

        raise AirflowConfigException(
            "The user that Airflow is running as has no username; you must run"
            "Airflow as a full user, with a username and home directory, "
            "in order for it to function properly."
        )
