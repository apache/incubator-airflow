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

from typing import NamedTuple, Optional

import airflow
from airflow.api_connexion.schemas.version_schema import version_info_schema
from airflow.utils.platform import get_airflow_git_version


class VersionInfo(NamedTuple):
    """Version information"""

    version: str
    git_version: Optional[str]


def get_version():
    """Get version information"""
    airflow_version = airflow.__version__

    git_version = get_airflow_git_version()
    version_info = VersionInfo(version=airflow_version, git_version=git_version)
    return version_info_schema.dump(version_info)
