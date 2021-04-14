#!/usr/bin/env bash
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

# This is an example docker build script. It is not intended for PRODUCTION use
set -euo pipefail
AIRFLOW_SOURCES="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../../" && pwd)"
cd "${AIRFLOW_SOURCES}"

# [START build]
docker build . -f Dockerfile \
    --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg AIRFLOW_VERSION="2.0.1" \
    --build-arg ADDITIONAL_AIRFLOW_EXTRAS="slack,odbc" \
    --build-arg ADDITIONAL_PYTHON_DEPS=" \
        azure-storage-blob \
        oauth2client \
        beautifulsoup4 \
        dateparser \
        rocketchat_API \
        typeform" \
    --build-arg ADDITIONAL_DEV_APT_COMMAND="curl https://packages.microsoft.com/keys/microsoft.asc | \
    apt-key add --no-tty - && \
    curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list" \
    --build-arg ADDITIONAL_DEV_APT_ENV="ACCEPT_EULA=Y" \
    --build-arg ADDITIONAL_DEV_APT_DEPS="msodbcsql17 unixodbc-dev g++" \
    --build-arg ADDITIONAL_RUNTIME_APT_COMMAND="curl https://packages.microsoft.com/keys/microsoft.asc | \
    apt-key add --no-tty - && \
    curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list" \
    --build-arg ADDITIONAL_RUNTIME_APT_ENV="ACCEPT_EULA=Y" \
    --build-arg ADDITIONAL_RUNTIME_APT_DEPS="msodbcsql17 unixodbc git procps vim" \
    --tag "$(basename "$0")"
# [END build]
docker rmi --force "$(basename "$0")"
