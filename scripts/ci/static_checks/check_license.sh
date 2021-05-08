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
export PYTHON_MAJOR_MINOR_VERSION=${PYTHON_MAJOR_MINOR_VERSION:-3.6}

# shellcheck source=scripts/ci/libraries/_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/../libraries/_script_init.sh"

function run_check_license() {
    echo
    echo "Running Licence check"
    echo

    # This is the target of a symlink in airflow/www/static/docs -
    # and rat exclude doesn't cope with the symlink target doesn't exist
    mkdir -p docs/_build/html/

    echo "Running license checks. This can take a while."
    # We mount ALL airflow files for the licence check. We want to check them all!
    if ! docker_v run -v "${AIRFLOW_SOURCES}:/opt/airflow" -t \
            --user "$(id -ur):$(id -gr)" \
            --rm --env-file "${AIRFLOW_SOURCES}/scripts/ci/docker-compose/_docker.env" \
            apache/airflow:apache-rat-2021.04.28-0.13 \
            --exclude-file /opt/airflow/.rat-excludes \
            --d /opt/airflow | tee "${AIRFLOW_SOURCES}/logs/rat-results.txt" ; then
        echo
        echo  "${COLOR_RED}ERROR: RAT exited abnormally  ${COLOR_RESET}"
        echo
        exit 1
    fi

    set +e
    local errors
    errors=$(grep -F "??" "${AIRFLOW_SOURCES}/logs/rat-results.txt")
    set -e
    if test ! -z "${errors}"; then
        echo
        echo  "${COLOR_RED}ERROR: Could not find Apache license headers in the following files:  ${COLOR_RESET}"
        echo
        echo "${errors}"
        exit 1
    else
        echo
        echo "${COLOR_GREEN}OK. RAT checks passed.  ${COLOR_RESET}"
        echo
    fi
}

run_check_license
