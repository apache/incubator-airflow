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
set -euo pipefail

test -v INSTALL_MYSQL_CLIENT
test -v AIRFLOW_INSTALL_USER_FLAG
test -v AIRFLOW_REPO
test -v AIRFLOW_BRANCH
test -v AIRFLOW_PIP_VERSION

set -x

function common::get_airflow_version_specification() {
    if [[ -z ${AIRFLOW_VERSION_SPECIFICATION}
        && -n ${AIRFLOW_VERSION}
        && ${AIRFLOW_INSTALLATION_METHOD} != "." ]]; then
        AIRFLOW_VERSION_SPECIFICATION="==${AIRFLOW_VERSION}"
    fi
}

function common::get_constraints_location() {
    # auto-detect Airflow-constraint reference and location
    if [[ -z "${AIRFLOW_CONSTRAINTS_REFERENCE}" ]]; then
        if [[ ${AIRFLOW_VERSION} =~ [^0-9]*1[^0-9]*10[^0-9]([0-9]*) ]]; then
            # All types of references/versions match this regexp for 1.10 series
            # for example v1_10_test, 1.10.10, 1.10.9 etc. ${BASH_REMATCH[1]} matches last
            # minor digit of version and it's length is 0 for v1_10_test, 1 for 1.10.9 and 2 for 1.10.10+
            AIRFLOW_MINOR_VERSION_NUMBER=${BASH_REMATCH[1]}
            if [[ ${#AIRFLOW_MINOR_VERSION_NUMBER} == "0" ]]; then
                # For v1_10_* branches use constraints-1-10 branch
                AIRFLOW_CONSTRAINTS_REFERENCE=constraints-1-10
            else
                AIRFLOW_CONSTRAINTS_REFERENCE=constraints-${AIRFLOW_VERSION}
            fi
        elif  [[ ${AIRFLOW_VERSION} =~ v?2.* ]]; then
            AIRFLOW_CONSTRAINTS_REFERENCE=constraints-${AIRFLOW_VERSION}
        else
            AIRFLOW_CONSTRAINTS_REFERENCE=${DEFAULT_CONSTRAINTS_BRANCH}
        fi
    fi

    if [[ -z ${AIRFLOW_CONSTRAINTS_LOCATION} ]]; then
        local constraints_base="https://raw.githubusercontent.com/${CONSTRAINTS_GITHUB_REPOSITORY}/${AIRFLOW_CONSTRAINTS_REFERENCE}"
        local python_version
        python_version="$(python --version 2>/dev/stdout | cut -d " " -f 2 | cut -d "." -f 1-2)"
        AIRFLOW_CONSTRAINTS_LOCATION="${constraints_base}/${AIRFLOW_CONSTRAINTS}-${python_version}.txt"
    fi
}
