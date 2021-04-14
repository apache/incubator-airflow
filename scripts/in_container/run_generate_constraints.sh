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
# shellcheck source=scripts/in_container/_in_container_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/_in_container_script_init.sh"

CONSTRAINTS_DIR="/files/constraints-${PYTHON_MAJOR_MINOR_VERSION}"

if [[ ${GENERATE_CONSTRAINTS_MODE} == "no-providers" ]]; then
    AIRFLOW_CONSTRAINTS="constraints-no-providers"
    NO_PROVIDERS_EXTRAS=$(python -c 'import setup; print(",".join(setup.CORE_EXTRAS_REQUIREMENTS.keys()))')
    echo
    echo "UnInstall All PIP packages."
    echo
    uninstall_all_pip_packages
    echo
    echo "Install airflow with [${NO_PROVIDERS_EXTRAS}] extras only (uninstall all packages first)."
    echo
    install_local_airflow_with_eager_upgrade "[${NO_PROVIDERS_EXTRAS}]"
elif [[ ${GENERATE_CONSTRAINTS_MODE} == "source-providers" ]]; then
    AIRFLOW_CONSTRAINTS="constraints-source-providers"
    echo
    echo "Providers are already installed from sources."
    echo
elif [[ ${GENERATE_CONSTRAINTS_MODE} == "pypi-providers" ]]; then
    AIRFLOW_CONSTRAINTS="constraints"
    echo
    echo "Install all providers from PyPI so that they are included in the constraints."
    echo
    install_all_providers_from_pypi_with_eager_upgrade
else
    echo
    echo "${COLOR_RED}Error! GENERATE_CONSTRAINTS_MODE has wrong value: '${GENERATE_CONSTRAINTS_MODE}' ${COLOR_RESET}"
    echo
    exit 1
fi

readonly AIRFLOW_CONSTRAINTS

LATEST_CONSTRAINT_FILE="${CONSTRAINTS_DIR}/original-${AIRFLOW_CONSTRAINTS}-${PYTHON_MAJOR_MINOR_VERSION}.txt"
CURRENT_CONSTRAINT_FILE="${CONSTRAINTS_DIR}/${AIRFLOW_CONSTRAINTS}-${PYTHON_MAJOR_MINOR_VERSION}.txt"

mkdir -pv "${CONSTRAINTS_DIR}"

CONSTRAINTS_LOCATION="https://raw.githubusercontent.com/${CONSTRAINTS_GITHUB_REPOSITORY}/${DEFAULT_CONSTRAINTS_BRANCH}/${AIRFLOW_CONSTRAINTS}-${PYTHON_MAJOR_MINOR_VERSION}.txt"
readonly CONSTRAINTS_LOCATION

touch "${LATEST_CONSTRAINT_FILE}"
curl --connect-timeout 60  --max-time 60 "${CONSTRAINTS_LOCATION}" --output "${LATEST_CONSTRAINT_FILE}" || true

echo
echo "Freezing constraints to ${CURRENT_CONSTRAINT_FILE}"
echo

pip freeze | sort | \
    grep -v "apache_airflow" | \
    grep -v "@" | \
    grep -v "/opt/airflow" >"${CURRENT_CONSTRAINT_FILE}"

echo
echo "Constraints generated in ${CURRENT_CONSTRAINT_FILE}"
echo

set +e
diff --color=always "${LATEST_CONSTRAINT_FILE}" "${CURRENT_CONSTRAINT_FILE}"

exit 0
