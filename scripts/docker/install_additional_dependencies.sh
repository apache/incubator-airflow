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
# shellcheck disable=SC2086
set -euo pipefail

test -v UPGRADE_TO_NEWER_DEPENDENCIES
test -v ADDITIONAL_PYTHON_DEPS
test -v EAGER_UPGRADE_ADDITIONAL_REQUIREMENTS
test -v AIRFLOW_INSTALL_USER_FLAG
test -v AIRFLOW_PIP_VERSION
test -v CONTINUE_ON_PIP_CHECK_FAILURE

set -x

# Installs additional dependencies passed as Argument to the Docker build command
function install_additional_dependencies() {
    if [[ "${UPGRADE_TO_NEWER_DEPENDENCIES}" != "false" ]]; then
        echo
        echo Installing additional dependencies while upgrading to newer dependencies
        echo
        pip install ${AIRFLOW_INSTALL_USER_FLAG} --upgrade --upgrade-strategy eager \
            ${ADDITIONAL_PYTHON_DEPS} ${EAGER_UPGRADE_ADDITIONAL_REQUIREMENTS}
        # make sure correct PIP version is used
        pip install ${AIRFLOW_INSTALL_USER_FLAG} --upgrade "pip==${AIRFLOW_PIP_VERSION}"
        pip check || ${CONTINUE_ON_PIP_CHECK_FAILURE}
    else
        echo
        echo Installing additional dependencies upgrading only if needed
        echo
        pip install ${AIRFLOW_INSTALL_USER_FLAG} \
            --upgrade --upgrade-strategy only-if-needed \
            ${ADDITIONAL_PYTHON_DEPS}
        # make sure correct PIP version is used
        pip install ${AIRFLOW_INSTALL_USER_FLAG} --upgrade "pip==${AIRFLOW_PIP_VERSION}"
        pip check || ${CONTINUE_ON_PIP_CHECK_FAILURE}
    fi
}

install_additional_dependencies
