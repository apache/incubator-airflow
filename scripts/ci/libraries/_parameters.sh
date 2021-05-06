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

# Reads environment variable passed as first parameter from the .build cache file
function parameters::read_from_file() {
    cat "${BUILD_CACHE_DIR}/.$1" 2>/dev/null || true
}

# Saves environment variable passed as first parameter to the .build cache file
function parameters::save_to_file() {
    # shellcheck disable=SC2005
    echo "$(eval echo "\$$1")" >"${BUILD_CACHE_DIR}/.$1"
}

# check if parameter set for the variable is allowed (should be on the _breeze_allowed list)
# parameters:
# $1 - name of the variable
# $2 - descriptive name of the parameter
# $3 - flag used to set the parameter
function parameters::check_allowed_param() {
    _VARIABLE_NAME="${1}"
    _VARIABLE_DESCRIPTIVE_NAME="${2}"
    _FLAG="${3}"
    _ALLOWED_VALUES_ENV_NAME="_breeze_allowed_$(echo "${_VARIABLE_NAME}" | tr '[:upper:]' '[:lower:]')s"
    _ALLOWED_VALUES=" ${!_ALLOWED_VALUES_ENV_NAME//$'\n'/ } "
    _VALUE=${!_VARIABLE_NAME}
    if [[ ${_ALLOWED_VALUES:=} != *" ${_VALUE} "* ]]; then
        echo
        echo  "${COLOR_RED}ERROR: Allowed ${_VARIABLE_DESCRIPTIVE_NAME}: [${_ALLOWED_VALUES}]. Passed: '${!_VARIABLE_NAME}'  ${COLOR_RESET}"
        echo
        echo "Switch to supported value with ${_FLAG} flag."
        echo
        if [[ -n ${!_VARIABLE_NAME} && -f "${BUILD_CACHE_DIR}/.${_VARIABLE_NAME}" && ${!_VARIABLE_NAME} == $(cat "${BUILD_CACHE_DIR}/.${_VARIABLE_NAME}") ]]; then
            echo
            echo  "${COLOR_YELLOW}WARNING: Removing ${BUILD_CACHE_DIR}/.${_VARIABLE_NAME}. Next time you run it, it should be OK.  ${COLOR_RESET}"
            echo
            echo
            rm -f "${BUILD_CACHE_DIR}/.${_VARIABLE_NAME}"
        fi
        exit 1
    fi
}
# check if parameter set for the variable is allowed (should be on the _breeze_allowed list)
# and if it is, it saves it to .build cache file. In case the parameter is wrong, the
# saved variable is removed (so that bad value is not used again in case it comes from there)
# and exits with an error
function parameters::check_and_save_allowed_param() {
    parameters::check_allowed_param "${@}"
    parameters::save_to_file "${1}"
}
