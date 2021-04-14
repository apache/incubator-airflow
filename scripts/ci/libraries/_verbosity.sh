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
# In case "VERBOSE_COMMANDS" is set to "true" set -x is used to enable debugging

DOCKER_BINARY_PATH="${DOCKER_BINARY_PATH:=$(command -v docker || echo "/bin/docker")}"
export DOCKER_BINARY_PATH

function verbosity::store_exit_on_error_status() {
    exit_on_error="false"
    # If 'set -e' is set before entering the function, remember it, so you can restore before return!
    if [[ $- == *e* ]]; then
        exit_on_error="true"
    fi
    set +e
}

function verbosity::restore_exit_on_error_status() {
    if [[ ${exit_on_error} == "true" ]]; then
        set -e
    fi
    unset exit_on_error
}

# In case "VERBOSE" is set to "true" (--verbose flag in Breeze) all docker commands run will be
# printed before execution. In case of DRY_RUN_DOCKER flag set to "true"
# show the command to execute instead of executing them
function docker {
    if [[ ${DRY_RUN_DOCKER} != "false" ]]; then
        echo
        echo "${COLOR_YELLOW}docker" "${@}" "${COLOR_RESET}"
        echo
        return
    fi
    verbosity::store_exit_on_error_status
    if [[ ${VERBOSE:="false"} == "true" && \
        # do not print echo if VERBOSE_COMMAND is set (set -x does it already)
        ${VERBOSE_COMMANDS:=} != "true" && \
        # And when generally printing info is disabled
        ${PRINT_INFO_FROM_SCRIPTS} == "true" ]]; then
        >&2 echo "docker" "${@}"
    fi
    if [[ ${PRINT_INFO_FROM_SCRIPTS} == "false" ]]; then
        ${DOCKER_BINARY_PATH} "${@}" >>"${OUTPUT_LOG}" 2>&1
    else
        ${DOCKER_BINARY_PATH} "${@}" 1> >(tee -a "${OUTPUT_LOG}") 2> >(tee -a "${OUTPUT_LOG}" >&2)
    fi
    res="$?"
    if [[ ${res} == "0" || ${exit_on_error} == "false" ]]; then
        # The log is removed on success or when exit_on_error is set to false
        # This way in the output log we only see the most recent failed command and what was echoed before
        # But if we expect that the command might fail, we also will not print it's output
        rm -f "${OUTPUT_LOG}"
    fi
    verbosity::restore_exit_on_error_status
    return ${res}
}

HELM_BINARY_PATH="${HELM_BINARY_PATH:=$(command -v helm || echo "/bin/helm")}"
export HELM_BINARY_PATH

# In case "VERBOSE" is set to "true" (--verbose flag in Breeze) all helm commands run will be
# printed before execution
function helm {
    verbosity::store_exit_on_error_status
    if [[ ${VERBOSE:="false"} == "true" && ${VERBOSE_COMMANDS:=} != "true" ]]; then
       # do not print echo if VERBOSE_COMMAND is set (set -x does it already)
        >&2 echo "helm" "${@}"
    fi
    ${HELM_BINARY_PATH} "${@}" > >(tee -a "${OUTPUT_LOG}") 2> >(tee -a "${OUTPUT_LOG}" >&2)
    local res="$?"
    if [[ ${res} == "0" || ${exit_on_error} == "false" ]]; then
        rm -f "${OUTPUT_LOG}"
    fi
    verbosity::restore_exit_on_error_status
    return ${res}
}

KUBECTL_BINARY_PATH=${KUBECTL_BINARY_PATH:=$(command -v kubectl || echo "/bin/kubectl")}
export KUBECTL_BINARY_PATH

# In case "VERBOSE" is set to "true" (--verbose flag in Breeze) all kubectl commands run will be
# printed before execution
function kubectl {
    verbosity::store_exit_on_error_status
    if [[ ${VERBOSE:="false"} == "true" && ${VERBOSE_COMMANDS:=} != "true" ]]; then
       # do not print echo if VERBOSE_COMMAND is set (set -x does it already)
        >&2 echo "kubectl" "${@}"
    fi
    ${KUBECTL_BINARY_PATH} "${@}" > >(tee -a "${OUTPUT_LOG}") 2> >(tee -a "${OUTPUT_LOG}" >&2)
    local res="$?"
    if [[ ${res} == "0" || ${exit_on_error} == "false" ]]; then
        rm -f "${OUTPUT_LOG}"
    fi
    verbosity::restore_exit_on_error_status
    return ${res}
}

KIND_BINARY_PATH="${KIND_BINARY_PATH:=$(command -v kind || echo "/bin/kind")}"
export KIND_BINARY_PATH

# In case "VERBOSE" is set to "true" (--verbose flag in Breeze) all kind commands run will be
# printed before execution
function kind {
    if [[ ${VERBOSE:="false"} == "true" && ${VERBOSE_COMMANDS:=} != "true" ]]; then
       # do not print echo if VERBOSE_COMMAND is set (set -x does it already)
        >&2 echo "kind" "${@}"
    fi
    # kind outputs nice output on terminal.
    ${KIND_BINARY_PATH} "${@}"
}


# Prints verbose information in case VERBOSE variable is set
function verbosity::print_info() {
    if [[ ${VERBOSE:="false"} == "true" && ${PRINT_INFO_FROM_SCRIPTS} == "true" ]]; then
        echo "$@"
    fi
}

function verbosity::set_verbosity() {
    # whether verbose output should be produced
    export VERBOSE=${VERBOSE:="false"}

    # whether every bash statement should be printed as they are executed
    export VERBOSE_COMMANDS=${VERBOSE_COMMANDS:="false"}

    # whether the output from script should be printed at all
    export PRINT_INFO_FROM_SCRIPTS=${PRINT_INFO_FROM_SCRIPTS:="true"}
}

verbosity::set_verbosity
