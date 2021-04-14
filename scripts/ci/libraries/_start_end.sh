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

# Starts group for GitHub Actions - makes logs much more readable
function start_end::group_start {
    if [[ ${PRINT_INFO_FROM_SCRIPTS} != "false" ]]; then
        if [[ ${GITHUB_ACTIONS=} == "true" ]]; then
            echo "::group::${1}"
        else
            echo
            echo "${1}"
            echo
        fi
    fi
}

# Ends group for GitHub Actions
function start_end::group_end {
    if [[ ${PRINT_INFO_FROM_SCRIPTS} != "false" ]]; then
        if [[ ${GITHUB_ACTIONS=} == "true" ]]; then
            echo -e "\033[0m"  # Disable any colors set in the group
            echo "::endgroup::"
        fi
    fi
}


#
# Starts the script.
# If VERBOSE_COMMANDS variable is set to true, it enables verbose output of commands executed
# Also prints some useful diagnostics information at start of the script if VERBOSE is set to true
#
function start_end::script_start {
    verbosity::print_info
    verbosity::print_info "Running '${COLOR_GREEN}$(basename "$0")${COLOR_RESET}'"
    verbosity::print_info
    verbosity::print_info "${COLOR_BLUE}Log is redirected to '${OUTPUT_LOG}'${COLOR_RESET}"
    verbosity::print_info
    if [[ ${VERBOSE_COMMANDS:="false"} == "true" ]]; then
        verbosity::print_info
        verbosity::print_info "${COLOR_BLUE}Variable VERBOSE_COMMANDS Set to \"true\"${COLOR_RESET}"
        verbosity::print_info "${COLOR_BLUE}You will see a lot of output${COLOR_RESET}"
        verbosity::print_info
        set -x
    else
        verbosity::print_info "You can increase verbosity by running 'export VERBOSE_COMMANDS=\"true\""
        if [[ ${SKIP_CACHE_DELETION:=} != "true" ]]; then
            verbosity::print_info "And skip deleting the output file with 'export SKIP_CACHE_DELETION=\"true\""
        fi
        verbosity::print_info
        set +x
    fi
    START_SCRIPT_TIME=$(date +%s)
}

function start_end::dump_container_logs() {
    start_end::group_start "${COLOR_BLUE}Dumping container logs ${container}${COLOR_RESET}"
    local container="${1}"
    local dump_file
    dump_file=${AIRFLOW_SOURCES}/files/container_logs_${container}_$(date "+%Y-%m-%d")_${CI_BUILD_ID}_${CI_JOB_ID}.log
    echo "${COLOR_BLUE}###########################################################################################${COLOR_RESET}"
    echo "                   Dumping logs from ${container} container"
    echo "${COLOR_BLUE}###########################################################################################${COLOR_RESET}"
    docker logs "${container}" > "${dump_file}"
    echo "                   Container ${container} logs dumped to ${dump_file}"
    echo "${COLOR_BLUE}###########################################################################################${COLOR_RESET}"
    start_end::group_end
}


#
# Trap function executed always at the end of the script. In case of verbose output it also
# Prints the exit code that the script exits with. Removes verbosity of commands in case it was run with
# command verbosity and in case the script was not run from Breeze (so via ci scripts) it displays
# total time spent in the script so that we can easily see it.
#
function start_end::script_end {
    #shellcheck disable=2181
    local exit_code=$?
    if [[ ${exit_code} != 0 ]]; then
        # Finish previous group so that output can be written
        # Cat output log in case we exit with error but only if we do not PRINT_INFO_FROM_SCRIPTS
        # Because it will be printed immediately by "tee"
        if [[ -f "${OUTPUT_LOG}" && ${PRINT_INFO_FROM_SCRIPTS} == "false" ]]; then
            cat "${OUTPUT_LOG}"
        fi
        start_end::group_end
        echo
        echo "${COLOR_RED}ERROR: The previous step completed with error. Please take a look at output above ${COLOR_RESET}"
        echo
        if [[ ${CI} == "true" ]]; then
            local container
            for container in $(docker ps --format '{{.Names}}')
            do
                start_end::dump_container_logs "${container}"
            done
        fi
        verbosity::print_info "${COLOR_RED}###########################################################################################${COLOR_RESET}"
        verbosity::print_info "${COLOR_RED}                   EXITING WITH STATUS CODE ${exit_code}${COLOR_RESET}"
        verbosity::print_info "${COLOR_RED}###########################################################################################${COLOR_RESET}"
    fi
    if [[ ${VERBOSE_COMMANDS:="false"} == "true" ]]; then
        set +x
    fi

    if [[ ${#FILES_TO_CLEANUP_ON_EXIT[@]} -gt 0 ]]; then
      rm -rf -- "${FILES_TO_CLEANUP_ON_EXIT[@]}"
    fi

    END_SCRIPT_TIME=$(date +%s)
    RUN_SCRIPT_TIME=$((END_SCRIPT_TIME-START_SCRIPT_TIME))
    if [[ ${BREEZE:=} != "true" && ${RUN_TESTS=} != "true" ]]; then
        verbosity::print_info
        verbosity::print_info "Finished the script ${COLOR_GREEN}$(basename "$0")${COLOR_RESET}"
        verbosity::print_info "Elapsed time spent in the script: ${COLOR_BLUE}${RUN_SCRIPT_TIME} seconds${COLOR_RESET}"
        if [[ ${exit_code} == "0" ]]; then
            verbosity::print_info "Exit code ${COLOR_GREEN}${exit_code}${COLOR_RESET}"
        else
            verbosity::print_info "Exit code ${COLOR_RED}${exit_code}${COLOR_RESET}"
        fi
        verbosity::print_info
    fi
}
