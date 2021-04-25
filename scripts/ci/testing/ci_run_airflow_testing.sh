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

# Enable automated tests execution
RUN_TESTS="true"
export RUN_TESTS

SKIPPED_FAILED_JOB="Quarantined"
export SKIPPED_FAILED_JOB

# shellcheck source=scripts/ci/libraries/_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/../libraries/_script_init.sh"

if [[ -f ${BUILD_CACHE_DIR}/.skip_tests ]]; then
    echo
    echo "Skipping running tests !!!!!"
    echo
    exit
fi

# In case we see too many failures on regular PRs from our users using GitHub Public runners
# We can uncomment this and come back to sequential test-type execution
#if [[ ${RUNS_ON} != *"self-hosted"* ]]; then
#    echo
#    echo "${COLOR_YELLOW}This is a Github Public runner - for now we are forcing max parallel jobs to 1 for those${COLOR_RESET}"
#    echo "${COLOR_YELLOW}Until we fix memory usage to allow up to 2 parallel runs on those runners${COLOR_RESET}"
#    echo
#    # Forces testing in parallel in case the script is run on self-hosted runners
#    export MAX_PARALLEL_TEST_JOBS="1"
#fi

SEMAPHORE_NAME="tests"

function prepare_tests_to_run() {
    DOCKER_COMPOSE_LOCAL+=("-f" "${SCRIPTS_CI_DIR}/docker-compose/files.yml")
    if [[ ${MOUNT_SELECTED_LOCAL_SOURCES} == "true" ]]; then
        DOCKER_COMPOSE_LOCAL+=("-f" "${SCRIPTS_CI_DIR}/docker-compose/local.yml")
    fi
    if [[ ${MOUNT_ALL_LOCAL_SOURCES} == "true" ]]; then
        DOCKER_COMPOSE_LOCAL+=("-f" "${SCRIPTS_CI_DIR}/docker-compose/local-all-sources.yml")
    fi

    if [[ ${GITHUB_ACTIONS} == "true" ]]; then
        DOCKER_COMPOSE_LOCAL+=("-f" "${SCRIPTS_CI_DIR}/docker-compose/ga.yml")
    fi

    if [[ ${FORWARD_CREDENTIALS} == "true" ]]; then
        DOCKER_COMPOSE_LOCAL+=("-f" "${SCRIPTS_CI_DIR}/docker-compose/forward-credentials.yml")
    fi

    if [[ -n ${INSTALL_AIRFLOW_VERSION=} || -n ${INSTALL_AIRFLOW_REFERENCE} ]]; then
        DOCKER_COMPOSE_LOCAL+=("-f" "${SCRIPTS_CI_DIR}/docker-compose/remove-sources.yml")
    fi
    readonly DOCKER_COMPOSE_LOCAL

    if [[ -n "${FORCE_TEST_TYPE=}" ]]; then
        # Handle case where test type is forced from outside
        export TEST_TYPES="${FORCE_TEST_TYPE}"
    fi

    if [[ -z "${TEST_TYPES=}" ]]; then
        TEST_TYPES="Core Providers API CLI Integration Other WWW"
        echo
        echo "Test types not specified. Adding all: ${TEST_TYPES}"
        echo
    fi

    if [[ -z "${FORCE_TEST_TYPE=}" ]]; then
        # Add Postgres/MySQL special test types in case we are running several test types
        if [[ ${BACKEND} == "postgres" && ${TEST_TYPES} != "Quarantined" ]]; then
            TEST_TYPES="${TEST_TYPES} Postgres"
            echo
            echo "Added Postgres. Tests to run: ${TEST_TYPES}"
            echo
        fi
        if [[ ${BACKEND} == "mysql" && ${TEST_TYPES} != "Quarantined" ]]; then
            TEST_TYPES="${TEST_TYPES} MySQL"
            echo
            echo "Added MySQL. Tests to run: ${TEST_TYPES}"
            echo
        fi
    fi
    readonly TEST_TYPES
}

function kill_all_running_docker_containers() {
    echo
    echo "${COLOR_BLUE}Kill all running docker containers${COLOR_RESET}"
    echo
    # shellcheck disable=SC2046
    docker kill $(docker ps -q) || true
}

function system_prune_docker() {
    echo
    echo "${COLOR_BLUE}System-prune docker${COLOR_RESET}"
    echo
    docker system prune --force --volumes
    echo
}

function get_maximum_parallel_test_jobs() {
    if [[ ${MAX_PARALLEL_TEST_JOBS=} != "" ]]; then
        echo
        echo "${COLOR_YELLOW}Maximum parallel test jobs forced vi MAX_PARALLEL_TEST_JOBS = ${MAX_PARALLEL_TEST_JOBS}${COLOR_RESET}"
        echo
    else
        MAX_PARALLEL_TEST_JOBS=${CPUS_AVAILABLE_FOR_DOCKER}
        echo
        echo "${COLOR_YELLOW}Maximum parallel test jobs set to number of CPUs available for Docker = ${MAX_PARALLEL_TEST_JOBS}${COLOR_RESET}"
        echo
    fi
    export MAX_PARALLEL_TEST_JOBS
}

# Cleans up runner before test execution.
#  * Kills all running docker containers
#  * System prune to clean all the temporary/unnamed images and left-over volumes
#  * Print information about available space and memory
#  * Kills stale semaphore locks
function cleanup_runner() {
    start_end::group_start "Cleanup runner"
    kill_all_running_docker_containers
    system_prune_docker
    docker_engine_resources::get_available_memory_in_docker
    docker_engine_resources::get_available_cpus_in_docker
    docker_engine_resources::get_available_disk_space_in_docker
    docker_engine_resources::print_overall_stats
    get_maximum_parallel_test_jobs
    parallel::kill_stale_semaphore_locks
    start_end::group_end
}

# Starts test types in parallel
# test_types_to_run - list of test types (it's not an array, it is space-separate list)
# ${@} - additional arguments to pass to test execution
function run_test_types_in_parallel() {
    start_end::group_start "Monitoring tests: ${test_types_to_run}"
    parallel::monitor_progress
    mkdir -p "${PARALLEL_MONITORED_DIR}/${SEMAPHORE_NAME}"
    for TEST_TYPE in ${test_types_to_run}
    do
        export TEST_TYPE
        mkdir -p "${PARALLEL_MONITORED_DIR}/${SEMAPHORE_NAME}/${TEST_TYPE}"
        mkdir -p "${PARALLEL_MONITORED_DIR}/${SEMAPHORE_NAME}/${TEST_TYPE}"
        export JOB_LOG="${PARALLEL_MONITORED_DIR}/${SEMAPHORE_NAME}/${TEST_TYPE}/stdout"
        export PARALLEL_JOB_STATUS="${PARALLEL_MONITORED_DIR}/${SEMAPHORE_NAME}/${TEST_TYPE}/status"
        # Each test job will get SIGTERM followed by SIGTERM 200ms later and SIGKILL 200ms later after 25 mins
        # shellcheck disable=SC2086
        parallel --ungroup --bg --semaphore --semaphorename "${SEMAPHORE_NAME}" \
            --jobs "${MAX_PARALLEL_TEST_JOBS}" --timeout 1500 \
            "$( dirname "${BASH_SOURCE[0]}" )/ci_run_single_airflow_test_in_docker.sh" "${@}" >${JOB_LOG} 2>&1
    done
    parallel --semaphore --semaphorename "${SEMAPHORE_NAME}" --wait
    parallel::kill_monitor
    start_end::group_end
}


export MEMORY_REQUIRED_FOR_INTEGRATION_TEST_PARALLEL_RUN=33000

# Runs all test types in parallel depending on the number of CPUs available
# We monitors their progress, display the progress  and summarize the result when finished.
#
# In case there is not enough memory (MEMORY_REQUIRED_FOR_INTEGRATION_TEST_PARALLEL_RUN) available for
# the docker engine, the integration tests (which take a lot of memory for all the integrations)
# are run sequentially after all other tests were run in parallel.
#
# Input:
#   * TEST_TYPES  - contains all test types that should be executed
#   * MEMORY_REQUIRED_FOR_INTEGRATION_TEST_PARALLEL_RUN - memory in bytes required to run integration tests
#             in parallel to other tests
#   * MEMORY_AVAILABLE_FOR_DOCKER - memory that is available in docker (set by cleanup_runners)
#
function run_all_test_types_in_parallel() {
    cleanup_runner

    start_end::group_start "Determine how to run the tests"
    echo
    echo "${COLOR_YELLOW}Running maximum ${MAX_PARALLEL_TEST_JOBS} test types in parallel${COLOR_RESET}"
    echo

    local run_integration_tests_separately="false"
    local test_types_to_run=${TEST_TYPES}

    if [[ ${test_types_to_run} == *"Integration"* ]]; then
        if (( MEMORY_AVAILABLE_FOR_DOCKER < MEMORY_REQUIRED_FOR_INTEGRATION_TEST_PARALLEL_RUN )) ; then
            # In case of Integration tests - they need more resources (Memory) thus we only run them in
            # parallel if we have more than 32 GB memory available. Otherwise we run them sequentially
            # after cleaning up the memory and stopping all docker instances
            echo ""
            echo "${COLOR_YELLOW}There is not enough memory to run Integration test in parallel${COLOR_RESET}"
            echo "${COLOR_YELLOW}   Available memory: ${MEMORY_AVAILABLE_FOR_DOCKER}${COLOR_RESET}"
            echo "${COLOR_YELLOW}   Required memory: ${MEMORY_REQUIRED_FOR_INTEGRATION_TEST_PARALLEL_RUN}${COLOR_RESET}"
            echo ""
            echo "${COLOR_YELLOW}Integration tests will be run separately at the end after cleaning up docker${COLOR_RESET}"
            echo ""
            # Remove Integration from list of tests to run in parallel
            test_types_to_run="${test_types_to_run//Integration/}"
            run_integration_tests_separately="true"
        fi
    fi
    set +e
    start_end::group_end

    parallel::initialize_monitoring

    run_test_types_in_parallel "${@}"
    if [[ ${run_integration_tests_separately} == "true" ]]; then
        cleanup_runner
        test_types_to_run="Integration"
        run_test_types_in_parallel "${@}"
    fi
    set -e
    # this will exit with error code in case some of the non-Quarantined tests failed
    parallel::print_job_summary_and_return_status_code
}

build_images::prepare_ci_build

build_images::rebuild_ci_image_if_needed_with_group

prepare_tests_to_run

parallel::make_sure_gnu_parallel_is_installed

run_all_test_types_in_parallel "${@}"
