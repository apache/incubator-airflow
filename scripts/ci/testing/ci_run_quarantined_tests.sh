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

# Enable automated tests execution
RUN_TESTS="true"
export RUN_TESTS

SKIPPED_FAILED_JOB="Quarantined"
export SKIPPED_FAILED_JOB

SEMAPHORE_NAME="tests"
export SEMAPHORE_NAME

# shellcheck source=scripts/ci/libraries/_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/../libraries/_script_init.sh"

initialization::set_output_color_variables

BACKEND_TEST_TYPES=(mysql postgres sqlite)

# Starts test types in parallel
# test_types_to_run - list of test types (it's not an array, it is space-separate list)
# ${@} - additional arguments to pass to test execution
function run_quarantined_backend_tests_in_parallel() {
    start_end::group_start "Determining how to run the tests"
    echo
    echo "${COLOR_YELLOW}Running maximum ${MAX_PARALLEL_QUARANTINED_TEST_JOBS} test types in parallel${COLOR_RESET}"
    echo
    start_end::group_end
    start_end::group_start "Monitoring Quarantined tests : ${BACKEND_TEST_TYPES[*]}"
    parallel::initialize_monitoring
    parallel::monitor_progress
    mkdir -p "${PARALLEL_MONITORED_DIR}/${SEMAPHORE_NAME}"
    TEST_TYPE="Quarantined"
    export TEST_TYPE
    for BACKEND in "${BACKEND_TEST_TYPES[@]}"
    do
        export BACKEND
        mkdir -p "${PARALLEL_MONITORED_DIR}/${SEMAPHORE_NAME}/${BACKEND}"
        mkdir -p "${PARALLEL_MONITORED_DIR}/${SEMAPHORE_NAME}/${BACKEND}"
        export JOB_LOG="${PARALLEL_MONITORED_DIR}/${SEMAPHORE_NAME}/${BACKEND}/stdout"
        export PARALLEL_JOB_STATUS="${PARALLEL_MONITORED_DIR}/${SEMAPHORE_NAME}/${BACKEND}/status"
        # Each test job will get SIGTERM followed by SIGTERM 200ms later and SIGKILL 200ms later after 25 mins
        # shellcheck disable=SC2086
        parallel --ungroup --bg --semaphore --semaphorename "${SEMAPHORE_NAME}" \
            --jobs "${MAX_PARALLEL_QUARANTINED_TEST_JOBS}" --timeout 1500 \
            "$( dirname "${BASH_SOURCE[0]}" )/ci_run_single_airflow_test_in_docker.sh" "${@}" >${JOB_LOG} 2>&1
    done
    parallel --semaphore --semaphorename "${SEMAPHORE_NAME}" --wait
    parallel::kill_monitor
    start_end::group_end
}

testing::skip_tests_if_requested

build_images::prepare_ci_build

build_images::rebuild_ci_image_if_needed_with_group

parallel::make_sure_gnu_parallel_is_installed

testing::get_maximum_parallel_test_jobs

testing::get_docker_compose_local

run_quarantined_backend_tests_in_parallel "${@}"

set +e

parallel::print_job_summary_and_return_status_code

echo "Those are quarantined tests so failure of those does not fail the whole build!"
echo "Please look above for the output of failed tests to fix them!"
echo
