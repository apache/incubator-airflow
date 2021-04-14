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
# shellcheck source=scripts/ci/libraries/_script_init.sh
. "$(dirname "${BASH_SOURCE[0]}")/../libraries/_script_init.sh"

SEMAPHORE_NAME="image_tests"
export SEMAPHORE_NAME

DOCKER_EXAMPLES_DIR=${AIRFLOW_SOURCES}/docs/docker-stack/docker-examples/
export DOCKER_EXAMPLES_DIR

# Launches parallel building of images. Redirects output to log set the right directories
# $1 - name of the job
# $2 - bash file to execute in parallel
function run_image_test_job() {
    local file=$1

    local job_name=$2
    mkdir -p "${PARALLEL_MONITORED_DIR}/${SEMAPHORE_NAME}/${job_name}"
    export JOB_LOG="${PARALLEL_MONITORED_DIR}/${SEMAPHORE_NAME}/${job_name}/stdout"
    export PARALLEL_JOB_STATUS="${PARALLEL_MONITORED_DIR}/${SEMAPHORE_NAME}/${job_name}/status"
    parallel --ungroup --bg --semaphore --semaphorename "${SEMAPHORE_NAME}" \
        --jobs "${MAX_PARALLEL_IMAGE_JOBS}" \
            "$(dirname "${BASH_SOURCE[0]}")/ci_run_prod_image_test.sh" "${job_name}" "${file}" >"${JOB_LOG}" 2>&1
}


function test_images() {
    if [[ ${CI=} == "true" ]]; then
        echo
        echo "Skipping the script builds on CI! "
        echo "They take very long time to build."
        echo
    else
        local scripts_to_test
        scripts_to_test=$(find "${DOCKER_EXAMPLES_DIR}" -type f -name '*.sh' )
        for file in ${scripts_to_test}
        do
            local job_name
            job_name=$(basename "${file}")
            run_image_test_job "${file}" "${job_name}"
        done
    fi
    local dockerfiles_to_test
    dockerfiles_to_test=$(find "${DOCKER_EXAMPLES_DIR}" -type f -name 'Dockerfile' )
    for file in ${dockerfiles_to_test}
    do
        local job_name
        job_name="$(basename "$(dirname "${file}")")"
        run_image_test_job "${file}" "${job_name}"
    done

}

cd "${AIRFLOW_SOURCES}" || exit 1

docker_engine_resources::get_available_cpus_in_docker

# Building max for images in parlallel helps to conserve docker image space
MAX_PARALLEL_IMAGE_JOBS=4
export MAX_PARALLEL_IMAGE_JOBS

parallel::make_sure_gnu_parallel_is_installed
parallel::kill_stale_semaphore_locks
parallel::initialize_monitoring

start_end::group_start "Testing image building"

parallel::monitor_progress

test_images

parallel --semaphore --semaphorename "${SEMAPHORE_NAME}" --wait
start_end::group_end

parallel::print_job_summary_and_return_status_code
