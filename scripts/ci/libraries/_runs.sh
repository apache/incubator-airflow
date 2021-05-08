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

# Docker command to build documentation
function runs::run_docs() {
    start_end::group_start "Run build docs"
    docker_v run "${EXTRA_DOCKER_FLAGS[@]}" -t \
        -e "GITHUB_ACTIONS=${GITHUB_ACTIONS="false"}" \
        --entrypoint "/usr/local/bin/dumb-init"  \
        "${AIRFLOW_CI_IMAGE}" \
        "--" "/opt/airflow/scripts/in_container/run_docs_build.sh" "${@}"
    start_end::group_end
}

# Docker command to generate constraint files.
function runs::run_generate_constraints() {
    start_end::group_start "Run generate constraints"
    docker_v run "${EXTRA_DOCKER_FLAGS[@]}" \
        --entrypoint "/usr/local/bin/dumb-init"  \
        "${AIRFLOW_CI_IMAGE}" \
        "--" "/opt/airflow/scripts/in_container/run_generate_constraints.sh"
    start_end::group_end
}

# Docker command to prepare airflow packages
function runs::run_prepare_airflow_packages() {
    start_end::group_start "Run prepare airflow packages"
    docker_v run "${EXTRA_DOCKER_FLAGS[@]}" \
        --entrypoint "/usr/local/bin/dumb-init"  \
        -t \
        -v "${AIRFLOW_SOURCES}:/opt/airflow" \
        "${AIRFLOW_CI_IMAGE}" \
        "--" "/opt/airflow/scripts/in_container/run_prepare_airflow_packages.sh" "${@}"
    start_end::group_end
}


# Docker command to prepare provider packages
function runs::run_prepare_provider_packages() {
    # No group here - groups are added internally
    docker_v run "${EXTRA_DOCKER_FLAGS[@]}" \
        --entrypoint "/usr/local/bin/dumb-init"  \
        -t \
        -v "${AIRFLOW_SOURCES}:/opt/airflow" \
        "${AIRFLOW_CI_IMAGE}" \
        "--" "/opt/airflow/scripts/in_container/run_prepare_provider_packages.sh" "${@}"
}

# Docker command to generate release notes for provider packages
function runs::run_prepare_provider_documentation_non_interactively() {
    # No group here - groups are added internally
    docker_v run "${EXTRA_DOCKER_FLAGS[@]}" \
        --entrypoint "/usr/local/bin/dumb-init"  \
        -t \
        -v "${AIRFLOW_SOURCES}:/opt/airflow" \
        -e "NO_INTERACTIVE=true" \
        "${AIRFLOW_CI_IMAGE}" \
        "--" "/opt/airflow/scripts/in_container/run_prepare_provider_documentation.sh" "${@}"
}

# Docker command to generate release notes for provider packages
function runs::run_prepare_provider_documentation_interactively() {
    # No group here - groups are added internally
    docker_v run "${EXTRA_DOCKER_FLAGS[@]}" \
        --entrypoint "/usr/local/bin/dumb-init"  \
        -it \
        -v "${AIRFLOW_SOURCES}:/opt/airflow" \
        "${AIRFLOW_CI_IMAGE}" \
        "--" "/opt/airflow/scripts/in_container/run_prepare_provider_documentation.sh" "${@}"
}
