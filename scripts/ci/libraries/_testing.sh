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

export MEMORY_REQUIRED_FOR_INTEGRATION_TEST_PARALLEL_RUN=33000

function testing::skip_tests_if_requested(){
    if [[ -f ${BUILD_CACHE_DIR}/.skip_tests ]]; then
        echo
        echo "Skipping running tests !!!!!"
        echo
        exit
    fi
}

function testing::get_docker_compose_local() {
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
}

function testing::get_maximum_parallel_test_jobs() {
    docker_engine_resources::get_available_cpus_in_docker
    if [[ -n ${RUNS_ON=} && ${RUNS_ON} != *"self-hosted"* ]]; then
        echo
        echo "${COLOR_YELLOW}This is a Github Public runner - for now we are forcing max parallel Quarantined tests jobs to 1 for those${COLOR_RESET}"
        echo
        export MAX_PARALLEL_QUARANTINED_TEST_JOBS="1"
    else
        if [[ ${MAX_PARALLEL_QUARANTINED_TEST_JOBS=} != "" ]]; then
            echo
            echo "${COLOR_YELLOW}Maximum parallel Quarantined test jobs forced via MAX_PARALLEL_QUARANTINED_TEST_JOBS = ${MAX_PARALLEL_QUARANTINED_TEST_JOBS}${COLOR_RESET}"
            echo
        else
            MAX_PARALLEL_QUARANTINED_TEST_JOBS=${CPUS_AVAILABLE_FOR_DOCKER}
            echo
            echo "${COLOR_YELLOW}Maximum parallel Quarantined test jobs set to number of CPUs available for Docker = ${MAX_PARALLEL_QUARANTINED_TEST_JOBS}${COLOR_RESET}"
            echo
        fi

    fi

    if [[ ${MAX_PARALLEL_TEST_JOBS=} != "" ]]; then
        echo
        echo "${COLOR_YELLOW}Maximum parallel test jobs forced via MAX_PARALLEL_TEST_JOBS = ${MAX_PARALLEL_TEST_JOBS}${COLOR_RESET}"
        echo
    else
        MAX_PARALLEL_TEST_JOBS=${CPUS_AVAILABLE_FOR_DOCKER}
        echo
        echo "${COLOR_YELLOW}Maximum parallel test jobs set to number of CPUs available for Docker = ${MAX_PARALLEL_TEST_JOBS}${COLOR_RESET}"
        echo
    fi
    export MAX_PARALLEL_TEST_JOBS
}

function testing::get_test_types_to_run() {
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
