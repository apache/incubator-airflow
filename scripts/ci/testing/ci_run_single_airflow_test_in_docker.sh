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
# Skip printing groups in CI
PRINT_INFO_FROM_SCRIPTS="false"
# shellcheck source=scripts/ci/libraries/_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/../libraries/_script_init.sh"

PRINT_INFO_FROM_SCRIPTS="true"
export PRINT_INFO_FROM_SCRIPTS

DOCKER_COMPOSE_LOCAL=()
INTEGRATIONS=()

function prepare_tests() {
    DOCKER_COMPOSE_LOCAL+=("-f" "${SCRIPTS_CI_DIR}/docker-compose/files.yml")
    if [[ ${MOUNT_SELECTED_LOCAL_SOURCES} == "true" ]]; then
        DOCKER_COMPOSE_LOCAL+=("-f" "${SCRIPTS_CI_DIR}/docker-compose/local.yml")
    fi
    if [[ ${MOUNT_ALL_LOCAL_SOURCES} == "true" ]]; then
        DOCKER_COMPOSE_LOCAL+=("-f" "${SCRIPTS_CI_DIR}/docker-compose/local-all-sources.yml")
    fi

    if [[ ${GITHUB_ACTIONS=} == "true" ]]; then
        DOCKER_COMPOSE_LOCAL+=("-f" "${SCRIPTS_CI_DIR}/docker-compose/ga.yml")
    fi

    if [[ ${FORWARD_CREDENTIALS} == "true" ]]; then
        DOCKER_COMPOSE_LOCAL+=("-f" "${SCRIPTS_CI_DIR}/docker-compose/forward-credentials.yml")
    fi

    if [[ -n ${INSTALL_AIRFLOW_VERSION=} || -n ${INSTALL_AIRFLOW_REFERENCE} ]]; then
        DOCKER_COMPOSE_LOCAL+=("-f" "${SCRIPTS_CI_DIR}/docker-compose/remove-sources.yml")
    fi
    readonly DOCKER_COMPOSE_LOCAL

    if [[ ${TEST_TYPE:=} == "Integration" ]]; then
        export ENABLED_INTEGRATIONS="${AVAILABLE_INTEGRATIONS}"
        export RUN_INTEGRATION_TESTS="${AVAILABLE_INTEGRATIONS}"
    else
        export ENABLED_INTEGRATIONS=""
        export RUN_INTEGRATION_TESTS=""
    fi

    for _INT in ${ENABLED_INTEGRATIONS}
    do
        INTEGRATIONS+=("-f")
        INTEGRATIONS+=("${SCRIPTS_CI_DIR}/docker-compose/integration-${_INT}.yml")
    done

    readonly INTEGRATIONS

    echo "**********************************************************************************************"
    echo
    echo "      TEST_TYPE: ${TEST_TYPE}, ENABLED INTEGRATIONS: ${ENABLED_INTEGRATIONS}"
    echo
    echo "**********************************************************************************************"
}

# Runs airflow testing in docker container
# You need to set variable TEST_TYPE - test type to run
# "${@}" - extra arguments to pass to docker command
function run_airflow_testing_in_docker() {
    set +u
    set +e
    local exit_code
    echo
    echo "Semaphore grabbed. Running tests for ${TEST_TYPE}"
    echo
    for try_num in {1..5}
    do
        echo
        echo "Starting try number ${try_num}"
        echo
        echo
        echo "Making sure docker-compose is down and remnants removed"
        echo
        docker-compose --log-level INFO -f "${SCRIPTS_CI_DIR}/docker-compose/base.yml" \
            --project-name "airflow-${TEST_TYPE}" \
            down --remove-orphans \
            --volumes --timeout 10
        docker-compose --log-level INFO \
          -f "${SCRIPTS_CI_DIR}/docker-compose/base.yml" \
          -f "${SCRIPTS_CI_DIR}/docker-compose/backend-${BACKEND}.yml" \
          "${INTEGRATIONS[@]}" \
          "${DOCKER_COMPOSE_LOCAL[@]}" \
          --project-name "airflow-${TEST_TYPE}" \
             run airflow "${@}"
        exit_code=$?
        docker-compose --log-level INFO -f "${SCRIPTS_CI_DIR}/docker-compose/base.yml" \
            --project-name "airflow-${TEST_TYPE}" \
            down --remove-orphans \
            --volumes --timeout 10
        if [[ ${exit_code} == "254" && ${try_num} != "5" ]]; then
            echo
            echo "Failed try num ${try_num}. Sleeping 5 seconds for retry"
            echo
            sleep 5
            continue
        else
            break
        fi
    done
    set -u
    set -e
    if [[ ${exit_code} != "0" ]]; then
        EXTRA_ARGS=""
        if [[ ${BACKEND} == "postgres" ]]; then
            EXTRA_ARGS="--postgres-version ${POSTGRES_VERSION} "
        elif [[ ${BACKEND} == "mysql" ]]; then
            EXTRA_ARGS="--mysql-version ${MYSQL_VERSION} "
        fi
        echo "${COLOR_RED}***********************************************************************************************${COLOR_RESET}"
        echo "${COLOR_RED}*${COLOR_RESET}"
        echo "${COLOR_RED}* ERROR! Some tests failed, unfortunately. Those might be transient errors,${COLOR_RESET}"
        echo "${COLOR_RED}*        but usually you have to fix something.${COLOR_RESET}"
        echo "${COLOR_RED}*        See the above log for details.${COLOR_RESET}"
        echo "${COLOR_RED}*${COLOR_RESET}"
        echo "${COLOR_RED}***********************************************************************************************${COLOR_RESET}"
        echo """
*  You can easily reproduce the failed tests on your dev machine/
*
*   When you have the source branch checked out locally:
*
*     Run all tests:
*
*       ./breeze --backend ${BACKEND} ${EXTRA_ARGS}--python ${PYTHON_MAJOR_MINOR_VERSION} --db-reset --skip-mounting-local-sources --test-type ${TEST_TYPE} tests
*
*     Enter docker shell:
*
*       ./breeze --backend ${BACKEND} ${EXTRA_ARGS}--python ${PYTHON_MAJOR_MINOR_VERSION} --db-reset --skip-mounting-local-sources --test-type ${TEST_TYPE} shell
*"""
    if [[ -n "${GITHUB_REGISTRY_PULL_IMAGE_TAG=}" ]]; then
        echo """
*   When you do not have sources:
*
*     Run all tests:
*
*      ./breeze --github-image-id ${GITHUB_REGISTRY_PULL_IMAGE_TAG} --backend ${BACKEND} ${EXTRA_ARGS}--python ${PYTHON_MAJOR_MINOR_VERSION} --db-reset --skip-mounting-local-sources --test-type ${TEST_TYPE} tests
*
*     Enter docker shell:
*
*      ./breeze --github-image-id ${GITHUB_REGISTRY_PULL_IMAGE_TAG} --backend ${BACKEND} ${EXTRA_ARGS}--python ${PYTHON_MAJOR_MINOR_VERSION} --db-reset --skip-mounting-local-sources --test-type ${TEST_TYPE} shell
*"""
    fi
    echo """
*
*   NOTE! Once you are in the docker shell, you can run failed test with:
*
*            pytest [TEST_NAME]
*
*   You can copy the test name from the output above
*
***********************************************************************************************"""

    fi

    echo ${exit_code} > "${PARALLEL_JOB_STATUS}"

    if [[ ${exit_code} == 0 ]]; then
        echo
        echo "${COLOR_GREEN}Test type: ${TEST_TYPE} succeeded.${COLOR_RESET}"
        echo
    else
        echo
        echo "${COLOR_RED}Test type: ${TEST_TYPE} failed.${COLOR_RESET}"
        echo
    fi
    return "${exit_code}"
}

prepare_tests

run_airflow_testing_in_docker "${@}"
