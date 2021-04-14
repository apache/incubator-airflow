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
function verify_image::run_command_in_image() {
    docker run --rm \
            -e COLUMNS=180 \
            --entrypoint /bin/bash "${DOCKER_IMAGE}" \
            -c "${@}"
}

IMAGE_VALID="true"

function verify_image::check_command() {
    DESCRIPTION="${1}"
    COMMAND=${2}
    set +e
    echo -n "Feature: ${DESCRIPTION} "
    local output
    output=$(verify_image::run_command_in_image "${COMMAND}" 2>&1)
    local res=$?
    if [[ ${res} == "0" ]]; then
        echo "${COLOR_GREEN}OK${COLOR_RESET}"
    else
        echo "${COLOR_RED}NOK${COLOR_RESET}"
        echo "${COLOR_BLUE}========================= OUTPUT start ============================${COLOR_RESET}"
        echo "${output}"
        echo "${COLOR_BLUE}========================= OUTPUT end   ===========================${COLOR_RESET}"
        IMAGE_VALID="false"
    fi
    set -e
}

function verify_image::verify_prod_image_has_airflow_and_providers() {
    start_end::group_start "Verify prod image: ${DOCKER_IMAGE}"
    echo
    echo "Checking if Providers are installed"
    echo

    all_providers_installed_in_image=$(verify_image::run_command_in_image "airflow providers list --output table")

    echo
    echo "Installed providers:"
    echo
    echo "${all_providers_installed_in_image}"
    echo
    local error="false"
    for provider in "${INSTALLED_PROVIDERS[@]}"; do
        echo -n "Verifying if provider ${provider} installed: "
        if [[ ${all_providers_installed_in_image} == *"apache-airflow-providers-${provider//./-}"* ]]; then
            echo "${COLOR_GREEN}OK${COLOR_RESET}"
        else
            echo "${COLOR_RED}NOK${COLOR_RESET}"
            error="true"
        fi
    done
    if [[ ${error} == "true" ]]; then
        echo
        echo "${COLOR_RED}ERROR: Some expected providers are not installed!${COLOR_RESET}"
        echo
        IMAGE_VALID="false"
    else
        echo
        echo "${COLOR_GREEN}OK. All expected providers installed!${COLOR_RESET}"
        echo
    fi
    start_end::group_end
}

function verify_image::verify_ci_image_dependencies() {
    start_end::group_start "Checking if Airflow dependencies are non-conflicting in ${DOCKER_IMAGE} image."
    set +e
    docker run --rm --entrypoint /bin/bash "${DOCKER_IMAGE}" -c 'pip check'
    local res=$?
    if [[ ${res} != "0" ]]; then
        echo  "${COLOR_RED}ERROR: ^^^ Some dependencies are conflicting. See instructions below on how to deal with it.  ${COLOR_RESET}"
        echo
        build_images::inform_about_pip_check ""
        IMAGE_VALID="false"
    else
        echo
        echo  "${COLOR_GREEN}OK. The ${DOCKER_IMAGE} image dependencies are consistent.  ${COLOR_RESET}"
        echo
    fi
    set -e
    start_end::group_end
}

function verify_image::verify_prod_image_dependencies() {
    start_end::group_start "Checking if Airflow dependencies are non-conflicting in ${DOCKER_IMAGE} image."

    set +e
    verify_image::run_command_in_image 'pip check'
    local res=$?
    if [[ ${res} != "0" ]]; then
        echo "${COLOR_RED}ERROR: ^^^ Some dependencies are conflicting. See instructions below on how to deal with it.  ${COLOR_RESET}"
        echo
        build_images::inform_about_pip_check "--production "
        IMAGE_VALID="false"
    else
        echo
        echo "${COLOR_GREEN}OK. The ${DOCKER_IMAGE} image dependencies are consistent.  ${COLOR_RESET}"
        echo
    fi
    set -e
    start_end::group_end
}

GOOGLE_IMPORTS=(
    'OpenSSL'
    'google.ads'
    'googleapiclient'
    'google.auth'
    'google_auth_httplib2'
    'google.cloud.automl'
    'google.cloud.bigquery_datatransfer'
    'google.cloud.bigtable'
    'google.cloud.container'
    'google.cloud.datacatalog'
    'google.cloud.dataproc'
    'google.cloud.dlp'
    'google.cloud.kms'
    'google.cloud.language'
    'google.cloud.logging'
    'google.cloud.memcache'
    'google.cloud.monitoring'
    'google.cloud.oslogin'
    'google.cloud.pubsub'
    'google.cloud.redis'
    'google.cloud.secretmanager'
    'google.cloud.spanner'
    'google.cloud.speech'
    'google.cloud.storage'
    'google.cloud.tasks'
    'google.cloud.texttospeech'
    'google.cloud.translate'
    'google.cloud.videointelligence'
    'google.cloud.vision'
)

AZURE_IMPORTS=(
    'azure.batch'
    'azure.cosmos'
    'azure.datalake.store'
    'azure.identity'
    'azure.keyvault'
    'azure.kusto.data'
    'azure.mgmt.containerinstance'
    'azure.mgmt.datalake.store'
    'azure.mgmt.resource'
    'azure.storage'
)

function verify_image::verify_production_image_python_modules() {
    start_end::group_start "Verify prod image features: ${DOCKER_IMAGE}"

    verify_image::check_command "Import: async" "python -c 'import gevent, eventlet, greenlet'"
    verify_image::check_command "Import: amazon" "python -c 'import boto3, botocore, watchtower'"
    verify_image::check_command "Import: celery" "python -c 'import celery, flower, vine'"
    verify_image::check_command "Import: cncf.kubernetes" "python -c 'import kubernetes, cryptography'"
    verify_image::check_command "Import: docker" "python -c 'import docker'"
    verify_image::check_command "Import: dask" "python -c 'import cloudpickle, distributed'"
    verify_image::check_command "Import: elasticsearch" "python -c 'import elasticsearch,es.elastic, elasticsearch_dsl'"
    verify_image::check_command "Import: grpc" "python -c 'import grpc, google.auth, google_auth_httplib2'"
    verify_image::check_command "Import: hashicorp" "python -c 'import hvac'"
    verify_image::check_command "Import: ldap" "python -c 'import ldap'"
    for google_import in "${GOOGLE_IMPORTS[@]}"
    do
        verify_image::check_command "Import google: ${google_import}" "python -c 'import ${google_import}'"
    done
    for azure_import in "${AZURE_IMPORTS[@]}"
    do
        verify_image::check_command "Import azure: ${azure_import}" "python -c 'import ${azure_import}'"
    done
    verify_image::check_command "Import: mysql" "python -c 'import mysql'"
    verify_image::check_command "Import: postgres" "python -c 'import psycopg2'"
    verify_image::check_command "Import: redis" "python -c 'import redis'"
    verify_image::check_command "Import: sendgrid" "python -c 'import sendgrid'"
    verify_image::check_command "Import: sftp/ssh" "python -c 'import paramiko, pysftp, sshtunnel'"
    verify_image::check_command "Import: slack" "python -c 'import slack_sdk'"
    verify_image::check_command "Import: statsd" "python -c 'import statsd'"
    verify_image::check_command "Import: virtualenv" "python -c 'import virtualenv'"

    start_end::group_end
}

function verify_image::verify_prod_image_as_root() {
    start_end::group_start "Checking if the image can be run as root."
    set +e
    echo "Checking airflow as root"
    local output
    local res
    output=$(docker run --rm --user 0 "${DOCKER_IMAGE}" "airflow" "info" 2>&1)
    res=$?
    if [[ ${res} == "0" ]]; then
        echo "${COLOR_GREEN}OK${COLOR_RESET}"
    else
        echo "${COLOR_RED}NOK${COLOR_RESET}"
        echo "${COLOR_BLUE}========================= OUTPUT start ============================${COLOR_RESET}"
        echo "${output}"
        echo "${COLOR_BLUE}========================= OUTPUT end   ===========================${COLOR_RESET}"
        IMAGE_VALID="false"
    fi

    echo "Checking root container with custom PYTHONPATH"
    local tmp_dir
    tmp_dir="$(mktemp -d)"
    touch "${tmp_dir}/__init__.py"
    echo 'print("Awesome")' >> "${tmp_dir}/awesome.py"
    output=$(docker run \
        --rm \
        -e "PYTHONPATH=${tmp_dir}" \
        -v "${tmp_dir}:${tmp_dir}" \
        --user 0 "${DOCKER_IMAGE}" \
            "python" "-c" "import awesome" \
        2>&1)
    res=$?
    if [[ ${res} == "0" ]]; then
        echo "${COLOR_GREEN}OK${COLOR_RESET}"
    else
        echo "${COLOR_RED}NOK${COLOR_RESET}"
        echo "${COLOR_BLUE}========================= OUTPUT start ============================${COLOR_RESET}"
        echo "${output}"
        echo "${COLOR_BLUE}========================= OUTPUT end   ===========================${COLOR_RESET}"
        IMAGE_VALID="false"
    fi
    rm -rf "${tmp_dir}"
    set -e
}

function verify_image::display_result {
    if [[ ${IMAGE_VALID} == "true" ]]; then
        echo
        echo "${COLOR_GREEN}OK. The ${DOCKER_IMAGE} features are all OK.  ${COLOR_RESET}"
        echo
    else
        echo
        echo "${COLOR_RED}ERROR: Some features were not ok!${COLOR_RESET}"
        echo
        exit 1
    fi
}

function verify_image::verify_prod_image {
    IMAGE_VALID="true"
    DOCKER_IMAGE="${1}"
    verify_image::verify_prod_image_has_airflow_and_providers

    verify_image::verify_production_image_python_modules

    verify_image::verify_prod_image_dependencies

    verify_image::verify_prod_image_as_root

    verify_image::display_result
}

function verify_image::verify_ci_image {
    IMAGE_VALID="true"
    DOCKER_IMAGE="${1}"
    verify_image::verify_ci_image_dependencies

    verify_image::display_result
}
