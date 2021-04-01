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

#
# Verifies if stored md5sum of the file changed since the last tme ot was checked
# The md5sum files are stored in .build directory - you can delete this directory
# If you want to rebuild everything from the scratch
#
function md5sum::calculate_file_md5sum {
    local FILE="${1}"
    local MD5SUM
    local MD5SUM_CACHE_DIR="${BUILD_CACHE_DIR}/${BRANCH_NAME}/${PYTHON_MAJOR_MINOR_VERSION}/${THE_IMAGE_TYPE}"
    mkdir -pv "${MD5SUM_CACHE_DIR}"
    MD5SUM=$(md5sum "${FILE}")
    local MD5SUM_FILE
    MD5SUM_FILE="${MD5SUM_CACHE_DIR}"/$(basename "$(dirname "${FILE}")")-$(basename "${FILE}").md5sum
    local MD5SUM_FILE_NEW
    MD5SUM_FILE_NEW=${CACHE_TMP_FILE_DIR}/$(basename "$(dirname "${FILE}")")-$(basename "${FILE}").md5sum.new
    echo "${MD5SUM}" > "${MD5SUM_FILE_NEW}"
    local RET_CODE=0
    if [[ ! -f "${MD5SUM_FILE}" ]]; then
        verbosity::print_info "Missing md5sum for ${FILE#${AIRFLOW_SOURCES}} (${MD5SUM_FILE#${AIRFLOW_SOURCES}})"
        RET_CODE=1
    else
        diff "${MD5SUM_FILE_NEW}" "${MD5SUM_FILE}" >/dev/null
        RES=$?
        if [[ "${RES}" != "0" ]]; then
            verbosity::print_info "The md5sum changed for ${FILE}: was $(cat "${MD5SUM_FILE}") now it is $(cat "${MD5SUM_FILE_NEW}")"
            if [[ ${CI} == "true" ]]; then
                echo "${COLOR_RED}The file has changed: ${FILE}${COLOR_RESET}"
                echo "${COLOR_BLUE}==============================${COLOR_RESET}"
                cat "${FILE}"
                echo "${COLOR_BLUE}==============================${COLOR_RESET}"
            fi
            RET_CODE=1
        fi
    fi
    return ${RET_CODE}
}

#
# Moves md5sum file from it's temporary location in CACHE_TMP_FILE_DIR to
# BUILD_CACHE_DIR - thus updating stored MD5 sum fo the file
#
function md5sum::move_file_md5sum {
    local FILE="${1}"
    local MD5SUM_FILE
    local MD5SUM_CACHE_DIR="${BUILD_CACHE_DIR}/${BRANCH_NAME}/${PYTHON_MAJOR_MINOR_VERSION}/${THE_IMAGE_TYPE}"
    mkdir -pv "${MD5SUM_CACHE_DIR}"
    MD5SUM_FILE="${MD5SUM_CACHE_DIR}"/$(basename "$(dirname "${FILE}")")-$(basename "${FILE}").md5sum
    local MD5SUM_FILE_NEW
    MD5SUM_FILE_NEW=${CACHE_TMP_FILE_DIR}/$(basename "$(dirname "${FILE}")")-$(basename "${FILE}").md5sum.new
    if [[ -f "${MD5SUM_FILE_NEW}" ]]; then
        mv "${MD5SUM_FILE_NEW}" "${MD5SUM_FILE}"
        verbosity::print_info "Updated md5sum file ${MD5SUM_FILE} for ${FILE}: $(cat "${MD5SUM_FILE}")"
    fi
}

#
# Stores md5sum files for all important files and
# records that we built the images locally so that next time we use
# it from the local docker cache rather than pull (unless forced)
#
function md5sum::update_all_md5() {
    verbosity::print_info
    verbosity::print_info "Updating md5sum files"
    verbosity::print_info
    for FILE in "${FILES_FOR_REBUILD_CHECK[@]}"
    do
        md5sum::move_file_md5sum "${AIRFLOW_SOURCES}/${FILE}"
    done
    mkdir -pv "${BUILD_CACHE_DIR}/${BRANCH_NAME}"
    touch "${BUILT_CI_IMAGE_FLAG_FILE}"
}

function md5sum::update_all_md5_with_group() {
    start_end::group_start "Update MD5 hashes for pulled images"
    md5sum::update_all_md5
    start_end::group_end
}

function md5sum::calculate_md5sum_for_all_files() {
    FILES_MODIFIED="false"
    set +e
    for FILE in "${FILES_FOR_REBUILD_CHECK[@]}"
    do
        if ! md5sum::calculate_file_md5sum "${AIRFLOW_SOURCES}/${FILE}"; then
            FILES_MODIFIED="true"
        fi
    done
    set -e
}

#
# Checks md5sum of all important files in order to optimise speed of running various operations
# That mount sources of Airflow to container and require docker image built with latest dependencies.
# the Docker image will only be marked for rebuilding only in case any of the important files change:
# * setup.py
# * setup.cfg
# * Dockerfile.ci
#
# This is needed because we want to skip rebuilding of the image when only airflow sources change but
# Trigger rebuild in case we need to change dependencies (setup.py, setup.cfg, change version of Airflow
# or the Dockerfile.ci itself changes.
#
# Another reason to skip rebuilding Docker is thar currently it takes a bit longer time than simple Docker
# We need to fix group permissions of files in Docker because different linux build services have
# different default umask and Docker uses group permissions in checking for cache invalidation.
#
# As result of this check - most of the static checks will start pretty much immediately.
#
function md5sum::check_if_docker_build_is_needed() {
    verbosity::print_info
    verbosity::print_info "Checking if image build is needed for ${THE_IMAGE_TYPE} image."
    verbosity::print_info
    if [[ ${FORCE_BUILD_IMAGES:=""} == "true" ]]; then
        verbosity::print_info "Docker image build is forced for ${THE_IMAGE_TYPE} image"
        md5sum::calculate_md5sum_for_all_files
        needs_docker_build="true"
    else
        md5sum::calculate_md5sum_for_all_files
        if [[ ${FILES_MODIFIED} == "true" ]]; then
            needs_docker_build="true"
        fi
        if [[ ${needs_docker_build} == "true" ]]; then
            verbosity::print_info "Docker image build is needed for ${THE_IMAGE_TYPE} image!"
        else
            verbosity::print_info "Docker image build is not needed for ${THE_IMAGE_TYPE} image!"
        fi
    fi
    verbosity::print_info
}
