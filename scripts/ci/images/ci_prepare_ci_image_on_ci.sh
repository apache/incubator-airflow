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
. "$( dirname "${BASH_SOURCE[0]}" )/../libraries/_script_init.sh"

# Builds or waits for the CI image in the CI environment
# Depending on "USE_GITHUB_REGISTRY" and "GITHUB_REGISTRY_WAIT_FOR_IMAGE" setting
function build_ci_image_on_ci() {
    build_images::prepare_ci_build
    start_end::group_start "Prepare CI image ${AIRFLOW_CI_IMAGE}"

    rm -rf "${BUILD_CACHE_DIR}"
    mkdir -pv "${BUILD_CACHE_DIR}"

    if [[ ${USE_GITHUB_REGISTRY} == "true" && ${GITHUB_REGISTRY_WAIT_FOR_IMAGE} == "true" ]]; then
        # Pretend that the image was build. We already have image with the right sources baked in!
        md5sum::calculate_md5sum_for_all_files

        # Tries to wait for the images indefinitely
        # skips further image checks - since we already have the target image

        local python_tag_suffix=""
        if [[ ${GITHUB_REGISTRY_PULL_IMAGE_TAG} != "latest" ]]; then
            python_tag_suffix="-${GITHUB_REGISTRY_PULL_IMAGE_TAG}"
        fi
        # first we pull base python image. We will need it to re-push it after master build
        # Becoming the new "latest" image for other builds
        build_images::wait_for_image_tag "${GITHUB_REGISTRY_PYTHON_BASE_IMAGE}" \
            "${python_tag_suffix}" "${AIRFLOW_PYTHON_BASE_IMAGE}"

        # And then the actual image
        build_images::wait_for_image_tag "${GITHUB_REGISTRY_AIRFLOW_CI_IMAGE}" \
            ":${GITHUB_REGISTRY_PULL_IMAGE_TAG}" "${AIRFLOW_CI_IMAGE}"

        md5sum::update_all_md5_with_group
    else
        build_images::rebuild_ci_image_if_needed
    fi

    # Disable force pulling forced above this is needed for the subsequent scripts so that
    # They do not try to pull/build images again.
    unset FORCE_PULL_IMAGES
    unset FORCE_BUILD
    # Skip the image check entirely for the rest of the script
    export CHECK_IMAGE_FOR_REBUILD="false"
    start_end::group_end
}

build_ci_image_on_ci
