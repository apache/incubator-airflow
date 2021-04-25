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

if [[ $1 == "" ]]; then
  >&2 echo "Requires python MAJOR/MINOR version as first parameter"
  exit 1
fi

export PYTHON_MAJOR_MINOR_VERSION=$1
shift


# shellcheck source=scripts/ci/libraries/_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/../libraries/_script_init.sh"

function pull_prod_image() {
    local image_name_with_tag="${GITHUB_REGISTRY_AIRFLOW_PROD_IMAGE}:${GITHUB_REGISTRY_PULL_IMAGE_TAG}"
    start_end::group_start "Pulling the ${image_name_with_tag} image and tagging with ${AIRFLOW_PROD_IMAGE}"

    push_pull_remove_images::pull_image_github_dockerhub "${AIRFLOW_PROD_IMAGE}" "${image_name_with_tag}"
    start_end::group_end
}

push_pull_remove_images::check_if_github_registry_wait_for_image_enabled

build_image::configure_docker_registry

export AIRFLOW_PROD_IMAGE_NAME="${BRANCH_NAME}-python${PYTHON_MAJOR_MINOR_VERSION}"

start_end::group_start "Waiting for ${AIRFLOW_PROD_IMAGE_NAME} image to appear"

push_pull_remove_images::wait_for_github_registry_image \
    "${AIRFLOW_PROD_IMAGE_NAME}${GITHUB_REGISTRY_IMAGE_SUFFIX}" "${GITHUB_REGISTRY_PULL_IMAGE_TAG}"
start_end::group_end

build_images::prepare_prod_build

pull_prod_image

verify_image::verify_prod_image "${AIRFLOW_PROD_IMAGE}"
