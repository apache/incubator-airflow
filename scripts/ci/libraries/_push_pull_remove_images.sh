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


# Tries to push the image several times in case we receive an intermittent error on push
# $1 - tag to push
function push_pull_remove_images::push_image_with_retries() {
    for try_num in 1 2 3 4
    do
        set +e
        echo
        echo "Trying to push the image ${1}. Number of try: ${try_num}"
        docker push "${1}"
        local res=$?
        set -e
        if [[ ${res} != "0" ]]; then
            echo
            echo  "${COLOR_YELLOW}WARNING: Error ${res} when pushing image on ${try_num} try  ${COLOR_RESET}"
            echo
            continue
        else
            return 0
        fi
    done
    echo
    echo  "${COLOR_RED}ERROR: Error ${res} when pushing image on ${try_num} try. Giving up!  ${COLOR_RESET}"
    echo
    return 1
}


# Pulls image in case it is needed (either has never been pulled or pulling was forced
# Should be run with set +e
# Parameters:
#   $1 -> image to pull
function push_pull_remove_images::pull_image_if_not_present_or_forced() {
    local IMAGE_TO_PULL="${1}"
    local IMAGE_HASH
    IMAGE_HASH=$(docker images -q "${IMAGE_TO_PULL}" 2> /dev/null || true)
    local PULL_IMAGE=${FORCE_PULL_IMAGES}

    if [[ -z "${IMAGE_HASH=}" ]]; then
        PULL_IMAGE="true"
    fi
    if [[ "${PULL_IMAGE}" == "true" ]]; then
        echo
        echo "Pulling the image ${IMAGE_TO_PULL}"
        echo
        docker pull "${IMAGE_TO_PULL}"
        EXIT_VALUE="$?"
        if [[ ${EXIT_VALUE} != "0" && ${FAIL_ON_GITHUB_DOCKER_PULL_ERROR} == "true" ]]; then
            echo
            echo """
${COLOR_RED}ERROR: Exiting on docker pull error

If you have authorisation problems, you might want to run:

docker login ${IMAGE_TO_PULL%%\/*}

You need to use generate token as the password, not your personal password.
You can generate one at https://github.com/settings/tokens
Make sure to choose 'read:packages' scope.
${COLOR_RESET}
"""
            exit ${EXIT_VALUE}
        fi
        echo
        return ${EXIT_VALUE}
    fi
}

# Pulls image if needed but tries to pull it from GitHub registry before
# It attempts to pull it from the DockerHub registry. This is used to speed up the builds
# In GitHub Actions and to pull appropriately tagged builds.
# Parameters:
#   $1 -> DockerHub image to pull
#   $2 -> GitHub image to try to pull first
function push_pull_remove_images::pull_image_github_dockerhub() {
    local DOCKERHUB_IMAGE="${1}"
    local GITHUB_IMAGE="${2}"

    set +e
    if push_pull_remove_images::pull_image_if_not_present_or_forced "${GITHUB_IMAGE}"; then
        # Tag the image to be the DockerHub one
        docker tag "${GITHUB_IMAGE}" "${DOCKERHUB_IMAGE}"
    else
        push_pull_remove_images::pull_image_if_not_present_or_forced "${DOCKERHUB_IMAGE}"
    fi
    set -e
}

# Rebuilds python base image from the latest available Python version
function push_pull_remove_images::rebuild_python_base_image() {
   echo
   echo "Rebuilding ${AIRFLOW_PYTHON_BASE_IMAGE} from latest ${PYTHON_BASE_IMAGE}"
   echo
   docker pull "${PYTHON_BASE_IMAGE}"
   echo "FROM ${PYTHON_BASE_IMAGE}" | \
        docker build \
            --label "org.opencontainers.image.source=https://github.com/${GITHUB_REPOSITORY}" \
            -t "${AIRFLOW_PYTHON_BASE_IMAGE}" -
}

# Pulls the base Python image. This image is used as base for CI and PROD images, depending on the parameters used:
#
# * if FORCE_PULL_BASE_PYTHON_IMAGE != false, then it rebuild the image using latest Python image available
#     and adds `org.opencontainers.image.source` label to it, so that it is linked to Airflow
#     repository when we push it to GHCR registry
# * Otherwise it pulls the Python base image from either GitHub registry or from DockerHub
#     depending on USE_GITHUB_REGISTRY variable. In case we pull specific build image (via suffix)
#     it will pull the right image using the specified suffix
function push_pull_remove_images::pull_base_python_image() {
    if [[ ${FORCE_PULL_BASE_PYTHON_IMAGE} == "true" ]] ; then
        push_pull_remove_images::rebuild_python_base_image
        return
    fi
    echo
    echo "Docker pulling base python image. Upgrade to newer deps: ${UPGRADE_TO_NEWER_DEPENDENCIES}"
    echo
    if [[ -n ${DETECTED_TERMINAL=} ]]; then
        echo -n "Docker pulling base python image. Upgrade to newer deps: ${UPGRADE_TO_NEWER_DEPENDENCIES}
" > "${DETECTED_TERMINAL}"
    fi
    if [[ ${USE_GITHUB_REGISTRY} == "true" ]]; then
        PYTHON_TAG_SUFFIX=""
        if [[ ${GITHUB_REGISTRY_PULL_IMAGE_TAG} != "latest" ]]; then
            PYTHON_TAG_SUFFIX="-${GITHUB_REGISTRY_PULL_IMAGE_TAG}"
        fi
        push_pull_remove_images::pull_image_github_dockerhub "${AIRFLOW_PYTHON_BASE_IMAGE}" \
            "${GITHUB_REGISTRY_PYTHON_BASE_IMAGE}${PYTHON_TAG_SUFFIX}"
    else
        docker pull "${AIRFLOW_PYTHON_BASE_IMAGE}"
    fi
}

# Pulls CI image in case caching strategy is "pulled" and the image needs to be pulled
function push_pull_remove_images::pull_ci_images_if_needed() {
    local python_image_hash
    python_image_hash=$(docker images -q "${AIRFLOW_PYTHON_BASE_IMAGE}" 2> /dev/null || true)
    if [[ -z "${python_image_hash=}" || "${FORCE_PULL_IMAGES}" == "true" || \
            ${FORCE_PULL_BASE_PYTHON_IMAGE} == "true" ]]; then
        push_pull_remove_images::pull_base_python_image
    fi
    if [[ "${DOCKER_CACHE}" == "pulled" ]]; then
        if [[ ${USE_GITHUB_REGISTRY} == "true" ]]; then
            push_pull_remove_images::pull_image_github_dockerhub "${AIRFLOW_CI_IMAGE}" \
                "${GITHUB_REGISTRY_AIRFLOW_CI_IMAGE}:${GITHUB_REGISTRY_PULL_IMAGE_TAG}"
        else
            push_pull_remove_images::pull_image_if_not_present_or_forced "${AIRFLOW_CI_IMAGE}"
        fi
    fi
}


# Pulls PROD image in case caching strategy is "pulled" and the image needs to be pulled
function push_pull_remove_images::pull_prod_images_if_needed() {
    local python_image_hash
    python_image_hash=$(docker images -q "${AIRFLOW_PYTHON_BASE_IMAGE}" 2> /dev/null || true)
    if [[ -z "${python_image_hash=}" || "${FORCE_PULL_IMAGES}" == "true"  || \
            ${FORCE_PULL_BASE_PYTHON_IMAGE} == "true" ]]; then
        push_pull_remove_images::pull_base_python_image
    fi
    if [[ "${DOCKER_CACHE}" == "pulled" ]]; then
        if [[ ${USE_GITHUB_REGISTRY} == "true" ]]; then
            # "Build" segment of production image
            push_pull_remove_images::pull_image_github_dockerhub "${AIRFLOW_PROD_BUILD_IMAGE}" \
                "${GITHUB_REGISTRY_AIRFLOW_PROD_BUILD_IMAGE}:${GITHUB_REGISTRY_PULL_IMAGE_TAG}"
            # "Main" segment of production image
            push_pull_remove_images::pull_image_github_dockerhub "${AIRFLOW_PROD_IMAGE}" \
                "${GITHUB_REGISTRY_AIRFLOW_PROD_IMAGE}:${GITHUB_REGISTRY_PULL_IMAGE_TAG}"
        else
            push_pull_remove_images::pull_image_if_not_present_or_forced "${AIRFLOW_PROD_BUILD_IMAGE}"
            push_pull_remove_images::pull_image_if_not_present_or_forced "${AIRFLOW_PROD_IMAGE}"
        fi
    fi
}

# Pushes Ci images and the manifest to the registry in DockerHub.
function push_pull_remove_images::push_ci_images_to_dockerhub() {
    push_pull_remove_images::push_image_with_retries "${AIRFLOW_PYTHON_BASE_IMAGE}"
    push_pull_remove_images::push_image_with_retries "${AIRFLOW_CI_IMAGE}"
    docker tag "${AIRFLOW_CI_LOCAL_MANIFEST_IMAGE}" "${AIRFLOW_CI_REMOTE_MANIFEST_IMAGE}"
    push_pull_remove_images::push_image_with_retries "${AIRFLOW_CI_REMOTE_MANIFEST_IMAGE}"
    if [[ -n ${DEFAULT_CI_IMAGE=} ]]; then
        # Only push default image to DockerHub registry if it is defined
        push_pull_remove_images::push_image_with_retries "${DEFAULT_CI_IMAGE}"
    fi
}


# Push image to GitHub registry with the push tag:
#     "${GITHUB_RUN_ID}" - in case of pull-request triggered 'workflow_run' builds
#     "latest"           - in case of push builds
# Push python image to GitHub registry with the push tag:
#     X.Y-slim-buster-"${GITHUB_RUN_ID}" - in case of pull-request triggered 'workflow_run' builds
#     X.Y-slim-buster                    - in case of push builds
function push_pull_remove_images::push_python_image_to_github() {
    PYTHON_TAG_SUFFIX=""
    if [[ ${GITHUB_REGISTRY_PUSH_IMAGE_TAG} != "latest" ]]; then
        PYTHON_TAG_SUFFIX="-${GITHUB_REGISTRY_PUSH_IMAGE_TAG}"
    fi
    docker tag "${AIRFLOW_PYTHON_BASE_IMAGE}" \
        "${GITHUB_REGISTRY_PYTHON_BASE_IMAGE}${PYTHON_TAG_SUFFIX}"
    push_pull_remove_images::push_image_with_retries \
        "${GITHUB_REGISTRY_PYTHON_BASE_IMAGE}${PYTHON_TAG_SUFFIX}"
}

# Pushes Ci images and their tags to registry in GitHub
function push_pull_remove_images::push_ci_images_to_github() {
    push_pull_remove_images::push_python_image_to_github
    AIRFLOW_CI_TAGGED_IMAGE="${GITHUB_REGISTRY_AIRFLOW_CI_IMAGE}:${GITHUB_REGISTRY_PUSH_IMAGE_TAG}"
    docker tag "${AIRFLOW_CI_IMAGE}" "${AIRFLOW_CI_TAGGED_IMAGE}"
    push_pull_remove_images::push_image_with_retries "${AIRFLOW_CI_TAGGED_IMAGE}"
    if [[ -n ${GITHUB_SHA=} ]]; then
        # Also push image to GitHub registry with commit SHA
        AIRFLOW_CI_SHA_IMAGE="${GITHUB_REGISTRY_AIRFLOW_CI_IMAGE}:${COMMIT_SHA}"
        docker tag "${AIRFLOW_CI_IMAGE}" "${AIRFLOW_CI_SHA_IMAGE}"
        push_pull_remove_images::push_image_with_retries "${AIRFLOW_CI_SHA_IMAGE}"
    fi
}


# Pushes Ci image and it's manifest to the registry.
function push_pull_remove_images::push_ci_images() {
    if [[ ${USE_GITHUB_REGISTRY} == "true" ]]; then
        push_pull_remove_images::push_ci_images_to_github
    else
        push_pull_remove_images::push_ci_images_to_dockerhub
    fi
}

# Pushes PROD image to registry in DockerHub
function push_pull_remove_images::push_prod_images_to_dockerhub () {
    push_pull_remove_images::push_image_with_retries "${AIRFLOW_PYTHON_BASE_IMAGE}"
    # Prod image
    push_pull_remove_images::push_image_with_retries "${AIRFLOW_PROD_IMAGE}"
    if [[ -n ${DEFAULT_PROD_IMAGE=} ]]; then
        push_pull_remove_images::push_image_with_retries "${DEFAULT_PROD_IMAGE}"
    fi
    # Prod build image
    push_pull_remove_images::push_image_with_retries "${AIRFLOW_PROD_BUILD_IMAGE}"

}

# Pushes PROD image to and their tags to registry in GitHub
# Push image to GitHub registry with chosen push tag
# the PUSH tag might be:
#     "${GITHUB_RUN_ID}" - in case of pull-request triggered 'workflow_run' builds
#     "latest"           - in case of push builds
function push_pull_remove_images::push_prod_images_to_github () {
    push_pull_remove_images::push_python_image_to_github
    AIRFLOW_PROD_TAGGED_IMAGE="${GITHUB_REGISTRY_AIRFLOW_PROD_IMAGE}:${GITHUB_REGISTRY_PUSH_IMAGE_TAG}"
    docker tag "${AIRFLOW_PROD_IMAGE}" "${AIRFLOW_PROD_TAGGED_IMAGE}"
    push_pull_remove_images::push_image_with_retries "${GITHUB_REGISTRY_AIRFLOW_PROD_IMAGE}:${GITHUB_REGISTRY_PUSH_IMAGE_TAG}"
    if [[ -n ${COMMIT_SHA=} ]]; then
        # Also push image to GitHub registry with commit SHA
        AIRFLOW_PROD_SHA_IMAGE="${GITHUB_REGISTRY_AIRFLOW_PROD_IMAGE}:${COMMIT_SHA}"
        docker tag "${AIRFLOW_PROD_IMAGE}" "${AIRFLOW_PROD_SHA_IMAGE}"
        push_pull_remove_images::push_image_with_retries "${AIRFLOW_PROD_SHA_IMAGE}"
    fi
    # Also push prod build image
    AIRFLOW_PROD_BUILD_TAGGED_IMAGE="${GITHUB_REGISTRY_AIRFLOW_PROD_BUILD_IMAGE}:${GITHUB_REGISTRY_PUSH_IMAGE_TAG}"
    docker tag "${AIRFLOW_PROD_BUILD_IMAGE}" "${AIRFLOW_PROD_BUILD_TAGGED_IMAGE}"
    push_pull_remove_images::push_image_with_retries "${AIRFLOW_PROD_BUILD_TAGGED_IMAGE}"
}


# Pushes PROD image to the registry. In case the image was taken from cache registry
# it is also pushed to the cache, not to the main registry
function push_pull_remove_images::push_prod_images() {
    if [[ ${USE_GITHUB_REGISTRY} == "true" ]]; then
        push_pull_remove_images::push_prod_images_to_github
    else
        push_pull_remove_images::push_prod_images_to_dockerhub
    fi
}

# waits for an image to be available in GitHub Packages. Should be run with `set +e`
# the buid automatically determines which registry to use based one the images available
function push_pull_remove_images::check_for_image_in_github_packages() {
    local github_repository_lowercase
    github_repository_lowercase="$(echo "${GITHUB_REPOSITORY}" |tr '[:upper:]' '[:lower:]')"
    local github_api_endpoint
    github_api_endpoint="https://docker.pkg.github.com/v2/${github_repository_lowercase}"
    local image_name_in_github_registry="${1}"
    local image_tag_in_github_registry=${2}
    local image_to_wait_for=${GITHUB_REPOSITORY}/${image_name_in_github_registry}:${image_tag_in_github_registry}
    local github_api_call
    github_api_call="${github_api_endpoint}/${image_name_in_github_registry}/manifests/${image_tag_in_github_registry}"
    echo "GitHub Packages: checking for ${image_to_wait_for} via ${github_api_call}!"
    http_status=$(curl --silent --output "${OUTPUT_LOG}" --write-out "%{http_code}" \
        --connect-timeout 60  --max-time 60 \
        -X GET "${github_api_call}" -u "${GITHUB_USERNAME}:${GITHUB_TOKEN}")
    if [[ ${http_status} == "200" ]]; then
        echo  "Image: ${image_to_wait_for} found in GitHub Packages: ${COLOR_GREEN}OK.  ${COLOR_RESET}"
        echo "::set-output name=githubRegistry::docker.pkg.github.com"
        echo
        echo "Setting githubRegistry output to docker.pkg.github.com"
        echo
        return 0
    else
        cat "${OUTPUT_LOG}"
        echo "${COLOR_YELLOW}Still waiting. Status code ${http_status}!${COLOR_RESET}"
        return 1
    fi
}

# waits for an image to be available in GitHub Container Registry. Should be run with `set +e`
function push_pull_remove_images::check_for_image_in_github_container_registry() {
    local image_name_in_github_registry="${1}"
    local image_tag_in_github_registry=${2}

    local image_to_wait_for="ghcr.io/${GITHUB_REPOSITORY}-${image_name_in_github_registry}:${image_tag_in_github_registry}"
    echo "GitHub Container Registry: checking for ${image_to_wait_for} via docker manifest inspect!"
    docker manifest inspect "${image_to_wait_for}"
    local res=$?
    if [[ ${res} == "0" ]]; then
        echo  "Image: ${image_to_wait_for} found in Container Registry: ${COLOR_GREEN}OK.${COLOR_RESET}"
        echo
        echo "Setting githubRegistry output to ghcr.io"
        echo
        echo "::set-output name=githubRegistry::ghcr.io"
        return 0
    else
        echo "${COLOR_YELLOW}Still waiting. Not found!${COLOR_RESET}"
        return 1
    fi
}

# waits for an image to be available in the GitHub registry
function push_pull_remove_images::wait_for_github_registry_image() {
    set +e
    echo " Waiting for github registry image: " "${@}"
    while true
    do
        if push_pull_remove_images::check_for_image_in_github_container_registry "${@}"; then
            break
        fi
        if push_pull_remove_images::check_for_image_in_github_packages "${@}"; then
            break
        fi
        sleep 30
    done
    set -e
}

function push_pull_remove_images::check_if_github_registry_wait_for_image_enabled() {
    if [[ ${USE_GITHUB_REGISTRY} != "true" ||  ${GITHUB_REGISTRY_WAIT_FOR_IMAGE} != "true" ]]; then
        echo
        echo "This script should not be called"
        echo "It need both USE_GITHUB_REGISTRY and GITHUB_REGISTRY_WAIT_FOR_IMAGE to true!"
        echo
        echo "USE_GITHUB_REGISTRY = ${USE_GITHUB_REGISTRY}"
        echo "GITHUB_REGISTRY_WAIT_FOR_IMAGE =${GITHUB_REGISTRY_WAIT_FOR_IMAGE}"
        echo
        exit 1
    fi
}
