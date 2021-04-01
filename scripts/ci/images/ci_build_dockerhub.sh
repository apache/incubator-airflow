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

# shellcheck disable=SC2030,SC2031

# This is hook build used by DockerHub. We are also using it
# on CI to potentially rebuild (and refresh layers that
# are not cached) Docker images that are used to run CI jobs
export FORCE_ANSWER_TO_QUESTIONS="yes"
export VERBOSE_COMMANDS="true"
export VERBOSE="true"

: "${DOCKER_REPO:?"ERROR: Please specify DOCKER_REPO variable following the pattern HOST/DOCKERHUB_USER/DOCKERHUB_REPO"}"

echo "DOCKER_REPO=${DOCKER_REPO}"

[[ ${DOCKER_REPO:=} =~ [^/]*/([^/]*)/([^/]*) ]] && \
    export DOCKERHUB_USER=${BASH_REMATCH[1]} &&
    export DOCKERHUB_REPO=${BASH_REMATCH[2]}

echo
echo "DOCKERHUB_USER=${DOCKERHUB_USER}"
echo "DOCKERHUB_REPO=${DOCKERHUB_REPO}"
echo

: "${DOCKER_TAG:?"ERROR: Please specify DOCKER_TAG variable following the pattern BRANCH-pythonX.Y"}"

echo "DOCKER_TAG=${DOCKER_TAG}"

[[ ${DOCKER_TAG:=} =~ .*-python([0-9.]*) ]] && export PYTHON_MAJOR_MINOR_VERSION=${BASH_REMATCH[1]}

: "${PYTHON_MAJOR_MINOR_VERSION:?"The tag '${DOCKER_TAG}' should follow the pattern .*-pythonX.Y"}"

echo "Detected PYTHON_MAJOR_MINOR_VERSION=${PYTHON_MAJOR_MINOR_VERSION}"
echo

if [[ ! "${DOCKER_TAG}" =~ ^[0-9].* ]]; then
    echo
    echo "Building airflow from branch or non-release tag: ${DOCKER_TAG}"
    echo
    # Only build and push CI image for the nightly-master, v1-10-test and v2-0-test branches
    # for tagged releases we build everything from PyPI, so we do not need CI images
    # For development images, we have to build all packages from current sources because we want to produce
    # `Latest and greatest` image from those branches. We need to build and push CI image as well as PROD
    # image but we need to build CI image first, in order to use it to prepare provider packages
    # The CI image provides an environment where we can reproducibly download the right .whl packages
    # and build the provider packages and then build the production image using those .whl packages
    # prepared. This is as close as it can get to production images - everything is build from
    # packages, but not from PyPI - those packages are built locally using the latest sources!

    # Note - we need sub-processes here, because we can run _script_init.sh only once per process
    # and it determines how to build the image - since we are building two images here
    # we need to run those in sub-processes
    (
        export INSTALL_FROM_PYPI="true"
        export INSTALL_PROVIDERS_FROM_SOURCES="true"
        export INSTALL_FROM_DOCKER_CONTEXT_FILES="false"
        export AIRFLOW_PRE_CACHED_PIP_PACKAGES="true"
        export DOCKER_CACHE="pulled"
        # shellcheck source=scripts/ci/libraries/_script_init.sh
        . "$( dirname "${BASH_SOURCE[0]}" )/../libraries/_script_init.sh"
        echo
        echo "Building and pushing CI image for ${PYTHON_MAJOR_MINOR_VERSION} in a sub-process"
        echo
        rm -rf "${BUILD_CACHE_DIR}"
        rm -rf "${AIRFLOW_SOURCES}/docker-context-files/*"
        build_images::prepare_ci_build
        build_images::rebuild_ci_image_if_needed
        push_pull_remove_images::push_ci_images
    )
    (
        export INSTALL_FROM_PYPI="false"
        export INSTALL_PROVIDERS_FROM_SOURCES="false"
        export INSTALL_FROM_DOCKER_CONTEXT_FILES="true"
        export AIRFLOW_PRE_CACHED_PIP_PACKAGES="false"
        export DOCKER_CACHE="pulled"
        # shellcheck source=scripts/ci/libraries/_script_init.sh
        . "$( dirname "${BASH_SOURCE[0]}" )/../libraries/_script_init.sh"
        echo
        echo "Building and pushing PROD image for ${PYTHON_MAJOR_MINOR_VERSION} in a sub-process"
        echo
        rm -rf "${BUILD_CACHE_DIR}"
        rm -rf "${AIRFLOW_SOURCES}/docker-context-files/*"
        build_images::prepare_prod_build
        build_images::build_prod_images_from_locally_built_airflow_packages
        push_pull_remove_images::push_prod_images
    )
else
    echo
    echo "Building airflow from release tag: ${DOCKER_TAG}"
    echo
    # This is an image built from the "release" tag (either RC or final one).
    # In this case all packages are taken from PyPI rather than from locally built sources
    export INSTALL_FROM_PYPI="true"
    export INSTALL_FROM_DOCKER_CONTEXT_FILES="false"
    export INSTALL_PROVIDERS_FROM_SOURCES="false"
    export AIRFLOW_PRE_CACHED_PIP_PACKAGES="false"
    export DOCKER_CACHE="local"
    export FORCE_PULL_BASE_PYTHON_IMAGE="true"
    # Name the image based on the TAG rather than based on the branch name
    export FORCE_AIRFLOW_PROD_BASE_TAG="${DOCKER_TAG}"
    export INSTALL_AIRFLOW_VERSION="${DOCKER_TAG%-python*}"
    export AIRFLOW_CONSTRAINTS_REFERENCE="constraints-${INSTALL_AIRFLOW_VERSION}"
    export AIRFLOW_CONSTRAINTS="constraints"
    # shellcheck source=scripts/ci/libraries/_script_init.sh
    . "$( dirname "${BASH_SOURCE[0]}" )/../libraries/_script_init.sh"
    echo
    echo "Building and pushing PROD image for ${PYTHON_MAJOR_MINOR_VERSION} in a sub-process"
    echo
    rm -rf "${BUILD_CACHE_DIR}"
    rm -rf "${AIRFLOW_SOURCES}/docker-context-files/*"
    build_images::prepare_prod_build
    build_images::build_prod_images
    push_pull_remove_images::push_prod_images
fi
