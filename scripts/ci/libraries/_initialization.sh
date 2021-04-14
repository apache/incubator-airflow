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

# Needs to be declared outside function in MacOS
# shellcheck disable=SC2034
CURRENT_PYTHON_MAJOR_MINOR_VERSIONS=()
CURRENT_KUBERNETES_VERSIONS=()
CURRENT_KUBERNETES_MODES=()
CURRENT_POSTGRES_VERSIONS=()
CURRENT_MYSQL_VERSIONS=()
CURRENT_KIND_VERSIONS=()
CURRENT_HELM_VERSIONS=()
ALL_PYTHON_MAJOR_MINOR_VERSIONS=()
INSTALLED_PROVIDERS=()

# Creates directories for Breeze
function initialization::create_directories() {
    # This folder is mounted to inside the container in /files folder. This is the way how
    # We can exchange DAGs, scripts, packages etc with the container environment
    export FILES_DIR="${AIRFLOW_SOURCES}/files"
    readonly FILES_DIR

    # Directory where all the build cache is stored - we keep there status of all the docker images
    # As well as hashes of the important files, but also we generate build scripts there that are
    # Used to execute the commands for breeze
    export BUILD_CACHE_DIR="${AIRFLOW_SOURCES}/.build"
    export BUILD_CACHE_DIR
    readonly BUILD_CACHE_DIR

    # Create those folders above in case they do not exist
    mkdir -p "${BUILD_CACHE_DIR}" >/dev/null
    mkdir -p "${FILES_DIR}" >/dev/null

    # By default we are not in CI environment GitHub Actions sets CI to "true"
    export CI="${CI="false"}"

    # Create useful directories if not yet created
    mkdir -p "${AIRFLOW_SOURCES}/.mypy_cache"
    mkdir -p "${AIRFLOW_SOURCES}/logs"
    mkdir -p "${AIRFLOW_SOURCES}/dist"

    CACHE_TMP_FILE_DIR=$(mktemp -d)
    export CACHE_TMP_FILE_DIR
    readonly CACHE_TMP_FILE_DIR

    if [[ ${SKIP_CACHE_DELETION=} != "true" ]]; then
        traps::add_trap "rm -rf -- '${CACHE_TMP_FILE_DIR}'" EXIT HUP INT TERM
    fi

    OUTPUT_LOG="${CACHE_TMP_FILE_DIR}/out.log"
    export OUTPUT_LOG
    readonly OUTPUT_LOG
}

# Very basic variables that MUST be set
function initialization::initialize_base_variables() {
    # Default port numbers for forwarded ports
    export WEBSERVER_HOST_PORT=${WEBSERVER_HOST_PORT:="28080"}
    export POSTGRES_HOST_PORT=${POSTGRES_HOST_PORT:="25433"}
    export MYSQL_HOST_PORT=${MYSQL_HOST_PORT:="23306"}
    export FLOWER_HOST_PORT=${FLOWER_HOST_PORT:="25555"}
    export REDIS_HOST_PORT=${REDIS_HOST_PORT:="26379"}

    # The SQLite URL used for sqlite runs
    export SQLITE_URL="sqlite:////root/airflow/airflow.db"

    # Disable writing .pyc files - slightly slower imports but not messing around when switching
    # Python version and avoids problems with root-owned .pyc files in host
    export PYTHONDONTWRITEBYTECODE=${PYTHONDONTWRITEBYTECODE:="true"}

    # By default we build CI images but we can switch to production image with PRODUCTION_IMAGE="true"
    export PRODUCTION_IMAGE="false"

    # All supported major/minor versions of python in all versions of Airflow
    ALL_PYTHON_MAJOR_MINOR_VERSIONS+=("2.7" "3.5" "3.6" "3.7" "3.8")
    export ALL_PYTHON_MAJOR_MINOR_VERSIONS

    # Currently supported major/minor versions of python
    CURRENT_PYTHON_MAJOR_MINOR_VERSIONS+=("3.6" "3.7" "3.8")
    export CURRENT_PYTHON_MAJOR_MINOR_VERSIONS

    # Currently supported versions of Postgres
    CURRENT_POSTGRES_VERSIONS+=("9.6" "13")
    export CURRENT_POSTGRES_VERSIONS

    # Currently supported versions of MySQL
    CURRENT_MYSQL_VERSIONS+=("5.7" "8")
    export CURRENT_MYSQL_VERSIONS

    BACKEND=${BACKEND:="sqlite"}
    export BACKEND

    # Default Postgres versions
    export POSTGRES_VERSION=${POSTGRES_VERSION:=${CURRENT_POSTGRES_VERSIONS[0]}}

    # Default MySQL versions
    export MYSQL_VERSION=${MYSQL_VERSION:=${CURRENT_MYSQL_VERSIONS[0]}}

    # If set to true, the database will be reset at entry. Works for Postgres and MySQL
    export DB_RESET=${DB_RESET:="false"}

    # If set to true, the database will be initialized, a user created and webserver and scheduler started
    export START_AIRFLOW=${START_AIRFLOW:="false"}

    # If set to true, the sample dags will be used
    export LOAD_EXAMPLES=${LOAD_EXAMPLES:="false"}

    # If set to true, the test connections will be created
    export LOAD_DEFAULT_CONNECTIONS=${LOAD_DEFAULT_CONNECTIONS:="false"}

    # If set to true, Breeze db volumes will be persisted when breeze is stopped and reused next time
    # Which means that you do not have to start from scratch
    export PRESERVE_VOLUMES="false"

    # If set to true, RBAC UI will not be used for 1.10 version
    export DISABLE_RBAC=${DISABLE_RBAC:="false"}

    # if set to true, the ci image will look for packages in dist folder and will install them
    # during entering the container
    export INSTALL_PACKAGES_FROM_DIST=${INSTALL_PACKAGES_FROM_DIST:="false"}

    # If set the specified file will be used to initialize Airflow after the environment is created,
    # otherwise it will use files/airflow-breeze-config/init.sh
    export INIT_SCRIPT_FILE=${INIT_SCRIPT_FILE:=""}

    # Read airflow version from the setup.py.
    AIRFLOW_VERSION=$(awk '/^version =/ {print $3}' "${AIRFLOW_SOURCES}/setup.py"  | sed "s/['+]//g")
    export AIRFLOW_VERSION

    # Whether credentials should be forwarded to inside the docker container
    export FORWARD_CREDENTIALS=${FORWARD_CREDENTIALS:="false"}

    # If no Airflow Home defined - fallback to ${HOME}/airflow
    AIRFLOW_HOME_DIR=${AIRFLOW_HOME:=${HOME}/airflow}
    export AIRFLOW_HOME_DIR

    # Dry run - only show docker-compose and docker commands but do not execute them
    export DRY_RUN_DOCKER=${DRY_RUN_DOCKER:="false"}
}

# Determine current branch
function initialization::initialize_branch_variables() {
    # Default branch used - this will be different in different branches
    export DEFAULT_BRANCH=${DEFAULT_BRANCH="master"}
    export DEFAULT_CONSTRAINTS_BRANCH=${DEFAULT_CONSTRAINTS_BRANCH="constraints-master"}
    readonly DEFAULT_BRANCH
    readonly DEFAULT_CONSTRAINTS_BRANCH

    # Default branch name for triggered builds is the one configured in default branch
    # We need to read it here as it comes from _common_values.sh
    export BRANCH_NAME=${BRANCH_NAME:=${DEFAULT_BRANCH}}
}

# Determine dockerhub user/repo used for push/pull
function initialization::initialize_dockerhub_variables() {
    # You can override DOCKERHUB_USER to use your own DockerHub account and play with your
    # own docker images. In this case you can build images locally and push them
    export DOCKERHUB_USER=${DOCKERHUB_USER:="apache"}

    # You can override DOCKERHUB_REPO to use your own DockerHub repository and play with your
    # own docker images. In this case you can build images locally and push them
    export DOCKERHUB_REPO=${DOCKERHUB_REPO:="airflow"}
}

# Determine available integrations
function initialization::initialize_available_integrations() {
    export AVAILABLE_INTEGRATIONS="cassandra kerberos mongo openldap pinot presto rabbitmq redis statsd"
}

# Needs to be declared outside of function for MacOS
FILES_FOR_REBUILD_CHECK=()

# Determine which files trigger rebuild check
function initialization::initialize_files_for_rebuild_check() {
    FILES_FOR_REBUILD_CHECK+=(
        "setup.py"
        "setup.cfg"
        "Dockerfile.ci"
        ".dockerignore"
        "scripts/docker/compile_www_assets.sh"
        "scripts/docker/common.sh"
        "scripts/docker/install_additional_dependencies.sh"
        "scripts/docker/install_airflow.sh"
        "scripts/docker/install_airflow_from_branch_tip.sh"
        "scripts/docker/install_from_docker_context_files.sh"
        "scripts/docker/install_mysql.sh"
        "airflow/www/package.json"
        "airflow/www/yarn.lock"
        "airflow/www/webpack.config.js"
    )
}

# Needs to be declared outside of function for MacOS

# extra flags passed to docker run for PROD image
# shellcheck disable=SC2034
EXTRA_DOCKER_PROD_BUILD_FLAGS=()

# files that should be cleaned up when the script exits
# shellcheck disable=SC2034
FILES_TO_CLEANUP_ON_EXIT=()

# extra flags passed to docker run for CI image
# shellcheck disable=SC2034
EXTRA_DOCKER_FLAGS=()

# Determine behaviour of mounting sources to the container
function initialization::initialize_mount_variables() {

    # Whether necessary for airflow run local sources are mounted to docker
    export MOUNT_SELECTED_LOCAL_SOURCES=${MOUNT_SELECTED_LOCAL_SOURCES:="true"}

    # Whether all airflow sources are mounted to docker
    export MOUNT_ALL_LOCAL_SOURCES=${MOUNT_ALL_LOCAL_SOURCES:="false"}

    if [[ ${MOUNT_SELECTED_LOCAL_SOURCES} == "true" ]]; then
        verbosity::print_info
        verbosity::print_info "Mounting necessary host volumes to Docker"
        verbosity::print_info
        read -r -a EXTRA_DOCKER_FLAGS <<<"$(local_mounts::convert_local_mounts_to_docker_params)"
    elif [[ ${MOUNT_ALL_LOCAL_SOURCES} == "true" ]]; then
        verbosity::print_info
        verbosity::print_info "Mounting whole airflow volume to Docker"
        verbosity::print_info
        EXTRA_DOCKER_FLAGS+=("-v" "${AIRFLOW_SOURCES}:/opt/airflow/:cached")
    else
        verbosity::print_info
        verbosity::print_info "Skip mounting host volumes to Docker"
        verbosity::print_info
    fi

    EXTRA_DOCKER_FLAGS+=(
        "-v" "${AIRFLOW_SOURCES}/files:/files"
        "-v" "${AIRFLOW_SOURCES}/dist:/dist"
        "--rm"
        "--env-file" "${AIRFLOW_SOURCES}/scripts/ci/docker-compose/_docker.env"
    )
    export EXTRA_DOCKER_FLAGS
}

# Determine values of force settings
function initialization::initialize_force_variables() {
    # By default we do not pull CI/PROD images. We can force-pull them when needed
    export FORCE_PULL_IMAGES=${FORCE_PULL_IMAGES:="false"}

    # By default we do not pull python base image. We should do that only when we run upgrade check in
    # CI master and when we manually refresh the images to latest versions
    export FORCE_PULL_BASE_PYTHON_IMAGE="false"

    # Determines whether to force build without checking if it is needed
    # Can be overridden by '--force-build-images' flag.
    export FORCE_BUILD_IMAGES=${FORCE_BUILD_IMAGES:="false"}

    # File to keep the last forced answer. This is useful for pre-commits where you need to
    # only answer once if the image should be rebuilt or not and your answer is used for
    # All the subsequent questions
    export LAST_FORCE_ANSWER_FILE="${BUILD_CACHE_DIR}/last_force_answer.sh"

    # Can be set to "yes/no/quit" in order to force specified answer to all questions asked to the user.
    export FORCE_ANSWER_TO_QUESTIONS=${FORCE_ANSWER_TO_QUESTIONS:=""}

    # Can be set to true to skip if the image is newer in registry
    export SKIP_CHECK_REMOTE_IMAGE=${SKIP_CHECK_REMOTE_IMAGE:="false"}

    # Should be set to true if you expect image frm GitHub to be present and downloaded
    export FAIL_ON_GITHUB_DOCKER_PULL_ERROR=${FAIL_ON_GITHUB_DOCKER_PULL_ERROR:="false"}
}

# Determine information about the host
function initialization::initialize_host_variables() {
    # Set host user id to current user. This is used to set the ownership properly when exiting
    # The container on Linux - all files created inside docker are created with root user
    # but they should be restored back to the host user
    HOST_USER_ID="$(id -ur)"
    export HOST_USER_ID

    # Set host group id to current group This is used to set the ownership properly when exiting
    # The container on Linux - all files created inside docker are created with root user
    # but they should be restored back to the host user
    HOST_GROUP_ID="$(id -gr)"
    export HOST_GROUP_ID

    # Set host OS. This is used to set the ownership properly when exiting
    # The container on Linux - all files created inside docker are created with root user
    # but they should be restored back to the host user
    HOST_OS="$(uname -s)"
    export HOST_OS

    # Home directory of the host user
    export HOST_HOME="${HOME}"

    # In case of MacOS we need to use gstat - gnu version of the stats
    export STAT_BIN=stat
    if [[ "${OSTYPE}" == "darwin"* ]]; then
        export STAT_BIN=gstat
    fi
}

# Determine image augmentation parameters
function initialization::initialize_image_build_variables() {
    # Default extras used for building CI image
    export DEFAULT_CI_EXTRAS="devel_ci"

    # Default build id
    export CI_BUILD_ID="${CI_BUILD_ID:="0"}"

    # Default extras used for building Production image. The canonical source of this information is in the Dockerfile
    DEFAULT_PROD_EXTRAS=$(grep "ARG AIRFLOW_EXTRAS=" "${AIRFLOW_SOURCES}/Dockerfile" |
        awk 'BEGIN { FS="=" } { print $2 }' | tr -d '"')
    export DEFAULT_PROD_EXTRAS

    # By default we are not upgrading to latest version of constraints when building Docker CI image
    # This will only be done in cron jobs
    export UPGRADE_TO_NEWER_DEPENDENCIES=${UPGRADE_TO_NEWER_DEPENDENCIES:="false"}

    # Checks if the image should be rebuilt
    export CHECK_IMAGE_FOR_REBUILD="${CHECK_IMAGE_FOR_REBUILD:="true"}"

    # Skips building production images altogether (assume they are already built)
    export SKIP_BUILDING_PROD_IMAGE="${SKIP_BUILDING_PROD_IMAGE:="false"}"

    # Additional airflow extras on top of the default ones
    export ADDITIONAL_AIRFLOW_EXTRAS="${ADDITIONAL_AIRFLOW_EXTRAS:=""}"
    # Additional python dependencies on top of the default ones
    export ADDITIONAL_PYTHON_DEPS="${ADDITIONAL_PYTHON_DEPS:=""}"
    # Use default DEV_APT_COMMAND
    export DEV_APT_COMMAND=""
    # Use default DEV_APT_DEPS
    export DEV_APT_DEPS=""
    # Use empty ADDITIONAL_DEV_APT_COMMAND
    export ADDITIONAL_DEV_APT_COMMAND=""
    # additional development apt dependencies on top of the default ones
    export ADDITIONAL_DEV_APT_DEPS="${ADDITIONAL_DEV_APT_DEPS:=""}"
    # Use empty ADDITIONAL_DEV_APT_ENV
    export ADDITIONAL_DEV_APT_ENV="${ADDITIONAL_DEV_APT_ENV:=""}"
    # Use default RUNTIME_APT_COMMAND
    export RUNTIME_APT_COMMAND=""
    # Use default RUNTIME_APT_DEPS
    export RUNTIME_APT_DEPS=""
    # Use empty ADDITIONAL_RUNTIME_APT_COMMAND
    export ADDITIONAL_RUNTIME_APT_COMMAND=""
    # additional runtime apt dependencies on top of the default ones
    export ADDITIONAL_RUNTIME_DEPS="${ADDITIONAL_RUNTIME_DEPS:=""}"
    export ADDITIONAL_RUNTIME_APT_DEPS="${ADDITIONAL_RUNTIME_APT_DEPS:=""}"
    # Use empty ADDITIONAL_RUNTIME_APT_ENV
    export ADDITIONAL_RUNTIME_APT_ENV="${ADDITIONAL_RUNTIME_APT_ENV:=""}"
    # whether pre cached pip packages are used during build
    export AIRFLOW_PRE_CACHED_PIP_PACKAGES="${AIRFLOW_PRE_CACHED_PIP_PACKAGES:="true"}"
    # by default install mysql client
    export INSTALL_MYSQL_CLIENT=${INSTALL_MYSQL_CLIENT:="true"}
    # additional tag for the image
    export IMAGE_TAG=${IMAGE_TAG:=""}

    INSTALL_PROVIDERS_FROM_SOURCES=${INSTALL_PROVIDERS_FROM_SOURCES:="true"}
    export INSTALL_PROVIDERS_FROM_SOURCES

    INSTALLED_PROVIDERS+=(
        "amazon"
        "celery"
        "cncf.kubernetes"
        "docker"
        "elasticsearch"
        "ftp"
        "grpc"
        "hashicorp"
        "http"
        "imap"
        "google"
        "microsoft.azure"
        "mysql"
        "postgres"
        "redis"
        "sendgrid"
        "sqlite"
        "sftp"
        "slack"
        "sqlite"
        "ssh"
    )
    export INSTALLED_PROVIDERS
    export INSTALLED_EXTRAS="async,amazon,celery,cncf.kubernetes,docker,dask,elasticsearch,ftp,grpc,hashicorp,http,imap,ldap,google,microsoft.azure,mysql,postgres,redis,sendgrid,sftp,slack,ssh,statsd,virtualenv"

    # default version of PIP USED (This has to be < 20.3 until https://github.com/apache/airflow/issues/12838 is solved)
    AIRFLOW_PIP_VERSION=${AIRFLOW_PIP_VERSION:="20.2.4"}
    export AIRFLOW_PIP_VERSION

    # We also pin version of wheel used to get consistent builds
    WHEEL_VERSION=${WHEEL_VERSION:="0.36.2"}
    export WHEEL_VERSION

    # And installed from there (breeze and ci)
    AIRFLOW_VERSION_SPECIFICATION=${AIRFLOW_VERSION_SPECIFICATION:=""}
    export AIRFLOW_VERSION_SPECIFICATION

    # By default no sources are copied to image
    AIRFLOW_SOURCES_FROM=${AIRFLOW_SOURCES_FROM:="empty"}
    export AIRFLOW_SOURCES_FROM

    AIRFLOW_SOURCES_TO=${AIRFLOW_SOURCES_TO:="/empty"}
    export AIRFLOW_SOURCES_TO

    # By default in scripts production docker image is installed from PyPI package
    export AIRFLOW_INSTALLATION_METHOD=${AIRFLOW_INSTALLATION_METHOD:="apache-airflow"}

    # Installs different airflow version than current from the sources
    export INSTALL_AIRFLOW_VERSION=${INSTALL_AIRFLOW_VERSION:=""}

    # Continue on PIP CHECK failure
    export CONTINUE_ON_PIP_CHECK_FAILURE=${CONTINUE_ON_PIP_CHECK_FAILURE:="false"}

    # Determines if airflow should be installed from a specified reference in GitHub
    export INSTALL_AIRFLOW_REFERENCE=${INSTALL_AIRFLOW_REFERENCE:=""}

    # Determines which providers are used to generate constraints - source, pypi or no providers
    export GENERATE_CONSTRAINTS_MODE=${GENERATE_CONSTRAINTS_MODE:="source-providers"}

    # whether installation of Airflow should be done via PIP. You can set it to false if you have
    # all the binary packages (including airflow) in the docker-context-files folder and use
    # INSTALL_FROM_DOCKER_CONTEXT_FILES="true" to install it from there.
    export INSTALL_FROM_PYPI="${INSTALL_FROM_PYPI:="true"}"

    # whether installation should be performed from the local wheel packages in "docker-context-files" folder
    export INSTALL_FROM_DOCKER_CONTEXT_FILES="${INSTALL_FROM_DOCKER_CONTEXT_FILES:="false"}"

    # reference to CONSTRAINTS. they can be overwritten manually or replaced with AIRFLOW_CONSTRAINTS_LOCATION
    export AIRFLOW_CONSTRAINTS_REFERENCE="${AIRFLOW_CONSTRAINTS_REFERENCE:=""}"

    # direct constraints Location - can be URL or path to local file. If empty, it will be calculated
    # based on which Airflow version is installed and from where
    export AIRFLOW_CONSTRAINTS_LOCATION="${AIRFLOW_CONSTRAINTS_LOCATION:=""}"

    # Suffix for constraints. Can be:
    #   * 'constraints' = for constraints with PyPI released providers (default for installations)
    #   * 'constraints-source-providers' for constraints with source version of providers (defaults in Breeze and CI)
    #   * 'constraints-no-providers' for constraints without providers
    export AIRFLOW_CONSTRAINTS="${AIRFLOW_CONSTRAINTS:="constraints-source-providers"}"
}

# Determine version suffixes used to build provider packages
function initialization::initialize_provider_package_building() {
    # Version suffix for PyPI packaging
    export VERSION_SUFFIX_FOR_PYPI="${VERSION_SUFFIX_FOR_PYPI=}"
    # Artifact name suffix for SVN packaging
    export VERSION_SUFFIX_FOR_SVN="${VERSION_SUFFIX_FOR_SVN=}"

}

# Determine versions of kubernetes cluster and tools used
function initialization::initialize_kubernetes_variables() {
    # Currently supported versions of Kubernetes
    CURRENT_KUBERNETES_VERSIONS+=("v1.18.6" "v1.17.5" "v1.16.9")
    export CURRENT_KUBERNETES_VERSIONS
    # Currently supported modes of Kubernetes
    CURRENT_KUBERNETES_MODES+=("image")
    export CURRENT_KUBERNETES_MODES
    # Currently supported versions of Kind
    CURRENT_KIND_VERSIONS+=("v0.8.0")
    export CURRENT_KIND_VERSIONS
    # Currently supported versions of Helm
    CURRENT_HELM_VERSIONS+=("v3.2.4")
    export CURRENT_HELM_VERSIONS
    # Default Kubernetes version
    export DEFAULT_KUBERNETES_VERSION="${CURRENT_KUBERNETES_VERSIONS[0]}"
    # Default Kubernetes mode
    export DEFAULT_KUBERNETES_MODE="${CURRENT_KUBERNETES_MODES[0]}"
    # Default KinD version
    export DEFAULT_KIND_VERSION="${CURRENT_KIND_VERSIONS[0]}"
    # Default Helm version
    export DEFAULT_HELM_VERSION="${CURRENT_HELM_VERSIONS[0]}"
    # Namespace where airflow is installed via helm
    export HELM_AIRFLOW_NAMESPACE="airflow"
    # Kubernetes version
    export KUBERNETES_VERSION=${KUBERNETES_VERSION:=${DEFAULT_KUBERNETES_VERSION}}
    # Kubernetes mode
    export KUBERNETES_MODE=${KUBERNETES_MODE:=${DEFAULT_KUBERNETES_MODE}}
    # Kind version
    export KIND_VERSION=${KIND_VERSION:=${DEFAULT_KIND_VERSION}}
    # Helm version
    export HELM_VERSION=${HELM_VERSION:=${DEFAULT_HELM_VERSION}}
    # Kubectl version
    export KUBECTL_VERSION=${KUBERNETES_VERSION:=${DEFAULT_KUBERNETES_VERSION}}
    # Local Kind path
    export KIND_BINARY_PATH="${BUILD_CACHE_DIR}/bin/kind"
    readonly KIND_BINARY_PATH
    # Local Helm path
    export HELM_BINARY_PATH="${BUILD_CACHE_DIR}/bin/helm"
    readonly HELM_BINARY_PATH
    # local Kubectl path
    export KUBECTL_BINARY_PATH="${BUILD_CACHE_DIR}/bin/kubectl"
    readonly KUBECTL_BINARY_PATH
}

function initialization::initialize_git_variables() {
    # SHA of the commit for the current sources
    COMMIT_SHA="$(git rev-parse HEAD 2>/dev/null || echo "Unknown")"
    export COMMIT_SHA
}

function initialization::initialize_github_variables() {
    # Defaults for interacting with GitHub
    export USE_GITHUB_REGISTRY=${USE_GITHUB_REGISTRY:="false"}
    export GITHUB_REGISTRY_IMAGE_SUFFIX=${GITHUB_REGISTRY_IMAGE_SUFFIX:="-v2"}
    export GITHUB_REGISTRY=${GITHUB_REGISTRY:="docker.pkg.github.com"}
    export GITHUB_REGISTRY_WAIT_FOR_IMAGE=${GITHUB_REGISTRY_WAIT_FOR_IMAGE:="false"}
    export GITHUB_REGISTRY_PULL_IMAGE_TAG=${GITHUB_REGISTRY_PULL_IMAGE_TAG:="latest"}
    export GITHUB_REGISTRY_PUSH_IMAGE_TAG=${GITHUB_REGISTRY_PUSH_IMAGE_TAG:="latest"}

    export GITHUB_REPOSITORY=${GITHUB_REPOSITORY:="apache/airflow"}
    # Allows to override the repository which is used as source of constraints during the build
    export CONSTRAINTS_GITHUB_REPOSITORY=${CONSTRAINTS_GITHUB_REPOSITORY:="apache/airflow"}

    # Used only in CI environment
    export GITHUB_TOKEN="${GITHUB_TOKEN=""}"
    export GITHUB_USERNAME="${GITHUB_USERNAME=""}"
}

function initialization::initialize_test_variables() {

    # In case we want to force certain test type to run, this variable should be set to this type
    # Otherwise TEST_TYPEs to run will be derived from TEST_TYPES space-separated string
    export FORCE_TEST_TYPE=${FORCE_TEST_TYPE:=""}
}

function initialization::initialize_package_variables() {
    export PACKAGE_FORMAT=${PACKAGE_FORMAT:="wheel"}
}


function initialization::initialize_build_image_variables() {
    REMOTE_IMAGE_CONTAINER_ID_FILE="${AIRFLOW_SOURCES}/manifests/remote-airflow-manifest-image"
    LOCAL_IMAGE_BUILD_CACHE_HASH_FILE="${AIRFLOW_SOURCES}/manifests/local-build-cache-hash"
    REMOTE_IMAGE_BUILD_CACHE_HASH_FILE="${AIRFLOW_SOURCES}/manifests/remote-build-cache-hash"
}

function initialization::set_output_color_variables() {
    COLOR_BLUE=$'\e[34m'
    COLOR_GREEN=$'\e[32m'
    COLOR_RED=$'\e[31m'
    COLOR_RESET=$'\e[0m'
    COLOR_YELLOW=$'\e[33m'
    export COLOR_BLUE
    export COLOR_GREEN
    export COLOR_RED
    export COLOR_RESET
    export COLOR_YELLOW
}

# Common environment that is initialized by both Breeze and CI scripts
function initialization::initialize_common_environment() {
    initialization::set_output_color_variables
    initialization::create_directories
    initialization::initialize_base_variables
    initialization::initialize_branch_variables
    initialization::initialize_available_integrations
    initialization::initialize_files_for_rebuild_check
    initialization::initialize_dockerhub_variables
    initialization::initialize_mount_variables
    initialization::initialize_force_variables
    initialization::initialize_host_variables
    initialization::initialize_image_build_variables
    initialization::initialize_provider_package_building
    initialization::initialize_kubernetes_variables
    initialization::initialize_git_variables
    initialization::initialize_github_variables
    initialization::initialize_test_variables
    initialization::initialize_package_variables
    initialization::initialize_build_image_variables
}

function initialization::set_default_python_version_if_empty() {
    # default version of python used to tag the "master" and "latest" images in DockerHub
    export DEFAULT_PYTHON_MAJOR_MINOR_VERSION=3.6

    # default python Major/Minor version
    export PYTHON_MAJOR_MINOR_VERSION=${PYTHON_MAJOR_MINOR_VERSION:=${DEFAULT_PYTHON_MAJOR_MINOR_VERSION}}

}

function initialization::summarize_build_environment() {
    cat <<EOF

Configured build variables:

Basic variables:

    PYTHON_MAJOR_MINOR_VERSION: ${PYTHON_MAJOR_MINOR_VERSION}
    DB_RESET: ${DB_RESET}
    START_AIRFLOW: ${START_AIRFLOW}

DockerHub variables:

    DOCKERHUB_USER=${DOCKERHUB_USER}
    DOCKERHUB_REPO=${DOCKERHUB_REPO}

Mount variables:

    MOUNT_SELECTED_LOCAL_SOURCES: ${MOUNT_SELECTED_LOCAL_SOURCES}
    MOUNT_ALL_LOCAL_SOURCES: ${MOUNT_ALL_LOCAL_SOURCES}

Force variables:

    FORCE_PULL_IMAGES: ${FORCE_PULL_IMAGES}
    FORCE_BUILD_IMAGES: ${FORCE_BUILD_IMAGES}
    FORCE_ANSWER_TO_QUESTIONS: ${FORCE_ANSWER_TO_QUESTIONS}
    SKIP_CHECK_REMOTE_IMAGE: ${SKIP_CHECK_REMOTE_IMAGE}
    FAIL_ON_GITHUB_DOCKER_PULL_ERROR: ${FAIL_ON_GITHUB_DOCKER_PULL_ERROR}

Host variables:

    HOST_USER_ID=${HOST_USER_ID}
    HOST_GROUP_ID=${HOST_GROUP_ID}
    HOST_OS=${HOST_OS}
    HOST_HOME=${HOST_HOME}

Version suffix variables:

    VERSION_SUFFIX_FOR_PYPI=${VERSION_SUFFIX_FOR_PYPI}
    VERSION_SUFFIX_FOR_SVN=${VERSION_SUFFIX_FOR_SVN}

Git variables:

    COMMIT_SHA = ${COMMIT_SHA}

Verbosity variables:

    VERBOSE: ${VERBOSE}
    VERBOSE_COMMANDS: ${VERBOSE_COMMANDS}

Common image build variables:

    INSTALL_AIRFLOW_VERSION: '${INSTALL_AIRFLOW_VERSION}'
    INSTALL_AIRFLOW_REFERENCE: '${INSTALL_AIRFLOW_REFERENCE}'
    INSTALL_FROM_PYPI: '${INSTALL_FROM_PYPI}'
    AIRFLOW_PRE_CACHED_PIP_PACKAGES: '${AIRFLOW_PRE_CACHED_PIP_PACKAGES}'
    UPGRADE_TO_NEWER_DEPENDENCIES: '${UPGRADE_TO_NEWER_DEPENDENCIES}'
    CONTINUE_ON_PIP_CHECK_FAILURE: '${CONTINUE_ON_PIP_CHECK_FAILURE}'
    CHECK_IMAGE_FOR_REBUILD: '${CHECK_IMAGE_FOR_REBUILD}'
    AIRFLOW_CONSTRAINTS_LOCATION: '${AIRFLOW_CONSTRAINTS_LOCATION}'
    AIRFLOW_CONSTRAINTS_REFERENCE: '${AIRFLOW_CONSTRAINTS_REFERENCE}'
    INSTALL_PROVIDERS_FROM_SOURCES: '${INSTALL_PROVIDERS_FROM_SOURCES}'
    INSTALL_FROM_DOCKER_CONTEXT_FILES: '${INSTALL_FROM_DOCKER_CONTEXT_FILES}'
    ADDITIONAL_AIRFLOW_EXTRAS: '${ADDITIONAL_AIRFLOW_EXTRAS}'
    ADDITIONAL_PYTHON_DEPS: '${ADDITIONAL_PYTHON_DEPS}'
    DEV_APT_COMMAND: '${DEV_APT_COMMAND}'
    ADDITIONAL_DEV_APT_COMMAND: '${ADDITIONAL_DEV_APT_COMMAND}'
    DEV_APT_DEPS: '${DEV_APT_DEPS}'
    ADDITIONAL_DEV_APT_DEPS: '${ADDITIONAL_DEV_APT_DEPS}'
    RUNTIME_APT_COMMAND: '${RUNTIME_APT_COMMAND}'
    ADDITIONAL_RUNTIME_APT_COMMAND: '${ADDITIONAL_RUNTIME_APT_COMMAND}'
    RUNTIME_APT_DEPS: '${RUNTIME_APT_DEPS}'
    ADDITIONAL_RUNTIME_APT_DEPS: '${ADDITIONAL_RUNTIME_APT_DEPS}'
    ADDITIONAL_RUNTIME_APT_ENV: '${ADDITIONAL_RUNTIME_APT_ENV}'

Production image build variables:

    AIRFLOW_INSTALLATION_METHOD: '${AIRFLOW_INSTALLATION_METHOD}'
    AIRFLOW_VERSION_SPECIFICATION: '${AIRFLOW_VERSION_SPECIFICATION}'
    AIRFLOW_SOURCES_FROM: '${AIRFLOW_SOURCES_FROM}'
    AIRFLOW_SOURCES_TO: '${AIRFLOW_SOURCES_TO}'

Detected GitHub environment:

    USE_GITHUB_REGISTRY: '${USE_GITHUB_REGISTRY}'
    GITHUB_REGISTRY: '${GITHUB_REGISTRY}'
    GITHUB_REPOSITORY: '${GITHUB_REPOSITORY}'
    GITHUB_USERNAME: '${GITHUB_USERNAME}'
    GITHUB_TOKEN: '${GITHUB_TOKEN}'
    GITHUB_REGISTRY_WAIT_FOR_IMAGE: '${GITHUB_REGISTRY_WAIT_FOR_IMAGE}'
    GITHUB_REGISTRY_PULL_IMAGE_TAG: '${GITHUB_REGISTRY_PULL_IMAGE_TAG}'
    GITHUB_REGISTRY_PUSH_IMAGE_TAG: '${GITHUB_REGISTRY_PUSH_IMAGE_TAG}'
    GITHUB_ACTIONS: '${GITHUB_ACTIONS=}'

Initialization variables:

    INIT_SCRIPT_FILE: '${INIT_SCRIPT_FILE=}'
    LOAD_DEFAULT_CONNECTIONS: '${LOAD_DEFAULT_CONNECTIONS}'
    LOAD_EXAMPLES: '${LOAD_EXAMPLES}'
    INSTALL_PACKAGES_FROM_DIST: '${INSTALL_PACKAGES_FROM_DIST=}'
    DISABLE_RBAC: '${DISABLE_RBAC}'

Test variables:

    TEST_TYPE: '${TEST_TYPE}'

EOF
    if [[ "${CI}" == "true" ]]; then
        cat <<EOF

Detected CI build environment:

    CI_TARGET_REPO=${CI_TARGET_REPO}
    CI_TARGET_BRANCH=${CI_TARGET_BRANCH}
    CI_BUILD_ID=${CI_BUILD_ID}
    CI_JOB_ID=${CI_JOB_ID}
    CI_EVENT_TYPE=${CI_EVENT_TYPE}
EOF
    fi
}

# Retrieves CI environment variables needed - depending on the CI system we run it in.
# We try to be CI - agnostic and our scripts should run the same way on different CI systems
# (This makes it easy to move between different CI systems)
# This function maps CI-specific variables into a generic ones (prefixed with CI_) that
# we used in other scripts
function initialization::get_environment_for_builds_on_ci() {
    if [[ ${CI:=} == "true" ]]; then
        export GITHUB_REPOSITORY="${GITHUB_REPOSITORY="apache/airflow"}"
        export CI_TARGET_REPO="${GITHUB_REPOSITORY}"
        export CI_TARGET_BRANCH="${GITHUB_BASE_REF:="master"}"
        export CI_BUILD_ID="${GITHUB_RUN_ID="0"}"
        export CI_JOB_ID="${GITHUB_JOB="0"}"
        export CI_EVENT_TYPE="${GITHUB_EVENT_NAME="pull_request"}"
        export CI_REF="${GITHUB_REF:="refs/head/master"}"
    else
        # CI PR settings
        export GITHUB_REPOSITORY="${GITHUB_REPOSITORY="apache/airflow"}"
        export CI_TARGET_REPO="${CI_TARGET_REPO="apache/airflow"}"
        export CI_TARGET_BRANCH="${DEFAULT_BRANCH="master"}"
        export CI_BUILD_ID="${CI_BUILD_ID="0"}"
        export CI_JOB_ID="${CI_JOB_ID="0"}"
        export CI_EVENT_TYPE="${CI_EVENT_TYPE="pull_request"}"
        export CI_REF="${CI_REF="refs/head/master"}"
    fi

    if [[ -z "${LIBRARY_PATH:-}" && -n "${LD_LIBRARY_PATH:-}" ]]; then
      export LIBRARY_PATH="${LD_LIBRARY_PATH}"
    fi
}

# shellcheck disable=SC2034

# By the time this method is run, nearly all constants have been already set to the final values
# so we can set them as readonly.
function initialization::make_constants_read_only() {
    # Set the arguments as read-only
    readonly PYTHON_MAJOR_MINOR_VERSION

    readonly HOST_USER_ID
    readonly HOST_GROUP_ID
    readonly HOST_HOME
    readonly HOST_OS

    readonly KUBERNETES_MODE
    readonly KUBERNETES_VERSION
    readonly KIND_VERSION
    readonly HELM_VERSION
    readonly KUBECTL_VERSION

    readonly BACKEND
    readonly POSTGRES_VERSION
    readonly MYSQL_VERSION

    readonly MOUNT_SELECTED_LOCAL_SOURCES
    readonly MOUNT_ALL_LOCAL_SOURCES

    readonly INSTALL_AIRFLOW_VERSION
    readonly INSTALL_AIRFLOW_REFERENCE

    readonly DB_RESET
    readonly VERBOSE

    readonly START_AIRFLOW

    readonly PRODUCTION_IMAGE

    # The FORCE_* variables are missing here because they are not constant - they are just exported variables.
    # Their value might change during the script execution - for example when during the
    # pre-commit the answer is "no", we set the FORCE_ANSWER_TO_QUESTIONS to "no"
    # for all subsequent questions. Also in CI environment we first force pulling and building
    # the images but then we disable it so that in subsequent steps the image is reused.
    # similarly CHECK_IMAGE_FOR_REBUILD variable.

    readonly SKIP_BUILDING_PROD_IMAGE
    readonly CI_BUILD_ID
    readonly CI_JOB_ID

    readonly IMAGE_TAG

    readonly AIRFLOW_PRE_CACHED_PIP_PACKAGES
    readonly INSTALL_FROM_PYPI
    readonly INSTALL_FROM_DOCKER_CONTEXT_FILES
    readonly AIRFLOW_CONSTRAINTS_REFERENCE
    readonly AIRFLOW_CONSTRAINTS_LOCATION

    # AIRFLOW_EXTRAS are made readonly by the time the image is built (either PROD or CI)
    readonly ADDITIONAL_AIRFLOW_EXTRAS
    readonly ADDITIONAL_PYTHON_DEPS

    readonly AIRFLOW_PRE_CACHED_PIP_PACKAGES

    readonly DEV_APT_COMMAND
    readonly DEV_APT_DEPS

    readonly ADDITIONAL_DEV_APT_COMMAND
    readonly ADDITIONAL_DEV_APT_DEPS
    readonly ADDITIONAL_DEV_APT_ENV

    readonly RUNTIME_APT_COMMAND
    readonly RUNTIME_APT_DEPS

    readonly ADDITIONAL_RUNTIME_APT_COMMAND
    readonly ADDITIONAL_RUNTIME_APT_DEPS
    readonly ADDITIONAL_RUNTIME_APT_ENV

    readonly DOCKERHUB_USER
    readonly DOCKERHUB_REPO
    readonly DOCKER_CACHE

    readonly USE_GITHUB_REGISTRY
    readonly GITHUB_REGISTRY
    readonly GITHUB_REGISTRY_WAIT_FOR_IMAGE
    readonly GITHUB_REGISTRY_PULL_IMAGE_TAG
    readonly GITHUB_REGISTRY_PUSH_IMAGE_TAG

    readonly GITHUB_REPOSITORY
    readonly GITHUB_TOKEN
    readonly GITHUB_USERNAME

    readonly FORWARD_CREDENTIALS
    readonly USE_GITHUB_REGISTRY

    readonly EXTRA_STATIC_CHECK_OPTIONS

    readonly VERSION_SUFFIX_FOR_PYPI
    readonly VERSION_SUFFIX_FOR_SVN

    readonly PYTHON_BASE_IMAGE_VERSION
    readonly PYTHON_BASE_IMAGE
    readonly AIRFLOW_PYTHON_BASE_IMAGE
    readonly AIRFLOW_CI_BASE_TAG
    readonly AIRFLOW_CI_IMAGE
    readonly AIRFLOW_CI_IMAGE_DEFAULT
    readonly AIRFLOW_PROD_BASE_TAG
    readonly AIRFLOW_PROD_IMAGE
    readonly AIRFLOW_PROD_BUILD_IMAGE
    readonly AIRFLOW_PROD_IMAGE_KUBERNETES
    readonly AIRFLOW_PROD_IMAGE_DEFAULT
    readonly BUILT_CI_IMAGE_FLAG_FILE
    readonly INIT_SCRIPT_FILE

    readonly REMOTE_IMAGE_CONTAINER_ID_FILE
    readonly LOCAL_IMAGE_BUILD_CACHE_HASH_FILE
    readonly REMOTE_IMAGE_BUILD_CACHE_HASH_FILE

    readonly INSTALLED_EXTRAS
    readonly INSTALLED_PROVIDERS

    readonly CURRENT_PYTHON_MAJOR_MINOR_VERSIONS
    readonly CURRENT_KUBERNETES_VERSIONS
    readonly CURRENT_KUBERNETES_MODES
    readonly CURRENT_POSTGRES_VERSIONS
    readonly CURRENT_MYSQL_VERSIONS
    readonly CURRENT_KIND_VERSIONS
    readonly CURRENT_HELM_VERSIONS
    readonly ALL_PYTHON_MAJOR_MINOR_VERSIONS
}

# converts parameters to json array
function initialization::parameters_to_json() {
    echo -n "["
    local separator=""
    local var
    for var in "${@}"; do
        echo -n "${separator}\"${var}\""
        separator=","
    done
    echo "]"
}

# output parameter name and value - both to stdout and to be set by GitHub Actions
function initialization::ga_output() {
    echo "::set-output name=${1}::${2}"
    echo "${1}=${2}"
}

function initialization::ga_env() {
    if [[ -n "${GITHUB_ENV=}" ]]; then
        echo "${1}=${2}" >>"${GITHUB_ENV}"
    fi
}
