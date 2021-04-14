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
. ./scripts/ci/libraries/_script_init.sh

# Parameter:
#
# $1 - COMMIT SHA of the incoming commit. If this parameter is missing, this script does not check anything,
#      it simply sets all the version outputs that determine that all tests should be run.
#      This happens in case the even triggering the workflow is 'schedule' or 'push'.
#
# The logic of retrieving changes works by comparing the incoming commit with the target branch
# The commit addresses.
#
#
declare -a pattern_array

if [[ ${PR_LABELS=} == *"full tests needed"* ]]; then
    echo
    echo "Found the right PR labels in '${PR_LABELS=}': 'full tests needed''"
    echo
    FULL_TESTS_NEEDED_LABEL="true"
else
    echo
    echo "Did not find the right PR labels in '${PR_LABELS=}': 'full tests needed'"
    echo
    FULL_TESTS_NEEDED_LABEL="false"
fi

function check_upgrade_to_newer_dependencies_needed() {
    # shellcheck disable=SC2153
    if [[ "${UPGRADE_TO_NEWER_DEPENDENCIES}" != "false" ||
            ${GITHUB_EVENT_NAME} == 'push' || ${GITHUB_EVENT_NAME} == "scheduled" ]]; then
        # Trigger upgrading to latest constraints where label is set or when
        # SHA of the merge commit triggers rebuilding layer in the docker image
        # Each build that upgrades to latest constraints will get truly latest constraints, not those
        # Cached in the image this way
        upgrade_to_newer_dependencies="${INCOMING_COMMIT_SHA}"
    fi
}

function output_all_basic_variables() {
    if [[ ${FULL_TESTS_NEEDED_LABEL} == "true" ]]; then
        initialization::ga_output python-versions \
            "$(initialization::parameters_to_json "${CURRENT_PYTHON_MAJOR_MINOR_VERSIONS[@]}")"
        initialization::ga_output all-python-versions \
            "$(initialization::parameters_to_json "${ALL_PYTHON_MAJOR_MINOR_VERSIONS[@]}")"
        initialization::ga_output python-versions-list-as-string "${CURRENT_PYTHON_MAJOR_MINOR_VERSIONS[*]}"
    else
        initialization::ga_output python-versions \
            "$(initialization::parameters_to_json "${DEFAULT_PYTHON_MAJOR_MINOR_VERSION}")"
        # this will work as long as DEFAULT_PYTHON_MAJOR_VERSION is the same on HEAD and v1-10
        # all-python-versions are used in BuildImage Workflow
        initialization::ga_output all-python-versions \
            "$(initialization::parameters_to_json "${DEFAULT_PYTHON_MAJOR_MINOR_VERSION}")"
        initialization::ga_output python-versions-list-as-string "${DEFAULT_PYTHON_MAJOR_MINOR_VERSION}"
    fi
    initialization::ga_output default-python-version "${DEFAULT_PYTHON_MAJOR_MINOR_VERSION}"

    if [[ ${FULL_TESTS_NEEDED_LABEL} == "true" ]]; then
        initialization::ga_output kubernetes-versions \
            "$(initialization::parameters_to_json "${CURRENT_KUBERNETES_VERSIONS[@]}")"
    else
        initialization::ga_output kubernetes-versions \
            "$(initialization::parameters_to_json "${KUBERNETES_VERSION}")"
    fi
    initialization::ga_output default-kubernetes-version "${KUBERNETES_VERSION}"

    initialization::ga_output kubernetes-modes \
        "$(initialization::parameters_to_json "${CURRENT_KUBERNETES_MODES[@]}")"
    initialization::ga_output default-kubernetes-mode "${KUBERNETES_MODE}"

    if [[ ${FULL_TESTS_NEEDED_LABEL} == "true" ]]; then
        initialization::ga_output postgres-versions \
            "$(initialization::parameters_to_json "${CURRENT_POSTGRES_VERSIONS[@]}")"
    else
        initialization::ga_output postgres-versions \
            "$(initialization::parameters_to_json "${POSTGRES_VERSION}")"
    fi
    initialization::ga_output default-postgres-version "${POSTGRES_VERSION}"

    if [[ ${FULL_TESTS_NEEDED_LABEL} == "true" ]]; then
        initialization::ga_output mysql-versions \
            "$(initialization::parameters_to_json "${CURRENT_MYSQL_VERSIONS[@]}")"
    else
        initialization::ga_output mysql-versions \
            "$(initialization::parameters_to_json "${MYSQL_VERSION}")"
    fi

    initialization::ga_output default-mysql-version "${MYSQL_VERSION}"

    initialization::ga_output kind-versions \
        "$(initialization::parameters_to_json "${CURRENT_KIND_VERSIONS[@]}")"
    initialization::ga_output default-kind-version "${KIND_VERSION}"

    initialization::ga_output helm-versions \
        "$(initialization::parameters_to_json "${CURRENT_HELM_VERSIONS[@]}")"
    initialization::ga_output default-helm-version "${HELM_VERSION}"

    if [[ ${FULL_TESTS_NEEDED_LABEL} == "true" ]]; then
        initialization::ga_output postgres-exclude '[{ "python-version": "3.6" }]'
        initialization::ga_output mysql-exclude '[{ "python-version": "3.7" }]'
        initialization::ga_output sqlite-exclude '[{ "python-version": "3.8" }]'
    else
        initialization::ga_output postgres-exclude '[]'
        initialization::ga_output mysql-exclude '[]'
        initialization::ga_output sqlite-exclude '[]'
    fi

    initialization::ga_output kubernetes-exclude '[]'
}

function get_changed_files() {
    start_end::group_start "Get changed files"
    echo
    echo "Incoming commit SHA: ${INCOMING_COMMIT_SHA}"
    echo
    echo "Changed files from ${INCOMING_COMMIT_SHA} vs it's first parent"
    echo
    CHANGED_FILES=$(git diff-tree --no-commit-id --name-only \
        -r "${INCOMING_COMMIT_SHA}^" "${INCOMING_COMMIT_SHA}" || true)
    if [[ -z "${CHANGED_FILES}" ]]; then
        echo
        echo  "${COLOR_YELLOW}WARNING: Could not find any changed files  ${COLOR_RESET}"
        echo Assuming that we should run all tests in this case
        echo
        set_outputs_run_everything_and_exit
    fi
    echo
    echo "Changed files:"
    echo
    echo "${CHANGED_FILES}"
    echo
    readonly CHANGED_FILES
    start_end::group_end
}

function run_tests() {
    initialization::ga_output run-tests "${@}"
}

function run_kubernetes_tests() {
    initialization::ga_output run-kubernetes-tests "${@}"
}

function needs_helm_tests() {
    initialization::ga_output needs-helm-tests "${@}"
}

function needs_api_tests() {
    initialization::ga_output needs-api-tests "${@}"
}

function needs_api_codegen() {
    initialization::ga_output needs-api-codegen "${@}"
}

function needs_javascript_scans() {
    initialization::ga_output needs-javascript-scans "${@}"
}

function needs_python_scans() {
    initialization::ga_output needs-python-scans "${@}"
}

function set_test_types() {
    initialization::ga_output test-types "${@}"
}

function set_docs_build() {
    initialization::ga_output docs-build "${@}"
}

function set_image_build() {
    initialization::ga_output image-build "${@}"
}

function set_basic_checks_only() {
    initialization::ga_output basic-checks-only "${@}"
}

function set_upgrade_to_newer_dependencies() {
    initialization::ga_output upgrade-to-newer-dependencies "${@}"
}


ALL_TESTS="Always API Core Other CLI Providers WWW Integration"
readonly ALL_TESTS

function set_outputs_run_everything_and_exit() {
    needs_api_tests "true"
    needs_api_codegen "true"
    needs_helm_tests "true"
    needs_javascript_scans "true"
    needs_python_scans "true"
    run_tests "true"
    run_kubernetes_tests "true"
    set_test_types "${ALL_TESTS}"
    set_basic_checks_only "false"
    set_docs_build "true"
    set_image_build "true"
    set_upgrade_to_newer_dependencies "${upgrade_to_newer_dependencies}"
    exit
}

function set_outputs_run_all_tests() {
    run_tests "true"
    run_kubernetes_tests "true"
    set_test_types "${ALL_TESTS}"
    set_basic_checks_only "false"
    set_image_build "true"
    kubernetes_tests_needed="true"
}

function set_output_skip_all_tests_and_docs_and_exit() {
    needs_api_tests "false"
    needs_api_codegen "false"
    needs_helm_tests "false"
    needs_javascript_scans "false"
    needs_python_scans "false"
    run_tests "false"
    run_kubernetes_tests "false"
    set_test_types ""
    set_basic_checks_only "true"
    set_docs_build "false"
    set_image_build "false"
    set_upgrade_to_newer_dependencies "false"
    exit
}

function set_output_skip_tests_but_build_images_and_exit() {
    needs_api_tests "false"
    needs_api_codegen "false"
    needs_helm_tests "false"
    needs_javascript_scans "false"
    needs_python_scans "false"
    run_tests "false"
    run_kubernetes_tests "false"
    set_test_types ""
    set_basic_checks_only "false"
    set_docs_build "true"
    set_image_build "true"
    set_upgrade_to_newer_dependencies "${upgrade_to_newer_dependencies}"
    exit
}

# Converts array of patterns into single | pattern string
#    pattern_array - array storing regexp patterns
# Outputs - pattern string
function get_regexp_from_patterns() {
    local test_triggering_regexp=""
    local separator=""
    local pattern
    for pattern in "${pattern_array[@]}"; do
        test_triggering_regexp="${test_triggering_regexp}${separator}${pattern}"
        separator="|"
    done
    echo "${test_triggering_regexp}"
}

# Shows changed files in the commit vs. the target.
# Input:
#    pattern_array - array storing regexp patterns
function show_changed_files() {
    local the_regexp
    the_regexp=$(get_regexp_from_patterns)
    echo
    echo "Changed files matching the ${the_regexp} pattern:"
    echo
    echo "${CHANGED_FILES}" | grep -E "${the_regexp}" || true
    echo
}

# Counts changed files in the commit vs. the target
# Input:
#    pattern_array - array storing regexp patterns
# Output:
#    Count of changed files matching the patterns
function count_changed_files() {
    echo "${CHANGED_FILES}" | grep -c -E "$(get_regexp_from_patterns)" || true
}

function check_if_python_security_scans_should_be_run() {
    start_end::group_start "Check Python security scans"
    local pattern_array=(
        "^airflow/.*\.py"
        "^setup.py"
    )
    show_changed_files

    if [[ $(count_changed_files) == "0" ]]; then
        needs_python_scans "false"
    else
        needs_python_scans "true"
    fi
    start_end::group_end
}

function check_if_setup_files_changed() {
    start_end::group_start "Check Python security scans"
    local pattern_array=(
        "^setup.cfg"
        "^setup.py"
    )
    show_changed_files

    if [[ $(count_changed_files) != "0" ]]; then
        upgrade_to_newer_dependencies="${INCOMING_COMMIT_SHA}"
    fi
    start_end::group_end
}


function check_if_javascript_security_scans_should_be_run() {
    start_end::group_start "Check Javascript security scans"
    local pattern_array=(
        "^airflow/.*\.js"
        "^airflow/.*\.lock"
    )
    show_changed_files

    if [[ $(count_changed_files) == "0" ]]; then
        needs_javascript_scans "false"
    else
        needs_javascript_scans "true"
    fi
    start_end::group_end
}

function check_if_api_tests_should_be_run() {
    start_end::group_start "Check API tests"
    local pattern_array=(
        "^airflow/api"
    )
    show_changed_files

    if [[ $(count_changed_files) == "0" ]]; then
        needs_api_tests "false"
    else
        needs_api_tests "true"
    fi
    start_end::group_end
}

function check_if_api_codegen_should_be_run() {
    start_end::group_start "Check API codegen"
    local pattern_array=(
        "^airflow/api_connexion/openapi/v1.yaml"
        "^clients/gen"
    )
    show_changed_files

    if [[ $(count_changed_files) == "0" ]]; then
        needs_api_codegen "false"
    else
        needs_api_codegen "true"
    fi
    start_end::group_end
}

function check_if_helm_tests_should_be_run() {
    start_end::group_start "Check helm tests"
    local pattern_array=(
        "^chart"
    )
    show_changed_files

    if [[ $(count_changed_files) == "0" ]]; then
        needs_helm_tests "false"
    else
        needs_helm_tests "true"
    fi
    start_end::group_end
}

function check_if_docs_should_be_generated() {
    start_end::group_start "Check docs"
    local pattern_array=(
        "^docs"
        "^airflow/.*\.py$"
        "^CHANGELOG\.txt"
        "^airflow/config_templates/config\.yml"
    )
    show_changed_files

    if [[ $(count_changed_files) == "0" ]]; then
        echo "None of the docs changed"
    else
        image_build_needed="true"
        docs_build_needed="true"
    fi
    start_end::group_end
}


ANY_PY_FILES_CHANGED=(
    "\.py$"
)
readonly ANY_PY_FILES_CHANGED

function check_if_any_py_files_changed() {
    start_end::group_start "Check if any Python files changed"
    local pattern_array=("${ANY_PY_FILES_CHANGED[@]}")
    show_changed_files

    if [[ $(count_changed_files) != "0" ]]; then
        image_build_needed="true"
    fi
    start_end::group_end
}


AIRFLOW_SOURCES_TRIGGERING_TESTS=(
    "^airflow"
    "^chart"
    "^tests"
    "^kubernetes_tests"
)
readonly AIRFLOW_SOURCES_TRIGGERING_TESTS

function check_if_tests_are_needed_at_all() {
    start_end::group_start "Check tests are needed"
    local pattern_array=("${AIRFLOW_SOURCES_TRIGGERING_TESTS[@]}")
    show_changed_files

    if [[ $(count_changed_files) == "0" ]]; then
        if [[ ${image_build_needed} == "true" ]]; then
            echo "No tests needed, Skipping tests but building images."
            set_output_skip_tests_but_build_images_and_exit
        else
            echo "None of the important files changed, Skipping tests"
            set_output_skip_all_tests_and_docs_and_exit
        fi
    else
        image_build_needed="true"
        tests_needed="true"
    fi
    start_end::group_end
}

function run_all_tests_if_environment_files_changed() {
    start_end::group_start "Check if everything should be run"
    local pattern_array=(
        "^.github/workflows/"
        "^Dockerfile"
        "^scripts"
        "^setup.py"
        "^setup.cfg"
    )
    show_changed_files

    if [[ $(count_changed_files) != "0" ]]; then
        echo "Important environment files changed. Running everything"
        set_outputs_run_everything_and_exit
    fi
    if [[ ${FULL_TESTS_NEEDED_LABEL} == "true" ]]; then
        echo "Full tests requested by label on PR. Running everything"
        set_outputs_run_everything_and_exit
    fi
    start_end::group_end
}

function get_count_all_files() {
    start_end::group_start "Count all airflow source files"
    local pattern_array=("${AIRFLOW_SOURCES_TRIGGERING_TESTS[@]}")
    show_changed_files
    COUNT_ALL_CHANGED_FILES=$(count_changed_files)
    echo "Files count: ${COUNT_ALL_CHANGED_FILES}"
    readonly COUNT_ALL_CHANGED_FILES
    start_end::group_end
}

function get_count_api_files() {
    start_end::group_start "Count API files"
    local pattern_array=(
        "^airflow/api"
        "^airflow/api_connexion"
        "^tests/api"
        "^tests/api_connexion"
    )
    show_changed_files
    COUNT_API_CHANGED_FILES=$(count_changed_files)
    echo "Files count: ${COUNT_API_CHANGED_FILES}"
    readonly COUNT_API_CHANGED_FILES
    start_end::group_end
}

function get_count_cli_files() {
    start_end::group_start "Count CLI files"
    local pattern_array=(
        "^airflow/cli"
        "^tests/cli"
    )
    show_changed_files
    COUNT_CLI_CHANGED_FILES=$(count_changed_files)
    echo "Files count: ${COUNT_CLI_CHANGED_FILES}"
    readonly COUNT_CLI_CHANGED_FILES
    start_end::group_end
}

function get_count_providers_files() {
    start_end::group_start "Count providers files"
    local pattern_array=(
        "^airflow/providers"
        "^tests/providers"
    )
    show_changed_files
    COUNT_PROVIDERS_CHANGED_FILES=$(count_changed_files)
    echo "Files count: ${COUNT_PROVIDERS_CHANGED_FILES}"
    readonly COUNT_PROVIDERS_CHANGED_FILES
    start_end::group_end
}

function get_count_www_files() {
    start_end::group_start "Count www files"
    local pattern_array=(
        "^airflow/www"
        "^tests/www"
    )
    show_changed_files
    COUNT_WWW_CHANGED_FILES=$(count_changed_files)
    echo "Files count: ${COUNT_WWW_CHANGED_FILES}"
    readonly COUNT_WWW_CHANGED_FILES
    start_end::group_end
}

function get_count_kubernetes_files() {
    start_end::group_start "Count kubernetes files"
    local pattern_array=(
        "^chart"
        "^kubernetes_tests"
        "^airflow/providers/cncf/kubernetes/"
        "^tests/providers/cncf/kubernetes/"
    )
    show_changed_files
    COUNT_KUBERNETES_CHANGED_FILES=$(count_changed_files)
    echo "Files count: ${COUNT_KUBERNETES_CHANGED_FILES}"
    readonly COUNT_KUBERNETES_CHANGED_FILES
    start_end::group_end
}

function calculate_test_types_to_run() {
    start_end::group_start "Count core/other files"
    COUNT_CORE_OTHER_CHANGED_FILES=$((COUNT_ALL_CHANGED_FILES - COUNT_WWW_CHANGED_FILES - COUNT_PROVIDERS_CHANGED_FILES - COUNT_CLI_CHANGED_FILES - COUNT_API_CHANGED_FILES - COUNT_KUBERNETES_CHANGED_FILES))

    readonly COUNT_CORE_OTHER_CHANGED_FILES
    echo
    echo "Files count: ${COUNT_CORE_OTHER_CHANGED_FILES}"
    echo
    if [[ ${COUNT_CORE_OTHER_CHANGED_FILES} -gt 0 ]]; then
        # Running all tests because some core or other files changed
        echo
        echo "Looks like ${COUNT_CORE_OTHER_CHANGED_FILES} files changed in the core/other area and"
        echo "We have to run all tests. This will take longer than usual"
        echo
        set_outputs_run_all_tests
    else
        if [[ ${COUNT_KUBERNETES_CHANGED_FILES} != "0" ]]; then
            kubernetes_tests_needed="true"
        fi
        tests_needed="true"
        SELECTED_TESTS=""
        if [[ ${COUNT_API_CHANGED_FILES} != "0" ]]; then
            echo
            echo "Adding API to selected files as ${COUNT_API_CHANGED_FILES} API files changed"
            echo
            SELECTED_TESTS="${SELECTED_TESTS} API"
        fi
        if [[ ${COUNT_CLI_CHANGED_FILES} != "0" ]]; then
            echo
            echo "Adding CLI and Kubernetes (they depend on CLI) to selected files as ${COUNT_CLI_CHANGED_FILES} CLI files changed"
            echo
            SELECTED_TESTS="${SELECTED_TESTS} CLI"
            kubernetes_tests_needed="true"
        fi
        if [[ ${COUNT_PROVIDERS_CHANGED_FILES} != "0" ]]; then
            echo
            echo "Adding Providers to selected files as ${COUNT_PROVIDERS_CHANGED_FILES} Provider files changed"
            echo
            SELECTED_TESTS="${SELECTED_TESTS} Providers"
        fi
        if [[ ${COUNT_WWW_CHANGED_FILES} != "0" ]]; then
            echo
            echo "Adding WWW to selected files as ${COUNT_WWW_CHANGED_FILES} WWW files changed"
            echo
            SELECTED_TESTS="${SELECTED_TESTS} WWW"
        fi
        initialization::ga_output test-types "Always Integration ${SELECTED_TESTS}"
    fi
    start_end::group_end
}



upgrade_to_newer_dependencies="false"

if (($# < 1)); then
    echo
    echo "No Commit SHA - running all tests (likely direct merge, or scheduled run)!"
    echo
    INCOMING_COMMIT_SHA=""
    readonly INCOMING_COMMIT_SHA
    # override FULL_TESTS_NEEDED_LABEL in main/scheduled run
    FULL_TESTS_NEEDED_LABEL="true"
    readonly FULL_TESTS_NEEDED_LABEL
    output_all_basic_variables
    set_outputs_run_everything_and_exit
else
    INCOMING_COMMIT_SHA="${1}"
    readonly INCOMING_COMMIT_SHA
    echo
    echo "Commit SHA passed: ${INCOMING_COMMIT_SHA}!"
    echo
    readonly FULL_TESTS_NEEDED_LABEL
fi

check_upgrade_to_newer_dependencies_needed

output_all_basic_variables

image_build_needed="false"
docs_build_needed="false"
tests_needed="false"
kubernetes_tests_needed="false"

get_changed_files
run_all_tests_if_environment_files_changed
check_if_setup_files_changed
check_if_any_py_files_changed
check_if_docs_should_be_generated
check_if_helm_tests_should_be_run
check_if_api_tests_should_be_run
check_if_api_codegen_should_be_run
check_if_javascript_security_scans_should_be_run
check_if_python_security_scans_should_be_run
check_if_tests_are_needed_at_all
get_count_all_files
get_count_api_files
get_count_cli_files
get_count_providers_files
get_count_www_files
get_count_kubernetes_files
calculate_test_types_to_run

set_image_build "${image_build_needed}"
if [[ ${image_build_needed} == "true" ]]; then
    set_basic_checks_only "false"
else
    set_basic_checks_only "true"
fi
set_docs_build "${docs_build_needed}"
run_tests "${tests_needed}"
run_kubernetes_tests "${kubernetes_tests_needed}"
set_upgrade_to_newer_dependencies "${upgrade_to_newer_dependencies}"
