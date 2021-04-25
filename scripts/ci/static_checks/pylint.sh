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

function run_pylint() {
    if [[ "${#@}" == "0" ]]; then
       docker run "${EXTRA_DOCKER_FLAGS[@]}" \
            --entrypoint "/usr/local/bin/dumb-init"  \
            "${AIRFLOW_CI_IMAGE}" \
            "--" "/opt/airflow/scripts/in_container/run_pylint.sh"
    else
        docker run "${EXTRA_DOCKER_FLAGS[@]}" \
            --entrypoint "/usr/local/bin/dumb-init" \
            "${AIRFLOW_CI_IMAGE}" \
            "--" "/opt/airflow/scripts/in_container/run_pylint.sh" "${@}"
    fi
}

build_images::prepare_ci_build

build_images::rebuild_ci_image_if_needed

# Bug: Pylint only looks at PYLINTRC if it can't find a file in the _default_
# locations, meaning we can't use this env var to over-ride it
args=()

if [[ -n "${PYLINTRC:-}" ]]; then
  args=(--rcfile "${PYLINTRC}")
fi

if [[ "${#@}" != "0" ]]; then
    pylint::filter_out_files_from_pylint_todo_list "$@"

    if [[ "${#FILTERED_FILES[@]}" == "0" ]]; then
        echo "Filtered out all files. Skipping pylint."
        exit 0
    fi
    args+=("${FILTERED_FILES[@]}")
fi
run_pylint "${args[@]}"
