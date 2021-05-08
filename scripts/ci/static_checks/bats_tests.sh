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

#######################################################################################################
# Runs tests for bash scripts in a docker container. This argument is the entrypoint for the bats-tests
# pre-commit hook where it runs all the bats tests (excluding in container tests).
# It runs All tests if no arguments specified
# If some arguments specified it runs:
#  * The .bats tests directly if specified as parameters
#  * The .bats tests corresponding (using the argument location) to the .sh files passed as parameters
#  * All .bats files found in the directory if directory specified
#
#  All of the above can be mixed in one set of command line arguments
########################################################################################################
function run_bats_tests() {
    local bats_arguments=()
    for argument in "${@}"
    do
        if [[ ${argument} =~ \.sh$ ]];
        then
            # Find tests corresponding to the modified scripts
            script_file_path=${argument#scripts/}
            bats_script="tests/bats/${script_file_path%.sh}.bats"
            if [[ -f ${bats_script} ]]; then
                bats_arguments+=( "${bats_script}" )
            fi
        elif [[ ${argument} =~ .*\.bats$ ]];
        then
            # or if the bats files were modified themselves - run them directly
            bats_arguments+=( "$argument" )
        elif [[ -d ${argument} ]]; then
            # add it if it is a directory
            bats_arguments+=( "$argument" )
        fi

    done
    # deduplicate
    FS=" " read -r -a bats_arguments <<< "$(tr ' ' '\n' <<< "${bats_arguments[@]}" | sort -u | tr '\n' ' ' )"
    if [[ ${#@} == "0" ]]; then
        # Run al tests
        docker run --workdir /airflow -v "$(pwd):/airflow" --rm \
            apache/airflow:bats-2021.04.28-1.2.1 --tap /airflow/tests/bats/
    elif [[ ${#bats_arguments} == "0" ]]; then
        # Skip running anything if all filtered out
        true
    else
        # Run selected tests
        docker run --workdir /airflow -v "$(pwd):/airflow" --rm \
            apache/airflow:bats-2021.04.28-1.2.1 --tap "${bats_arguments[@]}"
    fi
}

run_bats_tests "$@"
