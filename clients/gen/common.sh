#!/bin/bash

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

OPENAPI_GENERATOR_CLI_VER=5.1.0
readonly OPENAPI_GENERATOR_CLI_VER

GIT_USER=${GIT_USER:-apache}
readonly GIT_USER

function validate_input {
    if [ "$#" -ne 2 ]; then
        echo "USAGE: $0 SPEC_PATH OUTPUT_DIR"
        exit 1
    fi

    if ! [ -x "$(command -v realpath)" ]; then
      echo 'Error: realpath is not installed.' >&2
      exit 1
    fi

    SPEC_PATH=$(realpath "$1")
    readonly SPEC_PATH

    if [ ! -d "$2" ]; then
        echo "$2 is not a valid directory or does not exist."
        exit 1
    fi

    OUTPUT_DIR=$(realpath "$2")
    readonly OUTPUT_DIR

    # cleanup the existing generated code, otherwise generator would skip them
    for dir in "${CLEANUP_DIRS[@]}"
    do
        local dirToClean="${OUTPUT_DIR}/${dir}"
        echo "Cleaning up ${dirToClean}"
        rm -rf "${dirToClean:?}"
    done

    # create openapi ignore file to keep generated code clean
    cat <<EOF > "${OUTPUT_DIR}/.openapi-generator-ignore"
.travis.yml
git_push.sh
.gitlab-ci.yml
requirements.txt
setup.cfg
setup.py
test-requirements.txt
tox.ini
EOF
}

function run_pre_commit {
    cd "${OUTPUT_DIR}"

    # prepend license headers
    pre-commit run --all-files || true
}

function gen_client {
    lang=$1
    shift
    set -ex
    IFS=','
    docker run --rm \
        -u "$(id -u):$(id -g)" \
        -v "${SPEC_PATH}:/spec" \
        -v "${OUTPUT_DIR}:/output" \
        openapitools/openapi-generator-cli:v${OPENAPI_GENERATOR_CLI_VER} \
        generate \
        --input-spec "/spec" \
        --generator-name "${lang}" \
        --git-user-id "${GIT_USER}" \
        --output "/output" "$@"
}
