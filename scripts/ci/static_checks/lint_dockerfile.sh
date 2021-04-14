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

function run_docker_lint() {
    IMAGE_NAME="hadolint/hadolint:v1.22.1-8-gf0ef28b-alpine"
    if [[ "${#@}" == "0" ]]; then
        echo
        echo "Running docker lint for all Dockerfiles"
        echo
        # shellcheck disable=SC2046
        docker run \
            -v "$(pwd):/root" \
            -w "/root" \
            --rm \
           "${IMAGE_NAME}" "/bin/hadolint" $(git ls-files| grep 'Dockerfile')
        echo
        echo "Hadolint completed with no errors"
        echo
    else
        echo
        echo "Running docker lint for $*"
        echo
        docker run \
            -v "$(pwd):/root" \
            -w "/root" \
            --rm \
            "${IMAGE_NAME}" "/bin/hadolint" "${@}"
        echo
        echo "Hadolint completed with no errors"
        echo
    fi
}

run_docker_lint "$@"
