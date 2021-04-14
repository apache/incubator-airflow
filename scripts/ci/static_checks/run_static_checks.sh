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

if [[ -f "${BUILD_CACHE_DIR}/.skip_tests" ]]; then
    echo
    echo "Skip tests"
    echo
    exit
fi

build_images::prepare_ci_build

build_images::rebuild_ci_image_if_needed

python -m pip install --user pre-commit \
  --constraint "https://raw.githubusercontent.com/${CONSTRAINTS_GITHUB_REPOSITORY}/${DEFAULT_CONSTRAINTS_BRANCH}/constraints-${PYTHON_MAJOR_MINOR_VERSION}.txt"

export PATH=~/.local/bin:${PATH}

if [[ $# == "0" ]]; then
    pre-commit run --all-files --show-diff-on-failure --color always
else
    for pre_commit_check in "${@}"
    do
        pre-commit run "${pre_commit_check}" --all-files --show-diff-on-failure --color always
    done
fi
