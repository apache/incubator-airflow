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
# shellcheck source=scripts/ci/libraries/_initialization.sh
. "$(dirname "${BASH_SOURCE[0]}")/../libraries/_initialization.sh"

initialization::set_output_color_variables

job_name=$1
file=$2

set +e

if [[ ${file} == *".sh" ]]; then
    "${file}"
    res=$?
elif [[ ${file} == *"Dockerfile" ]]; then
    cd "$(dirname "${file}")" || exit 1
    docker build . --tag "${job_name}"
    res=$?
    docker rmi --force "${job_name}"
else
    echo "Bad file ${file}. Should be either a Dockerfile or script"
    exit 1
fi
# Print status to status file
echo "${res}" >"${PARALLEL_JOB_STATUS}"

echo
# print status to log
if [[ ${res} == "0" ]]; then
    echo "${COLOR_GREEN}Extend PROD image test ${job_name} succeeded${COLOR_RESET}"
else
    echo "${COLOR_RED}Extend PROD image test ${job_name} failed${COLOR_RESET}"
fi
echo
