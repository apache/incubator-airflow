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
set -euo pipefail

# Parameters:
#
# GITHUB_REPO - repository to delete the artifacts
# GITHUB_USER - your personal user name
# GITHUB_TOKEN - your personal token with `repo` scope
#
GITHUB_REPO=https://api.github.com/repos/apache/airflow
readonly GITHUB_REPO

if [[ -z ${GITHUB_USER} ]]; then
    echo 2>&1
    echo 2>&1 "Set GITHUB_USER variable to your user"
    echo 2>&1
    exit 1
fi
readonly GITHUB_USER

if [[ -z ${GITHUB_TOKEN} ]]; then
    echo 2>&1
    echo 2>&1 "Set GITHUB_TOKEN variable to a token with 'repo' scope"
    echo 2>&1
    exit 2
fi
readonly GITHUB_TOKEN

function github_api_call() {
    curl --connect-timeout 60 --max-time 60 \
        --silent --location --user "${GITHUB_USER}:${GITHUB_TOKEN}" "$@"
}

# A temporary file which receives HTTP response headers.
TEMPFILE=$(mktemp)
readonly TEMPFILE

function loop_through_artifacts_and_delete() {

    # Process all artifacts on this repository, loop on returned "pages".
    artifact_url=${GITHUB_REPO}/actions/artifacts

    while [[ -n "${artifact_url}" ]]; do
        # Get current page, get response headers in a temporary file.
        json=$(github_api_call --dump-header "${TEMPFILE}" "$artifact_url")

        # Get artifact_url of next page. Will be empty if we are at the last page.
        artifact_url=$(grep '^Link:' "$TEMPFILE" | tr ',' '\n' | \
            grep 'rel="next"' | head -1 | sed -e 's/.*<//' -e 's/>.*//')
        rm -f "${TEMPFILE}"

        # Number of artifacts on this page:
        count=$(($(jq <<<"${json}" -r '.artifacts | length')))

        # Loop on all artifacts on this page.
        for ((i = 0; "${i}" < "${count}"; i++)); do
            # Get the name of artifact and count instances of this name
            name=$(jq <<<"${json}" -r ".artifacts[$i].name?")
            id=$(jq <<<"${json}" -r ".artifacts[$i].id?")
            size=$(($(jq <<<"${json}" -r ".artifacts[$i].size_in_bytes?")))
            printf "Deleting '%s': [%s] : %'d bytes\n" "${name}" "${id}" "${size}"
            github_api_call -X DELETE "${GITHUB_REPO}/actions/artifacts/${id}"
            sleep 1 # There is a GitHub API limit of 5000 calls/hr. This is to limit the API calls below that
        done
    done
}

loop_through_artifacts_and_delete
