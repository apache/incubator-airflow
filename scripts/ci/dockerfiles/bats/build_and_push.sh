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
DOCKERHUB_USER=${DOCKERHUB_USER:="apache"}
DOCKERHUB_REPO=${DOCKERHUB_REPO:="airflow"}
readonly DOCKERHUB_USER
readonly DOCKERHUB_REPO

BATS_VERSION="1.2.1"
BATS_ASSERT_VERSION="2.0.0"
BATS_SUPPORT_VERSION="0.3.0"
BATS_FILE_VERSION="0.3.0"
readonly BATS_VERSION
readonly BATS_ASSERT_VERSION
readonly BATS_SUPPORT_VERSION
readonly BATS_FILE_VERSION

AIRFLOW_BATS_VERSION="2021.04.28"
readonly AIRFLOW_BATS_VERSION

COMMIT_SHA=$(git rev-parse HEAD)
readonly COMMIT_SHA

cd "$( dirname "${BASH_SOURCE[0]}" )" || exit 1

TAG="${DOCKERHUB_USER}/${DOCKERHUB_REPO}:bats-${AIRFLOW_BATS_VERSION}-${BATS_VERSION}"
readonly TAG

docker build . \
    --pull \
    --build-arg "BATS_VERSION=${BATS_VERSION}" \
    --build-arg "BATS_SUPPORT_VERSION=${BATS_SUPPORT_VERSION}" \
    --build-arg "BATS_FILE_VERSION=${BATS_FILE_VERSION}" \
    --build-arg "BATS_ASSERT_VERSION=${BATS_ASSERT_VERSION}" \
    --build-arg "COMMIT_SHA=${COMMIT_SHA}" \
    --tag "${TAG}"

docker push "${TAG}"
