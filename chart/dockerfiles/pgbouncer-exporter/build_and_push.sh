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
readonly DOCKERHUB_USER
DOCKERHUB_REPO=${DOCKERHUB_REPO:="airflow"}
readonly DOCKERHUB_REPO

PGBOUNCER_EXPORTER_VERSION="0.5.0"
readonly PGBOUNCER_EXPORTER_VERSION

AIRFLOW_PGBOUNCER_EXPORTER_VERSION="2021.04.28"
readonly AIRFLOW_PGBOUNCER_EXPORTER_VERSION

EXPECTED_GO_VERSION="1.15.1"
readonly EXPECTED_GO_VERSION

COMMIT_SHA=$(git rev-parse HEAD)
readonly COMMIT_SHA

cd "$( dirname "${BASH_SOURCE[0]}" )" || exit 1

PGBOUNCER_EXPORTER_DIR="$(pwd)/pgbouncer_exporter-${PGBOUNCER_EXPORTER_VERSION}"
readonly  PGBOUNCER_EXPORTER_DIR

# Needs to be set for alpine images to run net package of GO
rm -rf "$(pwd)/pgbouncer_exporter*"
mkdir -pv "${PGBOUNCER_EXPORTER_DIR}"

curl -L "https://github.com/jbub/pgbouncer_exporter/archive/v${PGBOUNCER_EXPORTER_VERSION}.tar.gz" | tar -zx

cd "${PGBOUNCER_EXPORTER_DIR}"

docker run --rm -e CGO_ENABLED=0 -v "$PWD":/usr/src/myapp -w /usr/src/myapp golang:${EXPECTED_GO_VERSION} go build -v

tag="${DOCKERHUB_USER}/${DOCKERHUB_REPO}:airflow-pgbouncer-exporter-${AIRFLOW_PGBOUNCER_EXPORTER_VERSION}-${PGBOUNCER_EXPORTER_VERSION}"

cd ..
docker build . \
    --pull \
    --build-arg "PGBOUNCER_EXPORTER_VERSION=${PGBOUNCER_EXPORTER_VERSION}" \
    --build-arg "AIRFLOW_PGBOUNCER_EXPORTER_VERSION=${AIRFLOW_PGBOUNCER_EXPORTER_VERSION}"\
    --build-arg "COMMIT_SHA=${COMMIT_SHA}" \
    --build-arg "GO_VERSION=${EXPECTED_GO_VERSION}" \
    --tag "${tag}"

docker push "${tag}"
