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

CLIENTS_GEN_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
readonly CLIENTS_GEN_DIR

CLEANUP_DIRS=(api docs)
readonly CLEANUP_DIRS

# shellcheck source=./clients/gen/common.sh
source "${CLIENTS_GEN_DIR}/common.sh"

VERSION=1.1.0
readonly VERSION

go_config=(
    "packageVersion=${VERSION}"
    "enumClassPrefix=true"
)

validate_input "$@"

gen_client go \
    --package-name airflow \
    --git-repo-id airflow-client-go/airflow \
    --additional-properties "${go_config[*]}"

run_pre_commit
