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

CLEANUP_DIRS=(client docs test README.md)
readonly CLEANUP_DIRS

# shellcheck source=./clients/gen/common.sh
source "${CLIENTS_GEN_DIR}/common.sh"

VERSION=1.0.0
readonly VERSION

python_config=(
    "packageVersion=${VERSION}"
)

validate_input "$@"

gen_client python \
    --package-name client \
    --git-repo-id airflow-client-python \
    --additional-properties "${python_config[*]}"

echo "--- Patching generated code..."

# Post-processing of the generated Python wrapper.

touch "${OUTPUT_DIR}/__init__.py"
find "${OUTPUT_DIR}/test" -type f -name \*.py -exec sed -i '' -e 's/client/airflow_client.client/g' {} +
find "${OUTPUT_DIR}" -type f -a -name \*.md -exec sed -i '' -e 's/# client/# Apache Airflow Python Client/g' {} +
find "${OUTPUT_DIR}" -type f -a -name \*.md -exec sed -i '' -e 's/import client/import airflow_client.client/g' {} +
find "${OUTPUT_DIR}" -type f -a -name \*.md -exec sed -i '' -e 's/from client/from airflow_client.client/g' {} +
find "${OUTPUT_DIR}" -type f -a -name \*.md -exec sed -i '' -e 's/getattr(client\.models/getattr(airflow_client.client.models/g' {} +

# fix imports
find "${OUTPUT_DIR}/client/" -type f -name \*.py -exec sed -i '' -e 's/import client\./import airflow_client.client./g' {} +
find "${OUTPUT_DIR}/client/" -type f -name \*.py -exec sed -i '' -e 's/from client/from airflow_client.client/g' {} +
find "${OUTPUT_DIR}/client/" -type f -name \*.py -exec sed -i '' -e 's/getattr(client\.models/getattr(airflow_client.client.models/g' {} +

run_pre_commit
