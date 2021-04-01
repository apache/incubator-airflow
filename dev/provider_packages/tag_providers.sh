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
AIRFLOW_SOURCES="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../" && pwd)"

for file in "${AIRFLOW_SOURCES}/dist/"*.whl
do
   if [[ ${file} =~ .*airflow_providers_(.*)-(.*)-py3.* ]]; then
        provider="providers-${BASH_REMATCH[1]}"
        tag="${provider//_/-}/${BASH_REMATCH[2]}"
        git tag "${tag}"
        git push apache "${tag}"
   fi
done
