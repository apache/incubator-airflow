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
if [[ $1 == "" ]]; then
  >&2 echo "Requires Kubernetes_version as first parameter"
  exit 1
fi
export KUBERNETES_VERSION=$1
shift


if [[ $1 == "" ]]; then
  >&2 echo "Requires Python Major/Minor version as second parameter"
  exit 1
fi
export PYTHON_MAJOR_MINOR_VERSION=$1
shift

# Requires PARALLEL_JOB_STATUS

if [[ -z "${PARALLEL_JOB_STATUS=}" ]]; then
    echo "Needs PARALLEL_JOB_STATUS to be set"
    exit 1
fi

echo
echo "KUBERNETES_VERSION:         ${KUBERNETES_VERSION}"
echo "PYTHON_MAJOR_MINOR_VERSION: ${PYTHON_MAJOR_MINOR_VERSION}"
echo

# shellcheck source=scripts/ci/libraries/_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/../libraries/_script_init.sh"

kind::get_kind_cluster_name
trap 'echo $? > "${PARALLEL_JOB_STATUS}"; kind::perform_kind_cluster_operation "stop"' EXIT HUP INT TERM

"$( dirname "${BASH_SOURCE[0]}" )/ci_setup_cluster_and_deploy_airflow_to_kubernetes.sh"

export CLUSTER_FORWARDED_PORT="${FORWARDED_PORT_NUMBER}"
"$( dirname "${BASH_SOURCE[0]}" )/ci_run_kubernetes_tests.sh"
