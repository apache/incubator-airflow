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

export AIRFLOW_SOURCES="${AIRFLOW_SOURCES:=$( cd "$( dirname "${BASH_SOURCE[0]}" )/../../.." && pwd )}"
readonly AIRFLOW_SOURCES

# shellcheck source=scripts/ci/libraries/_all_libs.sh
. "${AIRFLOW_SOURCES}/scripts/ci/libraries/_all_libs.sh"

start_end::group_start "Initialization"
initialization::initialize_common_environment
start_end::group_end

start_end::group_start "Basic sanity checks"
sanity_checks::basic_sanity_checks
start_end::group_end

start_end::group_start "Starting script $(basename "$0")"
start_end::script_start
start_end::group_end

start_end::group_start "Determine docker caching strategy"
build_images::determine_docker_cache_strategy
start_end::group_end

start_end::group_start "Get environment for builds on CI"
initialization::get_environment_for_builds_on_ci
start_end::group_end

start_end::group_start "Get docker image names"
build_images::get_docker_image_names
start_end::group_end

start_end::group_start "Make constants read-only"
initialization::make_constants_read_only
start_end::group_end

# Work around occasional unexplained failure on CI. Clear file flags on
# STDOUT (which is connected to a tmp file by GitHub Runner).
# The one error I did see: BlockingIOError: [Errno 11] write could not complete without blocking
[[ "$CI" == "true" ]] && python3 -c "import fcntl; fcntl.fcntl(1, fcntl.F_SETFL, 0)"

traps::add_trap start_end::script_end EXIT HUP INT TERM
