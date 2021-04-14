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

LIBRARIES_DIR=$(dirname "${BASH_SOURCE[0]}")
export LIBRARIES_DIR
readonly LIBRARIES_DIR

SCRIPTS_CI_DIR=$(cd "${LIBRARIES_DIR}/.." && pwd)
export SCRIPTS_CI_DIR
readonly SCRIPTS_CI_DIR

# shellcheck source=scripts/ci/libraries/_traps.sh
. "${LIBRARIES_DIR}"/_traps.sh
# shellcheck source=scripts/ci/libraries/_initialization.sh
. "${LIBRARIES_DIR}"/_initialization.sh
# shellcheck source=scripts/ci/libraries/_parallel.sh
. "${LIBRARIES_DIR}"/_parallel.sh
# shellcheck source=scripts/ci/libraries/_docker_engine_resources.sh
. "${LIBRARIES_DIR}"/_docker_engine_resources.sh
# shellcheck source=scripts/ci/libraries/_repeats.sh
. "${LIBRARIES_DIR}"/_repeats.sh
# shellcheck source=scripts/ci/libraries/_sanity_checks.sh
. "${LIBRARIES_DIR}"/_sanity_checks.sh
# shellcheck source=scripts/ci/libraries/_build_airflow_packages.sh
. "${LIBRARIES_DIR}"/_build_airflow_packages.sh
# shellcheck source=scripts/ci/libraries/_build_images.sh
. "${LIBRARIES_DIR}"/_build_images.sh
# shellcheck source=scripts/ci/libraries/_kind.sh
. "${LIBRARIES_DIR}"/_kind.sh
# shellcheck source=scripts/ci/libraries/_local_mounts.sh
. "${LIBRARIES_DIR}"/_local_mounts.sh
# shellcheck source=scripts/ci/libraries/_md5sum.sh
. "${LIBRARIES_DIR}"/_md5sum.sh
# shellcheck source=scripts/ci/libraries/_parameters.sh
. "${LIBRARIES_DIR}"/_parameters.sh
# shellcheck source=scripts/ci/libraries/_permissions.sh
. "${LIBRARIES_DIR}"/_permissions.sh
# shellcheck source=scripts/ci/libraries/_push_pull_remove_images.sh
. "${LIBRARIES_DIR}"/_push_pull_remove_images.sh
# shellcheck source=scripts/ci/libraries/_pylint.sh
. "${LIBRARIES_DIR}"/_pylint.sh
# shellcheck source=scripts/ci/libraries/_runs.sh
. "${LIBRARIES_DIR}"/_runs.sh
# shellcheck source=scripts/ci/libraries/_spinner.sh
. "${LIBRARIES_DIR}"/_spinner.sh
# shellcheck source=scripts/ci/libraries/_start_end.sh
. "${LIBRARIES_DIR}"/_start_end.sh
# shellcheck source=scripts/ci/libraries/_verbosity.sh
. "${LIBRARIES_DIR}"/_verbosity.sh
# shellcheck source=scripts/ci/libraries/_verify_image.sh
. "${LIBRARIES_DIR}"/_verify_image.sh
