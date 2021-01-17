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

if [[ ${WAIT_FOR_IMAGE} != "true" ]]; then
    # shellcheck source=scripts/in_container/_in_container_script_init.sh
    . "$( dirname "${BASH_SOURCE[0]}" )/../libraries/_script_init.sh"
    echo
    echo "Not waiting for all CI images to appear as they are built locally in this build"
    echo
    push_pull_remove_images::determine_github_registry
    exit
fi

echo
echo "Waiting for all CI images to appear: ${CURRENT_PYTHON_MAJOR_MINOR_VERSIONS_AS_STRING}"
echo

for PYTHON_MAJOR_MINOR_VERSION in ${CURRENT_PYTHON_MAJOR_MINOR_VERSIONS_AS_STRING}
do
    export PYTHON_MAJOR_MINOR_VERSION
    "$( dirname "${BASH_SOURCE[0]}" )/ci_wait_for_ci_image.sh"
done
