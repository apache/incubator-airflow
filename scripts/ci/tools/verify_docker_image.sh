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
# shellcheck source=scripts/ci/libraries/_script_init.sh
. "$(dirname "${BASH_SOURCE[0]}")/../libraries/_script_init.sh"

usage() {
local cmdname
cmdname="$(basename -- "$0")"

cat << EOF
Usage: ${cmdname} <IMAGE_TYPE> <DOCKER_IMAGE>

Verify the user-specified docker image.

Image Type can be one of the two values: CI or PROD

EOF
}


if [[ "$#" -ne 2 ]]; then
    >&2 echo "You must provide two argument - image type [PROD/CI] and image name."
    usage
    exit 1
fi

IMAGE_TYPE="${1}"
IMAGE_NAME="${2}"

if ! docker image inspect "${IMAGE_NAME}" &>/dev/null; then
    >&2 echo "Image '${IMAGE_NAME}' doesn't exists in local registry."
    exit 1
fi

if [ "$(echo "${IMAGE_TYPE}" | tr '[:lower:]' '[:upper:]')" = "PROD" ]; then
    verify_image::verify_prod_image "${IMAGE_NAME}"
elif [ "$(echo "${IMAGE_TYPE}" | tr '[:lower:]' '[:upper:]')" = "CI" ]; then
    verify_image::verify_ci_image "${IMAGE_NAME}"
else
    >&2 echo "Unsupported image type. Supported values: PROD, CI"
    exit 1
fi
