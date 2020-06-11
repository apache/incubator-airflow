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

if [ -z "${AIRFLOW_CI_IMAGE}" ]; then
    >&2 echo "Missing environment variable AIRFLOW_CI_IMAGE"
    exit 1
fi
if [ -z "${HOST_AIRFLOW_SOURCES}" ]; then
    >&2 echo "Missing environment variable HOST_AIRFLOW_SOURCES"
    exit 1
fi
if [ -z "${HOST_USER_ID}" ]; then
    >&2 echo "Missing environment variable HOST_USER_ID"
    exit 1
fi
if [ -z "${HOST_GROUP_ID}" ]; then
    >&2 echo "Missing environment variable HOST_GROUP_ID"
    exit 1
fi

SCRIPT_NAME="$( basename "${BASH_SOURCE[0]}")"
# Drop "-update" suffix, if exists
TOOL_NAME="$(echo "${SCRIPT_NAME}" | cut -d "-" -f 1)"

SUPPORTED_TOOL_NAMES=("aws" "az" "gcloud" "bq" "gsutil" "terraform" "java")

SCRIPT_PATH=$(readlink -e "${BASH_SOURCE[0]}")
if [ -L "$SCRIPT_PATH" ]
then
    # Direct execution - return installation script
    SCRIPT_PATH=$(readlink -e "${BASH_SOURCE[0]}")
    >&2 echo "# CLI tool wrappers"
    >&2 echo "#"
    >&2 echo "# To install, run the following command:"
    >&2 echo "#     source <(bash ${SCRIPT_PATH@Q})"
    >&2 echo "#"
    >&2 echo ""
    # Print installation script
    for NAME in "${SUPPORTED_TOOL_NAMES[@]}"
    do
        echo "ln -s ${SCRIPT_PATH@Q} /usr/bin/${NAME}"
        echo "ln -s ${SCRIPT_PATH@Q} /usr/bin/${NAME}-update"
        echo "chmod +x /usr/bin/${NAME} /usr/bin/${NAME}-update"
    done
    exit 0
fi

CONTAINER_ID="$(head -n 1 < /proc/self/cgroup | cut -d ":" -f 3 | cut -d "/" -f 3)"

COMMON_DOCKER_ARGS=(
    # Share namespaces between all containers.
    # This way we are even closer to run those tools like if they were installed.
    # More information: https://docs.docker.com/get-started/overview/#namespaces
    --ipc "container:${CONTAINER_ID}"
    --pid "container:${CONTAINER_ID}"
    --network "container:${CONTAINER_ID}"
    -v "${HOST_AIRFLOW_SOURCES}/tmp:/tmp"
    -v "${HOST_AIRFLOW_SOURCES}/files:/files"
    -v "${HOST_AIRFLOW_SOURCES}:/opt/airflow"
    --env-file <(env)
    -w "${PWD}"
)

AWS_CREDENTIALS_DOCKER_ARGS=(-v "${HOST_HOME}/.aws:/root/.aws")
AZURE_CREDENTIALS_DOCKER_ARGS=(-v "${HOST_HOME}/.azure:/root/.azure")
GOOGLE_CREDENTIALS_DOCKER_ARGS=(-v "${HOST_HOME}/.config/gcloud:/root/.config/gcloud")

DIRECTORIES_TO_FIX=('/tmp/' '/files/')

COMMAND=("${@}")

# Configure selected tool
case "${TOOL_NAME}" in
    aws )
        COMMON_DOCKER_ARGS+=("${AWS_CREDENTIALS_DOCKER_ARGS[@]}")
        DIRECTORIES_TO_FIX+=("/root/.aws")
        IMAGE_NAME="amazon/aws-cli:latest"
        ;;
    az )
        COMMON_DOCKER_ARGS+=("${AZURE_CREDENTIALS_DOCKER_ARGS[@]}")
        DIRECTORIES_TO_FIX+=("/root/.azure")
        IMAGE_NAME="mcr.microsoft.com/azure-cli:latest"
        ;;
    gcloud | bq | gsutil )
        COMMON_DOCKER_ARGS+=("${GOOGLE_CREDENTIALS_DOCKER_ARGS[@]}")
        DIRECTORIES_TO_FIX+=("/root/.config/gcloud")
        IMAGE_NAME="gcr.io/google.com/cloudsdktool/cloud-sdk:latest"
        COMMAND=("$TOOL_NAME" "${@}")
        ;;
    terraform )
        COMMON_DOCKER_ARGS+=(
            "${GOOGLE_CREDENTIALS_DOCKER_ARGS[@]}"
            "${AZURE_CREDENTIALS_DOCKER_ARGS[@]}"
            "${AWS_CREDENTIALS_DOCKER_ARGS[@]}"
        )
        DIRECTORIES_TO_FIX+=(
            "/root/.config/gcloud"
            "/root/.aws"
            "/root/.azure"
        )
        IMAGE_NAME="hashicorp/terraform:latest"
        ;;
    java )
        # TODO: Should we add other credentials?
        COMMON_DOCKER_ARGS+=("${GOOGLE_CREDENTIALS_DOCKER_ARGS[@]}")
        DIRECTORIES_TO_FIX+=("/root/.config/gcloud")
        IMAGE_NAME="openjdk:8-jre-slim"
        COMMAND=("/usr/local/openjdk-8/bin/java" "${@}")
        ;;
    * )
        >&2 echo "Unsupported tool name: ${TOOL_NAME}"
        exit 1
        ;;
esac

# Run update, if requested
if [[ "${SCRIPT_NAME}" == *-update ]]; then
    docker pull "${IMAGE_NAME}"
    exit $?
fi

# Otherwise, run tool
TOOL_DOCKER_ARGS=(--rm --interactive)
TOOL_DOCKER_ARGS+=("${COMMON_DOCKER_ARGS[@]}")

if [ -t 0 ] ; then
    TOOL_DOCKER_ARGS+=(
        --tty
    )
fi

docker run "${TOOL_DOCKER_ARGS[@]}" "${IMAGE_NAME}" "${COMMAND[@]}"

RES=$?

# Set file permissions to the host user
if [[ "${HOST_OS}" == "Linux" ]]; then
    FIX_DOCKER_ARGS=(--rm)
    FIX_DOCKER_ARGS+=("${COMMON_DOCKER_ARGS[@]}")
    FIX_COMMAND=(bash -c
        "find ${DIRECTORIES_TO_FIX[@]@Q} -user root -print0 | xargs --null chown '${HOST_USER_ID}.${HOST_GROUP_ID}' --no-dereference")

    docker run "${FIX_DOCKER_ARGS[@]}" "${AIRFLOW_CI_IMAGE}" "${FIX_COMMAND[@]}" >/dev/null 2>&1
fi

exit ${RES}
