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


function docker_engine_resources::print_overall_stats() {
    echo
    echo "Overall resource statistics"
    echo
    docker stats --all --no-stream --no-trunc
    docker run --rm --entrypoint /bin/bash "${AIRFLOW_CI_IMAGE}" -c "free -h"
    df --human || true
}


function docker_engine_resources::get_available_memory_in_docker() {
    MEMORY_AVAILABLE_FOR_DOCKER=$(docker run --rm --entrypoint /bin/bash debian:buster-slim -c \
        'echo $(($(getconf _PHYS_PAGES) * $(getconf PAGE_SIZE) / (1024 * 1024)))')
    echo "${COLOR_BLUE}Memory available for Docker${COLOR_RESET}: $(numfmt --to iec $((MEMORY_AVAILABLE_FOR_DOCKER * 1024 * 1024)))"
    export MEMORY_AVAILABLE_FOR_DOCKER
}

function docker_engine_resources::get_available_cpus_in_docker() {
    CPUS_AVAILABLE_FOR_DOCKER=$(docker run --rm --entrypoint /bin/bash debian:buster-slim -c \
        'grep -cE "cpu[0-9]+" </proc/stat')
    echo "${COLOR_BLUE}CPUS available for Docker${COLOR_RESET}: ${CPUS_AVAILABLE_FOR_DOCKER}"
    export CPUS_AVAILABLE_FOR_DOCKER
}

function docker_engine_resources::get_available_disk_space_in_docker() {
    DISK_SPACE_AVAILABLE_FOR_DOCKER=$(docker run --rm --entrypoint /bin/bash debian:buster-slim -c \
        'df  / | tail -1 | awk '\''{print $4}'\')
    echo "${COLOR_BLUE}Disk space available for Docker${COLOR_RESET}: $(numfmt --to iec $((DISK_SPACE_AVAILABLE_FOR_DOCKER * 1024)))"
    export DISK_SPACE_AVAILABLE_FOR_DOCKER
}

function docker_engine_resources::check_enough_resources() {
    local successful_resource_check="true"
    if (( MEMORY_AVAILABLE_FOR_DOCKER < 4000 )) ; then
        successful_resource_check="false"
        echo
        echo "${COLOR_RED}WARNING! Not enough memory to use breeze. At least 4GB memory is required for Docker engine to run Breeze${COLOR_RESET}"
    fi

    if (( CPUS_AVAILABLE_FOR_DOCKER < 2 )) ; then
        successful_resource_check="false"
        echo
        echo "${COLOR_RED}WARNING! Not enough CPUs to use breeze. At least 2 CPUS are required for Docker engine to run Breeze.${COLOR_RESET}"
    fi

    if (( DISK_SPACE_AVAILABLE_FOR_DOCKER < 40000000 )) ; then
        successful_resource_check="false"
        echo
        echo "${COLOR_RED}WARNING! Not enough disk space to use breeze. At least 40GB are required for Docker engine to run Breeze.${COLOR_RESET}"
    fi

    if [[ ${successful_resource_check} != "true" ]];then
        echo
        echo "${COLOR_RED}Please check https://github.com/apache/airflow/blob/master/BREEZE.rst#resources-required for details${COLOR_RESET}"
        echo
    fi
}

function docker_engine_resources::check_all_resources() {
    docker_engine_resources::get_available_memory_in_docker
    docker_engine_resources::get_available_cpus_in_docker
    docker_engine_resources::get_available_disk_space_in_docker
    docker_engine_resources::check_enough_resources
}
