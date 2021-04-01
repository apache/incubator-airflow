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
# Script to check licences for all code. Can be started from any working directory
# shellcheck source=scripts/in_container/_in_container_script_init.sh
EXIT_CODE=0

DISABLED_INTEGRATIONS=""

# We want to avoid misleading messages and perform only forward lookup of the service IP address.
# Netcat when run without -n performs both forward and reverse lookup and fails if the reverse
# lookup name does not match the original name even if the host is reachable via IP. This happens
# randomly with docker-compose in GitHub Actions.
# Since we are not using reverse lookup elsewhere, we can perform forward lookup in python
# And use the IP in NC and add '-n' switch to disable any DNS use.
# Even if this message might be harmless, it might hide the real reason for the problem
# Which is the long time needed to start some services, seeing this message might be totally misleading
# when you try to analyse the problem, that's why it's best to avoid it,
function run_nc() {
    local host=${1}
    local port=${2}
    local ip
    ip=$(python -c "import socket; print(socket.gethostbyname('${host}'))")

    nc -zvvn "${ip}" "${port}"
}

function check_service {
    LABEL=$1
    CALL=$2
    MAX_CHECK=${3:=1}

    echo -n "${LABEL}: "
    while true
    do
        set +e
        LAST_CHECK_RESULT=$(eval "${CALL}" 2>&1)
        RES=$?
        set -e
        if [[ ${RES} == 0 ]]; then
            echo  "${COLOR_GREEN}OK.  ${COLOR_RESET}"
            break
        else
            echo -n "."
            MAX_CHECK=$((MAX_CHECK-1))
        fi
        if [[ ${MAX_CHECK} == 0 ]]; then
            echo "${COLOR_RED}ERROR: Maximum number of retries while checking service. Exiting ${COLOR_RESET}"
            break
        else
            sleep 1
        fi
    done
    if [[ ${RES} != 0 ]]; then
        echo "Service could not be started!"
        echo
        echo "$ ${CALL}"
        echo "${LAST_CHECK_RESULT}"
        echo
        EXIT_CODE=${RES}
    fi
}

function check_integration {
    INTEGRATION_LABEL=$1
    INTEGRATION_NAME=$2
    CALL=$3
    MAX_CHECK=${4:=1}

    ENV_VAR_NAME=INTEGRATION_${INTEGRATION_NAME^^}
    if [[ ${!ENV_VAR_NAME:=} != "true" ]]; then
        if [[ ! ${DISABLED_INTEGRATIONS} == *" ${INTEGRATION_NAME}"* ]]; then
            DISABLED_INTEGRATIONS="${DISABLED_INTEGRATIONS} ${INTEGRATION_NAME}"
        fi
        return
    fi
    check_service "${INTEGRATION_LABEL}" "${CALL}" "${MAX_CHECK}"
}

function check_db_backend {
    MAX_CHECK=${1:=1}

    if [[ ${BACKEND} == "postgres" ]]; then
        check_service "PostgreSQL" "run_nc postgres 5432" "${MAX_CHECK}"
    elif [[ ${BACKEND} == "mysql" ]]; then
        check_service "MySQL" "run_nc mysql 3306" "${MAX_CHECK}"
    elif [[ ${BACKEND} == "sqlite" ]]; then
        return
    else
        echo "Unknown backend. Supported values: [postgres,mysql,sqlite]. Current value: [${BACKEND}]"
        exit 1
    fi
}

function resetdb_if_requested() {
    if [[ ${DB_RESET:="false"} == "true" ]]; then
        echo
        echo "Resetting the DB"
        echo
        if [[ ${RUN_AIRFLOW_1_10} == "true" ]]; then
            airflow resetdb -y
        else
            airflow db reset -y
        fi
        echo
        echo "Database has been reset"
        echo
    fi
    return $?
}

function startairflow_if_requested() {
    if [[ ${START_AIRFLOW:="false"} == "true" ]]; then
        echo
        echo "Starting Airflow"
        echo
        export AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS=${LOAD_DEFAULT_CONNECTIONS}
        export AIRFLOW__CORE__LOAD_EXAMPLES=${LOAD_EXAMPLES}

        . "$( dirname "${BASH_SOURCE[0]}" )/configure_environment.sh"

        # initialize db and create the admin user if it's a new run
        if [[ ${RUN_AIRFLOW_1_10} == "true" ]]; then
            airflow initdb
            airflow create_user -u admin -p admin -f Thor -l Adminstra -r Admin -e dummy@dummy.email || true
        else
            airflow db init
            airflow users create -u admin -p admin -f Thor -l Adminstra -r Admin -e dummy@dummy.email
        fi

        . "$( dirname "${BASH_SOURCE[0]}" )/run_init_script.sh"

    fi
    return $?
}

echo "==============================================================================================="
echo "             Checking integrations and backends"
echo "==============================================================================================="
if [[ -n ${BACKEND=} ]]; then
    check_db_backend 50
    echo "-----------------------------------------------------------------------------------------------"
fi
check_integration "Kerberos" "kerberos" "run_nc kdc-server-example-com 88" 50
check_integration "MongoDB" "mongo" "run_nc mongo 27017" 50
check_integration "Redis" "redis" "run_nc redis 6379" 50
check_integration "Cassandra" "cassandra" "run_nc cassandra 9042" 50
check_integration "OpenLDAP" "openldap" "run_nc openldap 389" 50
check_integration "Presto (HTTP)" "presto" "run_nc presto 8080" 50
check_integration "Presto (HTTPS)" "presto" "run_nc presto 7778" 50
check_integration "Presto (API)" "presto" \
    "curl --max-time 1 http://presto:8080/v1/info/ | grep '\"starting\":false'" 50
check_integration "Pinot (HTTP)" "pinot" "run_nc pinot 9000" 50
CMD="curl --max-time 1 -X GET 'http://pinot:9000/health' -H 'accept: text/plain' | grep OK"
check_integration "Presto (Controller API)" "pinot" "${CMD}" 50
CMD="curl --max-time 1 -X GET 'http://pinot:9000/pinot-controller/admin' -H 'accept: text/plain' | grep GOOD"
check_integration "Presto (Controller API)" "pinot" "${CMD}" 50
CMD="curl --max-time 1 -X GET 'http://pinot:8000/health' -H 'accept: text/plain' | grep OK"
check_integration "Presto (Broker API)" "pinot" "${CMD}" 50
check_integration "RabbitMQ" "rabbitmq" "run_nc rabbitmq 5672" 50

echo "-----------------------------------------------------------------------------------------------"

if [[ ${EXIT_CODE} != 0 ]]; then
    echo
    echo "Error: some of the CI environment failed to initialize!"
    echo
    # Fixed exit code on initialization
    # If the environment fails to initialize it is re-started several times
    exit 254
fi

resetdb_if_requested
startairflow_if_requested

if [[ -n ${DISABLED_INTEGRATIONS=} ]]; then
    echo
    echo "Disabled integrations:${DISABLED_INTEGRATIONS}"
    echo
    echo "Enable them via --integration <INTEGRATION_NAME> flags (you can use 'all' for all)"
    echo
fi
