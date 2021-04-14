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

# Might be empty
AIRFLOW_COMMAND="${1}"

set -euo pipefail

function run_check_with_retries {
    local cmd
    cmd="${1}"
    local countdown
    countdown="${CONNECTION_CHECK_MAX_COUNT}"

    while true
    do
        set +e
        local last_check_result
        local res
        last_check_result=$(eval "${cmd} 2>&1")
        res=$?
        set -e
        if [[ ${res} == 0 ]]; then
            echo
            break
        else
            echo -n "."
            countdown=$((countdown-1))
        fi
        if [[ ${countdown} == 0 ]]; then
            echo
            echo "ERROR! Maximum number of retries (${CONNECTION_CHECK_MAX_COUNT}) reached."
            echo
            echo "Last check result:"
            echo "$ ${cmd}"
            echo "${last_check_result}"
            echo
            exit 1
        else
            sleep "${CONNECTION_CHECK_SLEEP_TIME}"
        fi
    done
}

function run_nc() {
    # Checks if it is possible to connect to the host using netcat.
    #
    # We want to avoid misleading messages and perform only forward lookup of the service IP address.
    # Netcat when run without -n performs both forward and reverse lookup and fails if the reverse
    # lookup name does not match the original name even if the host is reachable via IP. This happens
    # randomly with docker-compose in GitHub Actions.
    # Since we are not using reverse lookup elsewhere, we can perform forward lookup in python
    # And use the IP in NC and add '-n' switch to disable any DNS use.
    # Even if this message might be harmless, it might hide the real reason for the problem
    # Which is the long time needed to start some services, seeing this message might be totally misleading
    # when you try to analyse the problem, that's why it's best to avoid it,
    local host="${1}"
    local port="${2}"
    local ip
    ip=$(python -c "import socket; print(socket.gethostbyname('${host}'))")
    nc -zvvn "${ip}" "${port}"
}

function wait_for_connection {
    # Waits for Connection to the backend specified via URL passed as first parameter
    # Detects backend type depending on the URL schema and assigns
    # default port numbers if not specified in the URL.
    # Then it loops until connection to the host/port specified can be established
    # It tries `CONNECTION_CHECK_MAX_COUNT` times and sleeps `CONNECTION_CHECK_SLEEP_TIME` between checks
    local connection_url
    connection_url="${1}"
    local detected_backend=""
    local detected_host=""
    local detected_port=""

    # Auto-detect DB parameters
    # Examples:
    #  postgres://YourUserName:password@YourHostname:5432/YourDatabaseName
    #  postgres://YourUserName:password@YourHostname:5432/YourDatabaseName
    #  postgres://YourUserName:@YourHostname:/YourDatabaseName
    #  postgres://YourUserName@YourHostname/YourDatabaseName
    [[ ${connection_url} =~ ([^:]*)://([^:@]*):?([^@]*)@?([^/:]*):?([0-9]*)/([^\?]*)\??(.*) ]] && \
        detected_backend=${BASH_REMATCH[1]} &&
        # Not used USER match
        # Not used PASSWORD match
        detected_host=${BASH_REMATCH[4]} &&
        detected_port=${BASH_REMATCH[5]} &&
        # Not used SCHEMA match
        # Not used PARAMS match

    echo BACKEND="${BACKEND:=${detected_backend}}"
    readonly BACKEND

    if [[ -z "${detected_port=}" ]]; then
        if [[ ${BACKEND} == "postgres"* ]]; then
            detected_port=5432
        elif [[ ${BACKEND} == "mysql"* ]]; then
            detected_port=3306
        elif [[ ${BACKEND} == "redis"* ]]; then
            detected_port=6379
        elif [[ ${BACKEND} == "amqp"* ]]; then
            detected_port=5672
        fi
    fi

    detected_host=${detected_host:="localhost"}

    # Allow the DB parameters to be overridden by environment variable
    echo DB_HOST="${DB_HOST:=${detected_host}}"
    readonly DB_HOST

    echo DB_PORT="${DB_PORT:=${detected_port}}"
    readonly DB_PORT
    run_check_with_retries "run_nc ${DB_HOST@Q} ${DB_PORT@Q}"
}

function create_www_user() {
    local local_password=""
    # Warning: command environment variables (*_CMD) have priority over usual configuration variables
    # for configuration parameters that require sensitive information. This is the case for the SQL database
    # and the broker backend in this entrypoint script.
    if [[ -n "${_AIRFLOW_WWW_USER_PASSWORD_CMD=}" ]]; then
        local_password=$(eval "${_AIRFLOW_WWW_USER_PASSWORD_CMD}")
        unset _AIRFLOW_WWW_USER_PASSWORD_CMD
    elif [[ -n "${_AIRFLOW_WWW_USER_PASSWORD=}" ]]; then
        local_password="${_AIRFLOW_WWW_USER_PASSWORD}"
        unset _AIRFLOW_WWW_USER_PASSWORD
    fi
    if [[ -z ${local_password} ]]; then
        echo
        echo "ERROR! Airflow Admin password not set via _AIRFLOW_WWW_USER_PASSWORD or _AIRFLOW_WWW_USER_PASSWORD_CMD variables!"
        echo
        exit 1
    fi

    airflow users create \
       --username "${_AIRFLOW_WWW_USER_USERNAME="admin"}" \
       --firstname "${_AIRFLOW_WWW_USER_FIRSTNAME="Airflow"}" \
       --lastname "${_AIRFLOW_WWW_USER_LASTNME="Admin"}" \
       --email "${_AIRFLOW_WWW_USER_EMAIL="airflowadmin@example.com"}" \
       --role "${_AIRFLOW_WWW_USER_ROLE="Admin"}" \
       --password "${local_password}" ||
    airflow create_user \
       --username "${_AIRFLOW_WWW_USER_USERNAME="admin"}" \
       --firstname "${_AIRFLOW_WWW_USER_FIRSTNAME="Airflow"}" \
       --lastname "${_AIRFLOW_WWW_USER_LASTNME="Admin"}" \
       --email "${_AIRFLOW_WWW_USER_EMAIL="airflowadmin@example.com"}" \
       --role "${_AIRFLOW_WWW_USER_ROLE="Admin"}" \
       --password "${local_password}" || true
}

function create_system_user_if_missing() {
    # This is needed in case of OpenShift-compatible container execution. In case of OpenShift random
    # User id is used when starting the image, however group 0 is kept as the user group. Our production
    # Image is OpenShift compatible, so all permissions on all folders are set so that 0 group can exercise
    # the same privileges as the default "airflow" user, this code checks if the user is already
    # present in /etc/passwd and will create the system user dynamically, including setting its
    # HOME directory to the /home/airflow so that (for example) the ${HOME}/.local folder where airflow is
    # Installed can be automatically added to PYTHONPATH
    if ! whoami &> /dev/null; then
      if [[ -w /etc/passwd ]]; then
        echo "${USER_NAME:-default}:x:$(id -u):0:${USER_NAME:-default} user:${AIRFLOW_USER_HOME_DIR}:/sbin/nologin" \
            >> /etc/passwd
      fi
      export HOME="${AIRFLOW_USER_HOME_DIR}"
    fi
}

function set_pythonpath_for_root_user() {
    # Airflow is installed as a local user application which means that if the container is running as root
    # the application is not available. because Python then only load system-wide applications.
    # Now also adds applications installed as local user "airflow".
    if [[ $UID == "0" ]]; then
        local python_major_minor
        python_major_minor="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
        export PYTHONPATH="${AIRFLOW_USER_HOME_DIR}/.local/lib/python${python_major_minor}/site-packages:${PYTHONPATH:-}"
        >&2 echo "The container is run as root user. For security, consider using a regular user account."
    fi
}

function wait_for_airflow_db() {
    # Check if Airflow has a command to check the connection to the database.
    if ! airflow db check --help >/dev/null 2>&1; then
        run_check_with_retries "airflow db check"
    else
        # Verify connections to the Airflow DB by guessing the database address based on environment variables,
        # then uses netcat to check that the host is reachable.
        # This is only used by Airflow 1.10+ as there are no built-in commands to check the db connection.
        local connection_url
        if [[ -n "${AIRFLOW__CORE__SQL_ALCHEMY_CONN_CMD=}" ]]; then
            connection_url="$(eval "${AIRFLOW__CORE__SQL_ALCHEMY_CONN_CMD}")"
        else
            # if no DB configured - use sqlite db by default
            connection_url="${AIRFLOW__CORE__SQL_ALCHEMY_CONN:="sqlite:///${AIRFLOW_HOME}/airflow.db"}"
        fi
        # SQLite doesn't require a remote connection, so we don't have to wait.
        if [[ ${connection_url} != sqlite* ]]; then
            wait_for_connection "${connection_url}"
        fi
    fi
}

function upgrade_db() {
    # Runs airflow db upgrade
    airflow db upgrade || airflow upgradedb || true
}

function wait_for_celery_backend() {
    # Verifies connection to Celery Broker
    if [[ -n "${AIRFLOW__CELERY__BROKER_URL_CMD=}" ]]; then
        wait_for_connection "$(eval "${AIRFLOW__CELERY__BROKER_URL_CMD}")"
    else
        AIRFLOW__CELERY__BROKER_URL=${AIRFLOW__CELERY__BROKER_URL:=}
        if [[ -n ${AIRFLOW__CELERY__BROKER_URL=} ]]; then
            wait_for_connection "${AIRFLOW__CELERY__BROKER_URL}"
        fi
    fi
}

function exec_to_bash_or_python_command_if_specified() {
    # If one of the commands: 'airflow', 'bash', 'python' is used, either run appropriate
    # command with exec or update the command line parameters
    if [[ ${AIRFLOW_COMMAND} == "bash" ]]; then
       shift
       exec "/bin/bash" "${@}"
    elif [[ ${AIRFLOW_COMMAND} == "python" ]]; then
       shift
       exec "python" "${@}"
    fi
}


CONNECTION_CHECK_MAX_COUNT=${CONNECTION_CHECK_MAX_COUNT:=20}
readonly CONNECTION_CHECK_MAX_COUNT

CONNECTION_CHECK_SLEEP_TIME=${CONNECTION_CHECK_SLEEP_TIME:=3}
readonly CONNECTION_CHECK_SLEEP_TIME

create_system_user_if_missing
set_pythonpath_for_root_user
if [[ "${CONNECTION_CHECK_MAX_COUNT}" -gt "0" ]]; then
    wait_for_airflow_db
fi

if [[ -n "${_AIRFLOW_DB_UPGRADE=}" ]] ; then
    upgrade_db
fi

if [[ -n "${_AIRFLOW_WWW_USER_CREATE=}" ]] ; then
    create_www_user
fi

# The `bash` and `python` commands should also verify the basic connections
# So they are run after the DB check
exec_to_bash_or_python_command_if_specified "${@}"

# Remove "airflow" if it is specified as airflow command
# This way both command types work the same way:
#
#     docker run IMAGE airflow webserver
#     docker run IMAGE webserver
#
if [[ ${AIRFLOW_COMMAND} == "airflow" ]]; then
   AIRFLOW_COMMAND="${2}"
   shift
fi

# Note: the broker backend configuration concerns only a subset of Airflow components
if [[ ${AIRFLOW_COMMAND} =~ ^(scheduler|celery|worker|flower)$ ]] \
    && [[ "${CONNECTION_CHECK_MAX_COUNT}" -gt "0" ]]; then
    wait_for_celery_backend "${@}"
fi

exec "airflow" "${@}"
