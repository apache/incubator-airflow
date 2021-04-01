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
# shellcheck disable=SC2086

# Installs Airflow from $AIRFLOW_BRANCH tip. This is pure optimisation. It is done because we do not want
# to reinstall all dependencies from scratch when setup.py changes. Problem with Docker caching is that
# when a file is changed, when added to docker context, it invalidates the cache and it causes Docker
# build to reinstall all dependencies from scratch. This can take a loooooot of time. Therefore we install
# the dependencies first from master (and uninstall airflow right after) so that we can start installing
# deps from those pre-installed dependencies. It saves few minutes of build time when setup.py changes.
#
# If INSTALL_MYSQL_CLIENT is set to false, mysql extra is removed
#
# shellcheck source=scripts/docker/common.sh
. "$( dirname "${BASH_SOURCE[0]}" )/common.sh"


function install_airflow_from_branch_tip() {
    echo
    echo "Installing airflow from ${AIRFLOW_BRANCH}. It is used to cache dependencies"
    echo
    if [[ ${INSTALL_MYSQL_CLIENT} != "true" ]]; then
       AIRFLOW_EXTRAS=${AIRFLOW_EXTRAS/mysql,}
    fi
    # Install latest set of dependencies using constraints
    pip install ${AIRFLOW_INSTALL_USER_FLAG} \
      "https://github.com/${AIRFLOW_REPO}/archive/${AIRFLOW_BRANCH}.tar.gz#egg=apache-airflow[${AIRFLOW_EXTRAS}]" \
      --constraint "${AIRFLOW_CONSTRAINTS_LOCATION}"
    # make sure correct PIP version is used
    pip install ${AIRFLOW_INSTALL_USER_FLAG} --upgrade "pip==${AIRFLOW_PIP_VERSION}"
    pip freeze | grep apache-airflow-providers | xargs pip uninstall --yes || true
    echo
    echo Uninstalling just airflow. Dependencies remain.
    echo
    pip uninstall --yes apache-airflow
}

common::get_constraints_location

install_airflow_from_branch_tip
