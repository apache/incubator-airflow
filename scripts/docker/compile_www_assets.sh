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
set -euo pipefail
set -x

# Installs additional dependencies passed as Argument to the Docker build command
function compile_www_assets() {
    echo
    echo Compiling WWW assets
    echo
    local md5sum_file
    md5sum_file="static/dist/sum.md5"
    readonly md5sum_file
    local airflow_site_package
    airflow_site_package="$(python -m site --user-site)"
    local www_dir=""
    if [[ -f "${airflow_site_package}/www_rbac/package.json" ]]; then
        www_dir="${airflow_site_package}/www_rbac"
    elif [[ -f "${airflow_site_package}/www/package.json" ]]; then
        www_dir="${airflow_site_package}/www"
    fi
    if [[ -n "${www_dir}" ]]; then
        pushd ${www_dir} || exit 1
        yarn install --frozen-lockfile --no-cache
        yarn run prod
        find package.json yarn.lock static/css static/js -type f | sort | xargs md5sum > "${md5sum_file}"
        rm -rf "${www_dir}/node_modules"
        rm -vf "${www_dir}"/{package.json,yarn.lock,.eslintignore,.eslintrc,.stylelintignore,.stylelintrc,compile_assets.sh,webpack.config.js}
        popd || exit 1
    fi
}

compile_www_assets
