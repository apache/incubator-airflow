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

# Those are files that are mounted locally when mounting local sources is requested
# By default not the whole airflow sources directory is mounted because there are often
# artifacts created there (for example .egg-info files) that are breaking the capability
# of running different python versions in Breeze. So we only mount what is needed by default.
function local_mounts::generate_local_mounts_list {
    local prefix="$1"
    LOCAL_MOUNTS=(
        "$prefix".bash_aliases:/root/.bash_aliases:cached
        "$prefix".bash_history:/root/.bash_history:cached
        "$prefix".coveragerc:/opt/airflow/.coveragerc:cached
        "$prefix".dockerignore:/opt/airflow/.dockerignore:cached
        "$prefix".flake8:/opt/airflow/.flake8:cached
        "$prefix".github:/opt/airflow/.github:cached
        "$prefix".inputrc:/root/.inputrc:cached
        "$prefix".rat-excludes:/opt/airflow/.rat-excludes:cached
        "$prefix"CHANGELOG.txt:/opt/airflow/CHANGELOG.txt:cached
        "$prefix"LICENSE:/opt/airflow/LICENSE:cached
        "$prefix"MANIFEST.in:/opt/airflow/MANIFEST.in:cached
        "$prefix"NOTICE:/opt/airflow/NOTICE:cached
        "$prefix"airflow:/opt/airflow/airflow:cached
        "$prefix"provider_packages:/opt/airflow/provider_packages:cached
        "$prefix"dags:/opt/airflow/dags:cached
        "$prefix"dev:/opt/airflow/dev:cached
        "$prefix"docs:/opt/airflow/docs:cached
        "$prefix"hooks:/opt/airflow/hooks:cached
        "$prefix"logs:/root/airflow/logs:cached
        "$prefix"pylintrc:/opt/airflow/pylintrc:cached
        "$prefix"pylintrc-tests:/opt/airflow/pylintrc-tests:cached
        "$prefix"pyproject.toml:/opt/airflow/pyproject.toml:cached
        "$prefix"pytest.ini:/opt/airflow/pytest.ini:cached
        "$prefix"scripts:/opt/airflow/scripts:cached
        "$prefix"scripts/in_container/entrypoint_ci.sh:/entrypoint:cached
        "$prefix"setup.cfg:/opt/airflow/setup.cfg:cached
        "$prefix"setup.py:/opt/airflow/setup.py:cached
        "$prefix"tests:/opt/airflow/tests:cached
        "$prefix"kubernetes_tests:/opt/airflow/kubernetes_tests:cached
        "$prefix"chart:/opt/airflow/chart:cached
        "$prefix"metastore_browser:/opt/airflow/metastore_browser:cached
    )
}

# Converts the local mounts that we defined above to the right set of -v
# volume mappings in docker-compose file. This is needed so that we only
# maintain the volumes in one place (above)
function local_mounts::convert_local_mounts_to_docker_params() {
    local_mounts::generate_local_mounts_list "${AIRFLOW_SOURCES}/"
    # Bash can't "return" arrays, so we need to quote any special characters
    printf -- '-v %q ' "${LOCAL_MOUNTS[@]}"
}
