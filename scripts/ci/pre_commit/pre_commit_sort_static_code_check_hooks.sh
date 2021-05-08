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
. "$( dirname "${BASH_SOURCE[0]}" )/../libraries/_script_init.sh"

BREEZE_COMPLETE="${AIRFLOW_SOURCES}/breeze-complete"
BREEZE_RST="${AIRFLOW_SOURCES}/BREEZE.rst"
STATIC_CODE_CHECKS="${AIRFLOW_SOURCES}/STATIC_CODE_CHECKS.rst"
readonly BREEZE_COMPLETE
readonly BREEZE_RST
readonly STATIC_CODE_CHECKS
export LC_ALL=C
temp_file=$(mktemp)
temp_hooks_file=$(mktemp)
temp_holder_file=$(mktemp)

< "${BREEZE_COMPLETE}" sed '/^all$/q' | sed '$d' > "${temp_file}"
temp_string=$(< "${BREEZE_COMPLETE}" awk '/^all$/,/^EOF$/' | sed '$d')
scc_array=$(IFS=$'\n'; echo "${temp_string}"; unset IFS)
# shellcheck disable=SC2206
a=(${scc_array[0]})
all_checks+=("${a[@]:0:2}")
sort_array+=("${a[@]:2}")
IFS=$'\n' sorted=("$(sort <<<"${sort_array[*]}" | uniq)")
unset IFS
all_checks+=("${sorted[@]:0}")
sorted_string=$(IFS=$'\n'; echo "${all_checks[*]}"; unset IFS)
printf '%s\nEOF\n)\n\n' "${sorted_string}" >> "${temp_file}"
< "${BREEZE_COMPLETE}" awk '/_breeze_default_dockerhub_user="apache"/,0' >> "${temp_file}"
cat "${temp_file}" > "${BREEZE_COMPLETE}"

< "${BREEZE_RST}" sed '/all all-but-pylint/q' | sed '$d' > "${temp_file}"
temp_string=$(< "${BREEZE_RST}" awk 'f && !NF{exit} /all all-but-pylint/ {f=1} f')
IFS=$'\n ' read -r -a scc_array <<< "$temp_string"
# shellcheck disable=SC2178
a="${scc_array[0]}"
all+=("${a[@]:0:2}")
sort_arr=("${a[@]:2}")
IFS=$' ' sorted=("$(sort <<<"${sort_arr[@]:0}")")
unset IFS
all+=("${sorted[@]}")
sorted_string=$(IFS=$' '; echo "${all[*]}"; unset IFS)
printf '%s\n\n' "${sorted_string}" | fold -w 83 -s | sed -e 's/^/                 /' | sed 's/ *$//g' >> "${temp_file}"
< "${BREEZE_RST}" awk '/You can pass extra arguments/,0' >> "${temp_file}"
cat "${temp_file}" > "${BREEZE_RST}"

awk '/^===================================/{i++}i==2; f; /^===================================/ && i==3{print; exit}' "${STATIC_CODE_CHECKS}" | grep '^``' | sort -k1,1 | uniq > "${temp_hooks_file}"
awk '/^.*/{f=1} f; /^===================================/ && ++c==2{exit}' "${STATIC_CODE_CHECKS}" > "${temp_file}"
: > "${temp_holder_file}"
while IFS= read -r line; do
    echo "${line}" >> "${temp_holder_file}"
    echo "----------------------------------- ---------------------------------------------------------------- ------------" >> "${temp_holder_file}"
done < "${temp_hooks_file}"
# shellcheck disable=SC2129
< "${temp_holder_file}" sed '$d' >> "${temp_file}"
printf "=================================== ================================================================ ============\n\n" >> "${temp_file}"
< "${STATIC_CODE_CHECKS}" awk '/The pre-commit hooks only check/,0' >> "${temp_file}"
cat "${temp_file}" > "${STATIC_CODE_CHECKS}"

rm "${temp_file}"
rm "${temp_holder_file}"
rm "${temp_hooks_file}"
