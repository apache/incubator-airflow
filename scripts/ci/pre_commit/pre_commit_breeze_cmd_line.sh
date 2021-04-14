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
PRE_COMMIT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export PRE_COMMIT_DIR
readonly PRE_COMMIT_DIR

AIRFLOW_SOURCES=$(cd "${PRE_COMMIT_DIR}/../../../" && pwd);
export AIRFLOW_SOURCES
readonly AIRFLOW_SOURCES

cd "${AIRFLOW_SOURCES}" || exit 1
export PRINT_INFO_FROM_SCRIPTS="false"
export SKIP_CHECK_REMOTE_IMAGE="true"

TMP_FILE=$(mktemp)
export TMP_FILE
readonly TMP_FILE

TMP_OUTPUT=$(mktemp)
export TMP_OUTPUT
readonly TMP_OUTPUT

echo "
.. code-block:: text
" >"${TMP_FILE}"

export MAX_SCREEN_WIDTH=100
readonly MAX_SCREEN_WIDTH

export FORCE_SCREEN_WIDTH="true"
readonly FORCE_SCREEN_WIDTH

export VERBOSE="false"
readonly VERBOSE

export BREEZE_REDIRECT="false"

./breeze help-all | sed 's/^/  /' | sed 's/ *$//' >>"${TMP_FILE}"

MAX_LEN_FOUND=$(awk '{ print length($0); }' "${TMP_FILE}" | sort -n | tail -1 )

MAX_LEN_EXPECTED=$((MAX_SCREEN_WIDTH + 2))
# 2 spaces added in front of the width for .rst formatting
if (( MAX_LEN_FOUND > MAX_LEN_EXPECTED )); then
    awk "length(\$0) > ${MAX_LEN_EXPECTED}" <"${TMP_FILE}"
    echo
    echo "ERROR! Some lines in generate breeze help-all command are too long. See above ^^"
    echo
    echo
    exit 1
fi

BREEZE_RST_FILE="${AIRFLOW_SOURCES}/BREEZE.rst"
readonly BREEZE_RST_FILE

lead_marker='^ \.\. START BREEZE HELP MARKER$'
tail_marker='^ \.\. END BREEZE HELP MARKER$'

beginning_of_generated_help_line_number=$(grep -n "${lead_marker}" <"${BREEZE_RST_FILE}" | sed 's/\(.*\):.*/\1/g')
end_beginning_of_generated_help_line_number=$(grep -n "${tail_marker}" <"${BREEZE_RST_FILE}" | sed 's/\(.*\):.*/\1/g')
cat <(head -n "${beginning_of_generated_help_line_number}" "${BREEZE_RST_FILE}") \
    "${TMP_FILE}" \
    <(tail -n +"${end_beginning_of_generated_help_line_number}" "${BREEZE_RST_FILE}") \
    >"${TMP_OUTPUT}"

mv "${TMP_OUTPUT}" "${BREEZE_RST_FILE}"
