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

if [[ ${GITHUB_REF} == 'refs/heads/main' ]]; then
  echo "::set-output name=branch::constraints-main"
elif [[ ${GITHUB_REF} == 'refs/heads/master' ]]; then
  echo "::set-output name=branch::constraints-master"
elif [[ ${GITHUB_REF} == 'refs/heads/v1-10-test' ]]; then
  echo "::set-output name=branch::constraints-1-10"
elif [[ ${GITHUB_REF} == 'refs/heads/v2-0-test' ]]; then
  echo "::set-output name=branch::constraints-2-0"
else
  echo
  echo "Unexpected ref ${GITHUB_REF}. Exiting!"
  echo
  exit 1
fi
