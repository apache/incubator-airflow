#
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
"""This module is deprecated. Please use :mod:`airflow.providers.databricks.hooks.databricks`."""

import warnings

# pylint: disable=unused-import
from airflow.providers.databricks.hooks.databricks import (  # noqa
    CANCEL_RUN_ENDPOINT,
    GET_RUN_ENDPOINT,
    RESTART_CLUSTER_ENDPOINT,
    RUN_LIFE_CYCLE_STATES,
    RUN_NOW_ENDPOINT,
    START_CLUSTER_ENDPOINT,
    SUBMIT_RUN_ENDPOINT,
    TERMINATE_CLUSTER_ENDPOINT,
    USER_AGENT_HEADER,
    DatabricksHook,
    RunState,
)

warnings.warn(
    "This module is deprecated. Please use `airflow.providers.databricks.hooks.databricks`.",
    DeprecationWarning,
    stacklevel=2,
)
