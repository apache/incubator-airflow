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
"""
This module is deprecated.
Please use :mod:`kubernetes.client.models for V1ResourceRequirements and Port.
"""
# flake8: noqa
# pylint: disable=unused-import
import warnings

with warnings.catch_warnings():
    from airflow.providers.cncf.kubernetes.backcompat.pod import (  # pylint: disable=unused-import
        Port,
        Resources,
    )

warnings.warn(
    "This module is deprecated. Please use `kubernetes.client.models for V1ResourceRequirements and Port.",
    DeprecationWarning,
    stacklevel=2,
)
