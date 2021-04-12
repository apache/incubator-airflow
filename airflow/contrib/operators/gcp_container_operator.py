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
Please use :mod:`airflow.providers.google.cloud.operators.kubernetes_engine`.
"""

import warnings

from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKECreateClusterOperator,
    GKEDeleteClusterOperator,
    GKEStartPodOperator,
)

warnings.warn(
    "This module is deprecated. Please use `airflow.providers.google.cloud.operators.kubernetes_engine`",
    DeprecationWarning,
    stacklevel=2,
)


class GKEClusterCreateOperator(GKECreateClusterOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.container.GKECreateClusterOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.container.GKECreateClusterOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class GKEClusterDeleteOperator(GKEDeleteClusterOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.container.GKEDeleteClusterOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.container.GKEDeleteClusterOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class GKEPodOperator(GKEStartPodOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.container.GKEStartPodOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.container.GKEStartPodOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)
