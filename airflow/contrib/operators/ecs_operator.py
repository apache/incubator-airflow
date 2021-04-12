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
"""This module is deprecated. Please use :mod:`airflow.providers.amazon.aws.operators.ecs`."""

import warnings

# pylint: disable=unused-import
from airflow.providers.amazon.aws.operators.ecs import ECSOperator, ECSProtocol as NewECSProtocol  # noqa
from airflow.typing_compat import Protocol, runtime_checkable

warnings.warn(
    "This module is deprecated. Please use `airflow.providers.amazon.aws.operators.ecs`.",
    DeprecationWarning,
    stacklevel=2,
)


@runtime_checkable
class ECSProtocol(NewECSProtocol, Protocol):
    """This class is deprecated. Please use `airflow.providers.amazon.aws.operators.ecs.ECSProtocol`."""

    # A Protocol cannot be instantiated

    def __new__(cls, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.amazon.aws.operators.ecs.ECSProtocol`.""",
            DeprecationWarning,
            stacklevel=2,
        )
