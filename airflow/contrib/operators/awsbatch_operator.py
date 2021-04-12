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
#

"""
This module is deprecated. Please use:

- :mod:`airflow.providers.amazon.aws.operators.batch`
- :mod:`airflow.providers.amazon.aws.hooks.batch_client`
- :mod:`airflow.providers.amazon.aws.hooks.batch_waiters``
"""

import warnings

from airflow.providers.amazon.aws.hooks.batch_client import AwsBatchProtocol
from airflow.providers.amazon.aws.operators.batch import AwsBatchOperator
from airflow.typing_compat import Protocol, runtime_checkable

warnings.warn(
    "This module is deprecated. "
    "Please use `airflow.providers.amazon.aws.operators.batch`, "
    "`airflow.providers.amazon.aws.hooks.batch_client`, and "
    "`airflow.providers.amazon.aws.hooks.batch_waiters`",
    DeprecationWarning,
    stacklevel=2,
)


# pylint: disable=too-many-ancestors
class AWSBatchOperator(AwsBatchOperator):
    """
    This class is deprecated. Please use
    `airflow.providers.amazon.aws.operators.batch.AwsBatchOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.amazon.aws.operators.batch.AwsBatchOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


@runtime_checkable
class BatchProtocol(AwsBatchProtocol, Protocol):
    """
    This class is deprecated. Please use
    `airflow.providers.amazon.aws.hooks.batch_client.AwsBatchProtocol`.
    """

    # A Protocol cannot be instantiated

    def __new__(cls, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.amazon.aws.hooks.batch_client.AwsBatchProtocol`.""",
            DeprecationWarning,
            stacklevel=2,
        )
