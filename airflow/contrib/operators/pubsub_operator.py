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
Please use :mod:`airflow.providers.google.cloud.operators.pubsub`.
"""

import warnings

from airflow.providers.google.cloud.operators.pubsub import (
    PubSubCreateSubscriptionOperator,
    PubSubCreateTopicOperator,
    PubSubDeleteSubscriptionOperator,
    PubSubDeleteTopicOperator,
    PubSubPublishMessageOperator,
)

warnings.warn(
    """This module is deprecated.
    "Please use `airflow.providers.google.cloud.operators.pubsub`.""",
    DeprecationWarning,
    stacklevel=2,
)


class PubSubPublishOperator(PubSubPublishMessageOperator):
    """This class is deprecated.

    Please use `airflow.providers.google.cloud.operators.pubsub.PubSubPublishMessageOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.pubsub.PubSubPublishMessageOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class PubSubSubscriptionCreateOperator(PubSubCreateSubscriptionOperator):
    """This class is deprecated.

    Please use `airflow.providers.google.cloud.operators.pubsub.PubSubCreateSubscriptionOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.pubsub.PubSubCreateSubscriptionOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class PubSubSubscriptionDeleteOperator(PubSubDeleteSubscriptionOperator):
    """This class is deprecated.

    Please use `airflow.providers.google.cloud.operators.pubsub.PubSubDeleteSubscriptionOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.pubsub.PubSubDeleteSubscriptionOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class PubSubTopicCreateOperator(PubSubCreateTopicOperator):
    """This class is deprecated.

    Please use `airflow.providers.google.cloud.operators.pubsub.PubSubCreateTopicOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.pubsub.PubSubCreateTopicOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class PubSubTopicDeleteOperator(PubSubDeleteTopicOperator):
    """This class is deprecated.

    Please use `airflow.providers.google.cloud.operators.pubsub.PubSubDeleteTopicOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.pubsub.PubSubDeleteTopicOperator`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)
