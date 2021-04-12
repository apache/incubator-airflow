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
"""This module is deprecated. Please use :mod:`airflow.providers.google.cloud.operators.natural_language`."""

import warnings

from airflow.providers.google.cloud.operators.natural_language import (
    CloudNaturalLanguageAnalyzeEntitiesOperator,
    CloudNaturalLanguageAnalyzeEntitySentimentOperator,
    CloudNaturalLanguageAnalyzeSentimentOperator,
    CloudNaturalLanguageClassifyTextOperator,
)

warnings.warn(
    """This module is deprecated.
    Please use `airflow.providers.google.cloud.operators.natural_language`
    """,
    DeprecationWarning,
    stacklevel=2,
)


class CloudLanguageAnalyzeEntitiesOperator(CloudNaturalLanguageAnalyzeEntitiesOperator):
    """
    This class is deprecated.
    Please use
    `airflow.providers.google.cloud.operators.natural_language.CloudNaturalLanguageAnalyzeEntitiesOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use
            `airflow.providers.google.cloud.operators.natural_language
            .CloudNaturalLanguageAnalyzeEntitiesOperator`.
            """,
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class CloudLanguageAnalyzeEntitySentimentOperator(CloudNaturalLanguageAnalyzeEntitySentimentOperator):
    """
    This class is deprecated.
    Please use
    `airflow.providers.google.cloud.operators.natural_language
    .CloudNaturalLanguageAnalyzeEntitySentimentOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use
            `airflow.providers.google.cloud.operators.natural_language
            .CloudNaturalLanguageAnalyzeEntitySentimentOperator`.
            """,
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class CloudLanguageAnalyzeSentimentOperator(CloudNaturalLanguageAnalyzeSentimentOperator):
    """
    This class is deprecated.
    Please use
    `airflow.providers.google.cloud.operators.natural_language.CloudNaturalLanguageAnalyzeSentimentOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.natural_language
            .CloudNaturalLanguageAnalyzeSentimentOperator`.
            """,
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class CloudLanguageClassifyTextOperator(CloudNaturalLanguageClassifyTextOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.natural_language
    .CloudNaturalLanguageClassifyTextOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.natural_language
            .CloudNaturalLanguageClassifyTextOperator`.
            """,
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)
