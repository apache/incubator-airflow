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
import unittest
from unittest.mock import patch

from google.cloud.language_v1.proto.language_service_pb2 import (
    AnalyzeEntitiesResponse,
    AnalyzeEntitySentimentResponse,
    AnalyzeSentimentResponse,
    ClassifyTextResponse,
    Document,
)

from airflow.providers.google.cloud.operators.natural_language import (
    CloudNaturalLanguageAnalyzeEntitiesOperator,
    CloudNaturalLanguageAnalyzeEntitySentimentOperator,
    CloudNaturalLanguageAnalyzeSentimentOperator,
    CloudNaturalLanguageClassifyTextOperator,
)

DOCUMENT = Document(
    content="Airflow is a platform to programmatically author, schedule and monitor workflows."
)

CLASSIFY_TEXT_RESPONSE = ClassifyTextResponse()
ANALYZE_ENTITIES_RESPONSE = AnalyzeEntitiesResponse()
ANALYZE_ENTITY_SENTIMENT_RESPONSE = AnalyzeEntitySentimentResponse()
ANALYZE_SENTIMENT_RESPONSE = AnalyzeSentimentResponse()

ENCODING_TYPE = "UTF32"


class TestCloudLanguageAnalyzeEntitiesOperator(unittest.TestCase):
    @patch("airflow.providers.google.cloud.operators.natural_language.CloudNaturalLanguageHook")
    def test_minimal_green_path(self, hook_mock):
        hook_mock.return_value.analyze_entities.return_value = ANALYZE_ENTITIES_RESPONSE
        op = CloudNaturalLanguageAnalyzeEntitiesOperator(task_id="task-id", document=DOCUMENT)
        resp = op.execute({})
        assert resp == {}


class TestCloudLanguageAnalyzeEntitySentimentOperator(unittest.TestCase):
    @patch("airflow.providers.google.cloud.operators.natural_language.CloudNaturalLanguageHook")
    def test_minimal_green_path(self, hook_mock):
        hook_mock.return_value.analyze_entity_sentiment.return_value = ANALYZE_ENTITY_SENTIMENT_RESPONSE
        op = CloudNaturalLanguageAnalyzeEntitySentimentOperator(task_id="task-id", document=DOCUMENT)
        resp = op.execute({})
        assert resp == {}


class TestCloudLanguageAnalyzeSentimentOperator(unittest.TestCase):
    @patch("airflow.providers.google.cloud.operators.natural_language.CloudNaturalLanguageHook")
    def test_minimal_green_path(self, hook_mock):
        hook_mock.return_value.analyze_sentiment.return_value = ANALYZE_SENTIMENT_RESPONSE
        op = CloudNaturalLanguageAnalyzeSentimentOperator(task_id="task-id", document=DOCUMENT)
        resp = op.execute({})
        assert resp == {}


class TestCloudLanguageClassifyTextOperator(unittest.TestCase):
    @patch("airflow.providers.google.cloud.operators.natural_language.CloudNaturalLanguageHook")
    def test_minimal_green_path(self, hook_mock):
        hook_mock.return_value.classify_text.return_value = CLASSIFY_TEXT_RESPONSE
        op = CloudNaturalLanguageClassifyTextOperator(task_id="task-id", document=DOCUMENT)
        resp = op.execute({})
        assert resp == {}
