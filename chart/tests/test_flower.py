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

import jmespath
import pytest

from tests.helm_template_generator import render_chart


class TestFlower:
    @pytest.mark.parametrize(
        "executor,flower_enabled,created",
        [
            ("CeleryExecutor", False, False),
            ("CeleryKubernetesExecutor", False, False),
            ("KubernetesExecutor", False, False),
            ("CeleryExecutor", True, True),
            ("CeleryKubernetesExecutor", True, True),
            ("KubernetesExecutor", True, False),
        ],
    )
    def test_create_flower(self, executor, flower_enabled, created):
        docs = render_chart(
            values={"executor": executor, "flower": {"enabled": flower_enabled}},
            show_only=["templates/flower/flower-deployment.yaml"],
        )

        assert bool(docs) is created
        if created:
            assert "RELEASE-NAME-flower" == jmespath.search("metadata.name", docs[0])
            assert "flower" == jmespath.search("spec.template.spec.containers[0].name", docs[0])

    def test_should_create_flower_deployment_with_authorization(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "flower": {"username": "flower", "password": "fl0w3r"},
                "ports": {"flowerUI": 7777},
            },
            show_only=["templates/flower/flower-deployment.yaml"],
        )

        assert "AIRFLOW__CELERY__FLOWER_BASIC_AUTH" == jmespath.search(
            "spec.template.spec.containers[0].env[0].name", docs[0]
        )
        assert ['curl', '--user', '$AIRFLOW__CELERY__FLOWER_BASIC_AUTH', 'localhost:7777'] == jmespath.search(
            "spec.template.spec.containers[0].livenessProbe.exec.command", docs[0]
        )
        assert ['curl', '--user', '$AIRFLOW__CELERY__FLOWER_BASIC_AUTH', 'localhost:7777'] == jmespath.search(
            "spec.template.spec.containers[0].readinessProbe.exec.command", docs[0]
        )

    def test_should_create_flower_deployment_without_authorization(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "ports": {"flowerUI": 7777},
            },
            show_only=["templates/flower/flower-deployment.yaml"],
        )

        assert "AIRFLOW__CORE__FERNET_KEY" == jmespath.search(
            "spec.template.spec.containers[0].env[0].name", docs[0]
        )
        assert ['curl', 'localhost:7777'] == jmespath.search(
            "spec.template.spec.containers[0].livenessProbe.exec.command", docs[0]
        )
        assert ['curl', 'localhost:7777'] == jmespath.search(
            "spec.template.spec.containers[0].readinessProbe.exec.command", docs[0]
        )
