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
import re
import unittest
from base64 import b64decode
from subprocess import CalledProcessError
from typing import Optional

import jmespath
import pytest
from parameterized import parameterized

from tests.helm_template_generator import prepare_k8s_lookup_dict, render_chart

RELEASE_NAME_REDIS = "TEST-REDIS"

REDIS_OBJECTS = {
    "NETWORK_POLICY": ("NetworkPolicy", f"{RELEASE_NAME_REDIS}-redis-policy"),
    "SERVICE": ("Service", f"{RELEASE_NAME_REDIS}-redis"),
    "STATEFUL_SET": ("StatefulSet", f"{RELEASE_NAME_REDIS}-redis"),
    "SECRET_PASSWORD": ("Secret", f"{RELEASE_NAME_REDIS}-redis-password"),
    "SECRET_BROKER_URL": ("Secret", f"{RELEASE_NAME_REDIS}-broker-url"),
}
SET_POSSIBLE_REDIS_OBJECT_KEYS = set(REDIS_OBJECTS.values())

CELERY_EXECUTORS_PARAMS = [("CeleryExecutor",), ("CeleryKubernetesExecutor",)]


class RedisTest(unittest.TestCase):
    @staticmethod
    def get_broker_url_in_broker_url_secret(k8s_obj_by_key):
        broker_url_in_obj = b64decode(
            k8s_obj_by_key[REDIS_OBJECTS["SECRET_BROKER_URL"]]["data"]["connection"]
        ).decode("utf-8")
        return broker_url_in_obj

    @staticmethod
    def get_redis_password_in_password_secret(k8s_obj_by_key):
        password_in_obj = b64decode(
            k8s_obj_by_key[REDIS_OBJECTS["SECRET_PASSWORD"]]["data"]["password"]
        ).decode("utf-8")
        return password_in_obj

    @staticmethod
    def get_broker_url_secret_in_deployment(k8s_obj_by_key, kind: str, name: str) -> str:
        deployment_obj = k8s_obj_by_key[(kind, f"{RELEASE_NAME_REDIS}-{name}")]
        containers = deployment_obj["spec"]["template"]["spec"]["containers"]
        container = next(obj for obj in containers if obj["name"] == name)

        envs = container["env"]
        env = next(obj for obj in envs if obj["name"] == "AIRFLOW__CELERY__BROKER_URL")
        return env["valueFrom"]["secretKeyRef"]["name"]

    def assert_password_and_broker_url_secrets(
        self, k8s_obj_by_key, expected_password_match: Optional[str], expected_broker_url_match: Optional[str]
    ):
        if expected_password_match is not None:
            redis_password_in_password_secret = self.get_redis_password_in_password_secret(k8s_obj_by_key)
            assert re.search(expected_password_match, redis_password_in_password_secret)
        else:
            assert REDIS_OBJECTS["SECRET_PASSWORD"] not in k8s_obj_by_key.keys()

        if expected_broker_url_match is not None:
            # assert redis broker url in secret
            broker_url_in_broker_url_secret = self.get_broker_url_in_broker_url_secret(k8s_obj_by_key)
            assert re.search(expected_broker_url_match, broker_url_in_broker_url_secret)
        else:
            assert REDIS_OBJECTS["SECRET_BROKER_URL"] not in k8s_obj_by_key.keys()

    def assert_broker_url_env(
        self, k8s_obj_by_key, expected_broker_url_secret_name=REDIS_OBJECTS["SECRET_BROKER_URL"][1]
    ):
        broker_url_secret_in_scheduler = self.get_broker_url_secret_in_deployment(
            k8s_obj_by_key, "StatefulSet", "worker"
        )
        assert broker_url_secret_in_scheduler == expected_broker_url_secret_name
        broker_url_secret_in_worker = self.get_broker_url_secret_in_deployment(
            k8s_obj_by_key, "Deployment", "scheduler"
        )
        assert broker_url_secret_in_worker == expected_broker_url_secret_name

    @parameterized.expand(CELERY_EXECUTORS_PARAMS)
    def test_redis_by_chart_default(self, executor):
        k8s_objects = render_chart(
            RELEASE_NAME_REDIS,
            {
                "executor": executor,
                "networkPolicies": {"enabled": True},
                "redis": {"enabled": True},
            },
        )
        k8s_obj_by_key = prepare_k8s_lookup_dict(k8s_objects)

        created_redis_objects = SET_POSSIBLE_REDIS_OBJECT_KEYS & set(k8s_obj_by_key.keys())
        assert created_redis_objects == SET_POSSIBLE_REDIS_OBJECT_KEYS

        self.assert_password_and_broker_url_secrets(
            k8s_obj_by_key,
            expected_password_match=r"\w+",
            expected_broker_url_match=fr"redis://:\w+@{RELEASE_NAME_REDIS}-redis:6379/0",
        )

        self.assert_broker_url_env(k8s_obj_by_key)

    @parameterized.expand(CELERY_EXECUTORS_PARAMS)
    def test_redis_by_chart_password(self, executor):
        k8s_objects = render_chart(
            RELEASE_NAME_REDIS,
            {
                "executor": executor,
                "networkPolicies": {"enabled": True},
                "redis": {"enabled": True, "password": "test-redis-password"},
            },
        )
        k8s_obj_by_key = prepare_k8s_lookup_dict(k8s_objects)

        created_redis_objects = SET_POSSIBLE_REDIS_OBJECT_KEYS & set(k8s_obj_by_key.keys())
        assert created_redis_objects == SET_POSSIBLE_REDIS_OBJECT_KEYS

        self.assert_password_and_broker_url_secrets(
            k8s_obj_by_key,
            expected_password_match="test-redis-password",
            expected_broker_url_match=f"redis://:test-redis-password@{RELEASE_NAME_REDIS}-redis:6379/0",
        )

        self.assert_broker_url_env(k8s_obj_by_key)

    @parameterized.expand(CELERY_EXECUTORS_PARAMS)
    def test_redis_by_chart_password_secret_name_missing_broker_url_secret_name(self, executor):
        with pytest.raises(CalledProcessError):
            render_chart(
                RELEASE_NAME_REDIS,
                {
                    "executor": executor,
                    "redis": {
                        "enabled": True,
                        "passwordSecretName": "test-redis-password-secret-name",
                    },
                },
            )

    @parameterized.expand(CELERY_EXECUTORS_PARAMS)
    def test_redis_by_chart_password_secret_name(self, executor):
        expected_broker_url_secret_name = "test-redis-broker-url-secret-name"
        k8s_objects = render_chart(
            RELEASE_NAME_REDIS,
            {
                "executor": executor,
                "networkPolicies": {"enabled": True},
                "data": {"brokerUrlSecretName": expected_broker_url_secret_name},
                "redis": {
                    "enabled": True,
                    "passwordSecretName": "test-redis-password-secret-name",
                },
            },
        )
        k8s_obj_by_key = prepare_k8s_lookup_dict(k8s_objects)

        created_redis_objects = SET_POSSIBLE_REDIS_OBJECT_KEYS & set(k8s_obj_by_key.keys())
        assert created_redis_objects == SET_POSSIBLE_REDIS_OBJECT_KEYS - {
            REDIS_OBJECTS["SECRET_PASSWORD"],
            REDIS_OBJECTS["SECRET_BROKER_URL"],
        }

        self.assert_password_and_broker_url_secrets(
            k8s_obj_by_key, expected_password_match=None, expected_broker_url_match=None
        )

        self.assert_broker_url_env(k8s_obj_by_key, expected_broker_url_secret_name)

    @parameterized.expand(CELERY_EXECUTORS_PARAMS)
    def test_external_redis_broker_url(self, executor):
        k8s_objects = render_chart(
            RELEASE_NAME_REDIS,
            {
                "executor": executor,
                "networkPolicies": {"enabled": True},
                "data": {
                    "brokerUrl": "redis://redis-user:password@redis-host:6379/0",
                },
                "redis": {"enabled": False},
            },
        )
        k8s_obj_by_key = prepare_k8s_lookup_dict(k8s_objects)

        created_redis_objects = SET_POSSIBLE_REDIS_OBJECT_KEYS & set(k8s_obj_by_key.keys())
        assert created_redis_objects == {REDIS_OBJECTS["SECRET_BROKER_URL"]}

        self.assert_password_and_broker_url_secrets(
            k8s_obj_by_key,
            expected_password_match=None,
            expected_broker_url_match="redis://redis-user:password@redis-host:6379/0",
        )

        self.assert_broker_url_env(k8s_obj_by_key)

    @parameterized.expand(CELERY_EXECUTORS_PARAMS)
    def test_external_redis_broker_url_secret_name(self, executor):
        expected_broker_url_secret_name = "redis-broker-url-secret-name"
        k8s_objects = render_chart(
            RELEASE_NAME_REDIS,
            {
                "executor": executor,
                "networkPolicies": {"enabled": True},
                "data": {"brokerUrlSecretName": expected_broker_url_secret_name},
                "redis": {"enabled": False},
            },
        )
        k8s_obj_by_key = prepare_k8s_lookup_dict(k8s_objects)

        created_redis_objects = SET_POSSIBLE_REDIS_OBJECT_KEYS & set(k8s_obj_by_key.keys())
        assert created_redis_objects == set()

        self.assert_password_and_broker_url_secrets(
            k8s_obj_by_key, expected_password_match=None, expected_broker_url_match=None
        )

        self.assert_broker_url_env(k8s_obj_by_key, expected_broker_url_secret_name)

    def test_should_create_valid_affinity_tolerations_and_node_selector(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "redis": {
                    "affinity": {
                        "nodeAffinity": {
                            "requiredDuringSchedulingIgnoredDuringExecution": {
                                "nodeSelectorTerms": [
                                    {
                                        "matchExpressions": [
                                            {"key": "foo", "operator": "In", "values": ["true"]},
                                        ]
                                    }
                                ]
                            }
                        }
                    },
                    "tolerations": [
                        {"key": "dynamic-pods", "operator": "Equal", "value": "true", "effect": "NoSchedule"}
                    ],
                    "nodeSelector": {"diskType": "ssd"},
                },
            },
            show_only=["templates/redis/redis-statefulset.yaml"],
        )

        assert "StatefulSet" == jmespath.search("kind", docs[0])
        assert "foo" == jmespath.search(
            "spec.template.spec.affinity.nodeAffinity."
            "requiredDuringSchedulingIgnoredDuringExecution."
            "nodeSelectorTerms[0]."
            "matchExpressions[0]."
            "key",
            docs[0],
        )
        assert "ssd" == jmespath.search(
            "spec.template.spec.nodeSelector.diskType",
            docs[0],
        )
        assert "dynamic-pods" == jmespath.search(
            "spec.template.spec.tolerations[0].key",
            docs[0],
        )
