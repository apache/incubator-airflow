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

import unittest

import jmespath
from parameterized import parameterized

from tests.helm_template_generator import render_chart


class WorkerTest(unittest.TestCase):
    @parameterized.expand(
        [
            ("CeleryExecutor", False, "Deployment"),
            ("CeleryExecutor", True, "StatefulSet"),
            ("CeleryKubernetesExecutor", False, "Deployment"),
            ("CeleryKubernetesExecutor", True, "StatefulSet"),
        ]
    )
    def test_worker_kind(self, executor, persistence, kind):
        """
        Test worker kind is StatefulSet when worker persistence is enabled.
        """
        docs = render_chart(
            values={
                "executor": executor,
                "workers": {"persistence": {"enabled": persistence}},
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert kind == jmespath.search("kind", docs[0])

    def test_should_add_extra_containers(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "workers": {
                    "extraContainers": [
                        {
                            "name": "test-container",
                            "image": "test-registry/test-repo:test-tag",
                            "imagePullPolicy": "Always",
                        }
                    ],
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert "test-container" == jmespath.search("spec.template.spec.containers[-1].name", docs[0])

    def test_should_add_extra_volume_and_extra_volume_mount(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "workers": {
                    "extraVolumes": [{"name": "test-volume", "emptyDir": {}}],
                    "extraVolumeMounts": [{"name": "test-volume", "mountPath": "/opt/test"}],
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert "test-volume" == jmespath.search("spec.template.spec.volumes[0].name", docs[0])
        assert "test-volume" == jmespath.search(
            "spec.template.spec.containers[0].volumeMounts[0].name", docs[0]
        )

    def test_workers_host_aliases(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "workers": {
                    "hostAliases": [{"ip": "127.0.0.2", "hostnames": ["test.hostname"]}],
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert "127.0.0.2" == jmespath.search("spec.template.spec.hostAliases[0].ip", docs[0])
        assert "test.hostname" == jmespath.search("spec.template.spec.hostAliases[0].hostnames[0]", docs[0])

    def test_workers_update_strategy(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "workers": {
                    "updateStrategy": {
                        "strategy": {"rollingUpdate": {"maxSurge": "100%", "maxUnavailable": "50%"}}
                    },
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert "100%" == jmespath.search("spec.strategy.rollingUpdate.maxSurge", docs[0])
        assert "50%" == jmespath.search("spec.strategy.rollingUpdate.maxUnavailable", docs[0])

    def test_should_create_valid_affinity_tolerations_and_node_selector(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "workers": {
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
            show_only=["templates/workers/worker-deployment.yaml"],
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
