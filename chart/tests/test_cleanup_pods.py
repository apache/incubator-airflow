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

from tests.helm_template_generator import render_chart


class CleanupPodsTest(unittest.TestCase):
    def test_should_create_cronjob_for_enabled_cleanup(self):
        docs = render_chart(
            values={
                "cleanup": {"enabled": True},
            },
            show_only=["templates/cleanup/cleanup-cronjob.yaml"],
        )

        assert "airflow-cleanup-pods" == jmespath.search(
            "spec.jobTemplate.spec.template.spec.containers[0].name", docs[0]
        )
        assert "apache/airflow:2.0.0" == jmespath.search(
            "spec.jobTemplate.spec.template.spec.containers[0].image", docs[0]
        )
        assert {"name": "config", "configMap": {"name": "RELEASE-NAME-airflow-config"}} in jmespath.search(
            "spec.jobTemplate.spec.template.spec.volumes", docs[0]
        )
        assert {
            "name": "config",
            "mountPath": "/opt/airflow/airflow.cfg",
            "subPath": "airflow.cfg",
            "readOnly": True,
        } in jmespath.search("spec.jobTemplate.spec.template.spec.containers[0].volumeMounts", docs[0])

    def test_should_change_image_when_set_airflow_image(self):
        docs = render_chart(
            values={
                "cleanup": {"enabled": True},
                "images": {"airflow": {"repository": "airflow", "tag": "test"}},
            },
            show_only=["templates/cleanup/cleanup-cronjob.yaml"],
        )

        assert "airflow:test" == jmespath.search(
            "spec.jobTemplate.spec.template.spec.containers[0].image", docs[0]
        )

    def test_should_create_valid_affinity_tolerations_and_node_selector(self):
        docs = render_chart(
            values={
                "cleanup": {
                    "enabled": True,
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
                }
            },
            show_only=["templates/cleanup/cleanup-cronjob.yaml"],
        )

        assert "CronJob" == jmespath.search("kind", docs[0])
        assert "foo" == jmespath.search(
            "spec.jobTemplate.spec.template.spec.affinity.nodeAffinity."
            "requiredDuringSchedulingIgnoredDuringExecution."
            "nodeSelectorTerms[0]."
            "matchExpressions[0]."
            "key",
            docs[0],
        )
        assert "ssd" == jmespath.search(
            "spec.jobTemplate.spec.template.spec.nodeSelector.diskType",
            docs[0],
        )
        assert "dynamic-pods" == jmespath.search(
            "spec.jobTemplate.spec.template.spec.tolerations[0].key",
            docs[0],
        )
