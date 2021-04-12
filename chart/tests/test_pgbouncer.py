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


class PgbouncerTest(unittest.TestCase):
    def test_should_create_pgbouncer(self):
        docs = render_chart(
            values={"pgbouncer": {"enabled": True}},
            show_only=["templates/pgbouncer/pgbouncer-deployment.yaml"],
        )

        assert "RELEASE-NAME-pgbouncer" == jmespath.search("metadata.name", docs[0])

        assert "pgbouncer" == jmespath.search("spec.template.spec.containers[0].name", docs[0])

    def test_should_create_valid_affinity_tolerations_and_node_selector(self):
        docs = render_chart(
            values={
                "pgbouncer": {
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
            show_only=["templates/pgbouncer/pgbouncer-deployment.yaml"],
        )

        assert "Deployment" == jmespath.search("kind", docs[0])
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

    def test_no_existing_secret(self):
        docs = render_chart(
            "TEST-PGBOUNCER-CONFIG",
            values={
                "pgbouncer": {"enabled": True},
            },
            show_only=["templates/pgbouncer/pgbouncer-deployment.yaml"],
        )

        assert {
            "name": "pgbouncer-config",
            "secret": {"secretName": "TEST-PGBOUNCER-CONFIG-pgbouncer-config"},
        } == jmespath.search("spec.template.spec.volumes[0]", docs[0])

    def test_existing_secret(self):
        docs = render_chart(
            "TEST-PGBOUNCER-CONFIG",
            values={
                "pgbouncer": {"enabled": True, "configSecretName": "pgbouncer-config-secret"},
            },
            show_only=["templates/pgbouncer/pgbouncer-deployment.yaml"],
        )

        assert {
            "name": "pgbouncer-config",
            "secret": {"secretName": "pgbouncer-config-secret"},
        } == jmespath.search("spec.template.spec.volumes[0]", docs[0])
