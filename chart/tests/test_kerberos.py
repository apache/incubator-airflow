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

import json
import unittest

import jmespath

from tests.helm_template_generator import render_chart


class KerberosTest(unittest.TestCase):
    def test_kerberos_not_mentioned_in_render_if_disabled(self):
        k8s_objects = render_chart(name="NO-KERBEROS", values={"kerberos": {'enabled': False}})
        # ignore airflow config map
        k8s_objects_to_consider = [
            obj for obj in k8s_objects if obj["metadata"]["name"] != "NO-KERBEROS-airflow-config"
        ]
        k8s_objects_to_consider_str = json.dumps(k8s_objects_to_consider)
        assert "kerberos" not in k8s_objects_to_consider_str

    def test_kerberos_envs_available_in_worker_with_persistence(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "workers": {
                    "kerberosSidecar": {"enabled": True},
                    "persistence": {
                        "enabled": True,
                    },
                },
                "kerberos": {
                    "enabled": True,
                    "configPath": "/etc/krb5.conf",
                    "ccacheMountPath": "/var/kerberos-ccache",
                    "ccacheFileName": "ccache",
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert {"name": "KRB5_CONFIG", "value": "/etc/krb5.conf"} in jmespath.search(
            "spec.template.spec.containers[0].env", docs[0]
        )
        assert {"name": "KRB5CCNAME", "value": "/var/kerberos-ccache/ccache"} in jmespath.search(
            "spec.template.spec.containers[0].env", docs[0]
        )

    def test_kerberos_sidecar_resources(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "workers": {
                    "kerberosSidecar": {
                        "enabled": True,
                        "resources": {
                            "requests": {
                                "cpu": "200m",
                                "memory": "200Mi",
                            },
                            "limits": {
                                "cpu": "201m",
                                "memory": "201Mi",
                            },
                        },
                    },
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[2].resources.requests.cpu", docs[0]) == "200m"
        assert (
            jmespath.search("spec.template.spec.containers[2].resources.requests.memory", docs[0]) == "200Mi"
        )
        assert jmespath.search("spec.template.spec.containers[2].resources.limits.cpu", docs[0]) == "201m"
        assert jmespath.search("spec.template.spec.containers[2].resources.limits.memory", docs[0]) == "201Mi"
