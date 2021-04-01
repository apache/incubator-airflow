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


class StatsdTest(unittest.TestCase):
    def test_should_create_statsd_default(self):
        docs = render_chart(show_only=["templates/statsd/statsd-deployment.yaml"])

        assert "RELEASE-NAME-statsd" == jmespath.search("metadata.name", docs[0])

        assert "statsd" == jmespath.search("spec.template.spec.containers[0].name", docs[0])

    def test_should_add_volume_and_volume_mount_when_exist_extra_mappings(self):
        extra_mapping = {
            "match": "airflow.pool.queued_slots.*",
            "name": "airflow_pool_queued_slots",
            "labels": {"pool": "$1"},
        }
        docs = render_chart(
            values={"statsd": {"enabled": True, "extraMappings": [extra_mapping]}},
            show_only=["templates/statsd/statsd-deployment.yaml"],
        )

        assert {"name": "config", "configMap": {"name": "RELEASE-NAME-statsd"}} in jmespath.search(
            "spec.template.spec.volumes", docs[0]
        )

        assert {
            "name": "config",
            "mountPath": "/etc/statsd-exporter/mappings.yml",
            "subPath": "mappings.yml",
        } in jmespath.search("spec.template.spec.containers[0].volumeMounts", docs[0])
