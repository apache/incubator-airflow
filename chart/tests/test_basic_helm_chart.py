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
from subprocess import CalledProcessError
from typing import Any, Dict, List, Union
from unittest import mock

import jmespath
from parameterized import parameterized

from tests.helm_template_generator import render_chart

OBJECT_COUNT_IN_BASIC_DEPLOYMENT = 22


class TestBaseChartTest(unittest.TestCase):
    def test_basic_deployments(self):
        k8s_objects = render_chart(
            "TEST-BASIC",
            values={
                "chart": {
                    'metadata': 'AA',
                },
                'labels': {"TEST-LABEL": "TEST-VALUE"},
            },
        )
        list_of_kind_names_tuples = [
            (k8s_object['kind'], k8s_object['metadata']['name']) for k8s_object in k8s_objects
        ]
        assert list_of_kind_names_tuples == [
            ('ServiceAccount', 'TEST-BASIC-scheduler'),
            ('ServiceAccount', 'TEST-BASIC-webserver'),
            ('ServiceAccount', 'TEST-BASIC-worker'),
            ('Secret', 'TEST-BASIC-postgresql'),
            ('Secret', 'TEST-BASIC-airflow-metadata'),
            ('Secret', 'TEST-BASIC-airflow-result-backend'),
            ('ConfigMap', 'TEST-BASIC-airflow-config'),
            ('Role', 'TEST-BASIC-pod-launcher-role'),
            ('Role', 'TEST-BASIC-pod-log-reader-role'),
            ('RoleBinding', 'TEST-BASIC-pod-launcher-rolebinding'),
            ('RoleBinding', 'TEST-BASIC-pod-log-reader-rolebinding'),
            ('Service', 'TEST-BASIC-postgresql-headless'),
            ('Service', 'TEST-BASIC-postgresql'),
            ('Service', 'TEST-BASIC-statsd'),
            ('Service', 'TEST-BASIC-webserver'),
            ('Deployment', 'TEST-BASIC-scheduler'),
            ('Deployment', 'TEST-BASIC-statsd'),
            ('Deployment', 'TEST-BASIC-webserver'),
            ('StatefulSet', 'TEST-BASIC-postgresql'),
            ('Secret', 'TEST-BASIC-fernet-key'),
            ('Job', 'TEST-BASIC-create-user'),
            ('Job', 'TEST-BASIC-run-airflow-migrations'),
        ]
        assert OBJECT_COUNT_IN_BASIC_DEPLOYMENT == len(k8s_objects)
        for k8s_object in k8s_objects:
            labels = jmespath.search('metadata.labels', k8s_object) or {}
            if 'postgresql' in labels.get('chart'):
                continue
            k8s_name = k8s_object['kind'] + ":" + k8s_object['metadata']['name']
            assert 'TEST-VALUE' == labels.get(
                "TEST-LABEL"
            ), f"Missing label TEST-LABEL on {k8s_name}. Current labels: {labels}"

    def test_basic_deployment_without_default_users(self):
        k8s_objects = render_chart("TEST-BASIC", {"webserver": {'defaultUser': {'enabled': False}}})
        list_of_kind_names_tuples = [
            (k8s_object['kind'], k8s_object['metadata']['name']) for k8s_object in k8s_objects
        ]
        assert ('Job', 'TEST-BASIC-create-user') not in list_of_kind_names_tuples
        assert OBJECT_COUNT_IN_BASIC_DEPLOYMENT - 1 == len(k8s_objects)

    def test_network_policies_are_valid(self):
        k8s_objects = render_chart(
            "TEST-BASIC",
            {
                "networkPolicies": {"enabled": True},
                "executor": "CeleryExecutor",
                "pgbouncer": {"enabled": True},
            },
        )
        kind_names_tuples = {
            (k8s_object['kind'], k8s_object['metadata']['name']) for k8s_object in k8s_objects
        }

        expected_kind_names = [
            ('NetworkPolicy', 'TEST-BASIC-redis-policy'),
            ('NetworkPolicy', 'TEST-BASIC-flower-policy'),
            ('NetworkPolicy', 'TEST-BASIC-pgbouncer-policy'),
            ('NetworkPolicy', 'TEST-BASIC-scheduler-policy'),
            ('NetworkPolicy', 'TEST-BASIC-statsd-policy'),
            ('NetworkPolicy', 'TEST-BASIC-webserver-policy'),
            ('NetworkPolicy', 'TEST-BASIC-worker-policy'),
        ]
        for kind_name in expected_kind_names:
            assert kind_name in kind_names_tuples

    def test_labels_are_valid(self):
        """Test labels are correctly applied on all objects created by this chart"""
        release_name = "TEST-BASIC"
        k8s_objects = render_chart(
            name=release_name,
            values={
                "labels": {"label1": "value1", "label2": "value2"},
                "executor": "CeleryExecutor",
                "pgbouncer": {"enabled": True},
                "redis": {"enabled": True},
                "networkPolicies": {"enabled": True},
            },
        )
        kind_k8s_obj_labels_tuples = {
            (k8s_object['metadata']['name'], k8s_object['kind']): k8s_object['metadata']['labels']
            for k8s_object in k8s_objects
        }

        kind_names_tuples = [
            (f"{release_name}-airflow-config", "ConfigMap", "config"),
            (f"{release_name}-create-user", "Job", "create-user-job"),
            (f"{release_name}-flower", "Deployment", "flower"),
            (f"{release_name}-flower", "Service", "flower"),
            (f"{release_name}-flower-policy", "NetworkPolicy", "airflow-flower-policy"),
            (f"{release_name}-pgbouncer", "Deployment", "pgbouncer"),
            (f"{release_name}-pgbouncer", "Service", "pgbouncer"),
            (f"{release_name}-pgbouncer-policy", "NetworkPolicy", "airflow-pgbouncer-policy"),
            (f"{release_name}-redis", "Service", "redis"),
            (f"{release_name}-redis", "StatefulSet", "redis"),
            (f"{release_name}-redis-policy", "NetworkPolicy", "redis-policy"),
            (f"{release_name}-run-airflow-migrations", "Job", "run-airflow-migrations"),
            (f"{release_name}-scheduler", "Deployment", "scheduler"),
            (f"{release_name}-scheduler-policy", "NetworkPolicy", "airflow-scheduler-policy"),
            (f"{release_name}-statsd", "Deployment", "statsd"),
            (f"{release_name}-statsd", "Service", "statsd"),
            (f"{release_name}-statsd-policy", "NetworkPolicy", "statsd-policy"),
            (f"{release_name}-webserver", "Deployment", "webserver"),
            (f"{release_name}-webserver", "Service", "webserver"),
            (f"{release_name}-webserver-policy", "NetworkPolicy", "airflow-webserver-policy"),
            (f"{release_name}-worker", "Service", "worker"),
            (f"{release_name}-worker", "StatefulSet", "worker"),
            (f"{release_name}-worker-policy", "NetworkPolicy", "airflow-worker-policy"),
        ]
        for k8s_object_name, kind, component in kind_names_tuples:
            assert kind_k8s_obj_labels_tuples[(k8s_object_name, kind)] == {
                "label1": "value1",
                "label2": "value2",
                "tier": "airflow",
                "release": release_name,
                "component": component,
                "heritage": "Helm",
                "chart": mock.ANY,
            }

    def test_annotations_on_airflow_pods_in_deployment(self):
        """
        Test Annotations are correctly applied on all pods created Scheduler, Webserver & Worker
        deployments.
        """
        release_name = "TEST-BASIC"
        k8s_objects = render_chart(
            name=release_name,
            values={
                "airflowPodAnnotations": {"test-annotation/safe-to-evict": "true"},
                "executor": "CeleryExecutor",
            },
            show_only=[
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
                "templates/webserver/webserver-deployment.yaml",
            ],
        )

        assert len(k8s_objects) == 3

        for k8s_object in k8s_objects:
            annotations = k8s_object["spec"]["template"]["metadata"]["annotations"]
            assert "test-annotation/safe-to-evict" in annotations
            assert "true" in annotations.get("test-annotation/safe-to-evict")

    def test_chart_is_consistent_with_official_airflow_image(self):
        def get_k8s_objs_with_image(obj: Union[List[Any], Dict[str, Any]]) -> List[Dict[str, Any]]:
            """
            Recursive helper to retrieve all the k8s objects that have an "image" key
            inside k8s obj or list of k8s obj
            """
            out = []
            if isinstance(obj, list):
                for item in obj:
                    out += get_k8s_objs_with_image(item)
            if isinstance(obj, dict):
                if "image" in obj:
                    out += [obj]
                # include sub objs, just in case
                for val in obj.values():
                    out += get_k8s_objs_with_image(val)
            return out

        image_repo = "test-airflow-repo/airflow"
        k8s_objects = render_chart("TEST-BASIC", {"defaultAirflowRepository": image_repo})

        objs_with_image = get_k8s_objs_with_image(k8s_objects)
        for obj in objs_with_image:
            image: str = obj["image"]  # pylint: disable=invalid-sequence-index
            if image.startswith(image_repo):
                # Make sure that a command is not specified
                assert "command" not in obj

    def test_unsupported_executor(self):
        with self.assertRaises(CalledProcessError) as ex_ctx:
            render_chart(
                "TEST-BASIC",
                {
                    "executor": "SequentialExecutor",
                },
            )
        assert (
            'executor must be one of the following: "LocalExecutor", "CeleryExecutor", '
            '"KubernetesExecutor", "CeleryKubernetesExecutor"' in ex_ctx.exception.stderr.decode()
        )

    @parameterized.expand(
        [
            ("airflow",),
            ("pod_template",),
            ("flower",),
            ("statsd",),
            ("redis",),
            ("pgbouncer",),
            ("pgbouncerExporter",),
            ("gitSync",),
        ]
    )
    def test_invalid_pull_policy(self, image):
        with self.assertRaises(CalledProcessError) as ex_ctx:
            render_chart(
                "TEST-BASIC",
                {
                    "images": {image: {"pullPolicy": "InvalidPolicy"}},
                },
            )
        assert (
            'pullPolicy must be one of the following: "Always", "Never", "IfNotPresent"'
            in ex_ctx.exception.stderr.decode()
        )

    def test_invalid_dags_access_mode(self):
        with self.assertRaises(CalledProcessError) as ex_ctx:
            render_chart(
                "TEST-BASIC",
                {
                    "dags": {"persistence": {"accessMode": "InvalidMode"}},
                },
            )
        assert (
            'accessMode must be one of the following: "ReadWriteOnce", "ReadOnlyMany", "ReadWriteMany"'
            in ex_ctx.exception.stderr.decode()
        )
