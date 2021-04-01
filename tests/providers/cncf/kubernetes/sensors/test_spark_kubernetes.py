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

import json
import unittest
from unittest.mock import patch

import pytest
from kubernetes.client.rest import ApiException

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.utils import db, timezone

TEST_COMPLETED_APPLICATION = {
    "apiVersion": "sparkoperator.k8s.io/v1beta2",
    "kind": "SparkApplication",
    "metadata": {
        "creationTimestamp": "2020-02-24T07:34:22Z",
        "generation": 1,
        "labels": {"spark_flow_name": "spark-pi"},
        "name": "spark-pi-2020-02-24-1",
        "namespace": "default",
        "resourceVersion": "455577",
        "selfLink": "/apis/sparkoperator.k8s.io/v1beta2/namespaces/default/sparkapplications/spark-pi",
        "uid": "9f825516-6e1a-4af1-8967-b05661e8fb08",
    },
    "spec": {
        "driver": {
            "coreLimit": "1200m",
            "cores": 1,
            "labels": {"spark_flow_name": "spark-pi", "version": "2.4.4"},
            "memory": "512m",
            "serviceAccount": "default",
            "volumeMounts": [{"mountPath": "/tmp", "name": "test-volume"}],
        },
        "executor": {
            "cores": 1,
            "instances": 3,
            "labels": {"spark_flow_name": "spark-pi", "version": "2.4.4"},
            "memory": "512m",
            "volumeMounts": [{"mountPath": "/tmp", "name": "test-volume"}],
        },
        "image": "gcr.io/spark-operator/spark:v2.4.4",
        "imagePullPolicy": "Always",
        "mainApplicationFile": "local:///opt/spark/examples/jars/spark-examples_2.11-2.4.4.jar",
        "mainClass": "org.apache.spark.examples.SparkPi",
        "mode": "cluster",
        "restartPolicy": {"type": "Never"},
        "sparkVersion": "2.4.4",
        "type": "Scala",
        "volumes": [{"hostPath": {"path": "/tmp", "type": "Directory"}, "name": "test-volume"}],
    },
    "status": {
        "applicationState": {"state": "COMPLETED"},
        "driverInfo": {
            "podName": "spark-pi-2020-02-24-1-driver",
            "webUIAddress": "10.97.130.44:4040",
            "webUIPort": 4040,
            "webUIServiceName": "spark-pi-2020-02-24-1-ui-svc",
        },
        "executionAttempts": 1,
        "executorState": {
            "spark-pi-2020-02-24-1-1582529666227-exec-1": "FAILED",
            "spark-pi-2020-02-24-1-1582529666227-exec-2": "FAILED",
            "spark-pi-2020-02-24-1-1582529666227-exec-3": "FAILED",
        },
        "lastSubmissionAttemptTime": "2020-02-24T07:34:30Z",
        "sparkApplicationId": "spark-7bb432c422ca46f3854838c419460fec",
        "submissionAttempts": 1,
        "submissionID": "1a1f9c5e-6bdd-4824-806f-40a814c1cf43",
        "terminationTime": "2020-02-24T07:35:01Z",
    },
}

TEST_FAILED_APPLICATION = {
    "apiVersion": "sparkoperator.k8s.io/v1beta2",
    "kind": "SparkApplication",
    "metadata": {
        "creationTimestamp": "2020-02-26T11:59:30Z",
        "generation": 1,
        "name": "spark-pi",
        "namespace": "default",
        "resourceVersion": "531657",
        "selfLink": "/apis/sparkoperator.k8s.io/v1beta2/namespaces/default/sparkapplications/spark-pi",
        "uid": "f507ee3a-4461-45ef-86d8-ff42e4211e7d",
    },
    "spec": {
        "arguments": ["100000"],
        "driver": {
            "coreLimit": "1200m",
            "cores": 1,
            "labels": {"version": "2.4.4"},
            "memory": "512m",
            "serviceAccount": "default",
        },
        "executor": {
            "cores": 1,
            "instances": 1,
            "labels": {"version": "2.4.4"},
            "memory": "512m",
        },
        "image": "gcr.io/spark-operator/spark:v2.4.4-gcs-prometheus",
        "imagePullPolicy": "Always",
        "mainApplicationFile": "local:///opt/spark/examples/jars/spark-examples_2.11-2.4.4.jar",
        "mainClass": "org.apache.spark.examples.SparkPi123",
        "mode": "cluster",
        "monitoring": {
            "exposeDriverMetrics": True,
            "exposeExecutorMetrics": True,
            "prometheus": {
                "jmxExporterJar": "/prometheus/jmx_prometheus_javaagent-0.11.0.jar",
                "port": 8090,
            },
        },
        "restartPolicy": {"type": "Never"},
        "sparkVersion": "2.4.4",
        "type": "Scala",
    },
    "status": {
        "applicationState": {
            "errorMessage": "driver pod failed with ExitCode: 101, Reason: Error",
            "state": "FAILED",
        },
        "driverInfo": {
            "podName": "spark-pi-driver",
            "webUIAddress": "10.108.18.168:4040",
            "webUIPort": 4040,
            "webUIServiceName": "spark-pi-ui-svc",
        },
        "executionAttempts": 1,
        "lastSubmissionAttemptTime": "2020-02-26T11:59:38Z",
        "sparkApplicationId": "spark-5fb7445d988f434cbe1e86166a0c038a",
        "submissionAttempts": 1,
        "submissionID": "26654a75-5bf6-4618-b191-0340280d2d3d",
        "terminationTime": "2020-02-26T11:59:49Z",
    },
}

TEST_UNKNOWN_APPLICATION = {
    "apiVersion": "sparkoperator.k8s.io/v1beta2",
    "kind": "SparkApplication",
    "metadata": {
        "creationTimestamp": "2020-02-24T07:34:22Z",
        "generation": 1,
        "labels": {"spark_flow_name": "spark-pi"},
        "name": "spark-pi-2020-02-24-1",
        "namespace": "default",
        "resourceVersion": "455577",
        "selfLink": "/apis/sparkoperator.k8s.io/v1beta2/namespaces/default/sparkapplications/spark-pi",
        "uid": "9f825516-6e1a-4af1-8967-b05661e8fb08",
    },
    "spec": {
        "driver": {
            "coreLimit": "1200m",
            "cores": 1,
            "labels": {"spark_flow_name": "spark-pi", "version": "2.4.4"},
            "memory": "512m",
            "serviceAccount": "default",
            "volumeMounts": [{"mountPath": "/tmp", "name": "test-volume"}],
        },
        "executor": {
            "cores": 1,
            "instances": 3,
            "labels": {"spark_flow_name": "spark-pi", "version": "2.4.4"},
            "memory": "512m",
            "volumeMounts": [{"mountPath": "/tmp", "name": "test-volume"}],
        },
        "image": "gcr.io/spark-operator/spark:v2.4.4",
        "imagePullPolicy": "Always",
        "mainApplicationFile": "local:///opt/spark/examples/jars/spark-examples_2.11-2.4.4.jar",
        "mainClass": "org.apache.spark.examples.SparkPi",
        "mode": "cluster",
        "restartPolicy": {"type": "Never"},
        "sparkVersion": "2.4.4",
        "type": "Scala",
        "volumes": [{"hostPath": {"path": "/tmp", "type": "Directory"}, "name": "test-volume"}],
    },
    "status": {
        "applicationState": {"state": "UNKNOWN"},
        "driverInfo": {
            "podName": "spark-pi-2020-02-24-1-driver",
            "webUIAddress": "10.97.130.44:4040",
            "webUIPort": 4040,
            "webUIServiceName": "spark-pi-2020-02-24-1-ui-svc",
        },
        "executionAttempts": 1,
        "executorState": {
            "spark-pi-2020-02-24-1-1582529666227-exec-1": "FAILED",
            "spark-pi-2020-02-24-1-1582529666227-exec-2": "FAILED",
            "spark-pi-2020-02-24-1-1582529666227-exec-3": "FAILED",
        },
        "lastSubmissionAttemptTime": "2020-02-24T07:34:30Z",
        "sparkApplicationId": "spark-7bb432c422ca46f3854838c419460fec",
        "submissionAttempts": 1,
        "submissionID": "1a1f9c5e-6bdd-4824-806f-40a814c1cf43",
        "terminationTime": "2020-02-24T07:35:01Z",
    },
}
TEST_NOT_PROCESSED_APPLICATION = {
    "apiVersion": "sparkoperator.k8s.io/v1beta2",
    "kind": "SparkApplication",
    "metadata": {
        "creationTimestamp": "2020-02-26T09:14:48Z",
        "generation": 1,
        "name": "spark-pi",
        "namespace": "default",
        "resourceVersion": "525235",
        "selfLink": "/apis/sparkoperator.k8s.io/v1beta2/namespaces/default/sparkapplications/spark-pi",
        "uid": "58da0778-fa72-4e90-8ddc-18b5e658f93d",
    },
    "spec": {
        "arguments": ["100000"],
        "driver": {
            "coreLimit": "1200m",
            "cores": 1,
            "labels": {"version": "2.4.4"},
            "memory": "512m",
            "serviceAccount": "default",
        },
        "executor": {
            "cores": 1,
            "instances": 1,
            "labels": {"version": "2.4.4"},
            "memory": "512m",
        },
        "image": "gcr.io/spark-operator/spark:v2.4.4-gcs-prometheus",
        "imagePullPolicy": "Always",
        "mainApplicationFile": "local:///opt/spark/examples/jars/spark-examples_2.11-2.4.4.jar",
        "mainClass": "org.apache.spark.examples.SparkPi",
        "mode": "cluster",
        "monitoring": {
            "exposeDriverMetrics": True,
            "exposeExecutorMetrics": True,
            "prometheus": {
                "jmxExporterJar": "/prometheus/jmx_prometheus_javaagent-0.11.0.jar",
                "port": 8090,
            },
        },
        "restartPolicy": {"type": "Never"},
        "sparkVersion": "2.4.4",
        "type": "Scala",
    },
}

TEST_RUNNING_APPLICATION = {
    "apiVersion": "sparkoperator.k8s.io/v1beta2",
    "kind": "SparkApplication",
    "metadata": {
        "creationTimestamp": "2020-02-26T09:11:25Z",
        "generation": 1,
        "name": "spark-pi",
        "namespace": "default",
        "resourceVersion": "525001",
        "selfLink": "/apis/sparkoperator.k8s.io/v1beta2/namespaces/default/sparkapplications/spark-pi",
        "uid": "95ff1418-eeb5-454c-b59e-9e021aa3a239",
    },
    "spec": {
        "arguments": ["100000"],
        "driver": {
            "coreLimit": "1200m",
            "cores": 1,
            "labels": {"version": "2.4.4"},
            "memory": "512m",
            "serviceAccount": "default",
        },
        "executor": {
            "cores": 1,
            "instances": 1,
            "labels": {"version": "2.4.4"},
            "memory": "512m",
        },
        "image": "gcr.io/spark-operator/spark:v2.4.4-gcs-prometheus",
        "imagePullPolicy": "Always",
        "mainApplicationFile": "local:///opt/spark/examples/jars/spark-examples_2.11-2.4.4.jar",
        "mainClass": "org.apache.spark.examples.SparkPi",
        "mode": "cluster",
        "monitoring": {
            "exposeDriverMetrics": True,
            "exposeExecutorMetrics": True,
            "prometheus": {
                "jmxExporterJar": "/prometheus/jmx_prometheus_javaagent-0.11.0.jar",
                "port": 8090,
            },
        },
        "restartPolicy": {"type": "Never"},
        "sparkVersion": "2.4.4",
        "type": "Scala",
    },
    "status": {
        "applicationState": {"state": "RUNNING"},
        "driverInfo": {
            "podName": "spark-pi-driver",
            "webUIAddress": "10.106.36.53:4040",
            "webUIPort": 4040,
            "webUIServiceName": "spark-pi-ui-svc",
        },
        "executionAttempts": 1,
        "executorState": {"spark-pi-1582708290692-exec-1": "RUNNING"},
        "lastSubmissionAttemptTime": "2020-02-26T09:11:35Z",
        "sparkApplicationId": "spark-a47a002df46448f1a8395d7dd79ba448",
        "submissionAttempts": 1,
        "submissionID": "d4f5a768-b9d1-4a79-92b0-54779124d997",
        "terminationTime": None,
    },
}

TEST_SUBMITTED_APPLICATION = {
    "apiVersion": "sparkoperator.k8s.io/v1beta2",
    "kind": "SparkApplication",
    "metadata": {
        "creationTimestamp": "2020-02-26T09:16:53Z",
        "generation": 1,
        "name": "spark-pi",
        "namespace": "default",
        "resourceVersion": "525536",
        "selfLink": "/apis/sparkoperator.k8s.io/v1beta2/namespaces/default/sparkapplications/spark-pi",
        "uid": "424a682b-6e5c-40d5-8a41-164253500b58",
    },
    "spec": {
        "arguments": ["100000"],
        "driver": {
            "coreLimit": "1200m",
            "cores": 1,
            "labels": {"version": "2.4.4"},
            "memory": "512m",
            "serviceAccount": "default",
        },
        "executor": {
            "cores": 1,
            "instances": 1,
            "labels": {"version": "2.4.4"},
            "memory": "512m",
        },
        "image": "gcr.io/spark-operator/spark:v2.4.4-gcs-prometheus",
        "imagePullPolicy": "Always",
        "mainApplicationFile": "local:///opt/spark/examples/jars/spark-examples_2.11-2.4.4.jar",
        "mainClass": "org.apache.spark.examples.SparkPi",
        "mode": "cluster",
        "monitoring": {
            "exposeDriverMetrics": True,
            "exposeExecutorMetrics": True,
            "prometheus": {
                "jmxExporterJar": "/prometheus/jmx_prometheus_javaagent-0.11.0.jar",
                "port": 8090,
            },
        },
        "restartPolicy": {"type": "Never"},
        "sparkVersion": "2.4.4",
        "type": "Scala",
    },
    "status": {
        "applicationState": {"state": "SUBMITTED"},
        "driverInfo": {
            "podName": "spark-pi-driver",
            "webUIAddress": "10.108.175.17:4040",
            "webUIPort": 4040,
            "webUIServiceName": "spark-pi-ui-svc",
        },
        "executionAttempts": 1,
        "lastSubmissionAttemptTime": "2020-02-26T09:17:03Z",
        "sparkApplicationId": "spark-ae1a522d200246a99470743e880c5650",
        "submissionAttempts": 1,
        "submissionID": "f8b70b0b-3c81-403f-8c6d-e7f6c3653409",
        "terminationTime": None,
    },
}

TEST_NEW_APPLICATION = {
    "apiVersion": "sparkoperator.k8s.io/v1beta2",
    "kind": "SparkApplication",
    "metadata": {
        "creationTimestamp": "2020-02-26T09:16:53Z",
        "generation": 1,
        "name": "spark-pi",
        "namespace": "default",
        "resourceVersion": "525536",
        "selfLink": "/apis/sparkoperator.k8s.io/v1beta2/namespaces/default/sparkapplications/spark-pi",
        "uid": "424a682b-6e5c-40d5-8a41-164253500b58",
    },
    "spec": {
        "arguments": ["100000"],
        "driver": {
            "coreLimit": "1200m",
            "cores": 1,
            "labels": {"version": "2.4.4"},
            "memory": "512m",
            "serviceAccount": "default",
        },
        "executor": {
            "cores": 1,
            "instances": 1,
            "labels": {"version": "2.4.4"},
            "memory": "512m",
        },
        "image": "gcr.io/spark-operator/spark:v2.4.4-gcs-prometheus",
        "imagePullPolicy": "Always",
        "mainApplicationFile": "local:///opt/spark/examples/jars/spark-examples_2.11-2.4.4.jar",
        "mainClass": "org.apache.spark.examples.SparkPi",
        "mode": "cluster",
        "monitoring": {
            "exposeDriverMetrics": True,
            "exposeExecutorMetrics": True,
            "prometheus": {
                "jmxExporterJar": "/prometheus/jmx_prometheus_javaagent-0.11.0.jar",
                "port": 8090,
            },
        },
        "restartPolicy": {"type": "Never"},
        "sparkVersion": "2.4.4",
        "type": "Scala",
    },
    "status": {"applicationState": {"state": ""}},
}

TEST_PENDING_RERUN_APPLICATION = {
    "apiVersion": "sparkoperator.k8s.io/v1beta2",
    "kind": "SparkApplication",
    "metadata": {
        "creationTimestamp": "2020-02-27T08:03:02Z",
        "generation": 4,
        "name": "spark-pi",
        "namespace": "default",
        "resourceVersion": "552073",
        "selfLink": "/apis/sparkoperator.k8s.io/v1beta2/namespaces/default/sparkapplications/spark-pi",
        "uid": "0c93527d-4dd9-4006-b40a-1672872e8d6f",
    },
    "spec": {
        "arguments": ["100000"],
        "driver": {
            "coreLimit": "1200m",
            "cores": 1,
            "labels": {"version": "2.4.4"},
            "memory": "512m",
            "serviceAccount": "default",
        },
        "executor": {
            "cores": 1,
            "instances": 1,
            "labels": {"version": "2.4.4"},
            "memory": "512m",
        },
        "image": "gcr.io/spark-operator/spark:v2.4.4-gcs-prometheus",
        "imagePullPolicy": "Always",
        "mainApplicationFile": "local:///opt/spark/examples/jars/spark-examples_2.11-2.4.4.jar",
        "mainClass": "org.apache.spark.examples.SparkPi",
        "mode": "cluster",
        "monitoring": {
            "exposeDriverMetrics": True,
            "exposeExecutorMetrics": True,
            "prometheus": {
                "jmxExporterJar": "/prometheus/jmx_prometheus_javaagent-0.11.0.jar",
                "port": 8090,
            },
        },
        "restartPolicy": {"type": "Never"},
        "sparkVersion": "2.4.4",
        "type": "Scala",
    },
    "status": {
        "applicationState": {"state": "PENDING_RERUN"},
        "driverInfo": {},
        "lastSubmissionAttemptTime": None,
        "terminationTime": None,
    },
}

TEST_POD_LOGS = [b"LOG LINE 1\n", b"LOG LINE 2"]
TEST_POD_LOG_RESULT = "LOG LINE 1\nLOG LINE 2"


@patch("airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook.get_conn")
class TestSparkKubernetesSensor(unittest.TestCase):
    def setUp(self):
        db.merge_conn(Connection(conn_id='kubernetes_default', conn_type='kubernetes', extra=json.dumps({})))
        db.merge_conn(
            Connection(
                conn_id="kubernetes_default",
                conn_type="kubernetes",
                extra=json.dumps({}),
            )
        )
        db.merge_conn(
            Connection(
                conn_id="kubernetes_with_namespace",
                conn_type="kubernetes",
                extra=json.dumps({"extra__kubernetes__namespace": "mock_namespace"}),
            )
        )
        args = {"owner": "airflow", "start_date": timezone.datetime(2020, 2, 1)}
        self.dag = DAG("test_dag_id", default_args=args)

    @patch(
        "kubernetes.client.api.custom_objects_api.CustomObjectsApi.get_namespaced_custom_object",
        return_value=TEST_COMPLETED_APPLICATION,
    )
    def test_completed_application(self, mock_get_namespaced_crd, mock_kubernetes_hook):
        sensor = SparkKubernetesSensor(application_name="spark_pi", dag=self.dag, task_id="test_task_id")
        assert sensor.poke(None)
        mock_kubernetes_hook.assert_called_once_with()
        mock_get_namespaced_crd.assert_called_once_with(
            group="sparkoperator.k8s.io",
            name="spark_pi",
            namespace="default",
            plural="sparkapplications",
            version="v1beta2",
        )

    @patch(
        "kubernetes.client.api.custom_objects_api.CustomObjectsApi.get_namespaced_custom_object",
        return_value=TEST_FAILED_APPLICATION,
    )
    def test_failed_application(self, mock_get_namespaced_crd, mock_kubernetes_hook):
        sensor = SparkKubernetesSensor(application_name="spark_pi", dag=self.dag, task_id="test_task_id")
        with pytest.raises(AirflowException):
            sensor.poke(None)
        mock_kubernetes_hook.assert_called_once_with()
        mock_get_namespaced_crd.assert_called_once_with(
            group="sparkoperator.k8s.io",
            name="spark_pi",
            namespace="default",
            plural="sparkapplications",
            version="v1beta2",
        )

    @patch(
        "kubernetes.client.api.custom_objects_api.CustomObjectsApi.get_namespaced_custom_object",
        return_value=TEST_NOT_PROCESSED_APPLICATION,
    )
    def test_not_processed_application(self, mock_get_namespaced_crd, mock_kubernetes_hook):
        sensor = SparkKubernetesSensor(application_name="spark_pi", dag=self.dag, task_id="test_task_id")
        assert not sensor.poke(None)
        mock_kubernetes_hook.assert_called_once_with()
        mock_get_namespaced_crd.assert_called_once_with(
            group="sparkoperator.k8s.io",
            name="spark_pi",
            namespace="default",
            plural="sparkapplications",
            version="v1beta2",
        )

    @patch(
        "kubernetes.client.api.custom_objects_api.CustomObjectsApi.get_namespaced_custom_object",
        return_value=TEST_NEW_APPLICATION,
    )
    def test_new_application(self, mock_get_namespaced_crd, mock_kubernetes_hook):
        sensor = SparkKubernetesSensor(application_name="spark_pi", dag=self.dag, task_id="test_task_id")
        assert not sensor.poke(None)
        mock_kubernetes_hook.assert_called_once_with()
        mock_get_namespaced_crd.assert_called_once_with(
            group="sparkoperator.k8s.io",
            name="spark_pi",
            namespace="default",
            plural="sparkapplications",
            version="v1beta2",
        )

    @patch(
        "kubernetes.client.api.custom_objects_api.CustomObjectsApi.get_namespaced_custom_object",
        return_value=TEST_RUNNING_APPLICATION,
    )
    def test_running_application(self, mock_get_namespaced_crd, mock_kubernetes_hook):
        sensor = SparkKubernetesSensor(application_name="spark_pi", dag=self.dag, task_id="test_task_id")
        assert not sensor.poke(None)
        mock_kubernetes_hook.assert_called_once_with()
        mock_get_namespaced_crd.assert_called_once_with(
            group="sparkoperator.k8s.io",
            name="spark_pi",
            namespace="default",
            plural="sparkapplications",
            version="v1beta2",
        )

    @patch(
        "kubernetes.client.api.custom_objects_api.CustomObjectsApi.get_namespaced_custom_object",
        return_value=TEST_SUBMITTED_APPLICATION,
    )
    def test_submitted_application(self, mock_get_namespaced_crd, mock_kubernetes_hook):
        sensor = SparkKubernetesSensor(application_name="spark_pi", dag=self.dag, task_id="test_task_id")
        assert not sensor.poke(None)
        mock_kubernetes_hook.assert_called_once_with()
        mock_get_namespaced_crd.assert_called_once_with(
            group="sparkoperator.k8s.io",
            name="spark_pi",
            namespace="default",
            plural="sparkapplications",
            version="v1beta2",
        )

    @patch(
        "kubernetes.client.api.custom_objects_api.CustomObjectsApi.get_namespaced_custom_object",
        return_value=TEST_PENDING_RERUN_APPLICATION,
    )
    def test_pending_rerun_application(self, mock_get_namespaced_crd, mock_kubernetes_hook):
        sensor = SparkKubernetesSensor(application_name="spark_pi", dag=self.dag, task_id="test_task_id")
        assert not sensor.poke(None)
        mock_kubernetes_hook.assert_called_once_with()
        mock_get_namespaced_crd.assert_called_once_with(
            group="sparkoperator.k8s.io",
            name="spark_pi",
            namespace="default",
            plural="sparkapplications",
            version="v1beta2",
        )

    @patch(
        "kubernetes.client.api.custom_objects_api.CustomObjectsApi.get_namespaced_custom_object",
        return_value=TEST_UNKNOWN_APPLICATION,
    )
    def test_unknown_application(self, mock_get_namespaced_crd, mock_kubernetes_hook):
        sensor = SparkKubernetesSensor(application_name="spark_pi", dag=self.dag, task_id="test_task_id")
        with pytest.raises(AirflowException):
            sensor.poke(None)
        mock_kubernetes_hook.assert_called_once_with()
        mock_get_namespaced_crd.assert_called_once_with(
            group="sparkoperator.k8s.io",
            name="spark_pi",
            namespace="default",
            plural="sparkapplications",
            version="v1beta2",
        )

    @patch(
        "kubernetes.client.api.custom_objects_api.CustomObjectsApi.get_namespaced_custom_object",
        return_value=TEST_COMPLETED_APPLICATION,
    )
    def test_namespace_from_sensor(self, mock_get_namespaced_crd, mock_kubernetes_hook):
        sensor = SparkKubernetesSensor(
            application_name="spark_pi",
            dag=self.dag,
            kubernetes_conn_id="kubernetes_with_namespace",
            namespace="sensor_namespace",
            task_id="test_task_id",
        )
        sensor.poke(None)
        mock_kubernetes_hook.assert_called_once_with()
        mock_get_namespaced_crd.assert_called_once_with(
            group="sparkoperator.k8s.io",
            name="spark_pi",
            namespace="sensor_namespace",
            plural="sparkapplications",
            version="v1beta2",
        )

    @patch(
        "kubernetes.client.api.custom_objects_api.CustomObjectsApi.get_namespaced_custom_object",
        return_value=TEST_COMPLETED_APPLICATION,
    )
    def test_api_group_and_version_from_sensor(self, mock_get_namespaced_crd, mock_kubernetes_hook):
        api_group = 'sparkoperator.example.com'
        api_version = 'v1alpha1'
        sensor = SparkKubernetesSensor(
            application_name="spark_pi",
            dag=self.dag,
            kubernetes_conn_id="kubernetes_with_namespace",
            task_id="test_task_id",
            api_group=api_group,
            api_version=api_version,
        )
        sensor.poke(None)
        mock_kubernetes_hook.assert_called_once_with()
        mock_get_namespaced_crd.assert_called_once_with(
            group=api_group,
            name="spark_pi",
            namespace="mock_namespace",
            plural="sparkapplications",
            version=api_version,
        )

    @patch(
        "kubernetes.client.api.custom_objects_api.CustomObjectsApi.get_namespaced_custom_object",
        return_value=TEST_COMPLETED_APPLICATION,
    )
    def test_namespace_from_connection(self, mock_get_namespaced_crd, mock_kubernetes_hook):
        sensor = SparkKubernetesSensor(
            application_name="spark_pi",
            dag=self.dag,
            kubernetes_conn_id="kubernetes_with_namespace",
            task_id="test_task_id",
        )
        sensor.poke(None)
        mock_kubernetes_hook.assert_called_once_with()
        mock_get_namespaced_crd.assert_called_once_with(
            group="sparkoperator.k8s.io",
            name="spark_pi",
            namespace="mock_namespace",
            plural="sparkapplications",
            version="v1beta2",
        )

    @patch(
        "kubernetes.client.api.custom_objects_api.CustomObjectsApi.get_namespaced_custom_object",
        return_value=TEST_FAILED_APPLICATION,
    )
    @patch("logging.Logger.error")
    @patch(
        "airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook.get_pod_logs",
        return_value=TEST_POD_LOGS,
    )
    def test_driver_logging_failure(
        self, mock_log_call, error_log_call, mock_get_namespaced_crd, mock_kube_conn
    ):
        sensor = SparkKubernetesSensor(
            application_name="spark_pi",
            attach_log=True,
            dag=self.dag,
            task_id="test_task_id",
        )
        with pytest.raises(AirflowException):
            sensor.poke(None)
        mock_log_call.assert_called_once_with("spark-pi-driver", namespace="default")
        error_log_call.assert_called_once_with(TEST_POD_LOG_RESULT)

    @patch(
        "kubernetes.client.api.custom_objects_api.CustomObjectsApi.get_namespaced_custom_object",
        return_value=TEST_COMPLETED_APPLICATION,
    )
    @patch("logging.Logger.info")
    @patch(
        "airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook.get_pod_logs",
        return_value=TEST_POD_LOGS,
    )
    def test_driver_logging_completed(
        self, mock_log_call, info_log_call, mock_get_namespaced_crd, mock_kube_conn
    ):
        sensor = SparkKubernetesSensor(
            application_name="spark_pi",
            attach_log=True,
            dag=self.dag,
            task_id="test_task_id",
        )
        sensor.poke(None)
        mock_log_call.assert_called_once_with("spark-pi-2020-02-24-1-driver", namespace="default")
        log_info_call = info_log_call.mock_calls[1]
        log_value = log_info_call[1][0]
        assert log_value == TEST_POD_LOG_RESULT

    @patch(
        "kubernetes.client.api.custom_objects_api.CustomObjectsApi.get_namespaced_custom_object",
        return_value=TEST_COMPLETED_APPLICATION,
    )
    @patch("logging.Logger.warning")
    @patch(
        "airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook.get_pod_logs",
        side_effect=ApiException("Test api exception"),
    )
    def test_driver_logging_error(
        self, mock_log_call, warn_log_call, mock_get_namespaced_crd, mock_kube_conn
    ):
        sensor = SparkKubernetesSensor(
            application_name="spark_pi",
            attach_log=True,
            dag=self.dag,
            task_id="test_task_id",
        )
        sensor.poke(None)
        warn_log_call.assert_called_once()
