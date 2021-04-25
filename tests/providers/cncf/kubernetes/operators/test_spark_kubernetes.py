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

from airflow import DAG
from airflow.models import Connection
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils import db, timezone

TEST_VALID_APPLICATION_YAML = """
apiVersion: "sparkoperator.k8s.io/v1beta2"
kind: SparkApplication
metadata:
  name: spark-pi
  namespace: default
spec:
  type: Scala
  mode: cluster
  image: "gcr.io/spark-operator/spark:v2.4.5"
  imagePullPolicy: Always
  mainClass: org.apache.spark.examples.SparkPi
  mainApplicationFile: "local:///opt/spark/examples/jars/spark-examples_2.11-2.4.5.jar"
  sparkVersion: "2.4.5"
  restartPolicy:
    type: Never
  volumes:
    - name: "test-volume"
      hostPath:
        path: "/tmp"
        type: Directory
  driver:
    cores: 1
    coreLimit: "1200m"
    memory: "512m"
    labels:
      version: 2.4.5
    serviceAccount: spark
    volumeMounts:
      - name: "test-volume"
        mountPath: "/tmp"
  executor:
    cores: 1
    instances: 1
    memory: "512m"
    labels:
      version: 2.4.5
    volumeMounts:
      - name: "test-volume"
        mountPath: "/tmp"
"""
TEST_VALID_APPLICATION_JSON = """
{
   "apiVersion":"sparkoperator.k8s.io/v1beta2",
   "kind":"SparkApplication",
   "metadata":{
      "name":"spark-pi",
      "namespace":"default"
   },
   "spec":{
      "type":"Scala",
      "mode":"cluster",
      "image":"gcr.io/spark-operator/spark:v2.4.5",
      "imagePullPolicy":"Always",
      "mainClass":"org.apache.spark.examples.SparkPi",
      "mainApplicationFile":"local:///opt/spark/examples/jars/spark-examples_2.11-2.4.5.jar",
      "sparkVersion":"2.4.5",
      "restartPolicy":{
         "type":"Never"
      },
      "volumes":[
         {
            "name":"test-volume",
            "hostPath":{
               "path":"/tmp",
               "type":"Directory"
            }
         }
      ],
      "driver":{
         "cores":1,
         "coreLimit":"1200m",
         "memory":"512m",
         "labels":{
            "version":"2.4.5"
         },
         "serviceAccount":"spark",
         "volumeMounts":[
            {
               "name":"test-volume",
               "mountPath":"/tmp"
            }
         ]
      },
      "executor":{
         "cores":1,
         "instances":1,
         "memory":"512m",
         "labels":{
            "version":"2.4.5"
         },
         "volumeMounts":[
            {
               "name":"test-volume",
               "mountPath":"/tmp"
            }
         ]
      }
   }
}
"""
TEST_APPLICATION_DICT = {
    'apiVersion': 'sparkoperator.k8s.io/v1beta2',
    'kind': 'SparkApplication',
    'metadata': {'name': 'spark-pi', 'namespace': 'default'},
    'spec': {
        'driver': {
            'coreLimit': '1200m',
            'cores': 1,
            'labels': {'version': '2.4.5'},
            'memory': '512m',
            'serviceAccount': 'spark',
            'volumeMounts': [{'mountPath': '/tmp', 'name': 'test-volume'}],
        },
        'executor': {
            'cores': 1,
            'instances': 1,
            'labels': {'version': '2.4.5'},
            'memory': '512m',
            'volumeMounts': [{'mountPath': '/tmp', 'name': 'test-volume'}],
        },
        'image': 'gcr.io/spark-operator/spark:v2.4.5',
        'imagePullPolicy': 'Always',
        'mainApplicationFile': 'local:///opt/spark/examples/jars/spark-examples_2.11-2.4.5.jar',
        'mainClass': 'org.apache.spark.examples.SparkPi',
        'mode': 'cluster',
        'restartPolicy': {'type': 'Never'},
        'sparkVersion': '2.4.5',
        'type': 'Scala',
        'volumes': [{'hostPath': {'path': '/tmp', 'type': 'Directory'}, 'name': 'test-volume'}],
    },
}


@patch('airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook.get_conn')
class TestSparkKubernetesOperator(unittest.TestCase):
    def setUp(self):
        db.merge_conn(
            Connection(conn_id='kubernetes_default_kube_config', conn_type='kubernetes', extra=json.dumps({}))
        )
        db.merge_conn(
            Connection(
                conn_id='kubernetes_with_namespace',
                conn_type='kubernetes',
                extra=json.dumps({'extra__kubernetes__namespace': 'mock_namespace'}),
            )
        )
        args = {'owner': 'airflow', 'start_date': timezone.datetime(2020, 2, 1)}
        self.dag = DAG('test_dag_id', default_args=args)

    @patch('kubernetes.client.api.custom_objects_api.CustomObjectsApi.create_namespaced_custom_object')
    def test_create_application_from_yaml(self, mock_create_namespaced_crd, mock_kubernetes_hook):
        op = SparkKubernetesOperator(
            application_file=TEST_VALID_APPLICATION_YAML,
            dag=self.dag,
            kubernetes_conn_id='kubernetes_default_kube_config',
            task_id='test_task_id',
        )
        op.execute(None)
        mock_kubernetes_hook.assert_called_once_with()
        mock_create_namespaced_crd.assert_called_with(
            body=TEST_APPLICATION_DICT,
            group='sparkoperator.k8s.io',
            namespace='default',
            plural='sparkapplications',
            version='v1beta2',
        )

    @patch('kubernetes.client.api.custom_objects_api.CustomObjectsApi.create_namespaced_custom_object')
    def test_create_application_from_json(self, mock_create_namespaced_crd, mock_kubernetes_hook):
        op = SparkKubernetesOperator(
            application_file=TEST_VALID_APPLICATION_JSON,
            dag=self.dag,
            kubernetes_conn_id='kubernetes_default_kube_config',
            task_id='test_task_id',
        )
        op.execute(None)
        mock_kubernetes_hook.assert_called_once_with()
        mock_create_namespaced_crd.assert_called_with(
            body=TEST_APPLICATION_DICT,
            group='sparkoperator.k8s.io',
            namespace='default',
            plural='sparkapplications',
            version='v1beta2',
        )

    @patch('kubernetes.client.api.custom_objects_api.CustomObjectsApi.create_namespaced_custom_object')
    def test_create_application_from_json_with_api_group_and_version(
        self, mock_create_namespaced_crd, mock_kubernetes_hook
    ):
        api_group = 'sparkoperator.example.com'
        api_version = 'v1alpha1'
        op = SparkKubernetesOperator(
            application_file=TEST_VALID_APPLICATION_JSON,
            dag=self.dag,
            kubernetes_conn_id='kubernetes_default_kube_config',
            task_id='test_task_id',
            api_group=api_group,
            api_version=api_version,
        )
        op.execute(None)
        mock_kubernetes_hook.assert_called_once_with()
        mock_create_namespaced_crd.assert_called_with(
            body=TEST_APPLICATION_DICT,
            group=api_group,
            namespace='default',
            plural='sparkapplications',
            version=api_version,
        )

    @patch('kubernetes.client.api.custom_objects_api.CustomObjectsApi.create_namespaced_custom_object')
    def test_namespace_from_operator(self, mock_create_namespaced_crd, mock_kubernetes_hook):
        op = SparkKubernetesOperator(
            application_file=TEST_VALID_APPLICATION_JSON,
            dag=self.dag,
            namespace='operator_namespace',
            kubernetes_conn_id='kubernetes_with_namespace',
            task_id='test_task_id',
        )
        op.execute(None)
        mock_kubernetes_hook.assert_called_once_with()
        mock_create_namespaced_crd.assert_called_with(
            body=TEST_APPLICATION_DICT,
            group='sparkoperator.k8s.io',
            namespace='operator_namespace',
            plural='sparkapplications',
            version='v1beta2',
        )

    @patch('kubernetes.client.api.custom_objects_api.CustomObjectsApi.create_namespaced_custom_object')
    def test_namespace_from_connection(self, mock_create_namespaced_crd, mock_kubernetes_hook):
        op = SparkKubernetesOperator(
            application_file=TEST_VALID_APPLICATION_JSON,
            dag=self.dag,
            kubernetes_conn_id='kubernetes_with_namespace',
            task_id='test_task_id',
        )
        op.execute(None)
        mock_kubernetes_hook.assert_called_once_with()
        mock_create_namespaced_crd.assert_called_with(
            body=TEST_APPLICATION_DICT,
            group='sparkoperator.k8s.io',
            namespace='mock_namespace',
            plural='sparkapplications',
            version='v1beta2',
        )
