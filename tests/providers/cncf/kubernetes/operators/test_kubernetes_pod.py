# pylint: disable=unused-argument
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
from tempfile import NamedTemporaryFile
from unittest import mock

import pendulum
import pytest
from kubernetes.client import ApiClient, models as k8s

from airflow.exceptions import AirflowException
from airflow.models import DAG, TaskInstance
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils import timezone
from airflow.utils.state import State


class TestKubernetesPodOperator(unittest.TestCase):
    def setUp(self):
        self.start_patch = mock.patch(
            "airflow.providers.cncf.kubernetes.utils.pod_launcher.PodLauncher.start_pod"
        )
        self.monitor_patch = mock.patch(
            "airflow.providers.cncf.kubernetes.utils.pod_launcher.PodLauncher.monitor_pod"
        )
        self.client_patch = mock.patch("airflow.kubernetes.kube_client.get_kube_client")
        self.start_mock = self.start_patch.start()
        self.monitor_mock = self.monitor_patch.start()
        self.client_mock = self.client_patch.start()
        self.addCleanup(self.start_patch.stop)
        self.addCleanup(self.monitor_patch.stop)
        self.addCleanup(self.client_patch.stop)

    @staticmethod
    def create_context(task):
        dag = DAG(dag_id="dag")
        tzinfo = pendulum.timezone("Europe/Amsterdam")
        execution_date = timezone.datetime(2016, 1, 1, 1, 0, 0, tzinfo=tzinfo)
        task_instance = TaskInstance(task=task, execution_date=execution_date)
        return {
            "dag": dag,
            "ts": execution_date.isoformat(),
            "task": task,
            "ti": task_instance,
        }

    def run_pod(self, operator) -> k8s.V1Pod:
        self.monitor_mock.return_value = (State.SUCCESS, None, None)
        context = self.create_context(operator)
        operator.execute(context=context)
        return self.start_mock.call_args[0][0]

    def sanitize_for_serialization(self, obj):
        return ApiClient().sanitize_for_serialization(obj)

    def test_config_path(self):
        file_path = "/tmp/fake_file"
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            config_file=file_path,
            cluster_context="default",
        )
        self.monitor_mock.return_value = (State.SUCCESS, None, None)
        self.client_mock.list_namespaced_pod.return_value = []
        context = self.create_context(k)
        k.execute(context=context)
        self.client_mock.assert_called_once_with(
            in_cluster=False,
            cluster_context="default",
            config_file=file_path,
        )

    def test_env_vars(self):
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            env_vars=[k8s.V1EnvVar(name="{{ bar }}", value="{{ foo }}")],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
        )
        k.render_template_fields(context={"foo": "footemplated", "bar": "bartemplated"})
        assert k.env_vars[0].value == "footemplated"
        assert k.env_vars[0].name == "bartemplated"

    def test_envs_from_configmaps(
        self,
    ):
        configmap_name = "test-config-map"
        env_from = [k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name=configmap_name))]
        # WHEN
        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            env_from=env_from,
        )
        pod = self.run_pod(k)
        assert pod.spec.containers[0].env_from == env_from

    def test_labels(self):
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
        )
        pod = self.run_pod(k)
        assert pod.metadata.labels == {
            "foo": "bar",
            "dag_id": "dag",
            "kubernetes_pod_operator": "True",
            "task_id": "task",
            "try_number": "1",
            "airflow_version": mock.ANY,
            "execution_date": mock.ANY,
        }

    def test_image_pull_secrets_correctly_set(self):
        fake_pull_secrets = "fakeSecret"
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            image_pull_secrets=[k8s.V1LocalObjectReference(fake_pull_secrets)],
            cluster_context="default",
        )
        pod = k.create_pod_request_obj()
        assert pod.spec.image_pull_secrets == [k8s.V1LocalObjectReference(name=fake_pull_secrets)]

    def test_image_pull_policy_not_set(self):
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            cluster_context="default",
        )
        pod = k.create_pod_request_obj()
        assert pod.spec.containers[0].image_pull_policy == "IfNotPresent"

    def test_image_pull_policy_correctly_set(self):
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            image_pull_policy="Always",
            cluster_context="default",
        )
        pod = k.create_pod_request_obj()
        assert pod.spec.containers[0].image_pull_policy == "Always"

    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_launcher.PodLauncher.delete_pod")
    def test_pod_delete_even_on_launcher_error(self, delete_pod_mock):
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            cluster_context="default",
            is_delete_operator_pod=True,
        )
        self.monitor_mock.side_effect = AirflowException("fake failure")
        with pytest.raises(AirflowException):
            context = self.create_context(k)
            k.execute(context=context)
        assert delete_pod_mock.called

    def test_randomize_pod_name(self):
        name_base = "test"

        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            name=name_base,
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            cluster_context="default",
        )
        pod = k.create_pod_request_obj()

        assert pod.metadata.name.startswith(name_base)
        assert pod.metadata.name != name_base

    def test_pod_name_required(self):
        with pytest.raises(AirflowException, match="`name` is required"):
            KubernetesPodOperator(
                namespace="default",
                image="ubuntu:16.04",
                task_id="task",
                in_cluster=False,
                do_xcom_push=False,
                cluster_context="default",
            )

    def test_full_pod_spec(self):
        pod_spec = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(name="hello", labels={"foo": "bar"}, namespace="mynamespace"),
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        image="ubuntu:16.04",
                        command=["something"],
                    )
                ]
            ),
        )

        k = KubernetesPodOperator(
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            cluster_context="default",
            full_pod_spec=pod_spec,
        )
        pod = self.run_pod(k)

        assert pod.metadata.name == pod_spec.metadata.name
        assert pod.metadata.namespace == pod_spec.metadata.namespace
        assert pod.spec.containers[0].image == pod_spec.spec.containers[0].image
        assert pod.spec.containers[0].command == pod_spec.spec.containers[0].command
        # Check labels are added from pod_template_file and
        # the pod identifying labels including Airflow version
        assert pod.metadata.labels == {
            "foo": "bar",
            "dag_id": "dag",
            "kubernetes_pod_operator": "True",
            "task_id": "task",
            "try_number": "1",
            "airflow_version": mock.ANY,
            "execution_date": mock.ANY,
        }

        # kwargs take precedence, however
        image = "some.custom.image:andtag"
        name_base = "world"
        k = KubernetesPodOperator(
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            cluster_context="default",
            full_pod_spec=pod_spec,
            name=name_base,
            image=image,
            labels={"hello": "world"},
        )
        pod = self.run_pod(k)

        # make sure the kwargs takes precedence (and that name is randomized)
        assert pod.metadata.name.startswith(name_base)
        assert pod.metadata.name != name_base
        assert pod.spec.containers[0].image == image
        # Check labels are added from pod_template_file, the operator itself and
        # the pod identifying labels including Airflow version
        assert pod.metadata.labels == {
            "foo": "bar",
            "hello": "world",
            "dag_id": "dag",
            "kubernetes_pod_operator": "True",
            "task_id": "task",
            "try_number": "1",
            "airflow_version": mock.ANY,
            "execution_date": mock.ANY,
        }

    def test_pod_template_file(self):
        pod_template_yaml = b"""
            apiVersion: v1
            kind: Pod
            metadata:
              name: hello
              namespace: mynamespace
              labels:
                foo: bar
            spec:
              containers:
                - name: base
                  image: ubuntu:16.04
                  command:
                    - something
        """

        with NamedTemporaryFile() as tpl_file:
            tpl_file.write(pod_template_yaml)
            tpl_file.flush()

            k = KubernetesPodOperator(
                task_id="task",
                pod_template_file=tpl_file.name,
            )
            pod = self.run_pod(k)

            assert pod.metadata.name == "hello"
            # Check labels are added from pod_template_file and
            # the pod identifying labels including Airflow version
            assert pod.metadata.labels == {
                "foo": "bar",
                "dag_id": "dag",
                "kubernetes_pod_operator": "True",
                "task_id": "task",
                "try_number": "1",
                "airflow_version": mock.ANY,
                "execution_date": mock.ANY,
            }
            assert pod.metadata.namespace == "mynamespace"
            assert pod.spec.containers[0].image == "ubuntu:16.04"
            assert pod.spec.containers[0].command == ["something"]

            # kwargs take precedence, however
            image = "some.custom.image:andtag"
            name_base = "world"
            k = KubernetesPodOperator(
                task_id="task",
                pod_template_file=tpl_file.name,
                name=name_base,
                image=image,
                labels={"hello": "world"},
            )
            pod = self.run_pod(k)

            # make sure the kwargs takes precedence (and that name is randomized)
            assert pod.metadata.name.startswith(name_base)
            assert pod.metadata.name != name_base
            assert pod.spec.containers[0].image == image
            # Check labels are added from pod_template_file, the operator itself and
            # the pod identifying labels including Airflow version
            assert pod.metadata.labels == {
                "foo": "bar",
                "hello": "world",
                "dag_id": "dag",
                "kubernetes_pod_operator": "True",
                "task_id": "task",
                "try_number": "1",
                "airflow_version": mock.ANY,
                "execution_date": mock.ANY,
            }

    def test_describes_pod_on_failure(self):
        name_base = "test"

        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name=name_base,
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            cluster_context="default",
        )
        failed_pod_status = "read_pod_namespaced_result"
        self.monitor_mock.return_value = (State.FAILED, failed_pod_status, None)
        read_namespaced_pod_mock = self.client_mock.return_value.read_namespaced_pod
        read_namespaced_pod_mock.return_value = failed_pod_status

        with pytest.raises(AirflowException) as ctx:
            context = self.create_context(k)
            k.execute(context=context)

        assert (
            str(ctx.value)
            == f"Pod Launching failed: Pod {k.pod.metadata.name} returned a failure: {failed_pod_status}"
        )
        assert not self.client_mock.return_value.read_namespaced_pod.called

    def test_no_need_to_describe_pod_on_success(self):
        name_base = "test"

        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name=name_base,
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            cluster_context="default",
        )
        self.monitor_mock.return_value = (State.SUCCESS, None, None)

        context = self.create_context(k)
        k.execute(context=context)

        assert not self.client_mock.return_value.read_namespaced_pod.called

    def test_create_with_affinity(self):
        name_base = "test"

        affinity = {
            "nodeAffinity": {
                "preferredDuringSchedulingIgnoredDuringExecution": [
                    {
                        "weight": 1,
                        "preference": {
                            "matchExpressions": [{"key": "disktype", "operator": "In", "values": ["ssd"]}]
                        },
                    }
                ]
            }
        }

        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name=name_base,
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            cluster_context="default",
            affinity=affinity,
        )

        pod = k.create_pod_request_obj()
        sanitized_pod = self.sanitize_for_serialization(pod)
        assert isinstance(pod.spec.affinity, k8s.V1Affinity)
        assert sanitized_pod["spec"]["affinity"] == affinity

        k8s_api_affinity = k8s.V1Affinity(
            node_affinity=k8s.V1NodeAffinity(
                preferred_during_scheduling_ignored_during_execution=[
                    k8s.V1PreferredSchedulingTerm(
                        weight=1,
                        preference=k8s.V1NodeSelectorTerm(
                            match_expressions=[
                                k8s.V1NodeSelectorRequirement(key="disktype", operator="In", values=["ssd"])
                            ]
                        ),
                    )
                ]
            ),
        )

        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name=name_base,
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            cluster_context="default",
            affinity=k8s_api_affinity,
        )

        pod = k.create_pod_request_obj()
        sanitized_pod = self.sanitize_for_serialization(pod)
        assert isinstance(pod.spec.affinity, k8s.V1Affinity)
        assert sanitized_pod["spec"]["affinity"] == affinity

    def test_tolerations(self):
        k8s_api_tolerations = [k8s.V1Toleration(key="key", operator="Equal", value="value")]

        tolerations = [{"key": "key", "operator": "Equal", "value": "value"}]

        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="name",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            cluster_context="default",
            tolerations=tolerations,
        )

        pod = k.create_pod_request_obj()
        sanitized_pod = self.sanitize_for_serialization(pod)
        assert isinstance(pod.spec.tolerations[0], k8s.V1Toleration)
        assert sanitized_pod["spec"]["tolerations"] == tolerations

        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="name",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            cluster_context="default",
            tolerations=k8s_api_tolerations,
        )

        pod = k.create_pod_request_obj()
        sanitized_pod = self.sanitize_for_serialization(pod)
        assert isinstance(pod.spec.tolerations[0], k8s.V1Toleration)
        assert sanitized_pod["spec"]["tolerations"] == tolerations

    def test_node_selector(self):
        node_selector = {"beta.kubernetes.io/os": "linux"}

        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="name",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            cluster_context="default",
            node_selector=node_selector,
        )

        pod = k.create_pod_request_obj()
        sanitized_pod = self.sanitize_for_serialization(pod)
        assert isinstance(pod.spec.node_selector, dict)
        assert sanitized_pod["spec"]["nodeSelector"] == node_selector

        # repeat tests using deprecated parameter
        with pytest.warns(
            DeprecationWarning, match="node_selectors is deprecated. Please use node_selector instead."
        ):
            k = KubernetesPodOperator(
                namespace="default",
                image="ubuntu:16.04",
                cmds=["bash", "-cx"],
                arguments=["echo 10"],
                labels={"foo": "bar"},
                name="name",
                task_id="task",
                in_cluster=False,
                do_xcom_push=False,
                cluster_context="default",
                node_selectors=node_selector,
            )

        pod = k.create_pod_request_obj()
        sanitized_pod = self.sanitize_for_serialization(pod)
        assert isinstance(pod.spec.node_selector, dict)
        assert sanitized_pod["spec"]["nodeSelector"] == node_selector
