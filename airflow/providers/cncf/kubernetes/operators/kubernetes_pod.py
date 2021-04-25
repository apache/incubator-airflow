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
"""Executes task in a Kubernetes POD"""
import re
import warnings
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Tuple

from kubernetes.client import CoreV1Api, models as k8s

try:
    import airflow.utils.yaml as yaml
except ImportError:
    import yaml

from airflow.exceptions import AirflowException
from airflow.kubernetes import kube_client, pod_generator, pod_launcher
from airflow.kubernetes.pod_generator import PodGenerator
from airflow.kubernetes.secret import Secret
from airflow.models import BaseOperator
from airflow.providers.cncf.kubernetes.backcompat.backwards_compat_converters import (
    convert_affinity,
    convert_configmap,
    convert_env_vars,
    convert_image_pull_secrets,
    convert_pod_runtime_info_env,
    convert_port,
    convert_resources,
    convert_toleration,
    convert_volume,
    convert_volume_mount,
)
from airflow.providers.cncf.kubernetes.backcompat.pod_runtime_info_env import PodRuntimeInfoEnv
from airflow.utils.decorators import apply_defaults
from airflow.utils.helpers import validate_key
from airflow.utils.state import State
from airflow.version import version as airflow_version

if TYPE_CHECKING:
    import jinja2


class KubernetesPodOperator(BaseOperator):  # pylint: disable=too-many-instance-attributes
    """
    Execute a task in a Kubernetes Pod

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:KubernetesPodOperator`

    .. note::
        If you use `Google Kubernetes Engine <https://cloud.google.com/kubernetes-engine/>`__
        and Airflow is not running in the same cluster, consider using
        :class:`~airflow.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperator`, which
        simplifies the authorization process.

    :param namespace: the namespace to run within kubernetes.
    :type namespace: str
    :param image: Docker image you wish to launch. Defaults to hub.docker.com,
        but fully qualified URLS will point to custom repositories. (templated)
    :type image: str
    :param name: name of the pod in which the task will run, will be used (plus a random
        suffix) to generate a pod id (DNS-1123 subdomain, containing only [a-z0-9.-]).
    :type name: str
    :param cmds: entrypoint of the container. (templated)
        The docker images's entrypoint is used if this is not provided.
    :type cmds: list[str]
    :param arguments: arguments of the entrypoint. (templated)
        The docker image's CMD is used if this is not provided.
    :type arguments: list[str]
    :param ports: ports for launched pod.
    :type ports: list[k8s.V1ContainerPort]
    :param volume_mounts: volumeMounts for launched pod.
    :type volume_mounts: list[k8s.V1VolumeMount]
    :param volumes: volumes for launched pod. Includes ConfigMaps and PersistentVolumes.
    :type volumes: list[k8s.V1Volume]
    :param env_vars: Environment variables initialized in the container. (templated)
    :type env_vars: list[k8s.V1EnvVar]
    :param secrets: Kubernetes secrets to inject in the container.
        They can be exposed as environment vars or files in a volume.
    :type secrets: list[airflow.kubernetes.secret.Secret]
    :param in_cluster: run kubernetes client with in_cluster configuration.
    :type in_cluster: bool
    :param cluster_context: context that points to kubernetes cluster.
        Ignored when in_cluster is True. If None, current-context is used.
    :type cluster_context: str
    :param reattach_on_restart: if the scheduler dies while the pod is running, reattach and monitor
    :type reattach_on_restart: bool
    :param labels: labels to apply to the Pod. (templated)
    :type labels: dict
    :param startup_timeout_seconds: timeout in seconds to startup the pod.
    :type startup_timeout_seconds: int
    :param get_logs: get the stdout of the container as logs of the tasks.
    :type get_logs: bool
    :param image_pull_policy: Specify a policy to cache or always pull an image.
    :type image_pull_policy: str
    :param annotations: non-identifying metadata you can attach to the Pod.
        Can be a large range of data, and can include characters
        that are not permitted by labels.
    :type annotations: dict
    :param resources: A dict containing resources requests and limits.
        Possible keys are request_memory, request_cpu, limit_memory, limit_cpu,
        and limit_gpu, which will be used to generate airflow.kubernetes.pod.Resources.
        See also kubernetes.io/docs/concepts/configuration/manage-compute-resources-container
    :type resources: k8s.V1ResourceRequirements
    :param affinity: A dict containing a group of affinity scheduling rules.
    :type affinity: k8s.V1Affinity
    :param config_file: The path to the Kubernetes config file. (templated)
        If not specified, default value is ``~/.kube/config``
    :type config_file: str
    :param node_selectors: A dict containing a group of scheduling rules.
    :type node_selectors: dict
    :param image_pull_secrets: Any image pull secrets to be given to the pod.
        If more than one secret is required, provide a
        comma separated list: secret_a,secret_b
    :type image_pull_secrets: List[k8s.V1LocalObjectReference]
    :param service_account_name: Name of the service account
    :type service_account_name: str
    :param is_delete_operator_pod: What to do when the pod reaches its final
        state, or the execution is interrupted.
        If False (default): do nothing, If True: delete the pod
    :type is_delete_operator_pod: bool
    :param hostnetwork: If True enable host networking on the pod.
    :type hostnetwork: bool
    :param tolerations: A list of kubernetes tolerations.
    :type tolerations: List[k8s.V1Toleration]
    :param security_context: security options the pod should run with (PodSecurityContext).
    :type security_context: dict
    :param dnspolicy: dnspolicy for the pod.
    :type dnspolicy: str
    :param schedulername: Specify a schedulername for the pod
    :type schedulername: str
    :param full_pod_spec: The complete podSpec
    :type full_pod_spec: kubernetes.client.models.V1Pod
    :param init_containers: init container for the launched Pod
    :type init_containers: list[kubernetes.client.models.V1Container]
    :param log_events_on_failure: Log the pod's events if a failure occurs
    :type log_events_on_failure: bool
    :param do_xcom_push: If True, the content of the file
        /airflow/xcom/return.json in the container will also be pushed to an
        XCom when the container completes.
    :type do_xcom_push: bool
    :param pod_template_file: path to pod template file (templated)
    :type pod_template_file: str
    :param priority_class_name: priority class name for the launched Pod
    :type priority_class_name: str
    :param termination_grace_period: Termination grace period if task killed in UI,
        defaults to kubernetes default
    :type termination_grace_period: int
    """

    template_fields: Iterable[str] = (
        'image',
        'cmds',
        'arguments',
        'env_vars',
        'labels',
        'config_file',
        'pod_template_file',
    )

    # fmt: off
    @apply_defaults
    def __init__(  # pylint: disable=too-many-arguments,too-many-locals
        # fmt: on
        self,
        *,
        namespace: Optional[str] = None,
        image: Optional[str] = None,
        name: Optional[str] = None,
        cmds: Optional[List[str]] = None,
        arguments: Optional[List[str]] = None,
        ports: Optional[List[k8s.V1ContainerPort]] = None,
        volume_mounts: Optional[List[k8s.V1VolumeMount]] = None,
        volumes: Optional[List[k8s.V1Volume]] = None,
        env_vars: Optional[List[k8s.V1EnvVar]] = None,
        env_from: Optional[List[k8s.V1EnvFromSource]] = None,
        secrets: Optional[List[Secret]] = None,
        in_cluster: Optional[bool] = None,
        cluster_context: Optional[str] = None,
        labels: Optional[Dict] = None,
        reattach_on_restart: bool = True,
        startup_timeout_seconds: int = 120,
        get_logs: bool = True,
        image_pull_policy: str = 'IfNotPresent',
        annotations: Optional[Dict] = None,
        resources: Optional[k8s.V1ResourceRequirements] = None,
        affinity: Optional[k8s.V1Affinity] = None,
        config_file: Optional[str] = None,
        node_selectors: Optional[dict] = None,
        node_selector: Optional[dict] = None,
        image_pull_secrets: Optional[List[k8s.V1LocalObjectReference]] = None,
        service_account_name: str = 'default',
        is_delete_operator_pod: bool = False,
        hostnetwork: bool = False,
        tolerations: Optional[List[k8s.V1Toleration]] = None,
        security_context: Optional[Dict] = None,
        dnspolicy: Optional[str] = None,
        schedulername: Optional[str] = None,
        full_pod_spec: Optional[k8s.V1Pod] = None,
        init_containers: Optional[List[k8s.V1Container]] = None,
        log_events_on_failure: bool = False,
        do_xcom_push: bool = False,
        pod_template_file: Optional[str] = None,
        priority_class_name: Optional[str] = None,
        pod_runtime_info_envs: List[PodRuntimeInfoEnv] = None,
        termination_grace_period: Optional[int] = None,
        configmaps: Optional[str] = None,
        **kwargs,
    ) -> None:
        if kwargs.get('xcom_push') is not None:
            raise AirflowException("'xcom_push' was deprecated, use 'do_xcom_push' instead")
        super().__init__(resources=None, **kwargs)

        self.do_xcom_push = do_xcom_push
        self.image = image
        self.namespace = namespace
        self.cmds = cmds or []
        self.arguments = arguments or []
        self.labels = labels or {}
        self.startup_timeout_seconds = startup_timeout_seconds
        self.env_vars = convert_env_vars(env_vars) if env_vars else []
        if pod_runtime_info_envs:
            self.env_vars.extend([convert_pod_runtime_info_env(p) for p in pod_runtime_info_envs])
        self.env_from = env_from or []
        if configmaps:
            self.env_from.extend([convert_configmap(c) for c in configmaps])
        self.ports = [convert_port(p) for p in ports] if ports else []
        self.volume_mounts = [convert_volume_mount(v) for v in volume_mounts] if volume_mounts else []
        self.volumes = [convert_volume(volume) for volume in volumes] if volumes else []
        self.secrets = secrets or []
        self.in_cluster = in_cluster
        self.cluster_context = cluster_context
        self.reattach_on_restart = reattach_on_restart
        self.get_logs = get_logs
        self.image_pull_policy = image_pull_policy
        if node_selectors:
            # Node selectors is incorrect based on k8s API
            warnings.warn("node_selectors is deprecated. Please use node_selector instead.")
            self.node_selector = node_selectors or {}
        elif node_selector:
            self.node_selector = node_selector or {}
        else:
            self.node_selector = None
        self.annotations = annotations or {}
        self.affinity = convert_affinity(affinity) if affinity else k8s.V1Affinity()
        self.k8s_resources = convert_resources(resources) if resources else {}
        self.config_file = config_file
        self.image_pull_secrets = convert_image_pull_secrets(image_pull_secrets) if image_pull_secrets else []
        self.service_account_name = service_account_name
        self.is_delete_operator_pod = is_delete_operator_pod
        self.hostnetwork = hostnetwork
        self.tolerations = [convert_toleration(toleration) for toleration in tolerations] \
            if tolerations else []
        self.security_context = security_context or {}
        self.dnspolicy = dnspolicy
        self.schedulername = schedulername
        self.full_pod_spec = full_pod_spec
        self.init_containers = init_containers or []
        self.log_events_on_failure = log_events_on_failure
        self.priority_class_name = priority_class_name
        self.pod_template_file = pod_template_file
        self.name = self._set_name(name)
        self.termination_grace_period = termination_grace_period
        self.client: CoreV1Api = None
        self.pod: k8s.V1Pod = None

    def _render_nested_template_fields(
        self,
        content: Any,
        context: Dict,
        jinja_env: "jinja2.Environment",
        seen_oids: set,
    ) -> None:
        if id(content) not in seen_oids and isinstance(content, k8s.V1EnvVar):
            seen_oids.add(id(content))
            self._do_render_template_fields(content, ('value', 'name'), context, jinja_env, seen_oids)
            return

        super()._render_nested_template_fields(
            content,
            context,
            jinja_env,
            seen_oids
        )

    @staticmethod
    def create_labels_for_pod(context) -> dict:
        """
        Generate labels for the pod to track the pod in case of Operator crash

        :param context: task context provided by airflow DAG
        :return: dict
        """
        labels = {
            'dag_id': context['dag'].dag_id,
            'task_id': context['task'].task_id,
            'execution_date': context['ts'],
            'try_number': context['ti'].try_number,
        }
        # In the case of sub dags this is just useful
        if context['dag'].is_subdag:
            labels['parent_dag_id'] = context['dag'].parent_dag.dag_id
        # Ensure that label is valid for Kube,
        # and if not truncate/remove invalid chars and replace with short hash.
        for label_id, label in labels.items():
            safe_label = pod_generator.make_safe_label_value(str(label))
            labels[label_id] = safe_label
        return labels

    def execute(self, context) -> Optional[str]:
        try:
            if self.in_cluster is not None:
                client = kube_client.get_kube_client(
                    in_cluster=self.in_cluster,
                    cluster_context=self.cluster_context,
                    config_file=self.config_file,
                )
            else:
                client = kube_client.get_kube_client(
                    cluster_context=self.cluster_context, config_file=self.config_file
                )

            self.pod = self.create_pod_request_obj()
            self.namespace = self.pod.metadata.namespace

            self.client = client

            # Add combination of labels to uniquely identify a running pod
            labels = self.create_labels_for_pod(context)

            label_selector = self._get_pod_identifying_label_string(labels)

            self.namespace = self.pod.metadata.namespace

            pod_list = client.list_namespaced_pod(self.namespace, label_selector=label_selector)

            if len(pod_list.items) > 1 and self.reattach_on_restart:
                raise AirflowException(
                    f'More than one pod running with labels: {label_selector}'
                )

            launcher = pod_launcher.PodLauncher(kube_client=client, extract_xcom=self.do_xcom_push)

            if len(pod_list.items) == 1:
                try_numbers_match = self._try_numbers_match(context, pod_list.items[0])
                final_state, result = self.handle_pod_overlap(
                    labels, try_numbers_match, launcher, pod_list.items[0]
                )
            else:
                self.log.info("creating pod with labels %s and launcher %s", labels, launcher)
                final_state, _, result = self.create_new_pod_for_operator(labels, launcher)
            if final_state != State.SUCCESS:
                status = self.client.read_namespaced_pod(self.pod.metadata.name, self.namespace)
                raise AirflowException(f'Pod {self.pod.metadata.name} returned a failure: {status}')
            return result
        except AirflowException as ex:
            raise AirflowException(f'Pod Launching failed: {ex}')

    def handle_pod_overlap(
        self, labels: dict, try_numbers_match: bool, launcher: Any, pod: k8s.V1Pod
    ) -> Tuple[State, Optional[str]]:
        """

        In cases where the Scheduler restarts while a KubernetesPodOperator task is running,
        this function will either continue to monitor the existing pod or launch a new pod
        based on the `reattach_on_restart` parameter.

        :param labels: labels used to determine if a pod is repeated
        :type labels: dict
        :param try_numbers_match: do the try numbers match? Only needed for logging purposes
        :type try_numbers_match: bool
        :param launcher: PodLauncher
        :param pod_list: list of pods found
        """
        if try_numbers_match:
            log_line = f"found a running pod with labels {labels} and the same try_number."
        else:
            log_line = f"found a running pod with labels {labels} but a different try_number."

        # In case of failed pods, should reattach the first time, but only once
        # as the task will have already failed.
        if self.reattach_on_restart and not pod.metadata.labels.get("already_checked"):
            log_line += " Will attach to this pod and monitor instead of starting new one"
            self.log.info(log_line)
            self.pod = pod
            final_state, result = self.monitor_launched_pod(launcher, pod)
        else:
            log_line += f"creating pod with labels {labels} and launcher {launcher}"
            self.log.info(log_line)
            final_state, _, result = self.create_new_pod_for_operator(labels, launcher)
        return final_state, result

    @staticmethod
    def _get_pod_identifying_label_string(labels) -> str:
        filtered_labels = {label_id: label for label_id, label in labels.items() if label_id != 'try_number'}
        return ','.join([label_id + '=' + label for label_id, label in sorted(filtered_labels.items())])

    @staticmethod
    def _try_numbers_match(context, pod) -> bool:
        return pod.metadata.labels['try_number'] == context['ti'].try_number

    def _set_name(self, name):
        if name is None:
            return None
        validate_key(name, max_length=220)
        return re.sub(r'[^a-z0-9.-]+', '-', name.lower())

    def create_pod_request_obj(self) -> k8s.V1Pod:
        """
        Creates a V1Pod based on user parameters. Note that a `pod` or `pod_template_file`
        will supersede all other values.

        """
        self.log.debug("Creating pod for K8sPodOperator task %s", self.task_id)
        if self.pod_template_file:
            self.log.debug("Pod template file found, will parse for base pod")
            pod_template = pod_generator.PodGenerator.deserialize_model_file(self.pod_template_file)
            if self.full_pod_spec:
                pod_template = PodGenerator.reconcile_pods(pod_template, self.full_pod_spec)
        elif self.full_pod_spec:
            pod_template = self.full_pod_spec
        else:
            pod_template = k8s.V1Pod(metadata=k8s.V1ObjectMeta(name="name"))

        pod = k8s.V1Pod(
            api_version="v1",
            kind="Pod",
            metadata=k8s.V1ObjectMeta(
                namespace=self.namespace,
                labels=self.labels,
                name=PodGenerator.make_unique_pod_id(self.name),
                annotations=self.annotations,
            ),
            spec=k8s.V1PodSpec(
                node_selector=self.node_selector,
                affinity=self.affinity,
                tolerations=self.tolerations,
                init_containers=self.init_containers,
                containers=[
                    k8s.V1Container(
                        image=self.image,
                        name="base",
                        command=self.cmds,
                        ports=self.ports,
                        image_pull_policy=self.image_pull_policy,
                        resources=self.k8s_resources,
                        volume_mounts=self.volume_mounts,
                        args=self.arguments,
                        env=self.env_vars,
                        env_from=self.env_from,
                    )
                ],
                image_pull_secrets=self.image_pull_secrets,
                service_account_name=self.service_account_name,
                host_network=self.hostnetwork,
                security_context=self.security_context,
                dns_policy=self.dnspolicy,
                scheduler_name=self.schedulername,
                restart_policy='Never',
                priority_class_name=self.priority_class_name,
                volumes=self.volumes,
            ),
        )

        pod = PodGenerator.reconcile_pods(pod_template, pod)

        for secret in self.secrets:
            self.log.debug("Adding secret to task %s", self.task_id)
            pod = secret.attach_to_pod(pod)
        if self.do_xcom_push:
            self.log.debug("Adding xcom sidecar to task %s", self.task_id)
            pod = PodGenerator.add_xcom_sidecar(pod)
        return pod

    def create_new_pod_for_operator(self, labels, launcher) -> Tuple[State, k8s.V1Pod, Optional[str]]:
        """
        Creates a new pod and monitors for duration of task

        :param labels: labels used to track pod
        :param launcher: pod launcher that will manage launching and monitoring pods
        :return:
        """
        if not (self.full_pod_spec or self.pod_template_file):
            # Add Airflow Version to the label
            # And a label to identify that pod is launched by KubernetesPodOperator
            self.log.debug("Adding k8spodoperator labels to pod before launch for task %s", self.task_id)
            self.labels.update(
                {
                    'airflow_version': airflow_version.replace('+', '-'),
                    'kubernetes_pod_operator': 'True',
                }
            )
            self.labels.update(labels)
            self.pod.metadata.labels = self.labels
        self.log.debug("Starting pod:\n%s", yaml.safe_dump(self.pod.to_dict()))
        try:
            launcher.start_pod(self.pod, startup_timeout=self.startup_timeout_seconds)
            final_state, result = launcher.monitor_pod(pod=self.pod, get_logs=self.get_logs)
        except AirflowException:
            if self.log_events_on_failure:
                for event in launcher.read_pod_events(self.pod).items:
                    self.log.error("Pod Event: %s - %s", event.reason, event.message)
            raise
        finally:
            if self.is_delete_operator_pod:
                self.log.debug("Deleting pod for task %s", self.task_id)
                launcher.delete_pod(self.pod)
        return final_state, self.pod, result

    def patch_already_checked(self, pod: k8s.V1Pod):
        """Add an "already tried annotation to ensure we only retry once"""
        pod.metadata.labels["already_checked"] = "True"
        body = PodGenerator.serialize_pod(pod)
        self.client.patch_namespaced_pod(pod.metadata.name, pod.metadata.namespace, body)

    def monitor_launched_pod(self, launcher, pod) -> Tuple[State, Optional[str]]:
        """
        Monitors a pod to completion that was created by a previous KubernetesPodOperator

        :param launcher: pod launcher that will manage launching and monitoring pods
        :param pod: podspec used to find pod using k8s API
        :return:
        """
        try:
            (final_state, result) = launcher.monitor_pod(pod, get_logs=self.get_logs)
        finally:
            if self.is_delete_operator_pod:
                launcher.delete_pod(pod)
        if final_state != State.SUCCESS:
            if self.log_events_on_failure:
                for event in launcher.read_pod_events(pod).items:
                    self.log.error("Pod Event: %s - %s", event.reason, event.message)
            self.patch_already_checked(self.pod)
            raise AirflowException(f'Pod returned a failure: {final_state}')
        return final_state, result

    def on_kill(self) -> None:
        if self.pod:
            pod: k8s.V1Pod = self.pod
            namespace = pod.metadata.namespace
            name = pod.metadata.name
            kwargs = {}
            if self.termination_grace_period is not None:
                kwargs = {"grace_period_seconds": self.termination_grace_period}
            self.client.delete_namespaced_pod(name=name, namespace=namespace, **kwargs)
