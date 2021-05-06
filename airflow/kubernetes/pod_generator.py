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
"""
This module provides an interface between the previous Pod
API and outputs a kubernetes.client.models.V1Pod.
The advantage being that the full Kubernetes API
is supported and no serialization need be written.
"""
import copy
import datetime
import hashlib
import os
import re
import uuid
import warnings
from functools import reduce
from typing import List, Optional, Union

from dateutil import parser
from kubernetes.client import models as k8s
from kubernetes.client.api_client import ApiClient

import airflow.utils.yaml as yaml
from airflow.exceptions import AirflowConfigException
from airflow.kubernetes.pod_generator_deprecated import PodDefaults, PodGenerator as PodGeneratorDeprecated
from airflow.version import version as airflow_version

MAX_LABEL_LEN = 63


def make_safe_label_value(string):
    """
    Valid label values must be 63 characters or less and must be empty or begin and
    end with an alphanumeric character ([a-z0-9A-Z]) with dashes (-), underscores (_),
    dots (.), and alphanumerics between.

    If the label value is greater than 63 chars once made safe, or differs in any
    way from the original value sent to this function, then we need to truncate to
    53 chars, and append it with a unique hash.
    """
    safe_label = re.sub(r"^[^a-z0-9A-Z]*|[^a-zA-Z0-9_\-\.]|[^a-z0-9A-Z]*$", "", string)

    if len(safe_label) > MAX_LABEL_LEN or string != safe_label:
        safe_hash = hashlib.md5(string.encode()).hexdigest()[:9]
        safe_label = safe_label[: MAX_LABEL_LEN - len(safe_hash) - 1] + "-" + safe_hash

    return safe_label


def datetime_to_label_safe_datestring(datetime_obj: datetime.datetime) -> str:
    """
    Kubernetes doesn't like ":" in labels, since ISO datetime format uses ":" but
    not "_" let's
    replace ":" with "_"

    :param datetime_obj: datetime.datetime object
    :return: ISO-like string representing the datetime
    """
    return datetime_obj.isoformat().replace(":", "_").replace('+', '_plus_')


def label_safe_datestring_to_datetime(string: str) -> datetime.datetime:
    """
    Kubernetes doesn't permit ":" in labels. ISO datetime format uses ":" but not
    "_", let's
    replace ":" with "_"

    :param string: str
    :return: datetime.datetime object
    """
    return parser.parse(string.replace('_plus_', '+').replace("_", ":"))


class PodGenerator:
    """
    Contains Kubernetes Airflow Worker configuration logic

    Represents a kubernetes pod and manages execution of a single pod.
    Any configuration that is container specific gets applied to
    the first container in the list of containers.

    :param pod: The fully specified pod. Mutually exclusive with `path_or_string`
    :type pod: Optional[kubernetes.client.models.V1Pod]
    :param pod_template_file: Path to YAML file. Mutually exclusive with `pod`
    :type pod_template_file: Optional[str]
    :param extract_xcom: Whether to bring up a container for xcom
    :type extract_xcom: bool
    """

    def __init__(  # pylint: disable=too-many-arguments,too-many-locals
        self,
        pod: Optional[k8s.V1Pod] = None,
        pod_template_file: Optional[str] = None,
        extract_xcom: bool = True,
    ):
        if not pod_template_file and not pod:
            raise AirflowConfigException(
                "Podgenerator requires either a `pod` or a `pod_template_file` argument"
            )
        if pod_template_file and pod:
            raise AirflowConfigException("Cannot pass both `pod` and `pod_template_file` arguments")

        if pod_template_file:
            self.ud_pod = self.deserialize_model_file(pod_template_file)
        else:
            self.ud_pod = pod

        # Attach sidecar
        self.extract_xcom = extract_xcom

    def gen_pod(self) -> k8s.V1Pod:
        """Generates pod"""
        result = self.ud_pod

        result.metadata.name = self.make_unique_pod_id(result.metadata.name)

        if self.extract_xcom:
            result = self.add_xcom_sidecar(result)

        return result

    @staticmethod
    def add_xcom_sidecar(pod: k8s.V1Pod) -> k8s.V1Pod:
        """Adds sidecar"""
        warnings.warn(
            "This function is deprecated. "
            "Please use airflow.providers.cncf.kubernetes.utils.xcom_sidecar.add_xcom_sidecar instead"
        )
        pod_cp = copy.deepcopy(pod)
        pod_cp.spec.volumes = pod.spec.volumes or []
        pod_cp.spec.volumes.insert(0, PodDefaults.VOLUME)
        pod_cp.spec.containers[0].volume_mounts = pod_cp.spec.containers[0].volume_mounts or []
        pod_cp.spec.containers[0].volume_mounts.insert(0, PodDefaults.VOLUME_MOUNT)
        pod_cp.spec.containers.append(PodDefaults.SIDECAR_CONTAINER)

        return pod_cp

    @staticmethod
    def from_obj(obj) -> Optional[Union[dict, k8s.V1Pod]]:
        """Converts to pod from obj"""
        if obj is None:
            return None

        k8s_legacy_object = obj.get("KubernetesExecutor", None)
        k8s_object = obj.get("pod_override", None)

        if k8s_legacy_object and k8s_object:
            raise AirflowConfigException(
                "Can not have both a legacy and new"
                "executor_config object. Please delete the KubernetesExecutor"
                "dict and only use the pod_override kubernetes.client.models.V1Pod"
                "object."
            )
        if not k8s_object and not k8s_legacy_object:
            return None

        if isinstance(k8s_object, k8s.V1Pod):
            return k8s_object
        elif isinstance(k8s_legacy_object, dict):
            warnings.warn(
                'Using a dictionary for the executor_config is deprecated and will soon be removed.'
                'please use a `kubernetes.client.models.V1Pod` class with a "pod_override" key'
                ' instead. ',
                category=DeprecationWarning,
            )
            return PodGenerator.from_legacy_obj(obj)
        else:
            raise TypeError(
                'Cannot convert a non-kubernetes.client.models.V1Pod object into a KubernetesExecutorConfig'
            )

    @staticmethod
    def from_legacy_obj(obj) -> Optional[k8s.V1Pod]:
        """Converts to pod from obj"""
        if obj is None:
            return None

        # We do not want to extract constant here from ExecutorLoader because it is just
        # A name in dictionary rather than executor selection mechanism and it causes cyclic import
        namespaced = obj.get("KubernetesExecutor", {})

        if not namespaced:
            return None

        resources = namespaced.get('resources')

        if resources is None:
            requests = {
                'cpu': namespaced.pop('request_cpu', None),
                'memory': namespaced.pop('request_memory', None),
                'ephemeral-storage': namespaced.get('ephemeral-storage'),  # We pop this one in limits
            }
            limits = {
                'cpu': namespaced.pop('limit_cpu', None),
                'memory': namespaced.pop('limit_memory', None),
                'ephemeral-storage': namespaced.pop('ephemeral-storage', None),
            }
            all_resources = list(requests.values()) + list(limits.values())
            if all(r is None for r in all_resources):
                resources = None
            else:
                # remove None's so they don't become 0's
                requests = {k: v for k, v in requests.items() if v is not None}
                limits = {k: v for k, v in limits.items() if v is not None}
                resources = k8s.V1ResourceRequirements(requests=requests, limits=limits)
        namespaced['resources'] = resources
        return PodGeneratorDeprecated(**namespaced).gen_pod()

    @staticmethod
    def reconcile_pods(base_pod: k8s.V1Pod, client_pod: Optional[k8s.V1Pod]) -> k8s.V1Pod:
        """
        :param base_pod: has the base attributes which are overwritten if they exist
            in the client pod and remain if they do not exist in the client_pod
        :type base_pod: k8s.V1Pod
        :param client_pod: the pod that the client wants to create.
        :type client_pod: k8s.V1Pod
        :return: the merged pods

        This can't be done recursively as certain fields some overwritten, and some concatenated.
        """
        if client_pod is None:
            return base_pod

        client_pod_cp = copy.deepcopy(client_pod)
        client_pod_cp.spec = PodGenerator.reconcile_specs(base_pod.spec, client_pod_cp.spec)
        client_pod_cp.metadata = PodGenerator.reconcile_metadata(base_pod.metadata, client_pod_cp.metadata)
        client_pod_cp = merge_objects(base_pod, client_pod_cp)

        return client_pod_cp

    @staticmethod
    def reconcile_metadata(base_meta, client_meta):
        """
        Merge kubernetes Metadata objects
        :param base_meta: has the base attributes which are overwritten if they exist
            in the client_meta and remain if they do not exist in the client_meta
        :type base_meta: k8s.V1ObjectMeta
        :param client_meta: the spec that the client wants to create.
        :type client_meta: k8s.V1ObjectMeta
        :return: the merged specs
        """
        if base_meta and not client_meta:
            return base_meta
        if not base_meta and client_meta:
            return client_meta
        elif client_meta and base_meta:
            client_meta.labels = merge_objects(base_meta.labels, client_meta.labels)
            client_meta.annotations = merge_objects(base_meta.annotations, client_meta.annotations)
            extend_object_field(base_meta, client_meta, 'managed_fields')
            extend_object_field(base_meta, client_meta, 'finalizers')
            extend_object_field(base_meta, client_meta, 'owner_references')
            return merge_objects(base_meta, client_meta)

        return None

    @staticmethod
    def reconcile_specs(
        base_spec: Optional[k8s.V1PodSpec], client_spec: Optional[k8s.V1PodSpec]
    ) -> Optional[k8s.V1PodSpec]:
        """
        :param base_spec: has the base attributes which are overwritten if they exist
            in the client_spec and remain if they do not exist in the client_spec
        :type base_spec: k8s.V1PodSpec
        :param client_spec: the spec that the client wants to create.
        :type client_spec: k8s.V1PodSpec
        :return: the merged specs
        """
        if base_spec and not client_spec:
            return base_spec
        if not base_spec and client_spec:
            return client_spec
        elif client_spec and base_spec:
            client_spec.containers = PodGenerator.reconcile_containers(
                base_spec.containers, client_spec.containers
            )
            merged_spec = extend_object_field(base_spec, client_spec, 'volumes')
            return merge_objects(base_spec, merged_spec)

        return None

    @staticmethod
    def reconcile_containers(
        base_containers: List[k8s.V1Container], client_containers: List[k8s.V1Container]
    ) -> List[k8s.V1Container]:
        """
        :param base_containers: has the base attributes which are overwritten if they exist
            in the client_containers and remain if they do not exist in the client_containers
        :type base_containers: List[k8s.V1Container]
        :param client_containers: the containers that the client wants to create.
        :type client_containers: List[k8s.V1Container]
        :return: the merged containers

        The runs recursively over the list of containers.
        """
        if not base_containers:
            return client_containers
        if not client_containers:
            return base_containers

        client_container = client_containers[0]
        base_container = base_containers[0]
        client_container = extend_object_field(base_container, client_container, 'volume_mounts')
        client_container = extend_object_field(base_container, client_container, 'env')
        client_container = extend_object_field(base_container, client_container, 'env_from')
        client_container = extend_object_field(base_container, client_container, 'ports')
        client_container = extend_object_field(base_container, client_container, 'volume_devices')
        client_container = merge_objects(base_container, client_container)

        return [client_container] + PodGenerator.reconcile_containers(
            base_containers[1:], client_containers[1:]
        )

    @staticmethod
    def construct_pod(  # pylint: disable=too-many-arguments
        dag_id: str,
        task_id: str,
        pod_id: str,
        try_number: int,
        kube_image: str,
        date: datetime.datetime,
        args: List[str],
        pod_override_object: Optional[k8s.V1Pod],
        base_worker_pod: k8s.V1Pod,
        namespace: str,
        scheduler_job_id: int,
    ) -> k8s.V1Pod:
        """
        Construct a pod by gathering and consolidating the configuration from 3 places:
            - airflow.cfg
            - executor_config
            - dynamic arguments
        """
        try:
            image = pod_override_object.spec.containers[0].image  # type: ignore
            if not image:
                image = kube_image
        except Exception:  # pylint: disable=W0703
            image = kube_image

        dynamic_pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(
                namespace=namespace,
                annotations={
                    'dag_id': dag_id,
                    'task_id': task_id,
                    'execution_date': date.isoformat(),
                    'try_number': str(try_number),
                },
                name=PodGenerator.make_unique_pod_id(pod_id),
                labels={
                    'airflow-worker': make_safe_label_value(str(scheduler_job_id)),
                    'dag_id': make_safe_label_value(dag_id),
                    'task_id': make_safe_label_value(task_id),
                    'execution_date': datetime_to_label_safe_datestring(date),
                    'try_number': str(try_number),
                    'airflow_version': airflow_version.replace('+', '-'),
                    'kubernetes_executor': 'True',
                },
            ),
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        args=args,
                        image=image,
                        env=[k8s.V1EnvVar(name="AIRFLOW_IS_K8S_EXECUTOR_POD", value="True")],
                    )
                ]
            ),
        )

        # Reconcile the pods starting with the first chronologically,
        # Pod from the pod_template_File -> Pod from executor_config arg -> Pod from the K8s executor
        pod_list = [base_worker_pod, pod_override_object, dynamic_pod]

        return reduce(PodGenerator.reconcile_pods, pod_list)

    @staticmethod
    def serialize_pod(pod: k8s.V1Pod) -> dict:
        """

        Converts a k8s.V1Pod into a jsonified object

        :param pod: k8s.V1Pod object
        :return: Serialized version of the pod returned as dict
        """
        api_client = ApiClient()
        return api_client.sanitize_for_serialization(pod)

    @staticmethod
    def deserialize_model_file(path: str) -> k8s.V1Pod:
        """
        :param path: Path to the file
        :return: a kubernetes.client.models.V1Pod

        Unfortunately we need access to the private method
        ``_ApiClient__deserialize_model`` from the kubernetes client.
        This issue is tracked here; https://github.com/kubernetes-client/python/issues/977.
        """
        if os.path.exists(path):
            with open(path) as stream:
                pod = yaml.safe_load(stream)
        else:
            pod = yaml.safe_load(path)

        # pylint: disable=protected-access
        return PodGenerator.deserialize_model_dict(pod)

    @staticmethod
    def deserialize_model_dict(pod_dict: dict) -> k8s.V1Pod:
        """
        Deserializes python dictionary to k8s.V1Pod

        :param pod_dict: Serialized dict of k8s.V1Pod object
        :return: De-serialized k8s.V1Pod
        """
        api_client = ApiClient()
        return api_client._ApiClient__deserialize_model(pod_dict, k8s.V1Pod)  # pylint: disable=W0212

    @staticmethod
    def make_unique_pod_id(pod_id: str) -> str:
        r"""
        Kubernetes pod names must consist of one or more lowercase
        rfc1035/rfc1123 labels separated by '.' with a maximum length of 253
        characters. Each label has a maximum length of 63 characters.

        Name must pass the following regex for validation
        ``^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$``

        For more details, see:
        https://github.com/kubernetes/kubernetes/blob/release-1.1/docs/design/identifiers.md

        :param pod_id: a dag_id with only alphanumeric characters
        :return: ``str`` valid Pod name of appropriate length
        """
        if not pod_id:
            return None

        safe_uuid = uuid.uuid4().hex  # safe uuid will always be less than 63 chars
        # Strip trailing '-' and '.' as they can't be followed by '.'
        trimmed_pod_id = pod_id[:MAX_LABEL_LEN].rstrip('-.')

        safe_pod_id = f"{trimmed_pod_id}.{safe_uuid}"
        return safe_pod_id


def merge_objects(base_obj, client_obj):
    """
    :param base_obj: has the base attributes which are overwritten if they exist
        in the client_obj and remain if they do not exist in the client_obj
    :param client_obj: the object that the client wants to create.
    :return: the merged objects
    """
    if not base_obj:
        return client_obj
    if not client_obj:
        return base_obj

    client_obj_cp = copy.deepcopy(client_obj)

    if isinstance(base_obj, dict) and isinstance(client_obj_cp, dict):
        base_obj_cp = copy.deepcopy(base_obj)
        base_obj_cp.update(client_obj_cp)
        return base_obj_cp

    for base_key in base_obj.to_dict().keys():
        base_val = getattr(base_obj, base_key, None)
        if not getattr(client_obj, base_key, None) and base_val:
            if not isinstance(client_obj_cp, dict):
                setattr(client_obj_cp, base_key, base_val)
            else:
                client_obj_cp[base_key] = base_val
    return client_obj_cp


def extend_object_field(base_obj, client_obj, field_name):
    """
    :param base_obj: an object which has a property `field_name` that is a list
    :param client_obj: an object which has a property `field_name` that is a list.
        A copy of this object is returned with `field_name` modified
    :param field_name: the name of the list field
    :type field_name: str
    :return: the client_obj with the property `field_name` being the two properties appended
    """
    client_obj_cp = copy.deepcopy(client_obj)
    base_obj_field = getattr(base_obj, field_name, None)
    client_obj_field = getattr(client_obj, field_name, None)

    if (not isinstance(base_obj_field, list) and base_obj_field is not None) or (
        not isinstance(client_obj_field, list) and client_obj_field is not None
    ):
        raise ValueError("The chosen field must be a list.")

    if not base_obj_field:
        return client_obj_cp
    if not client_obj_field:
        setattr(client_obj_cp, field_name, base_obj_field)
        return client_obj_cp

    appended_fields = base_obj_field + client_obj_field
    setattr(client_obj_cp, field_name, appended_fields)
    return client_obj_cp
