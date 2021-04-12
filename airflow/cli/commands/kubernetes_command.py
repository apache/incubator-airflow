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
"""Kubernetes sub-commands"""
import os
import sys

from kubernetes import client
from kubernetes.client.api_client import ApiClient
from kubernetes.client.rest import ApiException

import airflow.utils.yaml as yaml
from airflow.executors.kubernetes_executor import KubeConfig, create_pod_id
from airflow.kubernetes import pod_generator
from airflow.kubernetes.kube_client import get_kube_client
from airflow.kubernetes.pod_generator import PodGenerator
from airflow.models import TaskInstance
from airflow.settings import pod_mutation_hook
from airflow.utils import cli as cli_utils
from airflow.utils.cli import get_dag


@cli_utils.action_logging
def generate_pod_yaml(args):
    """Generates yaml files for each task in the DAG. Used for testing output of KubernetesExecutor"""
    execution_date = args.execution_date
    dag = get_dag(subdir=args.subdir, dag_id=args.dag_id)
    yaml_output_path = args.output_path
    kube_config = KubeConfig()
    for task in dag.tasks:
        ti = TaskInstance(task, execution_date)
        pod = PodGenerator.construct_pod(
            dag_id=args.dag_id,
            task_id=ti.task_id,
            pod_id=create_pod_id(args.dag_id, ti.task_id),
            try_number=ti.try_number,
            kube_image=kube_config.kube_image,
            date=ti.execution_date,
            args=ti.command_as_list(),
            pod_override_object=PodGenerator.from_obj(ti.executor_config),
            scheduler_job_id="worker-config",
            namespace=kube_config.executor_namespace,
            base_worker_pod=PodGenerator.deserialize_model_file(kube_config.pod_template_file),
        )
        pod_mutation_hook(pod)
        api_client = ApiClient()
        date_string = pod_generator.datetime_to_label_safe_datestring(execution_date)
        yaml_file_name = f"{args.dag_id}_{ti.task_id}_{date_string}.yml"
        os.makedirs(os.path.dirname(yaml_output_path + "/airflow_yaml_output/"), exist_ok=True)
        with open(yaml_output_path + "/airflow_yaml_output/" + yaml_file_name, "w") as output:
            sanitized_pod = api_client.sanitize_for_serialization(pod)
            output.write(yaml.dump(sanitized_pod))
    print(f"YAML output can be found at {yaml_output_path}/airflow_yaml_output/")


@cli_utils.action_logging
def cleanup_pods(args):
    """Clean up k8s pods in evicted/failed/succeeded states"""
    namespace = args.namespace

    # https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/
    # All Containers in the Pod have terminated in success, and will not be restarted.
    pod_succeeded = 'succeeded'

    # All Containers in the Pod have terminated, and at least one Container has terminated in failure.
    # That is, the Container either exited with non-zero status or was terminated by the system.
    pod_failed = 'failed'

    # https://kubernetes.io/docs/tasks/administer-cluster/out-of-resource/
    pod_reason_evicted = 'evicted'
    # If pod is failed and restartPolicy is:
    # * Always: Restart Container; Pod phase stays Running.
    # * OnFailure: Restart Container; Pod phase stays Running.
    # * Never: Pod phase becomes Failed.
    pod_restart_policy_never = 'never'

    print('Loading Kubernetes configuration')
    kube_client = get_kube_client()
    print(f'Listing pods in namespace {namespace}')
    airflow_pod_labels = [
        'dag_id',
        'task_id',
        'execution_date',
        'try_number',
        'airflow_version',
    ]
    list_kwargs = {
        "namespace": namespace,
        "limit": 500,
        "label_selector": client.V1LabelSelector(
            match_expressions=[
                client.V1LabelSelectorRequirement(key=label, operator="Exists")
                for label in airflow_pod_labels
            ]
        ),
    }
    while True:  # pylint: disable=too-many-nested-blocks
        pod_list = kube_client.list_namespaced_pod(**list_kwargs)
        for pod in pod_list.items:
            pod_name = pod.metadata.name
            print(f'Inspecting pod {pod_name}')
            pod_phase = pod.status.phase.lower()
            pod_reason = pod.status.reason.lower() if pod.status.reason else ''
            pod_restart_policy = pod.spec.restart_policy.lower()

            if (
                pod_phase == pod_succeeded
                or (pod_phase == pod_failed and pod_restart_policy == pod_restart_policy_never)
                or (pod_reason == pod_reason_evicted)
            ):
                print(
                    f'Deleting pod "{pod_name}" phase "{pod_phase}" and reason "{pod_reason}", '
                    f'restart policy "{pod_restart_policy}"'
                )
                try:
                    _delete_pod(pod.metadata.name, namespace)
                except ApiException as e:
                    print(f"Can't remove POD: {e}", file=sys.stderr)
                continue
            print(f'No action taken on pod {pod_name}')
        continue_token = pod_list.metadata._continue  # pylint: disable=protected-access
        if not continue_token:
            break
        list_kwargs["_continue"] = continue_token


def _delete_pod(name, namespace):
    """Helper Function for cleanup_pods"""
    core_v1 = client.CoreV1Api()
    delete_options = client.V1DeleteOptions()
    print(f'Deleting POD "{name}" from "{namespace}" namespace')
    api_response = core_v1.delete_namespaced_pod(name=name, namespace=namespace, body=delete_options)
    print(api_response)
