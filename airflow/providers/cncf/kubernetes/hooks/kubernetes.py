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
import tempfile
from typing import Any, Dict, Generator, Optional, Tuple, Union

try:
    from functools import cached_property
except ImportError:
    from cached_property import cached_property
from kubernetes import client, config, watch

try:
    import airflow.utils.yaml as yaml
except ImportError:
    import yaml

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


def _load_body_to_dict(body):
    try:
        body_dict = yaml.safe_load(body)
    except yaml.YAMLError as e:
        raise AirflowException(f"Exception when loading resource definition: {e}\n")
    return body_dict


class KubernetesHook(BaseHook):
    """
    Creates Kubernetes API connection.

    - use in cluster configuration by using ``extra__kubernetes__in_cluster`` in connection
    - use custom config by providing path to the file using ``extra__kubernetes__kube_config_path``
    - use custom configuration by providing content of kubeconfig file via
        ``extra__kubernetes__kube_config`` in connection
    - use default config by providing no extras

    This hook check for configuration option in the above order. Once an option is present it will
    use this configuration.

    .. seealso::
        For more information about Kubernetes connection:
        :doc:`/connections/kubernetes`

    :param conn_id: the connection to Kubernetes cluster
    :type conn_id: str
    """

    conn_name_attr = 'kubernetes_conn_id'
    default_conn_name = 'kubernetes_default'
    conn_type = 'kubernetes'
    hook_name = 'Kubernetes Cluster Connection'

    @staticmethod
    def get_connection_form_widgets() -> Dict[str, Any]:
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import BooleanField, StringField

        return {
            "extra__kubernetes__in_cluster": BooleanField(lazy_gettext('In cluster configuration')),
            "extra__kubernetes__kube_config_path": StringField(
                lazy_gettext('Kube config path'), widget=BS3TextFieldWidget()
            ),
            "extra__kubernetes__kube_config": StringField(
                lazy_gettext('Kube config (JSON format)'), widget=BS3TextFieldWidget()
            ),
            "extra__kubernetes__namespace": StringField(
                lazy_gettext('Namespace'), widget=BS3TextFieldWidget()
            ),
        }

    @staticmethod
    def get_ui_field_behaviour() -> Dict:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ['host', 'schema', 'login', 'password', 'port', 'extra'],
            "relabeling": {},
        }

    def __init__(
        self, conn_id: str = default_conn_name, client_configuration: Optional[client.Configuration] = None
    ) -> None:
        super().__init__()
        self.conn_id = conn_id
        self.client_configuration = client_configuration

    def get_conn(self) -> Any:
        """Returns kubernetes api session for use with requests"""
        connection = self.get_connection(self.conn_id)
        extras = connection.extra_dejson
        in_cluster = extras.get("extra__kubernetes__in_cluster")
        kubeconfig_path = extras.get("extra__kubernetes__kube_config_path")
        kubeconfig = extras.get("extra__kubernetes__kube_config")
        num_selected_configuration = len([o for o in [in_cluster, kubeconfig, kubeconfig_path] if o])

        if num_selected_configuration > 1:
            raise AirflowException(
                "Invalid connection configuration. Options extra__kubernetes__kube_config_path, "
                "extra__kubernetes__kube_config, extra__kubernetes__in_cluster are mutually exclusive. "
                "You can only use one option at a time."
            )
        if in_cluster:
            self.log.debug("loading kube_config from: in_cluster configuration")
            config.load_incluster_config()
            return client.ApiClient()

        if kubeconfig_path is not None:
            self.log.debug("loading kube_config from: %s", kubeconfig_path)
            config.load_kube_config(
                config_file=kubeconfig_path, client_configuration=self.client_configuration
            )
            return client.ApiClient()

        if kubeconfig is not None:
            with tempfile.NamedTemporaryFile() as temp_config:
                self.log.debug("loading kube_config from: connection kube_config")
                temp_config.write(kubeconfig.encode())
                temp_config.flush()
                config.load_kube_config(
                    config_file=temp_config.name, client_configuration=self.client_configuration
                )
            return client.ApiClient()

        self.log.debug("loading kube_config from: default file")
        config.load_kube_config(client_configuration=self.client_configuration)
        return client.ApiClient()

    @cached_property
    def api_client(self) -> Any:
        """Cached Kubernetes API client"""
        return self.get_conn()

    def create_custom_object(
        self, group: str, version: str, plural: str, body: Union[str, dict], namespace: Optional[str] = None
    ):
        """
        Creates custom resource definition object in Kubernetes

        :param group: api group
        :type group: str
        :param version: api version
        :type version: str
        :param plural: api plural
        :type plural: str
        :param body: crd object definition
        :type body: Union[str, dict]
        :param namespace: kubernetes namespace
        :type namespace: str
        """
        api = client.CustomObjectsApi(self.api_client)
        if namespace is None:
            namespace = self.get_namespace()
        if isinstance(body, str):
            body = _load_body_to_dict(body)
        try:
            response = api.create_namespaced_custom_object(
                group=group, version=version, namespace=namespace, plural=plural, body=body
            )
            self.log.debug("Response: %s", response)
            return response
        except client.rest.ApiException as e:
            raise AirflowException(f"Exception when calling -> create_custom_object: {e}\n")

    def get_custom_object(
        self, group: str, version: str, plural: str, name: str, namespace: Optional[str] = None
    ):
        """
        Get custom resource definition object from Kubernetes

        :param group: api group
        :type group: str
        :param version: api version
        :type version: str
        :param plural: api plural
        :type plural: str
        :param name: crd object name
        :type name: str
        :param namespace: kubernetes namespace
        :type namespace: str
        """
        api = client.CustomObjectsApi(self.api_client)
        if namespace is None:
            namespace = self.get_namespace()
        try:
            response = api.get_namespaced_custom_object(
                group=group, version=version, namespace=namespace, plural=plural, name=name
            )
            return response
        except client.rest.ApiException as e:
            raise AirflowException(f"Exception when calling -> get_custom_object: {e}\n")

    def get_namespace(self) -> str:
        """Returns the namespace that defined in the connection"""
        connection = self.get_connection(self.conn_id)
        extras = connection.extra_dejson
        namespace = extras.get("extra__kubernetes__namespace", "default")
        return namespace

    def get_pod_log_stream(
        self,
        pod_name: str,
        container: Optional[str] = "",
        namespace: Optional[str] = None,
    ) -> Tuple[watch.Watch, Generator[str, None, None]]:
        """
        Retrieves a log stream for a container in a kubernetes pod.

        :param pod_name: pod name
        :type pod_name: str
        :param container: container name
        :param namespace: kubernetes namespace
        :type namespace: str
        """
        api = client.CoreV1Api(self.api_client)
        watcher = watch.Watch()
        return (
            watcher,
            watcher.stream(
                api.read_namespaced_pod_log,
                name=pod_name,
                container=container,
                namespace=namespace if namespace else self.get_namespace(),
            ),
        )

    def get_pod_logs(
        self,
        pod_name: str,
        container: Optional[str] = "",
        namespace: Optional[str] = None,
    ):
        """
        Retrieves a container's log from the specified pod.

        :param pod_name: pod name
        :type pod_name: str
        :param container: container name
        :param namespace: kubernetes namespace
        :type namespace: str
        """
        api = client.CoreV1Api(self.api_client)
        return api.read_namespaced_pod_log(
            name=pod_name,
            container=container,
            _preload_content=False,
            namespace=namespace if namespace else self.get_namespace(),
        )
