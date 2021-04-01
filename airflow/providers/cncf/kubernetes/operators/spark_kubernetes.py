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
from typing import Optional

from airflow.models import BaseOperator
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.utils.decorators import apply_defaults


class SparkKubernetesOperator(BaseOperator):
    """
    Creates sparkApplication object in kubernetes cluster:

    .. seealso::
        For more detail about Spark Application Object have a look at the reference:
        https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/v1beta2-1.1.0-2.4.5/docs/api-docs.md#sparkapplication

    :param application_file: filepath to kubernetes custom_resource_definition of sparkApplication
    :type application_file:  str
    :param namespace: kubernetes namespace to put sparkApplication
    :type namespace: str
    :param kubernetes_conn_id: the connection to Kubernetes cluster
    :type kubernetes_conn_id: str
    :param api_group: kubernetes api group of sparkApplication
    :type api_group: str
    :param api_version: kubernetes api version of sparkApplication
    :type api_version: str
    """

    template_fields = ['application_file', 'namespace']
    template_ext = ('yaml', 'yml', 'json')
    ui_color = '#f4a460'

    @apply_defaults
    def __init__(
        self,
        *,
        application_file: str,
        namespace: Optional[str] = None,
        kubernetes_conn_id: str = 'kubernetes_default',
        api_group: str = 'sparkoperator.k8s.io',
        api_version: str = 'v1beta2',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.application_file = application_file
        self.namespace = namespace
        self.kubernetes_conn_id = kubernetes_conn_id
        self.api_group = api_group
        self.api_version = api_version

    def execute(self, context):
        self.log.info("Creating sparkApplication")
        hook = KubernetesHook(conn_id=self.kubernetes_conn_id)
        response = hook.create_custom_object(
            group=self.api_group,
            version=self.api_version,
            plural="sparkapplications",
            body=self.application_file,
            namespace=self.namespace,
        )
        return response
