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

from typing import Any, Dict

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.emr import EmrHook


class EmrModifyClusterOperator(BaseOperator):
    """
    An operator that modifies an existing EMR cluster.
    :param cluster_id: cluster identifier
    :type cluster_id: str
    :param step_concurrency_level: Concurrency of the cluster
    :type step_concurrency_level: int
    :param aws_conn_id: aws connection to uses
    :type aws_conn_id: str
        (Adding `aws_conn_id` param to template_fields so that it can be overriden using jinja template from Dags
         This feature can be useful if user wants to update/override the aws_conn_id for some kind of Dag Isolation etc)
    :param do_xcom_push: if True, cluster_id is pushed to XCom with key cluster_id.
    :type do_xcom_push: bool
    """

    template_fields = ['cluster_id', 'step_concurrency_level' , 'aws_conn_id']
    template_ext = ()
    ui_color = '#f9c915'

    def __init__(
        self, *, cluster_id: str, step_concurrency_level: int, aws_conn_id: str = 'aws_default', **kwargs
    ):
        if kwargs.get('xcom_push') is not None:
            raise AirflowException("'xcom_push' was deprecated, use 'do_xcom_push' instead")
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.cluster_id = cluster_id
        self.step_concurrency_level = step_concurrency_level

    def execute(self, context: Dict[str, Any]) -> int:
        emr_hook = EmrHook(aws_conn_id=self.aws_conn_id)

        emr = emr_hook.get_conn()

        if self.do_xcom_push:
            context['ti'].xcom_push(key='cluster_id', value=self.cluster_id)

        self.log.info('Modifying cluster %s', self.cluster_id)
        response = emr.modify_cluster(
            ClusterId=self.cluster_id, StepConcurrencyLevel=self.step_concurrency_level
        )

        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise AirflowException(f'Modify cluster failed: {response}')
        else:
            self.log.info('Steps concurrency level %d', response['StepConcurrencyLevel'])
            return response['StepConcurrencyLevel']
