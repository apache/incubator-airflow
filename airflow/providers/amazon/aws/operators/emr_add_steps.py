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
import ast
from typing import Any, Dict, List, Optional, Union

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.emr import EmrHook


class EmrAddStepsOperator(BaseOperator):
    """
    An operator that adds steps to an existing EMR job_flow.

    :param job_flow_id: id of the JobFlow to add steps to. (templated)
    :type job_flow_id: Optional[str]
    :param job_flow_name: name of the JobFlow to add steps to. Use as an alternative to passing
        job_flow_id. will search for id of JobFlow with matching name in one of the states in
        param cluster_states. Exactly one cluster like this should exist or will fail. (templated)
    :type job_flow_name: Optional[str]
    :param cluster_states: Acceptable cluster states when searching for JobFlow id by job_flow_name.
        (templated)
    :type cluster_states: list
    :param aws_conn_id: aws connection to uses
    :type aws_conn_id: str
         (adding this param to template_fields so that it can be overriden using jinja template from Dags
           This feature can be useful if user wants to update/override the aws_conn_id for some kind of Dag Isolation etc) 
    :param steps: boto3 style steps or reference to a steps file (must be '.json') to
        be added to the jobflow. (templated)
    :type steps: list|str
    :param do_xcom_push: if True, job_flow_id is pushed to XCom with key job_flow_id.
    :type do_xcom_push: bool
    """

    template_fields = ['job_flow_id', 'job_flow_name', 'cluster_states', 'steps' , 'aws_conn_id']
    template_ext = ('.json',)
    ui_color = '#f9c915'

    def __init__(
        self,
        *,
        job_flow_id: Optional[str] = None,
        job_flow_name: Optional[str] = None,
        cluster_states: Optional[List[str]] = None,
        aws_conn_id: str = 'aws_default',
        steps: Optional[Union[List[dict], str]] = None,
        **kwargs,
    ):
        if kwargs.get('xcom_push') is not None:
            raise AirflowException("'xcom_push' was deprecated, use 'do_xcom_push' instead")
        if not (job_flow_id is None) ^ (job_flow_name is None):
            raise AirflowException('Exactly one of job_flow_id or job_flow_name must be specified.')
        super().__init__(**kwargs)
        cluster_states = cluster_states or []
        steps = steps or []
        self.aws_conn_id = aws_conn_id
        self.job_flow_id = job_flow_id
        self.job_flow_name = job_flow_name
        self.cluster_states = cluster_states
        self.steps = steps

    def execute(self, context: Dict[str, Any]) -> List[str]:
        emr_hook = EmrHook(aws_conn_id=self.aws_conn_id)

        emr = emr_hook.get_conn()

        job_flow_id = self.job_flow_id or emr_hook.get_cluster_id_by_name(
            str(self.job_flow_name), self.cluster_states
        )

        if not job_flow_id:
            raise AirflowException(f'No cluster found for name: {self.job_flow_name}')

        if self.do_xcom_push:
            context['ti'].xcom_push(key='job_flow_id', value=job_flow_id)

        self.log.info('Adding steps to %s', job_flow_id)

        # steps may arrive as a string representing a list
        # e.g. if we used XCom or a file then: steps="[{ step1 }, { step2 }]"
        steps = self.steps
        if isinstance(steps, str):
            steps = ast.literal_eval(steps)

        response = emr.add_job_flow_steps(JobFlowId=job_flow_id, Steps=steps)

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            raise AirflowException(f'Adding steps failed: {response}')
        else:
            self.log.info('Steps %s added to JobFlow', response['StepIds'])
            return response['StepIds']
