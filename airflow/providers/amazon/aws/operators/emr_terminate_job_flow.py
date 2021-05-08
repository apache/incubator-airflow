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


class EmrTerminateJobFlowOperator(BaseOperator):
    """
    Operator to terminate EMR JobFlows.

    :param job_flow_id: id of the JobFlow to terminate. (templated)
    :type job_flow_id: str
    :param aws_conn_id: aws connection to uses
    :type aws_conn_id: str
       (Adding `aws_conn_id` param to template_fields so that it can be overriden using jinja template from Dags
        This feature can be useful if user wants to update/override the aws_conn_id for some kind of Dag Isolation etc)
    """

    template_fields = ['job_flow_id' , 'aws_conn_id']
    template_ext = ()
    ui_color = '#f9c915'

    def __init__(self, *, job_flow_id: str, aws_conn_id: str = 'aws_default', **kwargs):
        super().__init__(**kwargs)
        self.job_flow_id = job_flow_id
        self.aws_conn_id = aws_conn_id

    def execute(self, context: Dict[str, Any]) -> None:
        emr = EmrHook(aws_conn_id=self.aws_conn_id).get_conn()

        self.log.info('Terminating JobFlow %s', self.job_flow_id)
        response = emr.terminate_job_flows(JobFlowIds=[self.job_flow_id])

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            raise AirflowException(f'JobFlow termination failed: {response}')
        else:
            self.log.info('JobFlow with id %s terminated', self.job_flow_id)
