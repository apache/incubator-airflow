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
"""This module contains sensors for AWS CloudFormation."""
from typing import Optional

try:
    from functools import cached_property
except ImportError:
    from cached_property import cached_property

from airflow.providers.amazon.aws.hooks.cloud_formation import AWSCloudFormationHook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class CloudFormationCreateStackSensor(BaseSensorOperator):
    """
    Waits for a stack to be created successfully on AWS CloudFormation.

    :param stack_name: The name of the stack to wait for (templated)
    :type stack_name: str
    :param aws_conn_id: ID of the Airflow connection where credentials and extra configuration are
        stored
    :type aws_conn_id: str
    :param poke_interval: Time in seconds that the job should wait between each try
    :type poke_interval: int
    """

    template_fields = ['stack_name']
    ui_color = '#C5CAE9'

    @apply_defaults
    def __init__(self, *, stack_name, aws_conn_id='aws_default', region_name=None, **kwargs):
        super().__init__(**kwargs)
        self.stack_name = stack_name
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name

    def poke(self, context):
        stack_status = self.hook.get_stack_status(self.stack_name)
        if stack_status == 'CREATE_COMPLETE':
            return True
        if stack_status in ('CREATE_IN_PROGRESS', None):
            return False
        raise ValueError(f'Stack {self.stack_name} in bad state: {stack_status}')

    @cached_property
    def hook(self) -> AWSCloudFormationHook:
        """Create and return an AWSCloudFormationHook"""
        return AWSCloudFormationHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)


class CloudFormationDeleteStackSensor(BaseSensorOperator):
    """
    Waits for a stack to be deleted successfully on AWS CloudFormation.

    :param stack_name: The name of the stack to wait for (templated)
    :type stack_name: str
    :param aws_conn_id: ID of the Airflow connection where credentials and extra configuration are
        stored
    :type aws_conn_id: str
    :param poke_interval: Time in seconds that the job should wait between each try
    :type poke_interval: int
    """

    template_fields = ['stack_name']
    ui_color = '#C5CAE9'

    @apply_defaults
    def __init__(
        self,
        *,
        stack_name: str,
        aws_conn_id: str = 'aws_default',
        region_name: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.stack_name = stack_name

    def poke(self, context):
        stack_status = self.hook.get_stack_status(self.stack_name)
        if stack_status in ('DELETE_COMPLETE', None):
            return True
        if stack_status == 'DELETE_IN_PROGRESS':
            return False
        raise ValueError(f'Stack {self.stack_name} in bad state: {stack_status}')

    @cached_property
    def hook(self) -> AWSCloudFormationHook:
        """Create and return an AWSCloudFormationHook"""
        return AWSCloudFormationHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)
