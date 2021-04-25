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
from typing import Any, Optional

try:
    from functools import cached_property
except ImportError:
    from cached_property import cached_property

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.athena import AWSAthenaHook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class AthenaSensor(BaseSensorOperator):
    """
    Asks for the state of the Query until it reaches a failure state or success state.
    If the query fails, the task will fail.

    :param query_execution_id: query_execution_id to check the state of
    :type query_execution_id: str
    :param max_retries: Number of times to poll for query state before
        returning the current state, defaults to None
    :type max_retries: int
    :param aws_conn_id: aws connection to use, defaults to 'aws_default'
    :type aws_conn_id: str
    :param sleep_time: Time in seconds to wait between two consecutive call to
        check query status on athena, defaults to 10
    :type sleep_time: int
    """

    INTERMEDIATE_STATES = (
        'QUEUED',
        'RUNNING',
    )
    FAILURE_STATES = (
        'FAILED',
        'CANCELLED',
    )
    SUCCESS_STATES = ('SUCCEEDED',)

    template_fields = ['query_execution_id']
    template_ext = ()
    ui_color = '#66c3ff'

    @apply_defaults
    def __init__(
        self,
        *,
        query_execution_id: str,
        max_retries: Optional[int] = None,
        aws_conn_id: str = 'aws_default',
        sleep_time: int = 10,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.query_execution_id = query_execution_id
        self.sleep_time = sleep_time
        self.max_retries = max_retries

    def poke(self, context: dict) -> bool:
        state = self.hook.poll_query_status(self.query_execution_id, self.max_retries)

        if state in self.FAILURE_STATES:
            raise AirflowException('Athena sensor failed')

        if state in self.INTERMEDIATE_STATES:
            return False
        return True

    @cached_property
    def hook(self) -> AWSAthenaHook:
        """Create and return an AWSAthenaHook"""
        return AWSAthenaHook(self.aws_conn_id, self.sleep_time)
