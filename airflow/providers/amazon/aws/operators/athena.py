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
#
from typing import Any, Dict, Optional
from uuid import uuid4

try:
    from functools import cached_property
except ImportError:
    from cached_property import cached_property

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.athena import AWSAthenaHook
from airflow.utils.decorators import apply_defaults


class AWSAthenaOperator(BaseOperator):
    """
    An operator that submits a presto query to athena.

    :param query: Presto to be run on athena. (templated)
    :type query: str
    :param database: Database to select. (templated)
    :type database: str
    :param output_location: s3 path to write the query results into. (templated)
    :type output_location: str
    :param aws_conn_id: aws connection to use
    :type aws_conn_id: str
    :param client_request_token: Unique token created by user to avoid multiple executions of same query
    :type client_request_token: str
    :param workgroup: Athena workgroup in which query will be run
    :type workgroup: str
    :param query_execution_context: Context in which query need to be run
    :type query_execution_context: dict
    :param result_configuration: Dict with path to store results in and config related to encryption
    :type result_configuration: dict
    :param sleep_time: Time (in seconds) to wait between two consecutive calls to check query status on Athena
    :type sleep_time: int
    :param max_tries: Number of times to poll for query state before function exits
    :type max_tries: int
    """

    ui_color = '#44b5e2'
    template_fields = ('query', 'database', 'output_location')
    template_ext = ('.sql',)

    @apply_defaults
    def __init__(  # pylint: disable=too-many-arguments
        self,
        *,
        query: str,
        database: str,
        output_location: str,
        aws_conn_id: str = "aws_default",
        client_request_token: Optional[str] = None,
        workgroup: str = "primary",
        query_execution_context: Optional[Dict[str, str]] = None,
        result_configuration: Optional[Dict[str, Any]] = None,
        sleep_time: int = 30,
        max_tries: Optional[int] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.query = query
        self.database = database
        self.output_location = output_location
        self.aws_conn_id = aws_conn_id
        self.client_request_token = client_request_token or str(uuid4())
        self.workgroup = workgroup
        self.query_execution_context = query_execution_context or {}
        self.result_configuration = result_configuration or {}
        self.sleep_time = sleep_time
        self.max_tries = max_tries
        self.query_execution_id = None  # type: Optional[str]

    @cached_property
    def hook(self) -> AWSAthenaHook:
        """Create and return an AWSAthenaHook."""
        return AWSAthenaHook(self.aws_conn_id, sleep_time=self.sleep_time)

    def execute(self, context: dict) -> Optional[str]:
        """Run Presto Query on Athena"""
        self.query_execution_context['Database'] = self.database
        self.result_configuration['OutputLocation'] = self.output_location
        self.query_execution_id = self.hook.run_query(
            self.query,
            self.query_execution_context,
            self.result_configuration,
            self.client_request_token,
            self.workgroup,
        )
        query_status = self.hook.poll_query_status(self.query_execution_id, self.max_tries)

        if query_status in AWSAthenaHook.FAILURE_STATES:
            error_message = self.hook.get_state_change_reason(self.query_execution_id)
            raise Exception(
                'Final state of Athena job is {}, query_execution_id is {}. Error: {}'.format(
                    query_status, self.query_execution_id, error_message
                )
            )
        elif not query_status or query_status in AWSAthenaHook.INTERMEDIATE_STATES:
            raise Exception(
                'Final state of Athena job is {}. '
                'Max tries of poll status exceeded, query_execution_id is {}.'.format(
                    query_status, self.query_execution_id
                )
            )

        return self.query_execution_id

    def on_kill(self) -> None:
        """Cancel the submitted athena query"""
        if self.query_execution_id:
            self.log.info('Received a kill signal.')
            self.log.info('Stopping Query with executionId - %s', self.query_execution_id)
            response = self.hook.stop_query(self.query_execution_id)
            http_status_code = None
            try:
                http_status_code = response['ResponseMetadata']['HTTPStatusCode']
            except Exception as ex:  # pylint: disable=broad-except
                self.log.error('Exception while cancelling query: %s', ex)
            finally:
                if http_status_code is None or http_status_code != 200:
                    self.log.error('Unable to request query cancel on athena. Exiting')
                else:
                    self.log.info(
                        'Polling Athena for query with id %s to reach final state', self.query_execution_id
                    )
                    self.hook.poll_query_status(self.query_execution_id)
