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

from airflow.exceptions import AirflowException
from airflow.providers.tableau.hooks.tableau import TableauHook, TableauJobFinishCode
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class TableauJobFailedException(AirflowException):
    """An exception that indicates that a Job failed to complete."""


class TableauJobStatusSensor(BaseSensorOperator):
    """
    Watches the status of a Tableau Server Job.

    .. seealso:: https://tableau.github.io/server-client-python/docs/api-ref#jobs

    :param job_id: The job to watch.
    :type job_id: str
    :param site_id: The id of the site where the workbook belongs to.
    :type site_id: Optional[str]
    :param tableau_conn_id: The Tableau Connection id containing the credentials
        to authenticate to the Tableau Server.
    :type tableau_conn_id: str
    """

    template_fields = ('job_id',)

    @apply_defaults
    def __init__(
        self,
        *,
        job_id: str,
        site_id: Optional[str] = None,
        tableau_conn_id: str = 'tableau_default',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.tableau_conn_id = tableau_conn_id
        self.job_id = job_id
        self.site_id = site_id

    def poke(self, context: dict) -> bool:
        """
        Pokes until the job has successfully finished.

        :param context: The task context during execution.
        :type context: dict
        :return: True if it succeeded and False if not.
        :rtype: bool
        """
        with TableauHook(self.site_id, self.tableau_conn_id) as tableau_hook:
            finish_code = TableauJobFinishCode(
                int(tableau_hook.server.jobs.get_by_id(self.job_id).finish_code)
            )
            self.log.info('Current finishCode is %s (%s)', finish_code.name, finish_code.value)
            if finish_code in [TableauJobFinishCode.ERROR, TableauJobFinishCode.CANCELED]:
                raise TableauJobFailedException('The Tableau Refresh Workbook Job failed!')
            return finish_code == TableauJobFinishCode.SUCCESS
