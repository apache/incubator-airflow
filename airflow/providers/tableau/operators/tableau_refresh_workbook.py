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

from tableauserverclient import WorkbookItem

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.tableau.hooks.tableau import TableauHook
from airflow.utils.decorators import apply_defaults


class TableauRefreshWorkbookOperator(BaseOperator):
    """
    Refreshes a Tableau Workbook/Extract

    .. seealso:: https://tableau.github.io/server-client-python/docs/api-ref#workbooks

    :param workbook_name: The name of the workbook to refresh.
    :type workbook_name: str
    :param site_id: The id of the site where the workbook belongs to.
    :type site_id: Optional[str]
    :param blocking: By default the extract refresh will be blocking means it will wait until it has finished.
    :type blocking: bool
    :param tableau_conn_id: The Tableau Connection id containing the credentials
        to authenticate to the Tableau Server.
    :type tableau_conn_id: str
    """

    @apply_defaults
    def __init__(
        self,
        *,
        workbook_name: str,
        site_id: Optional[str] = None,
        blocking: bool = True,
        tableau_conn_id: str = 'tableau_default',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.workbook_name = workbook_name
        self.site_id = site_id
        self.blocking = blocking
        self.tableau_conn_id = tableau_conn_id

    def execute(self, context: dict) -> str:
        """
        Executes the Tableau Extract Refresh and pushes the job id to xcom.

        :param context: The task context during execution.
        :type context: dict
        :return: the id of the job that executes the extract refresh
        :rtype: str
        """
        with TableauHook(self.site_id, self.tableau_conn_id) as tableau_hook:
            workbook = self._get_workbook_by_name(tableau_hook)

            job_id = self._refresh_workbook(tableau_hook, workbook.id)
            if self.blocking:
                from airflow.providers.tableau.sensors.tableau_job_status import TableauJobStatusSensor

                TableauJobStatusSensor(
                    job_id=job_id,
                    site_id=self.site_id,
                    tableau_conn_id=self.tableau_conn_id,
                    task_id='wait_until_succeeded',
                    dag=None,
                ).execute(context={})
                self.log.info('Workbook %s has been successfully refreshed.', self.workbook_name)
            return job_id

    def _get_workbook_by_name(self, tableau_hook: TableauHook) -> WorkbookItem:
        for workbook in tableau_hook.get_all(resource_name='workbooks'):
            if workbook.name == self.workbook_name:
                self.log.info('Found matching workbook with id %s', workbook.id)
                return workbook

        raise AirflowException(f'Workbook {self.workbook_name} not found!')

    def _refresh_workbook(self, tableau_hook: TableauHook, workbook_id: str) -> str:
        job = tableau_hook.server.workbooks.refresh(workbook_id)
        self.log.info('Refreshing Workbook %s...', self.workbook_name)
        return job.id
