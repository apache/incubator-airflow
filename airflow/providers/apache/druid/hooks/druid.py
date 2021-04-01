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

import time
from typing import Any, Dict, Iterable, Optional, Tuple

import requests
from pydruid.db import connect

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.hooks.dbapi import DbApiHook


class DruidHook(BaseHook):
    """
    Connection to Druid overlord for ingestion

    To connect to a Druid cluster that is secured with the druid-basic-security
    extension, add the username and password to the druid ingestion connection.

    :param druid_ingest_conn_id: The connection id to the Druid overlord machine
                                 which accepts index jobs
    :type druid_ingest_conn_id: str
    :param timeout: The interval between polling
                    the Druid job for the status of the ingestion job.
                    Must be greater than or equal to 1
    :type timeout: int
    :param max_ingestion_time: The maximum ingestion time before assuming the job failed
    :type max_ingestion_time: int
    """

    def __init__(
        self,
        druid_ingest_conn_id: str = 'druid_ingest_default',
        timeout: int = 1,
        max_ingestion_time: Optional[int] = None,
    ) -> None:

        super().__init__()
        self.druid_ingest_conn_id = druid_ingest_conn_id
        self.timeout = timeout
        self.max_ingestion_time = max_ingestion_time
        self.header = {'content-type': 'application/json'}

        if self.timeout < 1:
            raise ValueError("Druid timeout should be equal or greater than 1")

    def get_conn_url(self) -> str:
        """Get Druid connection url"""
        conn = self.get_connection(self.druid_ingest_conn_id)
        host = conn.host
        port = conn.port
        conn_type = 'http' if not conn.conn_type else conn.conn_type
        endpoint = conn.extra_dejson.get('endpoint', '')
        return f"{conn_type}://{host}:{port}/{endpoint}"

    def get_auth(self) -> Optional[requests.auth.HTTPBasicAuth]:
        """
        Return username and password from connections tab as requests.auth.HTTPBasicAuth object.

        If these details have not been set then returns None.
        """
        conn = self.get_connection(self.druid_ingest_conn_id)
        user = conn.login
        password = conn.password
        if user is not None and password is not None:
            return requests.auth.HTTPBasicAuth(user, password)
        else:
            return None

    def submit_indexing_job(self, json_index_spec: Dict[str, Any]) -> None:
        """Submit Druid ingestion job"""
        url = self.get_conn_url()

        self.log.info("Druid ingestion spec: %s", json_index_spec)
        req_index = requests.post(url, data=json_index_spec, headers=self.header, auth=self.get_auth())
        if req_index.status_code != 200:
            raise AirflowException(f'Did not get 200 when submitting the Druid job to {url}')

        req_json = req_index.json()
        # Wait until the job is completed
        druid_task_id = req_json['task']
        self.log.info("Druid indexing task-id: %s", druid_task_id)

        running = True

        sec = 0
        while running:
            req_status = requests.get(f"{url}/{druid_task_id}/status", auth=self.get_auth())

            self.log.info("Job still running for %s seconds...", sec)

            if self.max_ingestion_time and sec > self.max_ingestion_time:
                # ensure that the job gets killed if the max ingestion time is exceeded
                requests.post(f"{url}/{druid_task_id}/shutdown", auth=self.get_auth())
                raise AirflowException('Druid ingestion took more than ' f'{self.max_ingestion_time} seconds')

            time.sleep(self.timeout)

            sec += self.timeout

            status = req_status.json()['status']['status']
            if status == 'RUNNING':
                running = True
            elif status == 'SUCCESS':
                running = False  # Great success!
            elif status == 'FAILED':
                raise AirflowException('Druid indexing job failed, check console for more info')
            else:
                raise AirflowException(f'Could not get status of the job, got {status}')

        self.log.info('Successful index')


class DruidDbApiHook(DbApiHook):
    """
    Interact with Druid broker

    This hook is purely for users to query druid broker.
    For ingestion, please use druidHook.
    """

    conn_name_attr = 'druid_broker_conn_id'
    default_conn_name = 'druid_broker_default'
    conn_type = 'druid'
    hook_name = 'Druid'
    supports_autocommit = False

    def get_conn(self) -> connect:
        """Establish a connection to druid broker."""
        conn = self.get_connection(self.conn_name_attr)
        druid_broker_conn = connect(
            host=conn.host,
            port=conn.port,
            path=conn.extra_dejson.get('endpoint', '/druid/v2/sql'),
            scheme=conn.extra_dejson.get('schema', 'http'),
            user=conn.login,
            password=conn.password,
        )
        self.log.info('Get the connection to druid broker on %s using user %s', conn.host, conn.login)
        return druid_broker_conn

    def get_uri(self) -> str:
        """
        Get the connection uri for druid broker.

        e.g: druid://localhost:8082/druid/v2/sql/
        """
        conn = self.get_connection(getattr(self, self.conn_name_attr))
        host = conn.host
        if conn.port is not None:
            host += f':{conn.port}'
        conn_type = 'druid' if not conn.conn_type else conn.conn_type
        endpoint = conn.extra_dejson.get('endpoint', 'druid/v2/sql')
        return f'{conn_type}://{host}/{endpoint}'

    def set_autocommit(self, conn: connect, autocommit: bool) -> NotImplemented:
        raise NotImplementedError()

    def insert_rows(
        self,
        table: str,
        rows: Iterable[Tuple[str]],
        target_fields: Optional[Iterable[str]] = None,
        commit_every: int = 1000,
        replace: bool = False,
        **kwargs: Any,
    ) -> NotImplemented:
        raise NotImplementedError()
