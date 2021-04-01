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
from typing import Any, Optional

from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook


class AirbyteHook(HttpHook):
    """
    Hook for Airbyte API

    :param airbyte_conn_id: Required. The name of the Airflow connection to get
        connection information for Airbyte.
    :type airbyte_conn_id: str
    :param api_version: Optional. Airbyte API version.
    :type api_version: str
    """

    RUNNING = "running"
    SUCCEEDED = "succeeded"
    CANCELLED = "cancelled"
    PENDING = "pending"
    FAILED = "failed"
    ERROR = "error"

    def __init__(self, airbyte_conn_id: str = "airbyte_default", api_version: Optional[str] = "v1") -> None:
        super().__init__(http_conn_id=airbyte_conn_id)
        self.api_version: str = api_version

    def wait_for_job(
        self, job_id: str, wait_seconds: Optional[float] = 3, timeout: Optional[float] = 3600
    ) -> None:
        """
        Helper method which polls a job to check if it finishes.

        :param job_id: Required. Id of the Airbyte job
        :type job_id: str
        :param wait_seconds: Optional. Number of seconds between checks.
        :type wait_seconds: float
        :param timeout: Optional. How many seconds wait for job to be ready.
            Used only if ``asynchronous`` is False.
        :type timeout: float
        """
        state = None
        start = time.monotonic()
        while True:
            if timeout and start + timeout < time.monotonic():
                raise AirflowException(f"Timeout: Airbyte job {job_id} is not ready after {timeout}s")
            time.sleep(wait_seconds)
            try:
                job = self.get_job(job_id=job_id)
                state = job.json()["job"]["status"]
            except AirflowException as err:
                self.log.info("Retrying. Airbyte API returned server error when waiting for job: %s", err)
                continue

            if state in (self.RUNNING, self.PENDING):
                continue
            if state == self.SUCCEEDED:
                break
            if state == self.ERROR:
                raise AirflowException(f"Job failed:\n{job}")
            elif state == self.CANCELLED:
                raise AirflowException(f"Job was cancelled:\n{job}")
            else:
                raise Exception(f"Encountered unexpected state `{state}` for job_id `{job_id}`")

    def submit_sync_connection(self, connection_id: str) -> Any:
        """
        Submits a job to a Airbyte server.

        :param connection_id: Required. The ConnectionId of the Airbyte Connection.
        :type connectiond_id: str
        """
        return self.run(
            endpoint=f"api/{self.api_version}/connections/sync",
            json={"connectionId": connection_id},
            headers={"accept": "application/json"},
        )

    def get_job(self, job_id: int) -> Any:
        """
        Gets the resource representation for a job in Airbyte.

        :param job_id: Required. Id of the Airbyte job
        :type job_id: int
        """
        return self.run(
            endpoint=f"api/{self.api_version}/jobs/get",
            json={"id": job_id},
            headers={"accept": "application/json"},
        )
