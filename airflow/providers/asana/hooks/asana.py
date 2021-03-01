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

"""Connect to Asana."""

from asana import Client
from cached_property import cached_property

from airflow.hooks.base import BaseHook


class AsanaHook(BaseHook):
    """Wrapper around Asana Python client library."""

    conn_name_attr = "asana_conn_id"
    default_conn_name = "asana_default"
    conn_type = "asana"
    hook_name = "Asana"

    def __init__(self, conn_id: str = default_conn_name, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.asana_conn_id = conn_id
        self.uri = None

    @cached_property
    def client(self) -> Client:
        connection = self.get_connection(self.asana_conn_id)

        if not connection.password:
            raise ValueError(
                "Asana connection password must contain a personal access token: "
                "https://developers.asana.com/docs/personal-access-token"
            )

        return Client.access_token(connection.password)

    def get_conn(self) -> Client:
        return self.client

    def create_task(self, task_name: str, task_parameters: dict) -> dict:
        params = {"name": task_name}
        if task_parameters is not None:
            params.update(task_parameters)
        response = self.client.tasks.create(params=params)  # pylint: disable=no-member
        return response

    def update_task(self, task_id: str, task_parameters: dict) -> dict:
        response = self.client.tasks.update(task_id, task_parameters)  # pylint: disable=no-member
        return response

    def find_task(self, search_parameters: dict) -> list:
        response = self.client.tasks.find_all(params=search_parameters)  # pylint: disable=no-member
        return list(response)

    def delete_task(self, task_id: str) -> dict:
        response = self.client.tasks.delete_task(task_id)  # pylint: disable=no-member
        return response
