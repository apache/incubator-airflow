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
        self.connection = self.get_connection(conn_id)
        self.default_params = self.connection.extra_dejson
        self.clean_default_parameters()

    def get_conn(self) -> Client:
        return self.client

    @cached_property
    def client(self) -> Client:
        """Instantiates python-asana Client"""
        if not self.connection.password:
            raise ValueError(
                "Asana connection password must contain a personal access token: "
                "https://developers.asana.com/docs/personal-access-token"
            )

        return Client.access_token(self.connection.password)

    def clean_default_parameters(self):
        """
        Downcase the parameter keys, check whether the user specified both 'projects' and
        'project', and rename the 'projects' key to 'project' if present.
        """
        self.default_params = {k.lower(): v for k, v in self.default_params.items()}
        if "projects" in self.default_params:
            if "project" in self.default_params:
                raise ValueError("You cannot specify both 'projects' and 'project' in default_params.")
            self.default_params["project"] = self.default_params.pop("projects")

    def create_task(self, task_name: str, task_parameters: dict) -> dict:
        """Creates an Asana task."""
        params = self.merge_create_task_parameters(task_name, task_parameters)
        self.validate_create_task_parameters(params)
        response = self.client.tasks.create(params=params)  # pylint: disable=no-member
        return response

    def merge_create_task_parameters(self, task_name: str, task_parameters: dict) -> dict:
        """Merge create_task parameters with default params."""
        params = {"name": task_name}
        params.update({k: v for k, v in self.default_params.items() if k != "project"})
        # The Asana API takes a "projects" parameter for create_task,
        # so rename the 'project' key if it appears in default_params
        if "project" in self.default_params:
            params["projects"] = self.default_params["project"]
        if task_parameters is not None:
            params.update(task_parameters)
        return params

    @staticmethod
    def validate_create_task_parameters(task_parameters: dict) -> None:
        """Check that user provided minimal create parameters."""
        required_parameters = {"workspace", "projects", "parent"}
        if required_parameters.isdisjoint(task_parameters):
            raise ValueError(f"You must specify at least one of {required_parameters} in the task_parameters")

    def delete_task(self, task_id: str) -> dict:
        """Deletes an Asana task."""
        response = self.client.tasks.delete_task(task_id)  # pylint: disable=no-member
        return response

    def find_task(self, search_parameters: dict) -> list:
        """Retrieves a list of Asana tasks that match search criteria."""
        params = {}
        params.update(self.default_params)
        params.update(search_parameters)
        self.validate_find_task_parameters(params)

        response = self.client.tasks.find_all(params=params)  # pylint: disable=no-member
        return list(response)

    @staticmethod
    def validate_find_task_parameters(search_parameters: dict) -> None:
        """Check that user provided minimal search parameters."""
        one_of_list = {"project", "section", "tag", "user_task_list"}
        both_of_list = {"assignee", "workspace"}
        contains_both = both_of_list.issubset(search_parameters)
        contains_one = not one_of_list.isdisjoint(search_parameters)
        if not (contains_both or contains_one):
            raise ValueError(
                f"You must specify at least one of {one_of_list} "
                f"or both of {both_of_list} in the search_parameters."
            )

    def update_task(self, task_id: str, task_parameters: dict) -> dict:
        """Updates an existing Asana task."""
        response = self.client.tasks.update(task_id, task_parameters)  # pylint: disable=no-member
        return response
