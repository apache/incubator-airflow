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
import json

from typing import Dict, List, Optional

from airflow.models import BaseOperator
from airflow.providers.asana.hooks.asana import AsanaHook
from airflow.utils.decorators import apply_defaults


class AsanaCreateTaskOperator(BaseOperator):
    """
    This operator can be used to create Asana tasks. For more information on Asana task parameters, see
    https://developers.asana.com/docs/create-a-task

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AsanaCreateTaskOperator`

    :param asana_conn_id: The Asana connection to use.
    :type asana_conn_id: str
    :param name: Name of the task.
    :type name: str
    :param workspace: Gid of a workspace.
    :type workspace: str
    :param approval_status: The approval status of the task
    :type approval_status: str
    :param assignee: User Gid of assignee.
    :type assignee: str
    :param custom_fields: Custom Fields for task.
    :type custom_fields: dict
    :param due_at: The UTC date and time on which this task is due.
    :type due_at: str
    :param due_on: The localized date on which this task is due, or null if the task has no due date.
    This takes a date with YYYY-MM-DD format and should not be used together with due_at.
    :type due_on: str
    :param followers: An array of strings identifying users.
    :type followers: list
    :param html_notes: The notes of the text with formatting as HTML.
    :type html_notes: str
    :param notes: Notes associated with the task.
    :type notes: str
    :param parent: Gid of parent task.
    :type parent: str
    :param projects: Array of project Gids.
    :type projects: list
    :param resource_subtype: The subtype of this resource.
    :type resource_subtype: str
    :param start_on: The day on which work begins for the task , or null if the task has no start date.
    This takes a date with YYYY-MM-DD format.
    :type start_on: str
    :param tags: Array of tag Gids.
    :type tags: list
    """

    @apply_defaults
    def __init__(
        self,
        *,
        asana_conn_id: str,
        name: str,
        workspace: str = None,
        approval_status: str = "pending",
        assignee: Optional[str] = None,
        custom_fields: Optional[dict] = None,
        due_at: Optional[str] = None,
        due_on: Optional[str] = None,
        followers: Optional[List[str]] = None,
        html_notes: Optional[str] = None,
        notes: Optional[str] = None,
        parent: Optional[str] = None,
        projects: Optional[List[str]] = None,
        resource_subtype: Optional[str] = None,
        start_on: Optional[str] = None,
        tags: Optional[List[str]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        if (due_at is not None) and (due_on is not None):
            raise ValueError("Please provide only one of the mutually exclusive arguments `due_at` and `due_on`.")

        self.asana_conn_id = asana_conn_id
        self.name = name
        self.workspace = workspace
        self.approval_status = approval_status
        self.assignee = assignee
        self.custom_fields = custom_fields
        self.due_at = due_at
        self.due_on = due_on
        self.followers = followers
        self.html_notes = html_notes
        self.notes = notes
        self.parent = parent
        self.projects = projects
        self.resource_subtype = resource_subtype
        self.start_on = start_on
        self.tags = tags
        self.hook = AsanaHook(conn_id=self.asana_conn_id)

    def execute(self, context: Dict) -> dict:
        asana_client = self.hook.get_conn()
        params = {}
        for key in ["name", "workspace", "approval_status", "assignee", "custom_fields",
                    "due_at", "due_on", "followers", "html_notes", "notes", "parent",
                    "projects", "resource_subtype", "start_on", "tags"]:
            if self.__dict__[key] is not None:
                params[key] = self.__dict__[key]
        response = asana_client.tasks.create(params=params)
        self.log.info(response)
        return response


class AsanaDeleteTaskOperator(BaseOperator):
    """
    This operator can be used to delete Asana tasks.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AsanaDeleteTaskOperator`

    :param asana_conn_id: The Asana connection to use.
    :type asana_conn_id: str
    :param task_gid: Task ID to delete.
    :type task_gid: str
    """

    @apply_defaults
    def __init__(
        self,
        *,
        asana_conn_id: str,
        task_gid: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.asana_conn_id = asana_conn_id
        self.task_gid = task_gid
        self.hook = AsanaHook(conn_id=self.asana_conn_id)

    def execute(self, context: Dict) -> None:
        asana_client = self.hook.get_conn()
        response = asana_client.tasks.delete_task(self.task_gid)
        self.log.info(response)
