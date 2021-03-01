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

from typing import Dict, Optional

from airflow.models import BaseOperator
from airflow.providers.asana.hooks.asana import AsanaHook
from airflow.utils.decorators import apply_defaults


class AsanaCreateTaskOperator(BaseOperator):
    """
    This operator can be used to create Asana tasks. For more information on
    Asana optional task parameters, see https://developers.asana.com/docs/create-a-task

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AsanaCreateTaskOperator`

    :param asana_conn_id: The Asana connection to use.
    :type asana_conn_id: str
    :param name: Name of the task.
    :type name: str
    :param optional_task_parameters: Any of the optional task creation parameters.
        See https://developers.asana.com/docs/create-a-task for a complete list.
        You must specify at least one of 'workspace', 'parent', or 'projects'.
    :type optional_task_parameters: dict
    """

    @apply_defaults
    def __init__(
        self,
        *,
        asana_conn_id: str,
        name: str,
        optional_task_parameters: Optional[dict] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.asana_conn_id = asana_conn_id
        self.name = name
        self.optional_task_parameters = optional_task_parameters
        self.hook = AsanaHook(conn_id=self.asana_conn_id)

    def execute(self, context: Dict) -> str:
        asana_client = self.hook.get_conn()

        params = {"name": self.name}
        if self.optional_task_parameters is not None:
            params.update(self.optional_task_parameters)
        response = asana_client.tasks.create(params=params)

        self.log.info(response)
        return response["gid"]


class AsanaUpdateTaskOperator(BaseOperator):
    """
    This operator can be used to update Asana tasks.
    For more information on Asana optional task parameters, see
    https://developers.asana.com/docs/update-a-task

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AsanaUpdateTaskOperator`

    :param asana_conn_id: The Asana connection to use.
    :type asana_conn_id: str
    :param asana_task_gid: Asana task ID to update
    :type asana_task_gid: str
    :param task_update_parameters: Any task parameters that should be updated.
        See https://developers.asana.com/docs/update-a-task for a complete list.
    :type task_update_parameters: dict
    """

    @apply_defaults
    def __init__(
        self,
        *,
        asana_conn_id: str,
        asana_task_gid: str,
        task_update_parameters: dict,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.asana_conn_id = asana_conn_id
        self.asana_task_gid = asana_task_gid
        self.task_update_parameters = task_update_parameters
        self.hook = AsanaHook(conn_id=self.asana_conn_id)

    def execute(self, context: Dict) -> None:
        asana_client = self.hook.get_conn()

        response = asana_client.tasks.update(task=self.asana_task_gid, params=self.task_update_parameters)
        self.log.info(response)


class AsanaDeleteTaskOperator(BaseOperator):
    """
    This operator can be used to delete Asana tasks.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AsanaDeleteTaskOperator`

    :param asana_conn_id: The Asana connection to use.
    :type asana_conn_id: str
    :param asana_task_gid: Asana Task ID to delete.
    :type asana_task_gid: str
    """

    @apply_defaults
    def __init__(
        self,
        *,
        asana_conn_id: str,
        asana_task_gid: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.asana_conn_id = asana_conn_id
        self.asana_task_gid = asana_task_gid
        self.hook = AsanaHook(conn_id=self.asana_conn_id)

    def execute(self, context: Dict) -> None:
        asana_client = self.hook.get_conn()
        response = asana_client.tasks.delete_task(self.asana_task_gid)
        self.log.info(response)


class AsanaFindTaskOperator(BaseOperator):
    """
    This operator can be used to retrieve Asana tasks that match various filters.
    You must specify at least one of `project`, `section`, `tag`, `user_task_list`,
    or both `assignee` and `workspace`.
    For a complete list of filters, see
    https://github.com/Asana/python-asana/blob/ec5f178606251e2776a72a82f660cc1521516988/asana/resources/tasks.py#L182

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AsanaFindTaskOperator`

    :param asana_conn_id: The Asana connection to use.
    :type asana_conn_id: str
    :param search_parameters: The parameters used to find relevant tasks
    :type search_parameters: dict
    """

    @apply_defaults
    def __init__(
        self,
        *,
        asana_conn_id: str,
        search_parameters: dict,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.asana_conn_id = asana_conn_id
        self.search_parameters = search_parameters
        self.hook = AsanaHook(conn_id=self.asana_conn_id)

    def execute(self, context: Dict) -> list:
        contains_needed_values = ("assignee" in self.search_parameters) and (
            "workspace" in self.search_parameters
        )
        for key in ["project", "section", "tag", "user_task_list"]:
            contains_needed_values |= key in self.search_parameters
        if not contains_needed_values:
            raise ValueError(
                "You must specify at least one of 'project', 'section', 'tag', 'user_task_list',"
                "or both 'assignee' and 'workspace' in the search_parameters."
            )

        asana_client = self.hook.get_conn()
        response = asana_client.tasks.find_all(params=self.search_parameters)

        # Convert the python-asana collection to a list
        response_lst = list(response)
        self.log.info(response_lst)
        return response_lst
