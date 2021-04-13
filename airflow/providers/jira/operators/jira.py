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

from typing import Any, Callable, Dict, Optional

from jira import JIRA
from jira.exceptions import JIRAError

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class JiraOperator(BaseOperator):
    """
    JiraOperator to interact and perform action on Jira issue tracking system.
    This operator is designed to use Jira Python SDK: http://jira.readthedocs.io

    :param jira_conn_id: reference to a pre-defined Jira Connection
    :type jira_conn_id: str
    :param jira_method: method name from Jira Python SDK to be called
    :type jira_method: str
    :param jira_method_args: required method parameters for the jira_method. (templated)
    :type jira_method_args: dict
    :param result_processor: function to further process the response from Jira
    :type result_processor: function
    :param get_jira_resource_method: function or operator to get jira resource
                                    on which the provided jira_method will be executed
    :type get_jira_resource_method: function
    """

    template_fields = ("jira_method_args",)

    @apply_defaults
    def __init__(
        self,
        *,
        jira_method: str,
        jira_conn_id: str = 'jira_default',
        jira_method_args: Optional[dict] = None,
        result_processor: Optional[Callable] = None,
        get_jira_resource_method: Optional[Callable] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.jira_conn_id = jira_conn_id
        self.method_name = jira_method
        self.jira_method_args = jira_method_args
        self.result_processor = result_processor
        self.get_jira_resource_method = get_jira_resource_method

    def execute(self, context: Dict) -> Any:
        try:
            if self.get_jira_resource_method is not None:
                # if get_jira_resource_method is provided, jira_method will be executed on
                # resource returned by executing the get_jira_resource_method.
                # This makes all the provided methods of JIRA sdk accessible and usable
                # directly at the JiraOperator without additional wrappers.
                # ref: http://jira.readthedocs.io/en/latest/api.html
                if isinstance(self.get_jira_resource_method, JiraOperator):
                    resource = self.get_jira_resource_method.execute(**context)
                else:
                    resource = self.get_jira_resource_method(**context)
            else:
                # Default method execution is on the top level jira client resource
                hook = JiraHook(jira_conn_id=self.jira_conn_id)
                resource = hook.client

            # Current Jira-Python SDK (1.0.7) has issue with pickling the jira response.
            # ex: self.xcom_push(context, key='operator_response', value=jira_response)
            # This could potentially throw error if jira_result is not picklable
            jira_result = getattr(resource, self.method_name)(**self.jira_method_args)
            if self.result_processor:
                return self.result_processor(context, jira_result)

            return jira_result

        except JIRAError as jira_error:
            raise AirflowException(f"Failed to execute jiraOperator, error: {str(jira_error)}")
        except Exception as e:
            raise AirflowException(f"Jira operator error: {str(e)}")
