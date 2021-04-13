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

from jira.exceptions import JIRAError
from jira.resources import Issue, Resource

from airflow.providers.jira.operators.jira import JiraOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class JiraSensor(BaseSensorOperator):
    """
    Monitors a jira ticket for any change.

    :param jira_conn_id: reference to a pre-defined Jira Connection
    :type jira_conn_id: str
    :param method_name: method name from jira-python-sdk to be execute
    :type method_name: str
    :param method_params: parameters for the method method_name
    :type method_params: dict
    :param result_processor: function that return boolean and act as a sensor response
    :type result_processor: function
    """

    @apply_defaults
    def __init__(
        self,
        *,
        method_name: str,
        jira_conn_id: str = 'jira_default',
        method_params: Optional[dict] = None,
        result_processor: Optional[Callable] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.jira_conn_id = jira_conn_id
        self.result_processor = None
        if result_processor is not None:
            self.result_processor = result_processor
        self.method_name = method_name
        self.method_params = method_params
        self.jira_operator = JiraOperator(
            task_id=self.task_id,
            jira_conn_id=self.jira_conn_id,
            jira_method=self.method_name,
            jira_method_args=self.method_params,
            result_processor=self.result_processor,
        )

    def poke(self, context: Dict) -> Any:
        return self.jira_operator.execute(context=context)


class JiraTicketSensor(JiraSensor):
    """
    Monitors a jira ticket for given change in terms of function.

    :param jira_conn_id: reference to a pre-defined Jira Connection
    :type jira_conn_id: str
    :param ticket_id: id of the ticket to be monitored
    :type ticket_id: str
    :param field: field of the ticket to be monitored
    :type field: str
    :param expected_value: expected value of the field
    :type expected_value: str
    :param result_processor: function that return boolean and act as a sensor response
    :type result_processor: function
    """

    template_fields = ("ticket_id",)

    @apply_defaults
    def __init__(
        self,
        *,
        jira_conn_id: str = 'jira_default',
        ticket_id: Optional[str] = None,
        field: Optional[str] = None,
        expected_value: Optional[str] = None,
        field_checker_func: Optional[Callable] = None,
        **kwargs,
    ) -> None:

        self.jira_conn_id = jira_conn_id
        self.ticket_id = ticket_id
        self.field = field
        self.expected_value = expected_value
        if field_checker_func is None:
            field_checker_func = self.issue_field_checker

        super().__init__(jira_conn_id=jira_conn_id, result_processor=field_checker_func, **kwargs)

    def poke(self, context: Dict) -> Any:
        self.log.info('Jira Sensor checking for change in ticket: %s', self.ticket_id)

        self.jira_operator.method_name = "issue"
        self.jira_operator.jira_method_args = {'id': self.ticket_id, 'fields': self.field}
        return JiraSensor.poke(self, context=context)

    def issue_field_checker(self, issue: Issue) -> Optional[bool]:
        """Check issue using different conditions to prepare to evaluate sensor."""
        result = None
        try:  # pylint: disable=too-many-nested-blocks
            if issue is not None and self.field is not None and self.expected_value is not None:

                field_val = getattr(issue.fields, self.field)
                if field_val is not None:
                    if isinstance(field_val, list):
                        result = self.expected_value in field_val
                    elif isinstance(field_val, str):
                        result = self.expected_value.lower() == field_val.lower()
                    elif isinstance(field_val, Resource) and getattr(field_val, 'name'):
                        result = self.expected_value.lower() == field_val.name.lower()
                    else:
                        self.log.warning(
                            "Not implemented checker for issue field %s which "
                            "is neither string nor list nor Jira Resource",
                            self.field,
                        )

        except JIRAError as jira_error:
            self.log.error("Jira error while checking with expected value: %s", jira_error)
        except Exception as e:  # pylint: disable=broad-except
            self.log.error("Error while checking with expected value %s:", self.expected_value)
            self.log.exception(e)
        if result is True:
            self.log.info(
                "Issue field %s has expected value %s, returning success", self.field, self.expected_value
            )
        else:
            self.log.info("Issue field %s don't have expected value %s yet.", self.field, self.expected_value)
        return result
