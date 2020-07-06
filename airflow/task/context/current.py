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

import contextlib
import logging
from typing import Any, Dict

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

_CURRENT_CONTEXT = []
log = logging.getLogger(__name__)


def get_additional_execution_contextmanager(task_instance, execution_context):
    """
    Retrieves the user defined execution context callback from the configuration,
    and validates that it is indeed a context manager

    :param execution_context: the current execution context to be passed to user ctx
    """
    additional_execution_contextmanager = conf.getimport(
        "core", "additional_execute_contextmanager"
    )
    if additional_execution_contextmanager:
        try:
            user_ctx_obj = additional_execution_contextmanager(
                task_instance, execution_context
            )
            if hasattr(user_ctx_obj, "__enter__") and hasattr(user_ctx_obj, "__exit__"):
                return user_ctx_obj
            else:
                raise AirflowException(
                    f"Loaded function {additional_execution_contextmanager} "
                    f"as additional execution contextmanager, but it does not have "
                    f"__enter__ or __exit__ method!"
                )
        except ImportError as e:
            raise AirflowException(
                f"Could not import additional execution contextmanager "
                f"{additional_execution_contextmanager}!",
                e,
            )


@contextlib.contextmanager
def set_current_context(context: Dict[str, Any], task_instance: BaseOperator):
    """
    Sets the current execution context to the provided context object.
    This method should be called once per Task execution, before calling operator.execute
    """
    _CURRENT_CONTEXT.append(context)

    user_defined_exec_context = get_additional_execution_contextmanager(
        context, task_instance
    )

    try:
        if user_defined_exec_context is not None:
            with user_defined_exec_context:
                yield context
        else:
            yield context
    finally:
        expected_state = _CURRENT_CONTEXT.pop()
        if expected_state != context:
            log.warning(
                "Current context is not equal to the state at context stack. Expected=%s, got=%s",
                context,
                expected_state,
            )


def get_current_context() -> Dict[str, Any]:
    """
    Obtain the execution context for the currently executing operator without
    altering user method's signature.
    This is the simplest method of retrieving the execution context dictionary.
    ** Old style:
        def my_task(**context):
            ti = context["ti"]
    ** New style:
        from airflow.task.context import get_current_context
        def my_task():
            context = get_current_context()
            ti = context["ti"]

    Current context will only have value if this method was called after an operator
    was starting to execute.
    """
    if not _CURRENT_CONTEXT:
        raise AirflowException(
            "Current context was requested but no context was found! "
            "Are you running within an airflow task?"
        )
    return _CURRENT_CONTEXT[-1]
