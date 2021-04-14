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
"""Contains the TaskNotRunningDep."""

from airflow.ti_deps.deps.base_ti_dep import BaseTIDep
from airflow.utils.session import provide_session
from airflow.utils.state import State


class TaskNotRunningDep(BaseTIDep):
    """Ensures that the task instance's state is not running."""

    NAME = "Task Instance Not Running"
    IGNORABLE = False

    def __eq__(self, other):
        return type(self) == type(other)  # pylint: disable=C0123

    def __hash__(self):
        return hash(type(self))

    @provide_session
    def _get_dep_statuses(self, ti, session, dep_context=None):
        if ti.state != State.RUNNING:
            yield self._passing_status(reason="Task is not in running state.")
            return

        yield self._failing_status(reason='Task is in the running state')
