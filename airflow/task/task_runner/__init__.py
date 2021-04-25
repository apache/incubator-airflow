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

# pylint: disable=missing-docstring
import logging

from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException
from airflow.utils.module_loading import import_string

log = logging.getLogger(__name__)

_TASK_RUNNER_NAME = conf.get('core', 'TASK_RUNNER')

STANDARD_TASK_RUNNER = "StandardTaskRunner"

CGROUP_TASK_RUNNER = "CgroupTaskRunner"

CORE_TASK_RUNNERS = {
    STANDARD_TASK_RUNNER: "airflow.task.task_runner.standard_task_runner.StandardTaskRunner",
    CGROUP_TASK_RUNNER: "airflow.task.task_runner.cgroup_task_runner.CgroupTaskRunner",
}


def get_task_runner(local_task_job):
    """
    Get the task runner that can be used to run the given job.

    :param local_task_job: The LocalTaskJob associated with the TaskInstance
        that needs to be executed.
    :type local_task_job: airflow.jobs.local_task_job.LocalTaskJob
    :return: The task runner to use to run the task.
    :rtype: airflow.task.task_runner.base_task_runner.BaseTaskRunner
    """
    if _TASK_RUNNER_NAME in CORE_TASK_RUNNERS:
        log.debug("Loading core task runner: %s", _TASK_RUNNER_NAME)
        task_runner_class = import_string(CORE_TASK_RUNNERS[_TASK_RUNNER_NAME])
    else:
        log.debug("Loading task runner from custom path: %s", _TASK_RUNNER_NAME)
        try:
            task_runner_class = import_string(_TASK_RUNNER_NAME)
        except ImportError:
            raise AirflowConfigException(
                f'The task runner could not be loaded. Please check "task_runner" key in "core" section. '
                f'Current value: "{_TASK_RUNNER_NAME}".'
            )

    task_runner = task_runner_class(local_task_job)
    return task_runner
