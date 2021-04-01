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
"""All executors."""
import logging
from contextlib import suppress
from typing import Optional

from airflow.exceptions import AirflowConfigException
from airflow.executors.base_executor import BaseExecutor
from airflow.executors.executor_constants import (
    CELERY_EXECUTOR,
    CELERY_KUBERNETES_EXECUTOR,
    DASK_EXECUTOR,
    DEBUG_EXECUTOR,
    KUBERNETES_EXECUTOR,
    LOCAL_EXECUTOR,
    SEQUENTIAL_EXECUTOR,
)
from airflow.utils.module_loading import import_string

log = logging.getLogger(__name__)


class ExecutorLoader:
    """Keeps constants for all the currently available executors."""

    _default_executor: Optional[BaseExecutor] = None
    executors = {
        LOCAL_EXECUTOR: 'airflow.executors.local_executor.LocalExecutor',
        SEQUENTIAL_EXECUTOR: 'airflow.executors.sequential_executor.SequentialExecutor',
        CELERY_EXECUTOR: 'airflow.executors.celery_executor.CeleryExecutor',
        CELERY_KUBERNETES_EXECUTOR: 'airflow.executors.celery_kubernetes_executor.CeleryKubernetesExecutor',
        DASK_EXECUTOR: 'airflow.executors.dask_executor.DaskExecutor',
        KUBERNETES_EXECUTOR: 'airflow.executors.kubernetes_executor.KubernetesExecutor',
        DEBUG_EXECUTOR: 'airflow.executors.debug_executor.DebugExecutor',
    }

    @classmethod
    def get_default_executor(cls) -> BaseExecutor:
        """Creates a new instance of the configured executor if none exists and returns it"""
        if cls._default_executor is not None:
            return cls._default_executor

        from airflow.configuration import conf

        executor_name = conf.get('core', 'EXECUTOR')

        cls._default_executor = cls.load_executor(executor_name)

        return cls._default_executor

    @classmethod
    def load_executor(cls, executor_name: str) -> BaseExecutor:
        """
        Loads the executor.

        This supports the following formats:
        * by executor name for core executor
        * by ``{plugin_name}.{class_name}`` for executor from plugins
        * by import path.

        :return: an instance of executor class via executor_name
        """
        if executor_name == CELERY_KUBERNETES_EXECUTOR:
            return cls.__load_celery_kubernetes_executor()

        if executor_name in cls.executors:
            log.debug("Loading core executor: %s", executor_name)
            return import_string(cls.executors[executor_name])()
        # If the executor name looks like "plugin executor path" then try to load plugins.
        if executor_name.count(".") == 1:
            log.debug(
                "The executor name looks like the plugin path (executor_name=%s). Trying to load a "
                "executor from a plugin",
                executor_name,
            )
            with suppress(ImportError, AttributeError):
                # Load plugins here for executors as at that time the plugins might not have been
                # initialized yet
                from airflow import plugins_manager

                plugins_manager.integrate_executor_plugins()
                return import_string(f"airflow.executors.{executor_name}")()

        log.debug("Loading executor from custom path: %s", executor_name)
        try:
            executor = import_string(executor_name)()
        except ImportError as e:
            log.error(e)
            raise AirflowConfigException(
                f'The module/attribute could not be loaded. Please check "executor" key in "core" section. '
                f'Current value: "{executor_name}".'
            )
        log.info("Loaded executor: %s", executor_name)

        return executor

    @classmethod
    def __load_celery_kubernetes_executor(cls) -> BaseExecutor:
        """:return: an instance of CeleryKubernetesExecutor"""
        celery_executor = import_string(cls.executors[CELERY_EXECUTOR])()
        kubernetes_executor = import_string(cls.executors[KUBERNETES_EXECUTOR])()

        celery_kubernetes_executor_cls = import_string(cls.executors[CELERY_KUBERNETES_EXECUTOR])
        return celery_kubernetes_executor_cls(celery_executor, kubernetes_executor)


UNPICKLEABLE_EXECUTORS = (
    LOCAL_EXECUTOR,
    SEQUENTIAL_EXECUTOR,
    DASK_EXECUTOR,
)
