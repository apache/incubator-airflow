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

"""This module contains the Apache Livy sensor."""
from typing import Any, Dict, Optional, Union

from airflow.providers.apache.livy.hooks.livy import LivyHook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class LivySensor(BaseSensorOperator):
    """
    Monitor a Livy sessions for termination.

    :param livy_conn_id: reference to a pre-defined Livy connection
    :type livy_conn_id: str
    :param batch_id: identifier of the monitored batch
    :type batch_id: Union[int, str]
    :type extra_options: A dictionary of options, where key is string and value
        depends on the option that's being modified.
    """

    template_fields = ('batch_id',)

    @apply_defaults
    def __init__(
        self,
        *,
        batch_id: Union[int, str],
        livy_conn_id: str = 'livy_default',
        extra_options: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.batch_id = batch_id
        self._livy_conn_id = livy_conn_id
        self._livy_hook: Optional[LivyHook] = None
        self._extra_options = extra_options or {}

    def get_hook(self) -> LivyHook:
        """
        Get valid hook.

        :return: hook
        :rtype: LivyHook
        """
        if self._livy_hook is None or not isinstance(self._livy_hook, LivyHook):
            self._livy_hook = LivyHook(livy_conn_id=self._livy_conn_id, extra_options=self._extra_options)
        return self._livy_hook

    def poke(self, context: Dict[Any, Any]) -> bool:
        batch_id = self.batch_id

        status = self.get_hook().get_batch_state(batch_id)
        return status in self.get_hook().TERMINAL_STATES
