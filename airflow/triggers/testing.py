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

from typing import Any, Dict, Tuple

from airflow.triggers.base import BaseTrigger, TriggerEvent


class SuccessTrigger(BaseTrigger):
    """
    A trigger that always succeeds immediately.

    Should only be used for testing.
    """

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        return ("airflow.triggers.testing.SuccessTrigger", {})

    async def run(self):
        yield TriggerEvent(True)


class FailureTrigger(BaseTrigger):
    """
    A trigger that always errors immediately.

    Should only be used for testing.
    """

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        return ("airflow.triggers.testing.FailureTrigger", {})

    async def run(self):
        # Python needs at least one "yield" keyword in the body to make
        # this an async generator.
        if False:  # pylint: disable=using-constant-test
            yield None
        raise ValueError("Deliberate trigger failure")
