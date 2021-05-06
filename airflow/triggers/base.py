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

from typing import Any, AsyncIterator, Dict, Tuple


class BaseTrigger:
    """
    Base class for all triggers.

    A trigger has two contexts it can exist in:

     - As part of a DAG declaration, where it's declared.
     - Actively running in a trigger worker

    We use the same class for both situations, and rely on all Trigger classes
    to be able to return the (Airflow-JSON-encodable) arguments that will
    let them be reinsantiated elsewhere.
    """

    def __init__(self):
        pass

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """
        Returns the information needed to reconstruct this Trigger.

        The first element of the returned tuple is the class path, the second
        is the keyword arguments needed to re-instantiate it.
        """
        raise NotImplementedError("Triggers must implement serialize()")

    async def run(self) -> AsyncIterator["TriggerEvent"]:
        """
        Runs the trigger in an asynchronous context.

        The trigger should yield an Event whenever it wants to fire off
        an event, and return None if it is finished. Single-event triggers
        should thus yield and then immediately return.

        If it yields, it is likely that it will be resumed very quickly,
        but it may not be (e.g. if the workload is being moved to another
        trigger process, or a multi-event trigger was being used for a
        single-event task defer).

        In either case, Trigger classes should assume they will be persisted,
        and then rely on cleanup() being called when they are no longer needed.
        """
        raise NotImplementedError("Triggers must implement run()")

    def cleanup(self) -> None:
        """
        Called when the trigger is no longer needed and it's being removed
        from the active trigger process.
        """

    def __repr__(self) -> str:
        classpath, kwargs = self.serialize()
        kwargs_str = ", ".join(f"{k}={v}" for k, v in kwargs.items())
        return f"<{classpath} {kwargs_str}>"


class TriggerEvent:
    """
    Something that a trigger can fire when its conditions are met.

    Events must have a uniquely identifying value that would be the same
    wherever the trigger is run; this is to ensure that if the same trigger
    is being run in two locations (for HA reasons) that we can deduplicate its
    events.
    """

    def __init__(self, payload: Any):
        self.payload = payload

    def __repr__(self) -> str:
        return f"TriggerEvent<{self.payload!r}>"

    def __eq__(self, other):
        if isinstance(other, TriggerEvent):
            return other.payload == self.payload
        return False
