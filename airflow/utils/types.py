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
import enum
from typing import Optional

from airflow.typing_compat import TypedDict


class DagRunType(str, enum.Enum):
    """Class with DagRun types"""

    BACKFILL_JOB = "backfill"
    SCHEDULED = "scheduled"
    MANUAL = "manual"

    def __str__(self) -> str:  # pylint: disable=invalid-str-returned
        return self.value

    @staticmethod
    def from_run_id(run_id: str) -> "DagRunType":
        """Resolved DagRun type from run_id."""
        for run_type in DagRunType:
            if run_id and run_id.startswith(f"{run_type.value}__"):
                return run_type
        return DagRunType.MANUAL


class EdgeInfoType(TypedDict):
    """
    Represents extra metadata that the DAG can store about an edge,
    usually generated from an EdgeModifier.
    """

    label: Optional[str]
