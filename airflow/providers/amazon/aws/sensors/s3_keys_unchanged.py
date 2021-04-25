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

import os
from datetime import datetime
from typing import Optional, Set, Union

try:
    from functools import cached_property
except ImportError:
    from cached_property import cached_property

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sensors.base import BaseSensorOperator, poke_mode_only
from airflow.utils.decorators import apply_defaults


@poke_mode_only
class S3KeysUnchangedSensor(BaseSensorOperator):
    """
    Checks for changes in the number of objects at prefix in AWS S3
    bucket and returns True if the inactivity period has passed with no
    increase in the number of objects. Note, this sensor will not behave correctly
    in reschedule mode, as the state of the listed objects in the S3 bucket will
    be lost between rescheduled invocations.

    :param bucket_name: Name of the S3 bucket
    :type bucket_name: str
    :param prefix: The prefix being waited on. Relative path from bucket root level.
    :type prefix: str
    :param aws_conn_id: a reference to the s3 connection
    :type aws_conn_id: str
    :param verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used
                 (unless use_ssl is False), but SSL certificates will not be
                 verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                 You can specify this argument if you want to use a different
                 CA cert bundle than the one used by botocore.
    :type verify: Optional[Union[bool, str]]
    :param inactivity_period: The total seconds of inactivity to designate
        keys unchanged. Note, this mechanism is not real time and
        this operator may not return until a poke_interval after this period
        has passed with no additional objects sensed.
    :type inactivity_period: float
    :param min_objects: The minimum number of objects needed for keys unchanged
        sensor to be considered valid.
    :type min_objects: int
    :param previous_objects: The set of object ids found during the last poke.
    :type previous_objects: Optional[Set[str]]
    :param allow_delete: Should this sensor consider objects being deleted
        between pokes valid behavior. If true a warning message will be logged
        when this happens. If false an error will be raised.
    :type allow_delete: bool
    """

    template_fields = ('bucket_name', 'prefix')

    @apply_defaults
    def __init__(
        self,
        *,
        bucket_name: str,
        prefix: str,
        aws_conn_id: str = 'aws_default',
        verify: Optional[Union[bool, str]] = None,
        inactivity_period: float = 60 * 60,
        min_objects: int = 1,
        previous_objects: Optional[Set[str]] = None,
        allow_delete: bool = True,
        **kwargs,
    ) -> None:

        super().__init__(**kwargs)

        self.bucket_name = bucket_name
        self.prefix = prefix
        if inactivity_period < 0:
            raise ValueError("inactivity_period must be non-negative")
        self.inactivity_period = inactivity_period
        self.min_objects = min_objects
        self.previous_objects = previous_objects or set()
        self.inactivity_seconds = 0
        self.allow_delete = allow_delete
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.last_activity_time: Optional[datetime] = None

    @cached_property
    def hook(self):
        """Returns S3Hook."""
        return S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)

    def is_keys_unchanged(self, current_objects: Set[str]) -> bool:
        """
        Checks whether new objects have been uploaded and the inactivity_period
        has passed and updates the state of the sensor accordingly.

        :param current_objects: set of object ids in bucket during last poke.
        :type current_objects: set[str]
        """
        current_num_objects = len(current_objects)
        if current_objects > self.previous_objects:
            # When new objects arrived, reset the inactivity_seconds
            # and update previous_objects for the next poke.
            self.log.info(
                "New objects found at %s, resetting last_activity_time.",
                os.path.join(self.bucket_name, self.prefix),
            )
            self.log.debug("New objects: %s", current_objects - self.previous_objects)
            self.last_activity_time = datetime.now()
            self.inactivity_seconds = 0
            self.previous_objects = current_objects
            return False

        if self.previous_objects - current_objects:
            # During the last poke interval objects were deleted.
            if self.allow_delete:
                deleted_objects = self.previous_objects - current_objects
                self.previous_objects = current_objects
                self.last_activity_time = datetime.now()
                self.log.info(
                    "Objects were deleted during the last poke interval. Updating the "
                    "file counter and resetting last_activity_time:\n%s",
                    deleted_objects,
                )
                return False

            raise AirflowException(
                "Illegal behavior: objects were deleted in %s between pokes."
                % os.path.join(self.bucket_name, self.prefix)
            )

        if self.last_activity_time:
            self.inactivity_seconds = int((datetime.now() - self.last_activity_time).total_seconds())
        else:
            # Handles the first poke where last inactivity time is None.
            self.last_activity_time = datetime.now()
            self.inactivity_seconds = 0

        if self.inactivity_seconds >= self.inactivity_period:
            path = os.path.join(self.bucket_name, self.prefix)

            if current_num_objects >= self.min_objects:
                self.log.info(
                    "SUCCESS: \nSensor found %s objects at %s.\n"
                    "Waited at least %s seconds, with no new objects uploaded.",
                    current_num_objects,
                    path,
                    self.inactivity_period,
                )
                return True

            self.log.error("FAILURE: Inactivity Period passed, not enough objects found in %s", path)

            return False
        return False

    def poke(self, context):
        return self.is_keys_unchanged(set(self.hook.list_keys(self.bucket_name, prefix=self.prefix)))
