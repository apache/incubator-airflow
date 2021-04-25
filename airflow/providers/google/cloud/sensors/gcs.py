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
"""This module contains Google Cloud Storage sensors."""

import os
import warnings
from datetime import datetime
from typing import Callable, List, Optional, Sequence, Set, Union

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.sensors.base import BaseSensorOperator, poke_mode_only
from airflow.utils.decorators import apply_defaults


class GCSObjectExistenceSensor(BaseSensorOperator):
    """
    Checks for the existence of a file in Google Cloud Storage.

    :param bucket: The Google Cloud Storage bucket where the object is.
    :type bucket: str
    :param object: The name of the object to check in the Google cloud
        storage bucket.
    :type object: str
    :param google_cloud_conn_id: The connection ID to use when
        connecting to Google Cloud Storage.
    :type google_cloud_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = (
        'bucket',
        'object',
        'impersonation_chain',
    )
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(
        self,
        *,
        bucket: str,
        object: str,  # pylint: disable=redefined-builtin
        google_cloud_conn_id: str = 'google_cloud_default',
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:

        super().__init__(**kwargs)
        self.bucket = bucket
        self.object = object
        self.google_cloud_conn_id = google_cloud_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def poke(self, context: dict) -> bool:
        self.log.info('Sensor checks existence of : %s, %s', self.bucket, self.object)
        hook = GCSHook(
            gcp_conn_id=self.google_cloud_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        return hook.exists(self.bucket, self.object)


def ts_function(context):
    """
    Default callback for the GoogleCloudStorageObjectUpdatedSensor. The default
    behaviour is check for the object being updated after execution_date +
    schedule_interval.
    """
    return context['dag'].following_schedule(context['execution_date'])


class GCSObjectUpdateSensor(BaseSensorOperator):
    """
    Checks if an object is updated in Google Cloud Storage.

    :param bucket: The Google Cloud Storage bucket where the object is.
    :type bucket: str
    :param object: The name of the object to download in the Google cloud
        storage bucket.
    :type object: str
    :param ts_func: Callback for defining the update condition. The default callback
        returns execution_date + schedule_interval. The callback takes the context
        as parameter.
    :type ts_func: function
    :param google_cloud_conn_id: The connection ID to use when
        connecting to Google Cloud Storage.
    :type google_cloud_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = (
        'bucket',
        'object',
        'impersonation_chain',
    )
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(
        self,
        bucket: str,
        object: str,  # pylint: disable=redefined-builtin
        ts_func: Callable = ts_function,
        google_cloud_conn_id: str = 'google_cloud_default',
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:

        super().__init__(**kwargs)
        self.bucket = bucket
        self.object = object
        self.ts_func = ts_func
        self.google_cloud_conn_id = google_cloud_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def poke(self, context: dict) -> bool:
        self.log.info('Sensor checks existence of : %s, %s', self.bucket, self.object)
        hook = GCSHook(
            gcp_conn_id=self.google_cloud_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        return hook.is_updated_after(self.bucket, self.object, self.ts_func(context))


class GCSObjectsWithPrefixExistenceSensor(BaseSensorOperator):
    """
    Checks for the existence of GCS objects at a given prefix, passing matches via XCom.

    When files matching the given prefix are found, the poke method's criteria will be
    fulfilled and the matching objects will be returned from the operator and passed
    through XCom for downstream tasks.

    :param bucket: The Google Cloud Storage bucket where the object is.
    :type bucket: str
    :param prefix: The name of the prefix to check in the Google cloud
        storage bucket.
    :type prefix: str
    :param google_cloud_conn_id: The connection ID to use when
        connecting to Google Cloud Storage.
    :type google_cloud_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = (
        'bucket',
        'prefix',
        'impersonation_chain',
    )
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(
        self,
        bucket: str,
        prefix: str,
        google_cloud_conn_id: str = 'google_cloud_default',
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.bucket = bucket
        self.prefix = prefix
        self.google_cloud_conn_id = google_cloud_conn_id
        self.delegate_to = delegate_to
        self._matches: List[str] = []
        self.impersonation_chain = impersonation_chain

    def poke(self, context: dict) -> bool:
        self.log.info('Sensor checks existence of objects: %s, %s', self.bucket, self.prefix)
        hook = GCSHook(
            gcp_conn_id=self.google_cloud_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        self._matches = hook.list(self.bucket, prefix=self.prefix)
        return bool(self._matches)

    def execute(self, context: dict) -> List[str]:
        """Overridden to allow matches to be passed"""
        super().execute(context)
        return self._matches


class GCSObjectsWtihPrefixExistenceSensor(GCSObjectsWithPrefixExistenceSensor):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.sensors.gcs.GCSObjectsWithPrefixExistenceSensor`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.sensors.gcs.GCSObjectsWithPrefixExistenceSensor`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


def get_time():
    """
    This is just a wrapper of datetime.datetime.now to simplify mocking in the
    unittests.
    """
    return datetime.now()


@poke_mode_only
class GCSUploadSessionCompleteSensor(BaseSensorOperator):
    """
    Checks for changes in the number of objects at prefix in Google Cloud Storage
    bucket and returns True if the inactivity period has passed with no
    increase in the number of objects. Note, this sensor will no behave correctly
    in reschedule mode, as the state of the listed objects in the GCS bucket will
    be lost between rescheduled invocations.

    :param bucket: The Google Cloud Storage bucket where the objects are.
        expected.
    :type bucket: str
    :param prefix: The name of the prefix to check in the Google cloud
        storage bucket.
    :param inactivity_period: The total seconds of inactivity to designate
        an upload session is over. Note, this mechanism is not real time and
        this operator may not return until a poke_interval after this period
        has passed with no additional objects sensed.
    :type inactivity_period: float
    :param min_objects: The minimum number of objects needed for upload session
        to be considered valid.
    :type min_objects: int
    :param previous_objects: The set of object ids found during the last poke.
    :type previous_objects: set[str]
    :param allow_delete: Should this sensor consider objects being deleted
        between pokes valid behavior. If true a warning message will be logged
        when this happens. If false an error will be raised.
    :type allow_delete: bool
    :param google_cloud_conn_id: The connection ID to use when connecting
        to Google Cloud Storage.
    :type google_cloud_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = (
        'bucket',
        'prefix',
        'impersonation_chain',
    )
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(
        self,
        bucket: str,
        prefix: str,
        inactivity_period: float = 60 * 60,
        min_objects: int = 1,
        previous_objects: Optional[Set[str]] = None,
        allow_delete: bool = True,
        google_cloud_conn_id: str = 'google_cloud_default',
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:

        super().__init__(**kwargs)

        self.bucket = bucket
        self.prefix = prefix
        if inactivity_period < 0:
            raise ValueError("inactivity_period must be non-negative")
        self.inactivity_period = inactivity_period
        self.min_objects = min_objects
        self.previous_objects = previous_objects if previous_objects else set()
        self.inactivity_seconds = 0
        self.allow_delete = allow_delete
        self.google_cloud_conn_id = google_cloud_conn_id
        self.delegate_to = delegate_to
        self.last_activity_time = None
        self.impersonation_chain = impersonation_chain
        self.hook: Optional[GCSHook] = None

    def _get_gcs_hook(self) -> Optional[GCSHook]:
        if not self.hook:
            self.hook = GCSHook(
                gcp_conn_id=self.google_cloud_conn_id,
                delegate_to=self.delegate_to,
                impersonation_chain=self.impersonation_chain,
            )
        return self.hook

    def is_bucket_updated(self, current_objects: Set[str]) -> bool:
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
                "New objects found at %s resetting last_activity_time.",
                os.path.join(self.bucket, self.prefix),
            )
            self.log.debug("New objects: %s", "\n".join(current_objects - self.previous_objects))
            self.last_activity_time = get_time()
            self.inactivity_seconds = 0
            self.previous_objects = current_objects
            return False

        if self.previous_objects - current_objects:
            # During the last poke interval objects were deleted.
            if self.allow_delete:
                self.previous_objects = current_objects
                self.last_activity_time = get_time()
                self.log.warning(
                    """
                    Objects were deleted during the last
                    poke interval. Updating the file counter and
                    resetting last_activity_time.
                    %s
                    """,
                    self.previous_objects - current_objects,
                )
                return False

            raise AirflowException(
                """
                Illegal behavior: objects were deleted in {} between pokes.
                """.format(
                    os.path.join(self.bucket, self.prefix)
                )
            )

        if self.last_activity_time:
            self.inactivity_seconds = (get_time() - self.last_activity_time).total_seconds()
        else:
            # Handles the first poke where last inactivity time is None.
            self.last_activity_time = get_time()
            self.inactivity_seconds = 0

        if self.inactivity_seconds >= self.inactivity_period:
            path = os.path.join(self.bucket, self.prefix)

            if current_num_objects >= self.min_objects:
                self.log.info(
                    """SUCCESS:
                    Sensor found %s objects at %s.
                    Waited at least %s seconds, with no new objects dropped.
                    """,
                    current_num_objects,
                    path,
                    self.inactivity_period,
                )
                return True

            self.log.error("FAILURE: Inactivity Period passed, not enough objects found in %s", path)

            return False
        return False

    def poke(self, context: dict) -> bool:
        return self.is_bucket_updated(
            set(self._get_gcs_hook().list(self.bucket, prefix=self.prefix))  # type: ignore[union-attr]
        )
