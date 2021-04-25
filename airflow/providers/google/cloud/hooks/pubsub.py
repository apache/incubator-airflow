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
"""This module contains a Google Pub/Sub Hook."""
import warnings
from base64 import b64decode
from typing import Dict, List, Optional, Sequence, Tuple, Union
from uuid import uuid4

try:
    from functools import cached_property
except ImportError:
    from cached_property import cached_property
from google.api_core.exceptions import AlreadyExists, GoogleAPICallError
from google.api_core.retry import Retry
from google.cloud.exceptions import NotFound
from google.cloud.pubsub_v1 import PublisherClient, SubscriberClient
from google.cloud.pubsub_v1.types import (
    DeadLetterPolicy,
    Duration,
    ExpirationPolicy,
    MessageStoragePolicy,
    PushConfig,
    ReceivedMessage,
    RetryPolicy,
)
from googleapiclient.errors import HttpError

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.version import version


class PubSubException(Exception):
    """Alias for Exception."""


class PubSubHook(GoogleBaseHook):
    """
    Hook for accessing Google Pub/Sub.

    The Google Cloud project against which actions are applied is determined by
    the project embedded in the Connection referenced by gcp_conn_id.
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )
        self._client = None

    def get_conn(self) -> PublisherClient:
        """
        Retrieves connection to Google Cloud Pub/Sub.

        :return: Google Cloud Pub/Sub client object.
        :rtype: google.cloud.pubsub_v1.PublisherClient
        """
        if not self._client:
            self._client = PublisherClient(credentials=self._get_credentials(), client_info=self.client_info)
        return self._client

    @cached_property
    def subscriber_client(self) -> SubscriberClient:
        """
        Creates SubscriberClient.

        :return: Google Cloud Pub/Sub client object.
        :rtype: google.cloud.pubsub_v1.SubscriberClient
        """
        return SubscriberClient(credentials=self._get_credentials(), client_info=self.client_info)

    @GoogleBaseHook.fallback_to_default_project_id
    def publish(
        self,
        topic: str,
        messages: List[dict],
        project_id: str,
    ) -> None:
        """
        Publishes messages to a Pub/Sub topic.

        :param topic: the Pub/Sub topic to which to publish; do not
            include the ``projects/{project}/topics/`` prefix.
        :type topic: str
        :param messages: messages to publish; if the data field in a
            message is set, it should be a bytestring (utf-8 encoded)
        :type messages: list of PubSub messages; see
            http://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
        :param project_id: Optional, the Google Cloud project ID in which to publish.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :type project_id: str
        """
        self._validate_messages(messages)

        publisher = self.get_conn()
        topic_path = f"projects/{project_id}/topics/{topic}"

        self.log.info("Publish %d messages to topic (path) %s", len(messages), topic_path)
        try:
            for message in messages:
                future = publisher.publish(
                    topic=topic_path, data=message.get("data", b''), **message.get('attributes', {})
                )
                future.result()
        except GoogleAPICallError as e:
            raise PubSubException(f'Error publishing to topic {topic_path}', e)

        self.log.info("Published %d messages to topic (path) %s", len(messages), topic_path)

    @staticmethod
    def _validate_messages(messages) -> None:
        for message in messages:
            # To warn about broken backward compatibility
            # TODO: remove one day
            if "data" in message and isinstance(message["data"], str):
                try:
                    b64decode(message["data"])
                    warnings.warn(
                        "The base 64 encoded string as 'data' field has been deprecated. "
                        "You should pass bytestring (utf-8 encoded).",
                        DeprecationWarning,
                        stacklevel=4,
                    )
                except ValueError:
                    pass

            if not isinstance(message, dict):
                raise PubSubException("Wrong message type. Must be a dictionary.")
            if "data" not in message and "attributes" not in message:
                raise PubSubException("Wrong message. Dictionary must contain 'data' or 'attributes'.")
            if "data" in message and not isinstance(message["data"], bytes):
                raise PubSubException("Wrong message. 'data' must be send as a bytestring")
            if ("data" not in message and "attributes" in message and not message["attributes"]) or (
                "attributes" in message and not isinstance(message["attributes"], dict)
            ):
                raise PubSubException(
                    "Wrong message. If 'data' is not provided 'attributes' must be a non empty dictionary."
                )

    # pylint: disable=too-many-arguments
    @GoogleBaseHook.fallback_to_default_project_id
    def create_topic(
        self,
        topic: str,
        project_id: str,
        fail_if_exists: bool = False,
        labels: Optional[Dict[str, str]] = None,
        message_storage_policy: Union[Dict, MessageStoragePolicy] = None,
        kms_key_name: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> None:
        """
        Creates a Pub/Sub topic, if it does not already exist.

        :param topic: the Pub/Sub topic name to create; do not
            include the ``projects/{project}/topics/`` prefix.
        :type topic: str
        :param project_id: Optional, the Google Cloud project ID in which to create the topic
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :type project_id: str
        :param fail_if_exists: if set, raise an exception if the topic
            already exists
        :type fail_if_exists: bool
        :param labels: Client-assigned labels; see
            https://cloud.google.com/pubsub/docs/labels
        :type labels: Dict[str, str]
        :param message_storage_policy: Policy constraining the set
            of Google Cloud regions where messages published to
            the topic may be stored. If not present, then no constraints
            are in effect.
        :type message_storage_policy:
            Union[Dict, google.cloud.pubsub_v1.types.MessageStoragePolicy]
        :param kms_key_name: The resource name of the Cloud KMS CryptoKey
            to be used to protect access to messages published on this topic.
            The expected format is
            ``projects/*/locations/*/keyRings/*/cryptoKeys/*``.
        :type kms_key_name: str
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :type timeout: float
        :param metadata: (Optional) Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]]
        """
        publisher = self.get_conn()
        topic_path = f"projects/{project_id}/topics/{topic}"

        # Add airflow-version label to the topic
        labels = labels or {}
        labels['airflow-version'] = 'v' + version.replace('.', '-').replace('+', '-')

        self.log.info("Creating topic (path) %s", topic_path)
        try:
            # pylint: disable=no-member
            publisher.create_topic(
                request={
                    "name": topic_path,
                    "labels": labels,
                    "message_storage_policy": message_storage_policy,
                    "kms_key_name": kms_key_name,
                },
                retry=retry,
                timeout=timeout,
                metadata=metadata or (),
            )
        except AlreadyExists:
            self.log.warning('Topic already exists: %s', topic)
            if fail_if_exists:
                raise PubSubException(f'Topic already exists: {topic}')
        except GoogleAPICallError as e:
            raise PubSubException(f'Error creating topic {topic}', e)

        self.log.info("Created topic (path) %s", topic_path)

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_topic(
        self,
        topic: str,
        project_id: str,
        fail_if_not_exists: bool = False,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> None:
        """
        Deletes a Pub/Sub topic if it exists.

        :param topic: the Pub/Sub topic name to delete; do not
            include the ``projects/{project}/topics/`` prefix.
        :type topic: str
        :param project_id: Optional, the Google Cloud project ID in which to delete the topic.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :type project_id: str
        :param fail_if_not_exists: if set, raise an exception if the topic
            does not exist
        :type fail_if_not_exists: bool
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :type timeout: float
        :param metadata: (Optional) Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]]
        """
        publisher = self.get_conn()
        topic_path = f"projects/{project_id}/topics/{topic}"

        self.log.info("Deleting topic (path) %s", topic_path)
        try:
            # pylint: disable=no-member
            publisher.delete_topic(
                request={"topic": topic_path}, retry=retry, timeout=timeout, metadata=metadata or ()
            )
        except NotFound:
            self.log.warning('Topic does not exist: %s', topic_path)
            if fail_if_not_exists:
                raise PubSubException(f'Topic does not exist: {topic_path}')
        except GoogleAPICallError as e:
            raise PubSubException(f'Error deleting topic {topic}', e)
        self.log.info("Deleted topic (path) %s", topic_path)

    # pylint: disable=too-many-arguments
    @GoogleBaseHook.fallback_to_default_project_id
    def create_subscription(
        self,
        topic: str,
        project_id: str,
        subscription: Optional[str] = None,
        subscription_project_id: Optional[str] = None,
        ack_deadline_secs: int = 10,
        fail_if_exists: bool = False,
        push_config: Optional[Union[dict, PushConfig]] = None,
        retain_acked_messages: Optional[bool] = None,
        message_retention_duration: Optional[Union[dict, Duration]] = None,
        labels: Optional[Dict[str, str]] = None,
        enable_message_ordering: bool = False,
        expiration_policy: Optional[Union[dict, ExpirationPolicy]] = None,
        filter_: Optional[str] = None,
        dead_letter_policy: Optional[Union[dict, DeadLetterPolicy]] = None,
        retry_policy: Optional[Union[dict, RetryPolicy]] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> str:
        """
        Creates a Pub/Sub subscription, if it does not already exist.

        :param topic: the Pub/Sub topic name that the subscription will be bound
            to create; do not include the ``projects/{project}/subscriptions/`` prefix.
        :type topic: str
        :param project_id: Optional, the Google Cloud project ID of the topic that the subscription will be
            bound to. If set to None or missing, the default project_id from the Google Cloud connection
            is used.
        :type project_id: str
        :param subscription: the Pub/Sub subscription name. If empty, a random
            name will be generated using the uuid module
        :type subscription: str
        :param subscription_project_id: the Google Cloud project ID where the subscription
            will be created. If unspecified, ``project_id`` will be used.
        :type subscription_project_id: str
        :param ack_deadline_secs: Number of seconds that a subscriber has to
            acknowledge each message pulled from the subscription
        :type ack_deadline_secs: int
        :param fail_if_exists: if set, raise an exception if the topic
            already exists
        :type fail_if_exists: bool
        :param push_config: If push delivery is used with this subscription,
            this field is used to configure it. An empty ``pushConfig`` signifies
            that the subscriber will pull and ack messages using API methods.
        :type push_config: Union[Dict, google.cloud.pubsub_v1.types.PushConfig]
        :param retain_acked_messages: Indicates whether to retain acknowledged
            messages. If true, then messages are not expunged from the subscription's
            backlog, even if they are acknowledged, until they fall out of the
            ``message_retention_duration`` window. This must be true if you would
            like to Seek to a timestamp.
        :type retain_acked_messages: bool
        :param message_retention_duration: How long to retain unacknowledged messages
            in the subscription's backlog, from the moment a message is published. If
            ``retain_acked_messages`` is true, then this also configures the
            retention of acknowledged messages, and thus configures how far back in
            time a ``Seek`` can be done. Defaults to 7 days. Cannot be more than 7
            days or less than 10 minutes.
        :type message_retention_duration: Union[Dict, google.cloud.pubsub_v1.types.Duration]
        :param labels: Client-assigned labels; see
            https://cloud.google.com/pubsub/docs/labels
        :type labels: Dict[str, str]
        :param enable_message_ordering: If true, messages published with the same
            ordering_key in PubsubMessage will be delivered to the subscribers in the order
            in which they are received by the Pub/Sub system. Otherwise, they may be
            delivered in any order.
        :type enable_message_ordering: bool
        :param expiration_policy: A policy that specifies the conditions for this
            subscription’s expiration. A subscription is considered active as long as any
            connected subscriber is successfully consuming messages from the subscription or
            is issuing operations on the subscription. If expiration_policy is not set,
            a default policy with ttl of 31 days will be used. The minimum allowed value for
            expiration_policy.ttl is 1 day.
        :type expiration_policy: Union[Dict, google.cloud.pubsub_v1.types.ExpirationPolicy`]
        :param filter_: An expression written in the Cloud Pub/Sub filter language. If
            non-empty, then only PubsubMessages whose attributes field matches the filter are
            delivered on this subscription. If empty, then no messages are filtered out.
        :type filter_: str
        :param dead_letter_policy: A policy that specifies the conditions for dead lettering
            messages in this subscription. If dead_letter_policy is not set, dead lettering is
            disabled.
        :type dead_letter_policy: Union[Dict, google.cloud.pubsub_v1.types.DeadLetterPolicy]
        :param retry_policy: A policy that specifies how Pub/Sub retries message delivery
            for this subscription. If not set, the default retry policy is applied. This
            generally implies that messages will be retried as soon as possible for healthy
            subscribers. RetryPolicy will be triggered on NACKs or acknowledgement deadline
            exceeded events for a given message.
        :type retry_policy: Union[Dict, google.cloud.pubsub_v1.types.RetryPolicy]
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :type timeout: float
        :param metadata: (Optional) Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]]
        :return: subscription name which will be the system-generated value if
            the ``subscription`` parameter is not supplied
        :rtype: str
        """
        subscriber = self.subscriber_client

        if not subscription:
            subscription = f'sub-{uuid4()}'
        if not subscription_project_id:
            subscription_project_id = project_id

        # Add airflow-version label to the subscription
        labels = labels or {}
        labels['airflow-version'] = 'v' + version.replace('.', '-').replace('+', '-')

        # pylint: disable=no-member
        subscription_path = f"projects/{subscription_project_id}/subscriptions/{subscription}"
        topic_path = f"projects/{project_id}/topics/{topic}"

        self.log.info("Creating subscription (path) %s for topic (path) %a", subscription_path, topic_path)
        try:
            subscriber.create_subscription(
                request={
                    "name": subscription_path,
                    "topic": topic_path,
                    "push_config": push_config,
                    "ack_deadline_seconds": ack_deadline_secs,
                    "retain_acked_messages": retain_acked_messages,
                    "message_retention_duration": message_retention_duration,
                    "labels": labels,
                    "enable_message_ordering": enable_message_ordering,
                    "expiration_policy": expiration_policy,
                    "filter": filter_,
                    "dead_letter_policy": dead_letter_policy,
                    "retry_policy": retry_policy,
                },
                retry=retry,
                timeout=timeout,
                metadata=metadata or (),
            )
        except AlreadyExists:
            self.log.warning('Subscription already exists: %s', subscription_path)
            if fail_if_exists:
                raise PubSubException(f'Subscription already exists: {subscription_path}')
        except GoogleAPICallError as e:
            raise PubSubException(f'Error creating subscription {subscription_path}', e)

        self.log.info("Created subscription (path) %s for topic (path) %s", subscription_path, topic_path)
        return subscription

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_subscription(
        self,
        subscription: str,
        project_id: str,
        fail_if_not_exists: bool = False,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> None:
        """
        Deletes a Pub/Sub subscription, if it exists.

        :param subscription: the Pub/Sub subscription name to delete; do not
            include the ``projects/{project}/subscriptions/`` prefix.
        :param project_id: Optional, the Google Cloud project ID where the subscription exists
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :type project_id: str
        :type subscription: str
        :param fail_if_not_exists: if set, raise an exception if the topic does not exist
        :type fail_if_not_exists: bool
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :type timeout: float
        :param metadata: (Optional) Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]]
        """
        subscriber = self.subscriber_client
        # noqa E501 # pylint: disable=no-member
        subscription_path = f"projects/{project_id}/subscriptions/{subscription}"

        self.log.info("Deleting subscription (path) %s", subscription_path)
        try:
            # pylint: disable=no-member
            subscriber.delete_subscription(
                request={"subscription": subscription_path},
                retry=retry,
                timeout=timeout,
                metadata=metadata or (),
            )

        except NotFound:
            self.log.warning('Subscription does not exist: %s', subscription_path)
            if fail_if_not_exists:
                raise PubSubException(f'Subscription does not exist: {subscription_path}')
        except GoogleAPICallError as e:
            raise PubSubException(f'Error deleting subscription {subscription_path}', e)

        self.log.info("Deleted subscription (path) %s", subscription_path)

    @GoogleBaseHook.fallback_to_default_project_id
    def pull(
        self,
        subscription: str,
        max_messages: int,
        project_id: str,
        return_immediately: bool = False,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> List[ReceivedMessage]:
        """
        Pulls up to ``max_messages`` messages from Pub/Sub subscription.

        :param subscription: the Pub/Sub subscription name to pull from; do not
            include the 'projects/{project}/topics/' prefix.
        :type subscription: str
        :param max_messages: The maximum number of messages to return from
            the Pub/Sub API.
        :type max_messages: int
        :param project_id: Optional, the Google Cloud project ID where the subscription exists.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :type project_id: str
        :param return_immediately: If set, the Pub/Sub API will immediately
            return if no messages are available. Otherwise, the request will
            block for an undisclosed, but bounded period of time
        :type return_immediately: bool
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :type timeout: float
        :param metadata: (Optional) Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]]
        :return: A list of Pub/Sub ReceivedMessage objects each containing
            an ``ackId`` property and a ``message`` property, which includes
            the base64-encoded message content. See
            https://cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions/pull#ReceivedMessage
        """
        subscriber = self.subscriber_client
        # noqa E501 # pylint: disable=no-member,line-too-long
        subscription_path = f"projects/{project_id}/subscriptions/{subscription}"

        self.log.info("Pulling max %d messages from subscription (path) %s", max_messages, subscription_path)
        try:
            # pylint: disable=no-member
            response = subscriber.pull(
                request={
                    "subscription": subscription_path,
                    "max_messages": max_messages,
                    "return_immediately": return_immediately,
                },
                retry=retry,
                timeout=timeout,
                metadata=metadata or (),
            )
            result = getattr(response, 'received_messages', [])
            self.log.info("Pulled %d messages from subscription (path) %s", len(result), subscription_path)
            return result
        except (HttpError, GoogleAPICallError) as e:
            raise PubSubException(f'Error pulling messages from subscription {subscription_path}', e)

    @GoogleBaseHook.fallback_to_default_project_id
    def acknowledge(
        self,
        subscription: str,
        project_id: str,
        ack_ids: Optional[List[str]] = None,
        messages: Optional[List[ReceivedMessage]] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> None:
        """
        Acknowledges the messages associated with the ``ack_ids`` from Pub/Sub subscription.

        :param subscription: the Pub/Sub subscription name to delete; do not
            include the 'projects/{project}/topics/' prefix.
        :type subscription: str
        :param ack_ids: List of ReceivedMessage ackIds from a previous pull response.
            Mutually exclusive with ``messages`` argument.
        :type ack_ids: list
        :param messages: List of ReceivedMessage objects to acknowledge.
            Mutually exclusive with ``ack_ids`` argument.
        :type messages: list
        :param project_id: Optional, the Google Cloud project name or ID in which to create the topic
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :type project_id: str
        :param retry: (Optional) A retry object used to retry requests.
            If None is specified, requests will not be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :type timeout: float
        :param metadata: (Optional) Additional metadata that is provided to the method.
        :type metadata: Sequence[Tuple[str, str]]]
        """
        if ack_ids is not None and messages is None:
            pass
        elif ack_ids is None and messages is not None:
            ack_ids = [message.ack_id for message in messages]
        else:
            raise ValueError("One and only one of 'ack_ids' and 'messages' arguments have to be provided")

        subscriber = self.subscriber_client
        # noqa E501 # pylint: disable=no-member
        subscription_path = f"projects/{project_id}/subscriptions/{subscription}"

        self.log.info("Acknowledging %d ack_ids from subscription (path) %s", len(ack_ids), subscription_path)
        try:
            # pylint: disable=no-member
            subscriber.acknowledge(
                request={"subscription": subscription_path, "ack_ids": ack_ids},
                retry=retry,
                timeout=timeout,
                metadata=metadata or (),
            )
        except (HttpError, GoogleAPICallError) as e:
            raise PubSubException(
                'Error acknowledging {} messages pulled from subscription {}'.format(
                    len(ack_ids), subscription_path
                ),
                e,
            )

        self.log.info("Acknowledged ack_ids from subscription (path) %s", subscription_path)
