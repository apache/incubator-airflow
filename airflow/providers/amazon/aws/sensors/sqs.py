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
"""Reads and then deletes the message from SQS queue"""
from typing import Optional

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.sqs import SQSHook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class SQSSensor(BaseSensorOperator):
    """
    Get messages from an SQS queue and then deletes  the message from the SQS queue.
    If deletion of messages fails an AirflowException is thrown otherwise, the message
    is pushed through XCom with the key ``messages``.

    :param aws_conn_id: AWS connection id
    :type aws_conn_id: str
    :param sqs_queue: The SQS queue url (templated)
    :type sqs_queue: str
    :param max_messages: The maximum number of messages to retrieve for each poke (templated)
    :type max_messages: int
    :param wait_time_seconds: The time in seconds to wait for receiving messages (default: 1 second)
    :type wait_time_seconds: int
    """

    template_fields = ('sqs_queue', 'max_messages')

    @apply_defaults
    def __init__(
        self,
        *,
        sqs_queue,
        aws_conn_id: str = 'aws_default',
        max_messages: int = 5,
        wait_time_seconds: int = 1,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.sqs_queue = sqs_queue
        self.aws_conn_id = aws_conn_id
        self.max_messages = max_messages
        self.wait_time_seconds = wait_time_seconds
        self.hook: Optional[SQSHook] = None

    def poke(self, context):
        """
        Check for message on subscribed queue and write to xcom the message with key ``messages``

        :param context: the context object
        :type context: dict
        :return: ``True`` if message is available or ``False``
        """
        sqs_conn = self.get_hook().get_conn()

        self.log.info('SQSSensor checking for message on queue: %s', self.sqs_queue)

        messages = sqs_conn.receive_message(
            QueueUrl=self.sqs_queue,
            MaxNumberOfMessages=self.max_messages,
            WaitTimeSeconds=self.wait_time_seconds,
        )

        self.log.info("received message %s", str(messages))

        if 'Messages' in messages and messages['Messages']:
            entries = [
                {'Id': message['MessageId'], 'ReceiptHandle': message['ReceiptHandle']}
                for message in messages['Messages']
            ]

            result = sqs_conn.delete_message_batch(QueueUrl=self.sqs_queue, Entries=entries)

            if 'Successful' in result:
                context['ti'].xcom_push(key='messages', value=messages)
                return True
            else:
                raise AirflowException(
                    'Delete SQS Messages failed ' + str(result) + ' for messages ' + str(messages)
                )

        return False

    def get_hook(self) -> SQSHook:
        """Create and return an SQSHook"""
        if self.hook:
            return self.hook

        self.hook = SQSHook(aws_conn_id=self.aws_conn_id)
        return self.hook
