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

import watchtower

try:
    from functools import cached_property
except ImportError:
    from cached_property import cached_property

from airflow.configuration import conf
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.log.logging_mixin import LoggingMixin


class CloudwatchTaskHandler(FileTaskHandler, LoggingMixin):
    """
    CloudwatchTaskHandler is a python log handler that handles and reads task instance logs.

    It extends airflow FileTaskHandler and uploads to and reads from Cloudwatch.

    :param base_log_folder: base folder to store logs locally
    :type base_log_folder: str
    :param log_group_arn: ARN of the Cloudwatch log group for remote log storage
        with format ``arn:aws:logs:{region name}:{account id}:log-group:{group name}``
    :type log_group_arn: str
    :param filename_template: template for file name (local storage) or log stream name (remote)
    :type filename_template: str
    """

    def __init__(self, base_log_folder: str, log_group_arn: str, filename_template: str):
        super().__init__(base_log_folder, filename_template)
        split_arn = log_group_arn.split(':')

        self.handler = None
        self.log_group = split_arn[6]
        self.region_name = split_arn[3]
        self.closed = False

    @cached_property
    def hook(self):
        """Returns AwsLogsHook."""
        remote_conn_id = conf.get('logging', 'REMOTE_LOG_CONN_ID')
        try:
            from airflow.providers.amazon.aws.hooks.logs import AwsLogsHook

            return AwsLogsHook(aws_conn_id=remote_conn_id, region_name=self.region_name)
        except Exception as e:  # pylint: disable=broad-except
            self.log.error(
                'Could not create an AwsLogsHook with connection id "%s". '
                'Please make sure that airflow[aws] is installed and '
                'the Cloudwatch logs connection exists. Exception: "%s"',
                remote_conn_id,
                e,
            )
            return None

    def _render_filename(self, ti, try_number):
        # Replace unsupported log group name characters
        return super()._render_filename(ti, try_number).replace(':', '_')

    def set_context(self, ti):
        super().set_context(ti)
        self.handler = watchtower.CloudWatchLogHandler(
            log_group=self.log_group,
            stream_name=self._render_filename(ti, ti.try_number),
            boto3_session=self.hook.get_session(self.region_name),
        )

    def close(self):
        """Close the handler responsible for the upload of the local log file to Cloudwatch."""
        # When application exit, system shuts down all handlers by
        # calling close method. Here we check if logger is already
        # closed to prevent uploading the log to remote storage multiple
        # times when `logging.shutdown` is called.
        if self.closed:
            return

        if self.handler is not None:
            self.handler.close()
        # Mark closed so we don't double write if close is called twice
        self.closed = True

    def _read(self, task_instance, try_number, metadata=None):
        stream_name = self._render_filename(task_instance, try_number)
        return (
            '*** Reading remote log from Cloudwatch log_group: {} log_stream: {}.\n{}\n'.format(
                self.log_group, stream_name, self.get_cloudwatch_logs(stream_name=stream_name)
            ),
            {'end_of_log': True},
        )

    def get_cloudwatch_logs(self, stream_name: str) -> str:
        """
        Return all logs from the given log stream.

        :param stream_name: name of the Cloudwatch log stream to get all logs from
        :return: string of all logs from the given log stream
        """
        try:
            events = list(
                self.hook.get_log_events(
                    log_group=self.log_group, log_stream_name=stream_name, start_from_head=True
                )
            )
            return '\n'.join([event['message'] for event in events])
        except Exception:  # pylint: disable=broad-except
            msg = 'Could not read remote logs from log_group: {} log_stream: {}.'.format(
                self.log_group, stream_name
            )
            self.log.exception(msg)
            return msg
