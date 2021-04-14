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
import os

try:
    from functools import cached_property
except ImportError:
    from cached_property import cached_property

from airflow.configuration import conf
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.log.logging_mixin import LoggingMixin


class S3TaskHandler(FileTaskHandler, LoggingMixin):
    """
    S3TaskHandler is a python log handler that handles and reads
    task instance logs. It extends airflow FileTaskHandler and
    uploads to and reads from S3 remote storage.
    """

    def __init__(self, base_log_folder: str, s3_log_folder: str, filename_template: str):
        super().__init__(base_log_folder, filename_template)
        self.remote_base = s3_log_folder
        self.log_relative_path = ''
        self._hook = None
        self.closed = False
        self.upload_on_close = True

    @cached_property
    def hook(self):
        """Returns S3Hook."""
        remote_conn_id = conf.get('logging', 'REMOTE_LOG_CONN_ID')
        try:
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook

            return S3Hook(remote_conn_id, transfer_config_args={"use_threads": False})
        except Exception as e:  # pylint: disable=broad-except
            self.log.exception(
                'Could not create an S3Hook with connection id "%s". '
                'Please make sure that airflow[aws] is installed and '
                'the S3 connection exists. Exception : "%s"',
                remote_conn_id,
                e,
            )
            return None

    def set_context(self, ti):
        super().set_context(ti)
        # Local location and remote location is needed to open and
        # upload local log file to S3 remote storage.
        self.log_relative_path = self._render_filename(ti, ti.try_number)
        self.upload_on_close = not ti.raw

        # Clear the file first so that duplicate data is not uploaded
        # when re-using the same path (e.g. with rescheduled sensors)
        if self.upload_on_close:
            with open(self.handler.baseFilename, 'w'):
                pass

    def close(self):
        """Close and upload local log file to remote storage S3."""
        # When application exit, system shuts down all handlers by
        # calling close method. Here we check if logger is already
        # closed to prevent uploading the log to remote storage multiple
        # times when `logging.shutdown` is called.
        if self.closed:
            return

        super().close()

        if not self.upload_on_close:
            return

        local_loc = os.path.join(self.local_base, self.log_relative_path)
        remote_loc = os.path.join(self.remote_base, self.log_relative_path)
        if os.path.exists(local_loc):
            # read log and remove old logs to get just the latest additions
            with open(local_loc) as logfile:
                log = logfile.read()
            self.s3_write(log, remote_loc)

        # Mark closed so we don't double write if close is called twice
        self.closed = True

    def _read(self, ti, try_number, metadata=None):
        """
        Read logs of given task instance and try_number from S3 remote storage.
        If failed, read the log from task instance host machine.

        :param ti: task instance object
        :param try_number: task instance try_number to read logs from
        :param metadata: log metadata,
                         can be used for steaming log reading and auto-tailing.
        """
        # Explicitly getting log relative path is necessary as the given
        # task instance might be different than task instance passed in
        # in set_context method.
        log_relative_path = self._render_filename(ti, try_number)
        remote_loc = os.path.join(self.remote_base, log_relative_path)

        log_exists = False
        log = ""

        try:
            log_exists = self.s3_log_exists(remote_loc)
        except Exception as error:  # pylint: disable=broad-except
            self.log.exception(error)
            log = f'*** Failed to verify remote log exists {remote_loc}.\n{str(error)}\n'

        if log_exists:
            # If S3 remote file exists, we do not fetch logs from task instance
            # local machine even if there are errors reading remote logs, as
            # returned remote_log will contain error messages.
            remote_log = self.s3_read(remote_loc, return_error=True)
            log = f'*** Reading remote log from {remote_loc}.\n{remote_log}\n'
            return log, {'end_of_log': True}
        else:
            log += '*** Falling back to local log\n'
            local_log, metadata = super()._read(ti, try_number)
            return log + local_log, metadata

    def s3_log_exists(self, remote_log_location: str) -> bool:
        """
        Check if remote_log_location exists in remote storage

        :param remote_log_location: log's location in remote storage
        :type remote_log_location: str
        :return: True if location exists else False
        """
        return self.hook.check_for_key(remote_log_location)

    def s3_read(self, remote_log_location: str, return_error: bool = False) -> str:
        """
        Returns the log found at the remote_log_location. Returns '' if no
        logs are found or there is an error.

        :param remote_log_location: the log's location in remote storage
        :type remote_log_location: str (path)
        :param return_error: if True, returns a string error message if an
            error occurs. Otherwise returns '' when an error occurs.
        :type return_error: bool
        :return: the log found at the remote_log_location
        """
        try:
            return self.hook.read_key(remote_log_location)
        except Exception as error:  # pylint: disable=broad-except
            msg = f'Could not read logs from {remote_log_location} with error: {error}'
            self.log.exception(msg)
            # return error if needed
            if return_error:
                return msg
        return ''

    def s3_write(self, log: str, remote_log_location: str, append: bool = True):
        """
        Writes the log to the remote_log_location. Fails silently if no hook
        was created.

        :param log: the log to write to the remote_log_location
        :type log: str
        :param remote_log_location: the log's location in remote storage
        :type remote_log_location: str (path)
        :param append: if False, any existing log file is overwritten. If True,
            the new log is appended to any existing logs.
        :type append: bool
        """
        try:
            if append and self.s3_log_exists(remote_log_location):
                old_log = self.s3_read(remote_log_location)
                log = '\n'.join([old_log, log]) if old_log else log
        except Exception as error:  # pylint: disable=broad-except
            self.log.exception('Could not verify previous log to append: %s', str(error))

        try:
            self.hook.load_string(
                log,
                key=remote_log_location,
                replace=True,
                encrypt=conf.getboolean('logging', 'ENCRYPT_S3_LOGS'),
            )
        except Exception:  # pylint: disable=broad-except
            self.log.exception('Could not write logs to %s', remote_log_location)
