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
import unittest
from unittest import mock

import pytest
from requests.exceptions import BaseHTTPError

from airflow.exceptions import AirflowException
from airflow.kubernetes.pod_launcher import PodLauncher, PodStatus


class TestPodLauncher(unittest.TestCase):
    def setUp(self):
        self.mock_kube_client = mock.Mock()
        self.pod_launcher = PodLauncher(kube_client=self.mock_kube_client)

    def test_read_pod_logs_successfully_returns_logs(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod_log.return_value = mock.sentinel.logs
        logs = self.pod_launcher.read_pod_logs(mock.sentinel)
        assert mock.sentinel.logs == logs

    def test_read_pod_logs_retries_successfully(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod_log.side_effect = [
            BaseHTTPError('Boom'),
            mock.sentinel.logs,
        ]
        logs = self.pod_launcher.read_pod_logs(mock.sentinel)
        assert mock.sentinel.logs == logs
        self.mock_kube_client.read_namespaced_pod_log.assert_has_calls(
            [
                mock.call(
                    _preload_content=False,
                    container='base',
                    follow=True,
                    timestamps=False,
                    name=mock.sentinel.metadata.name,
                    namespace=mock.sentinel.metadata.namespace,
                ),
                mock.call(
                    _preload_content=False,
                    container='base',
                    follow=True,
                    timestamps=False,
                    name=mock.sentinel.metadata.name,
                    namespace=mock.sentinel.metadata.namespace,
                ),
            ]
        )

    def test_read_pod_logs_retries_fails(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod_log.side_effect = [
            BaseHTTPError('Boom'),
            BaseHTTPError('Boom'),
            BaseHTTPError('Boom'),
        ]
        with pytest.raises(AirflowException):
            self.pod_launcher.read_pod_logs(mock.sentinel)

    def test_read_pod_logs_successfully_with_tail_lines(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod_log.side_effect = [mock.sentinel.logs]
        logs = self.pod_launcher.read_pod_logs(mock.sentinel, tail_lines=100)
        assert mock.sentinel.logs == logs
        self.mock_kube_client.read_namespaced_pod_log.assert_has_calls(
            [
                mock.call(
                    _preload_content=False,
                    container='base',
                    follow=True,
                    timestamps=False,
                    name=mock.sentinel.metadata.name,
                    namespace=mock.sentinel.metadata.namespace,
                    tail_lines=100,
                ),
            ]
        )

    def test_read_pod_logs_successfully_with_since_seconds(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod_log.side_effect = [mock.sentinel.logs]
        logs = self.pod_launcher.read_pod_logs(mock.sentinel, since_seconds=2)
        assert mock.sentinel.logs == logs
        self.mock_kube_client.read_namespaced_pod_log.assert_has_calls(
            [
                mock.call(
                    _preload_content=False,
                    container='base',
                    follow=True,
                    timestamps=False,
                    name=mock.sentinel.metadata.name,
                    namespace=mock.sentinel.metadata.namespace,
                    since_seconds=2,
                ),
            ]
        )

    def test_read_pod_events_successfully_returns_events(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.list_namespaced_event.return_value = mock.sentinel.events
        events = self.pod_launcher.read_pod_events(mock.sentinel)
        assert mock.sentinel.events == events

    def test_read_pod_events_retries_successfully(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.list_namespaced_event.side_effect = [
            BaseHTTPError('Boom'),
            mock.sentinel.events,
        ]
        events = self.pod_launcher.read_pod_events(mock.sentinel)
        assert mock.sentinel.events == events
        self.mock_kube_client.list_namespaced_event.assert_has_calls(
            [
                mock.call(
                    namespace=mock.sentinel.metadata.namespace,
                    field_selector=f"involvedObject.name={mock.sentinel.metadata.name}",
                ),
                mock.call(
                    namespace=mock.sentinel.metadata.namespace,
                    field_selector=f"involvedObject.name={mock.sentinel.metadata.name}",
                ),
            ]
        )

    def test_read_pod_events_retries_fails(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.list_namespaced_event.side_effect = [
            BaseHTTPError('Boom'),
            BaseHTTPError('Boom'),
            BaseHTTPError('Boom'),
        ]
        with pytest.raises(AirflowException):
            self.pod_launcher.read_pod_events(mock.sentinel)

    def test_read_pod_returns_logs(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod.return_value = mock.sentinel.pod_info
        pod_info = self.pod_launcher.read_pod(mock.sentinel)
        assert mock.sentinel.pod_info == pod_info

    def test_read_pod_retries_successfully(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod.side_effect = [
            BaseHTTPError('Boom'),
            mock.sentinel.pod_info,
        ]
        pod_info = self.pod_launcher.read_pod(mock.sentinel)
        assert mock.sentinel.pod_info == pod_info
        self.mock_kube_client.read_namespaced_pod.assert_has_calls(
            [
                mock.call(mock.sentinel.metadata.name, mock.sentinel.metadata.namespace),
                mock.call(mock.sentinel.metadata.name, mock.sentinel.metadata.namespace),
            ]
        )

    def test_monitor_pod_empty_logs(self):
        mock.sentinel.metadata = mock.MagicMock()
        running_status = mock.MagicMock()
        running_status.configure_mock(**{'name': 'base', 'state.running': True})
        pod_info_running = mock.MagicMock(**{'status.container_statuses': [running_status]})
        pod_info_succeeded = mock.MagicMock(**{'status.phase': PodStatus.SUCCEEDED})

        def pod_state_gen():
            yield pod_info_running
            while True:
                yield pod_info_succeeded

        self.mock_kube_client.read_namespaced_pod.side_effect = pod_state_gen()
        self.mock_kube_client.read_namespaced_pod_log.return_value = iter(())
        self.pod_launcher.monitor_pod(mock.sentinel, get_logs=True)

    def test_read_pod_retries_fails(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod.side_effect = [
            BaseHTTPError('Boom'),
            BaseHTTPError('Boom'),
            BaseHTTPError('Boom'),
        ]
        with pytest.raises(AirflowException):
            self.pod_launcher.read_pod(mock.sentinel)

    def test_parse_log_line(self):
        timestamp, message = self.pod_launcher.parse_log_line(
            '2020-10-08T14:16:17.793417674Z Valid message\n'
        )

        assert timestamp == '2020-10-08T14:16:17.793417674Z'
        assert message == 'Valid message'

        with pytest.raises(Exception):
            self.pod_launcher.parse_log_line('2020-10-08T14:16:17.793417674ZInvalidmessage\n')
