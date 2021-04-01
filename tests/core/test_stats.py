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
import importlib
import re
import unittest
from unittest import mock
from unittest.mock import Mock

import pytest
import statsd

import airflow
from airflow.exceptions import AirflowConfigException, InvalidStatsNameException
from airflow.stats import AllowListValidator, SafeDogStatsdLogger, SafeStatsdLogger
from tests.test_utils.config import conf_vars


class CustomStatsd(statsd.StatsClient):
    incr_calls = 0

    def __init__(self, host=None, port=None, prefix=None):
        super().__init__()

    def incr(self, stat, count=1, rate=1):  # pylint: disable=unused-argument
        CustomStatsd.incr_calls += 1

    @classmethod
    def _reset(cls):
        cls.incr_calls = 0


class InvalidCustomStatsd:
    """
    This custom Statsd class is invalid because it does not subclass
    statsd.StatsClient.
    """

    incr_calls = 0

    def __init__(self, host=None, port=None, prefix=None):
        pass

    def incr(self, stat, count=1, rate=1):  # pylint: disable=unused-argument
        InvalidCustomStatsd.incr_calls += 1

    @classmethod
    def _reset(cls):
        cls.incr_calls = 0


class TestStats(unittest.TestCase):
    def setUp(self):
        self.statsd_client = Mock()
        self.stats = SafeStatsdLogger(self.statsd_client)
        CustomStatsd._reset()
        InvalidCustomStatsd._reset()

    def test_increment_counter_with_valid_name(self):
        self.stats.incr('test_stats_run')
        self.statsd_client.incr.assert_called_once_with('test_stats_run', 1, 1)

    def test_stat_name_must_be_a_string(self):
        self.stats.incr([])
        self.statsd_client.assert_not_called()

    def test_stat_name_must_not_exceed_max_length(self):
        self.stats.incr('X' * 300)
        self.statsd_client.assert_not_called()

    def test_stat_name_must_only_include_allowed_characters(self):
        self.stats.incr('test/$tats')
        self.statsd_client.assert_not_called()

    @conf_vars({('metrics', 'statsd_on'): 'True'})
    @mock.patch("statsd.StatsClient")
    def test_does_send_stats_using_statsd(self, mock_statsd):
        importlib.reload(airflow.stats)
        airflow.stats.Stats.incr("dummy_key")
        mock_statsd.return_value.incr.assert_called_once_with('dummy_key', 1, 1)

    @conf_vars({('metrics', 'statsd_on'): 'True'})
    @mock.patch("datadog.DogStatsd")
    def test_does_not_send_stats_using_dogstatsd(self, mock_dogstatsd):
        importlib.reload(airflow.stats)
        airflow.stats.Stats.incr("dummy_key")
        mock_dogstatsd.return_value.assert_not_called()

    @conf_vars(
        {
            ("metrics", "statsd_on"): "True",
            ("metrics", "statsd_custom_client_path"): "tests.core.test_stats.CustomStatsd",
        }
    )
    def test_load_custom_statsd_client(self):
        importlib.reload(airflow.stats)
        assert 'CustomStatsd' == type(airflow.stats.Stats.statsd).__name__  # noqa: E721

    @conf_vars(
        {
            ("metrics", "statsd_on"): "True",
            ("metrics", "statsd_custom_client_path"): "tests.core.test_stats.CustomStatsd",
        }
    )
    def test_does_use_custom_statsd_client(self):
        importlib.reload(airflow.stats)
        airflow.stats.Stats.incr("dummy_key")
        assert airflow.stats.Stats.statsd.incr_calls == 1

    @conf_vars(
        {
            ("metrics", "statsd_on"): "True",
            ("metrics", "statsd_custom_client_path"): "tests.core.test_stats.InvalidCustomStatsd",
        }
    )
    def test_load_invalid_custom_stats_client(self):
        with pytest.raises(
            AirflowConfigException,
            match=re.escape(
                'Your custom Statsd client must extend the statsd.'
                'StatsClient in order to ensure backwards compatibility.'
            ),
        ):
            importlib.reload(airflow.stats)
            airflow.stats.Stats.incr("dummy_key")

    def tearDown(self) -> None:
        # To avoid side-effect
        importlib.reload(airflow.stats)


class TestDogStats(unittest.TestCase):
    def setUp(self):
        self.dogstatsd_client = Mock()
        self.dogstatsd = SafeDogStatsdLogger(self.dogstatsd_client)

    def test_increment_counter_with_valid_name_with_dogstatsd(self):
        self.dogstatsd.incr('test_stats_run')
        self.dogstatsd_client.increment.assert_called_once_with(
            metric='test_stats_run', sample_rate=1, tags=[], value=1
        )

    def test_stat_name_must_be_a_string_with_dogstatsd(self):
        self.dogstatsd.incr([])
        self.dogstatsd_client.assert_not_called()

    def test_stat_name_must_not_exceed_max_length_with_dogstatsd(self):
        self.dogstatsd.incr('X' * 300)
        self.dogstatsd_client.assert_not_called()

    def test_stat_name_must_only_include_allowed_characters_with_dogstatsd(self):
        self.dogstatsd.incr('test/$tats')
        self.dogstatsd_client.assert_not_called()

    @conf_vars({('metrics', 'statsd_datadog_enabled'): 'True'})
    @mock.patch("datadog.DogStatsd")
    def test_does_send_stats_using_dogstatsd_when_dogstatsd_on(self, mock_dogstatsd):
        importlib.reload(airflow.stats)
        airflow.stats.Stats.incr("dummy_key")
        mock_dogstatsd.return_value.increment.assert_called_once_with(
            metric='dummy_key', sample_rate=1, tags=[], value=1
        )

    @conf_vars({('metrics', 'statsd_datadog_enabled'): 'True'})
    @mock.patch("datadog.DogStatsd")
    def test_does_send_stats_using_dogstatsd_with_tags(self, mock_dogstatsd):
        importlib.reload(airflow.stats)
        airflow.stats.Stats.incr("dummy_key", 1, 1, ['key1:value1', 'key2:value2'])
        mock_dogstatsd.return_value.increment.assert_called_once_with(
            metric='dummy_key', sample_rate=1, tags=['key1:value1', 'key2:value2'], value=1
        )

    @conf_vars({('metrics', 'statsd_on'): 'True', ('metrics', 'statsd_datadog_enabled'): 'True'})
    @mock.patch("datadog.DogStatsd")
    def test_does_send_stats_using_dogstatsd_when_statsd_and_dogstatsd_both_on(self, mock_dogstatsd):
        importlib.reload(airflow.stats)
        airflow.stats.Stats.incr("dummy_key")
        mock_dogstatsd.return_value.increment.assert_called_once_with(
            metric='dummy_key', sample_rate=1, tags=[], value=1
        )

    @conf_vars({('metrics', 'statsd_on'): 'True', ('metrics', 'statsd_datadog_enabled'): 'True'})
    @mock.patch("statsd.StatsClient")
    def test_does_not_send_stats_using_statsd_when_statsd_and_dogstatsd_both_on(self, mock_statsd):
        importlib.reload(airflow.stats)
        airflow.stats.Stats.incr("dummy_key")
        mock_statsd.return_value.assert_not_called()

    def tearDown(self) -> None:
        # To avoid side-effect
        importlib.reload(airflow.stats)


class TestStatsWithAllowList(unittest.TestCase):
    def setUp(self):
        self.statsd_client = Mock()
        self.stats = SafeStatsdLogger(self.statsd_client, AllowListValidator("stats_one, stats_two"))

    def test_increment_counter_with_allowed_key(self):
        self.stats.incr('stats_one')
        self.statsd_client.incr.assert_called_once_with('stats_one', 1, 1)

    def test_increment_counter_with_allowed_prefix(self):
        self.stats.incr('stats_two.bla')
        self.statsd_client.incr.assert_called_once_with('stats_two.bla', 1, 1)

    def test_not_increment_counter_if_not_allowed(self):
        self.stats.incr('stats_three')
        self.statsd_client.assert_not_called()


class TestDogStatsWithAllowList(unittest.TestCase):
    def setUp(self):
        self.dogstatsd_client = Mock()
        self.dogstats = SafeDogStatsdLogger(self.dogstatsd_client, AllowListValidator("stats_one, stats_two"))

    def test_increment_counter_with_allowed_key(self):
        self.dogstats.incr('stats_one')
        self.dogstatsd_client.increment.assert_called_once_with(
            metric='stats_one', sample_rate=1, tags=[], value=1
        )

    def test_increment_counter_with_allowed_prefix(self):
        self.dogstats.incr('stats_two.bla')
        self.dogstatsd_client.increment.assert_called_once_with(
            metric='stats_two.bla', sample_rate=1, tags=[], value=1
        )

    def test_not_increment_counter_if_not_allowed(self):
        self.dogstats.incr('stats_three')
        self.dogstatsd_client.assert_not_called()


def always_invalid(stat_name):
    raise InvalidStatsNameException(f"Invalid name: {stat_name}")


def always_valid(stat_name):
    return stat_name


class TestCustomStatsName(unittest.TestCase):
    @conf_vars(
        {
            ('metrics', 'statsd_on'): 'True',
            ('metrics', 'stat_name_handler'): 'tests.core.test_stats.always_invalid',
        }
    )
    @mock.patch("statsd.StatsClient")
    def test_does_not_send_stats_using_statsd_when_the_name_is_not_valid(self, mock_statsd):
        importlib.reload(airflow.stats)
        airflow.stats.Stats.incr("dummy_key")
        mock_statsd.return_value.assert_not_called()

    @conf_vars(
        {
            ('metrics', 'statsd_datadog_enabled'): 'True',
            ('metrics', 'stat_name_handler'): 'tests.core.test_stats.always_invalid',
        }
    )
    @mock.patch("datadog.DogStatsd")
    def test_does_not_send_stats_using_dogstatsd_when_the_name_is_not_valid(self, mock_dogstatsd):
        importlib.reload(airflow.stats)
        airflow.stats.Stats.incr("dummy_key")
        mock_dogstatsd.return_value.assert_not_called()

    @conf_vars(
        {
            ('metrics', 'statsd_on'): 'True',
            ('metrics', 'stat_name_handler'): 'tests.core.test_stats.always_valid',
        }
    )
    @mock.patch("statsd.StatsClient")
    def test_does_send_stats_using_statsd_when_the_name_is_valid(self, mock_statsd):
        importlib.reload(airflow.stats)
        airflow.stats.Stats.incr("dummy_key")
        mock_statsd.return_value.incr.assert_called_once_with('dummy_key', 1, 1)

    @conf_vars(
        {
            ('metrics', 'statsd_datadog_enabled'): 'True',
            ('metrics', 'stat_name_handler'): 'tests.core.test_stats.always_valid',
        }
    )
    @mock.patch("datadog.DogStatsd")
    def test_does_send_stats_using_dogstatsd_when_the_name_is_valid(self, mock_dogstatsd):
        importlib.reload(airflow.stats)
        airflow.stats.Stats.incr("dummy_key")
        mock_dogstatsd.return_value.increment.assert_called_once_with(
            metric='dummy_key', sample_rate=1, tags=[], value=1
        )

    def tearDown(self) -> None:
        # To avoid side-effect
        importlib.reload(airflow.stats)
