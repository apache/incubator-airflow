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

"""
NOTE: this module can be removed once upstream client supports token refresh
see: https://github.com/kubernetes-client/python/issues/741
"""

import calendar
import logging
import os
import time
from typing import Optional, cast

import pendulum
from kubernetes.client import Configuration
from kubernetes.config.exec_provider import ExecProvider
from kubernetes.config.kube_config import KUBE_CONFIG_DEFAULT_LOCATION, KubeConfigLoader

import airflow.utils.yaml as yaml


def _parse_timestamp(ts_str: str) -> int:
    parsed_dt = cast(pendulum.DateTime, pendulum.parse(ts_str))
    return calendar.timegm(parsed_dt.timetuple())


class RefreshKubeConfigLoader(KubeConfigLoader):
    """
    Patched KubeConfigLoader, this subclass takes expirationTimestamp into
    account and sets api key refresh callback hook in Configuration object
    """

    def __init__(self, *args, **kwargs):
        KubeConfigLoader.__init__(self, *args, **kwargs)
        self.api_key_expire_ts = None

    def _load_from_exec_plugin(self):
        """
        We override _load_from_exec_plugin method to also read and store
        expiration timestamp for aws-iam-authenticator. It will be later
        used for api token refresh.
        """
        if 'exec' not in self._user:
            return None
        try:
            status = ExecProvider(self._user['exec']).run()
            if 'token' not in status:
                logging.error('exec: missing token field in plugin output')
                return None
            self.token = f"Bearer {status['token']}"  # pylint: disable=W0201
            ts_str = status.get('expirationTimestamp')
            if ts_str:
                self.api_key_expire_ts = _parse_timestamp(ts_str)
            return True
        except Exception as e:  # pylint: disable=W0703
            logging.error(str(e))
            return None

    def refresh_api_key(self, client_configuration):
        """Refresh API key if expired"""
        if self.api_key_expire_ts and time.time() >= self.api_key_expire_ts:
            self.load_and_set(client_configuration)

    def load_and_set(self, client_configuration):
        KubeConfigLoader.load_and_set(self, client_configuration)
        client_configuration.refresh_api_key = self.refresh_api_key


class RefreshConfiguration(Configuration):
    """
    Patched Configuration, this subclass takes api key refresh callback hook
    into account
    """

    def __init__(self, *args, **kwargs):
        Configuration.__init__(self, *args, **kwargs)
        self.refresh_api_key = None

    def get_api_key_with_prefix(self, identifier):
        if self.refresh_api_key:
            self.refresh_api_key(self)  # pylint: disable=E1102
        return Configuration.get_api_key_with_prefix(self, identifier)


def _get_kube_config_loader_for_yaml_file(filename, **kwargs) -> Optional[RefreshKubeConfigLoader]:
    """
    Adapted from the upstream _get_kube_config_loader_for_yaml_file function, changed
    KubeConfigLoader to RefreshKubeConfigLoader
    """
    with open(filename) as f:
        return RefreshKubeConfigLoader(
            config_dict=yaml.safe_load(f),
            config_base_path=os.path.abspath(os.path.dirname(filename)),
            **kwargs,
        )


def load_kube_config(client_configuration, config_file=None, context=None):
    """
    Adapted from the upstream load_kube_config function, changes:
        - removed persist_config argument since it's not being used
        - remove `client_configuration is None` branch since we always pass
        in client configuration
    """
    if config_file is None:
        config_file = os.path.expanduser(KUBE_CONFIG_DEFAULT_LOCATION)

    loader = _get_kube_config_loader_for_yaml_file(config_file, active_context=context, config_persister=None)
    loader.load_and_set(client_configuration)
