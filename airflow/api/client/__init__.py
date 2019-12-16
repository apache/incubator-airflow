airflow/api/client/__init__.py
# -*- coding: utf-8 -*-
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
"""
API Client that allows interacting with Airflow API
"""
from importlib import import_module
from typing import Any

from airflow import api, conf
from airflow.api.client.api_client import Client


def get_current_api_client() -> Client:
    """
    Return current API Client based on current Airflow configuration
    """
    api_module = import_module(conf.get('cli', 'api_client'))  # type: Any
    api_client = api_module.Client(
        api_base_url=conf.get('cli', 'endpoint_url'),
        auth=api.API_AUTH.api_auth.CLIENT_AUTH
    )
    return api_client
