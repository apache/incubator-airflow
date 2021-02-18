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

"""Connect to Asana."""

from asana import Client

from airflow.hooks.base import BaseHook


class AsanaHook(BaseHook):
    """
    Wrapper around Asana Python client library.
    """

    conn_name_attr = 'asana_conn_id'
    default_conn_name = 'asana_default'
    conn_type = 'asana'
    hook_name = 'Asana'

    def __init__(self, conn_id: str = default_conn_name, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.asana_conn_id = conn_id
        self.connection = kwargs.pop("connection", None)
        self.client = None
        self.extras = None
        self.uri = None

    def get_conn(self) -> Client:
        """
        Creates Asana Client
        """
        self.connection = self.get_connection(self.asana_conn_id)
        self.extras = self.connection.extra_dejson.copy()

        if self.client is not None:
            return self.client

        self.client = Client.access_token(self.connection.password)

        return self.client
