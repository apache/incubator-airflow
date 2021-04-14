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
from enum import Enum
from typing import Any, Optional

from tableauserverclient import Pager, PersonalAccessTokenAuth, Server, TableauAuth
from tableauserverclient.server import Auth

from airflow.hooks.base import BaseHook


class TableauJobFinishCode(Enum):
    """
    The finish code indicates the status of the job.

    .. seealso:: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref.htm#query_job

    """

    PENDING = -1
    SUCCESS = 0
    ERROR = 1
    CANCELED = 2


class TableauHook(BaseHook):
    """
    Connects to the Tableau Server Instance and allows to communicate with it.

    .. seealso:: https://tableau.github.io/server-client-python/docs/

    :param site_id: The id of the site where the workbook belongs to.
        It will connect to the default site if you don't provide an id.
    :type site_id: Optional[str]
    :param tableau_conn_id: The Tableau Connection id containing the credentials
        to authenticate to the Tableau Server.
    :type tableau_conn_id: str
    """

    conn_name_attr = 'tableau_conn_id'
    default_conn_name = 'tableau_default'
    conn_type = 'tableau'
    hook_name = 'Tableau'

    def __init__(self, site_id: Optional[str] = None, tableau_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.tableau_conn_id = tableau_conn_id
        self.conn = self.get_connection(self.tableau_conn_id)
        self.site_id = site_id or self.conn.extra_dejson.get('site_id', '')
        self.server = Server(self.conn.host, use_server_version=True)
        self.tableau_conn = None

    def __enter__(self):
        if not self.tableau_conn:
            self.tableau_conn = self.get_conn()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.server.auth.sign_out()

    def get_conn(self) -> Auth.contextmgr:
        """
        Signs in to the Tableau Server and automatically signs out if used as ContextManager.

        :return: an authorized Tableau Server Context Manager object.
        :rtype: tableauserverclient.server.Auth.contextmgr
        """
        if self.conn.login and self.conn.password:
            return self._auth_via_password()
        if 'token_name' in self.conn.extra_dejson and 'personal_access_token' in self.conn.extra_dejson:
            return self._auth_via_token()
        raise NotImplementedError('No Authentication method found for given Credentials!')

    def _auth_via_password(self) -> Auth.contextmgr:
        tableau_auth = TableauAuth(
            username=self.conn.login, password=self.conn.password, site_id=self.site_id
        )
        return self.server.auth.sign_in(tableau_auth)

    def _auth_via_token(self) -> Auth.contextmgr:
        tableau_auth = PersonalAccessTokenAuth(
            token_name=self.conn.extra_dejson['token_name'],
            personal_access_token=self.conn.extra_dejson['personal_access_token'],
            site_id=self.site_id,
        )
        return self.server.auth.sign_in_with_personal_access_token(tableau_auth)

    def get_all(self, resource_name: str) -> Pager:
        """
        Get all items of the given resource.

        .. seealso:: https://tableau.github.io/server-client-python/docs/page-through-results

        :param resource_name: The name of the resource to paginate.
            For example: jobs or workbooks
        :type resource_name: str
        :return: all items by returning a Pager.
        :rtype: tableauserverclient.Pager
        """
        resource = getattr(self.server, resource_name)
        return Pager(resource.get)
