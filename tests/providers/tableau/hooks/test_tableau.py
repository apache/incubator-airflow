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
from unittest.mock import patch

from airflow import configuration, models
from airflow.providers.tableau.hooks.tableau import TableauHook
from airflow.utils import db


class TestTableauHook(unittest.TestCase):
    """
    Test class for TableauHook
    """

    def setUp(self):
        """
        setup
        """
        configuration.conf.load_test_config()

        db.merge_conn(
            models.Connection(
                conn_id='tableau_test_password',
                conn_type='tableau',
                host='tableau',
                login='user',
                password='password',
                extra='{"site_id": "my_site"}',
            )
        )
        db.merge_conn(
            models.Connection(
                conn_id='tableau_test_token',
                conn_type='tableau',
                host='tableau',
                extra='{"token_name": "my_token", "personal_access_token": "my_personal_access_token"}',
            )
        )

    @patch('airflow.providers.tableau.hooks.tableau.TableauAuth')
    @patch('airflow.providers.tableau.hooks.tableau.Server')
    def test_get_conn_auth_via_password_and_site_in_connection(self, mock_server, mock_tableau_auth):
        """
        Test get conn auth via password
        """
        with TableauHook(tableau_conn_id='tableau_test_password') as tableau_hook:
            mock_server.assert_called_once_with(tableau_hook.conn.host, use_server_version=True)
            mock_tableau_auth.assert_called_once_with(
                username=tableau_hook.conn.login,
                password=tableau_hook.conn.password,
                site_id=tableau_hook.conn.extra_dejson['site_id'],
            )
            mock_server.return_value.auth.sign_in.assert_called_once_with(mock_tableau_auth.return_value)
        mock_server.return_value.auth.sign_out.assert_called_once_with()

    @patch('airflow.providers.tableau.hooks.tableau.PersonalAccessTokenAuth')
    @patch('airflow.providers.tableau.hooks.tableau.Server')
    def test_get_conn_auth_via_token_and_site_in_init(self, mock_server, mock_tableau_auth):
        """
        Test get conn auth via token
        """
        with TableauHook(site_id='test', tableau_conn_id='tableau_test_token') as tableau_hook:
            mock_server.assert_called_once_with(tableau_hook.conn.host, use_server_version=True)
            mock_tableau_auth.assert_called_once_with(
                token_name=tableau_hook.conn.extra_dejson['token_name'],
                personal_access_token=tableau_hook.conn.extra_dejson['personal_access_token'],
                site_id=tableau_hook.site_id,
            )
            mock_server.return_value.auth.sign_in_with_personal_access_token.assert_called_once_with(
                mock_tableau_auth.return_value
            )
        mock_server.return_value.auth.sign_out.assert_called_once_with()

    @patch('airflow.providers.tableau.hooks.tableau.TableauAuth')
    @patch('airflow.providers.tableau.hooks.tableau.Server')
    @patch('airflow.providers.tableau.hooks.tableau.Pager', return_value=[1, 2, 3])
    def test_get_all(self, mock_pager, mock_server, mock_tableau_auth):  # pylint: disable=unused-argument
        """
        Test get all
        """
        with TableauHook(tableau_conn_id='tableau_test_password') as tableau_hook:
            jobs = tableau_hook.get_all(resource_name='jobs')
            assert jobs == mock_pager.return_value

        mock_pager.assert_called_once_with(mock_server.return_value.jobs.get)
