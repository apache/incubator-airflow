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
#

import unittest
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from numpy import nan
from simple_salesforce import Salesforce

from airflow.models.connection import Connection
from airflow.providers.salesforce.hooks.salesforce import SalesforceHook


class TestSalesforceHook(unittest.TestCase):
    def setUp(self):
        self.salesforce_hook = SalesforceHook(conn_id="conn_id")

    def test_get_conn_exists(self):
        self.salesforce_hook.conn = Mock(spec=Salesforce)

        self.salesforce_hook.get_conn()

        assert self.salesforce_hook.conn.return_value is not None

    @patch(
        "airflow.providers.salesforce.hooks.salesforce.SalesforceHook.get_connection",
        return_value=Connection(
            login="username", password="password", extra='{"security_token": "token", "domain": "test"}'
        ),
    )
    @patch("airflow.providers.salesforce.hooks.salesforce.Salesforce")
    def test_get_conn(self, mock_salesforce, mock_get_connection):
        self.salesforce_hook.get_conn()

        assert self.salesforce_hook.conn == mock_salesforce.return_value
        mock_salesforce.assert_called_once_with(
            username=mock_get_connection.return_value.login,
            password=mock_get_connection.return_value.password,
            security_token=mock_get_connection.return_value.extra_dejson["security_token"],
            instance_url=mock_get_connection.return_value.host,
            domain=mock_get_connection.return_value.extra_dejson.get("domain"),
        )

    @patch("airflow.providers.salesforce.hooks.salesforce.Salesforce")
    def test_make_query(self, mock_salesforce):
        mock_salesforce.return_value.query_all.return_value = dict(totalSize=123, done=True)
        self.salesforce_hook.conn = mock_salesforce.return_value
        query = "SELECT * FROM table"

        query_results = self.salesforce_hook.make_query(query, include_deleted=True)

        mock_salesforce.return_value.query_all.assert_called_once_with(query, include_deleted=True)
        assert query_results == mock_salesforce.return_value.query_all.return_value

    @patch("airflow.providers.salesforce.hooks.salesforce.Salesforce")
    def test_describe_object(self, mock_salesforce):
        obj = "obj_name"
        mock_salesforce.return_value.__setattr__(obj, Mock(spec=Salesforce))
        self.salesforce_hook.conn = mock_salesforce.return_value

        obj_description = self.salesforce_hook.describe_object(obj)

        mock_salesforce.return_value.__getattr__(obj).describe.assert_called_once_with()
        assert obj_description == mock_salesforce.return_value.__getattr__(obj).describe.return_value

    @patch("airflow.providers.salesforce.hooks.salesforce.SalesforceHook.get_conn")
    @patch(
        "airflow.providers.salesforce.hooks.salesforce.SalesforceHook.describe_object",
        return_value={"fields": [{"name": "field_1"}, {"name": "field_2"}]},
    )
    def test_get_available_fields(self, mock_describe_object, mock_get_conn):
        obj = "obj_name"

        available_fields = self.salesforce_hook.get_available_fields(obj)

        mock_get_conn.assert_called_once_with()
        mock_describe_object.assert_called_once_with(obj)
        assert available_fields == ["field_1", "field_2"]

    @patch("airflow.providers.salesforce.hooks.salesforce.SalesforceHook.make_query")
    def test_get_object_from_salesforce(self, mock_make_query):
        salesforce_objects = self.salesforce_hook.get_object_from_salesforce(
            obj="obj_name", fields=["field_1", "field_2"]
        )

        mock_make_query.assert_called_once_with("SELECT field_1,field_2 FROM obj_name")
        assert salesforce_objects == mock_make_query.return_value

    def test_write_object_to_file_invalid_format(self):
        with pytest.raises(ValueError):
            self.salesforce_hook.write_object_to_file(query_results=[], filename="test", fmt="test")

    @patch(
        "airflow.providers.salesforce.hooks.salesforce.pd.DataFrame.from_records",
        return_value=pd.DataFrame({"test": [1, 2, 3], "dict": [nan, nan, {"foo": "bar"}]}),
    )
    def test_write_object_to_file_csv(self, mock_data_frame):
        mock_data_frame.return_value.to_csv = Mock()
        filename = "test"

        data_frame = self.salesforce_hook.write_object_to_file(query_results=[], filename=filename, fmt="csv")

        mock_data_frame.return_value.to_csv.assert_called_once_with(filename, index=False)
        # Note that the latest version of pandas dataframes (1.1.2) returns "nan" rather than "None" here
        pd.testing.assert_frame_equal(
            data_frame,
            pd.DataFrame({"test": [1, 2, 3], "dict": ["nan", "nan", str({"foo": "bar"})]}),
            check_index_type=False,
        )

    @patch(
        "airflow.providers.salesforce.hooks.salesforce.SalesforceHook.describe_object",
        return_value={"fields": [{"name": "field_1", "type": "date"}]},
    )
    @patch(
        "airflow.providers.salesforce.hooks.salesforce.pd.DataFrame.from_records",
        return_value=pd.DataFrame({"test": [1, 2, 3], "field_1": ["2019-01-01", "2019-01-02", "2019-01-03"]}),
    )
    def test_write_object_to_file_json_with_timestamp_conversion(self, mock_data_frame, mock_describe_object):
        mock_data_frame.return_value.to_json = Mock()
        filename = "test"
        obj_name = "obj_name"

        data_frame = self.salesforce_hook.write_object_to_file(
            query_results=[{"attributes": {"type": obj_name}}],
            filename=filename,
            fmt="json",
            coerce_to_timestamp=True,
        )

        mock_describe_object.assert_called_once_with(obj_name)
        mock_data_frame.return_value.to_json.assert_called_once_with(filename, "records", date_unit="s")
        pd.testing.assert_frame_equal(
            data_frame, pd.DataFrame({"test": [1, 2, 3], "field_1": [1.546301e09, 1.546387e09, 1.546474e09]})
        )

    @patch("airflow.providers.salesforce.hooks.salesforce.time.time", return_value=1.23)
    @patch(
        "airflow.providers.salesforce.hooks.salesforce.pd.DataFrame.from_records",
        return_value=pd.DataFrame({"test": [1, 2, 3]}),
    )
    def test_write_object_to_file_ndjson_with_record_time(self, mock_data_frame, mock_time):
        mock_data_frame.return_value.to_json = Mock()
        filename = "test"

        data_frame = self.salesforce_hook.write_object_to_file(
            query_results=[], filename=filename, fmt="ndjson", record_time_added=True
        )

        mock_data_frame.return_value.to_json.assert_called_once_with(
            filename, "records", lines=True, date_unit="s"
        )
        pd.testing.assert_frame_equal(
            data_frame,
            pd.DataFrame(
                {
                    "test": [1, 2, 3],
                    "time_fetched_from_salesforce": [
                        mock_time.return_value,
                        mock_time.return_value,
                        mock_time.return_value,
                    ],
                }
            ),
        )

    @patch(
        "airflow.providers.salesforce.hooks.salesforce.SalesforceHook.describe_object",
        return_value={"fields": [{"name": "field_1", "type": "date"}]},
    )
    @patch(
        "airflow.providers.salesforce.hooks.salesforce.pd.DataFrame.from_records",
        return_value=pd.DataFrame({"test": [1, 2, 3], "field_1": ["2019-01-01", "2019-01-02", "2019-01-03"]}),
    )
    def test_object_to_df_with_timestamp_conversion(self, mock_data_frame, mock_describe_object):
        obj_name = "obj_name"

        data_frame = self.salesforce_hook.object_to_df(
            query_results=[{"attributes": {"type": obj_name}}],
            coerce_to_timestamp=True,
        )

        mock_describe_object.assert_called_once_with(obj_name)
        pd.testing.assert_frame_equal(
            data_frame, pd.DataFrame({"test": [1, 2, 3], "field_1": [1.546301e09, 1.546387e09, 1.546474e09]})
        )

    @patch("airflow.providers.salesforce.hooks.salesforce.time.time", return_value=1.23)
    @patch(
        "airflow.providers.salesforce.hooks.salesforce.pd.DataFrame.from_records",
        return_value=pd.DataFrame({"test": [1, 2, 3]}),
    )
    def test_object_to_df_with_record_time(self, mock_data_frame, mock_time):
        data_frame = self.salesforce_hook.object_to_df(query_results=[], record_time_added=True)

        pd.testing.assert_frame_equal(
            data_frame,
            pd.DataFrame(
                {
                    "test": [1, 2, 3],
                    "time_fetched_from_salesforce": [
                        mock_time.return_value,
                        mock_time.return_value,
                        mock_time.return_value,
                    ],
                }
            ),
        )
