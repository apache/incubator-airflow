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
"""
Unit Tests for the GSheets Hook
"""

import unittest
from unittest import mock

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.google.suite.hooks.sheets import GSheetsHook
from tests.providers.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id

GCP_CONN_ID = 'test'
SPREADSHEET_ID = '1234567890'
RANGE_ = 'test!A:E'
RANGES = ['test!A:Q', 'test!R:Z']
VALUES = [[1, 2, 3]]
VALUES_BATCH = [[[1, 2, 3]], [[4, 5, 6]]]
MAJOR_DIMENSION = 'ROWS'
VALUE_RENDER_OPTION = 'FORMATTED_VALUE'
DATE_TIME_RENDER_OPTION = 'SERIAL_NUMBER'
INCLUDE_VALUES_IN_RESPONSE = True
VALUE_INPUT_OPTION = 'RAW'
INSERT_DATA_OPTION = 'OVERWRITE'
NUM_RETRIES = 5
API_RESPONSE = {'test': 'response'}


class TestGSheetsHook(unittest.TestCase):
    def setUp(self):
        with mock.patch(
            'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__',
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = GSheetsHook(gcp_conn_id=GCP_CONN_ID)

    @mock.patch("airflow.providers.google.suite.hooks.sheets.GSheetsHook._authorize")
    @mock.patch("airflow.providers.google.suite.hooks.sheets.build")
    def test_gsheets_client_creation(self, mock_build, mock_authorize):
        result = self.hook.get_conn()
        mock_build.assert_called_once_with(
            'sheets', 'v4', http=mock_authorize.return_value, cache_discovery=False
        )
        assert mock_build.return_value == result

    @mock.patch("airflow.providers.google.suite.hooks.sheets.GSheetsHook.get_conn")
    def test_get_values(self, get_conn):
        get_method = get_conn.return_value.spreadsheets.return_value.values.return_value.get
        execute_method = get_method.return_value.execute
        execute_method.return_value = {"values": VALUES}
        result = self.hook.get_values(
            spreadsheet_id=SPREADSHEET_ID,
            range_=RANGE_,
            major_dimension=MAJOR_DIMENSION,
            value_render_option=VALUE_RENDER_OPTION,
            date_time_render_option=DATE_TIME_RENDER_OPTION,
        )
        assert result is VALUES
        execute_method.assert_called_once_with(num_retries=NUM_RETRIES)
        get_method.assert_called_once_with(
            spreadsheetId=SPREADSHEET_ID,
            range=RANGE_,
            majorDimension=MAJOR_DIMENSION,
            valueRenderOption=VALUE_RENDER_OPTION,
            dateTimeRenderOption=DATE_TIME_RENDER_OPTION,
        )

    @mock.patch("airflow.providers.google.suite.hooks.sheets.GSheetsHook.get_conn")
    def test_batch_get_values(self, get_conn):
        batch_get_method = get_conn.return_value.spreadsheets.return_value.values.return_value.batchGet
        execute_method = batch_get_method.return_value.execute
        execute_method.return_value = API_RESPONSE
        result = self.hook.batch_get_values(
            spreadsheet_id=SPREADSHEET_ID,
            ranges=RANGES,
            major_dimension=MAJOR_DIMENSION,
            value_render_option=VALUE_RENDER_OPTION,
            date_time_render_option=DATE_TIME_RENDER_OPTION,
        )
        assert result is API_RESPONSE
        execute_method.assert_called_once_with(num_retries=NUM_RETRIES)
        batch_get_method.assert_called_once_with(
            spreadsheetId=SPREADSHEET_ID,
            ranges=RANGES,
            majorDimension=MAJOR_DIMENSION,
            valueRenderOption=VALUE_RENDER_OPTION,
            dateTimeRenderOption=DATE_TIME_RENDER_OPTION,
        )

    @mock.patch("airflow.providers.google.suite.hooks.sheets.GSheetsHook.get_conn")
    def test_update_values(self, get_conn):
        update_method = get_conn.return_value.spreadsheets.return_value.values.return_value.update
        execute_method = update_method.return_value.execute
        execute_method.return_value = API_RESPONSE
        result = self.hook.update_values(
            spreadsheet_id=SPREADSHEET_ID,
            range_=RANGE_,
            values=VALUES,
            major_dimension=MAJOR_DIMENSION,
            value_input_option=VALUE_INPUT_OPTION,
            include_values_in_response=INCLUDE_VALUES_IN_RESPONSE,
            value_render_option=VALUE_RENDER_OPTION,
            date_time_render_option=DATE_TIME_RENDER_OPTION,
        )
        body = {"range": RANGE_, "majorDimension": MAJOR_DIMENSION, "values": VALUES}
        assert result is API_RESPONSE
        execute_method.assert_called_once_with(num_retries=NUM_RETRIES)
        update_method.assert_called_once_with(
            spreadsheetId=SPREADSHEET_ID,
            range=RANGE_,
            valueInputOption=VALUE_INPUT_OPTION,
            includeValuesInResponse=INCLUDE_VALUES_IN_RESPONSE,
            responseValueRenderOption=VALUE_RENDER_OPTION,
            responseDateTimeRenderOption=DATE_TIME_RENDER_OPTION,
            body=body,
        )

    @mock.patch("airflow.providers.google.suite.hooks.sheets.GSheetsHook.get_conn")
    def test_batch_update_values(self, get_conn):
        batch_update_method = get_conn.return_value.spreadsheets.return_value.values.return_value.batchUpdate
        execute_method = batch_update_method.return_value.execute
        execute_method.return_value = API_RESPONSE
        result = self.hook.batch_update_values(
            spreadsheet_id=SPREADSHEET_ID,
            ranges=RANGES,
            values=VALUES_BATCH,
            major_dimension=MAJOR_DIMENSION,
            value_input_option=VALUE_INPUT_OPTION,
            include_values_in_response=INCLUDE_VALUES_IN_RESPONSE,
            value_render_option=VALUE_RENDER_OPTION,
            date_time_render_option=DATE_TIME_RENDER_OPTION,
        )
        data = []
        for idx, range_ in enumerate(RANGES):
            value_range = {"range": range_, "majorDimension": MAJOR_DIMENSION, "values": VALUES_BATCH[idx]}
            data.append(value_range)
        body = {
            "valueInputOption": VALUE_INPUT_OPTION,
            "data": data,
            "includeValuesInResponse": INCLUDE_VALUES_IN_RESPONSE,
            "responseValueRenderOption": VALUE_RENDER_OPTION,
            "responseDateTimeRenderOption": DATE_TIME_RENDER_OPTION,
        }
        assert result is API_RESPONSE
        execute_method.assert_called_once_with(num_retries=NUM_RETRIES)
        batch_update_method.assert_called_once_with(spreadsheetId=SPREADSHEET_ID, body=body)

    @mock.patch("airflow.providers.google.suite.hooks.sheets.GSheetsHook.get_conn")
    def test_batch_update_values_with_bad_data(self, get_conn):
        batch_update_method = get_conn.return_value.spreadsheets.return_value.values.return_value.batchUpdate
        execute_method = batch_update_method.return_value.execute
        execute_method.return_value = API_RESPONSE
        with pytest.raises(AirflowException) as ctx:
            self.hook.batch_update_values(
                spreadsheet_id=SPREADSHEET_ID,
                ranges=['test!A1:B2', 'test!C1:C2'],
                values=[[1, 2, 3]],  # bad data
                major_dimension=MAJOR_DIMENSION,
                value_input_option=VALUE_INPUT_OPTION,
                include_values_in_response=INCLUDE_VALUES_IN_RESPONSE,
                value_render_option=VALUE_RENDER_OPTION,
                date_time_render_option=DATE_TIME_RENDER_OPTION,
            )
        batch_update_method.assert_not_called()
        execute_method.assert_not_called()
        err = ctx.value
        assert "must be of equal length." in str(err)

    @mock.patch("airflow.providers.google.suite.hooks.sheets.GSheetsHook.get_conn")
    def test_append_values(self, get_conn):
        append_method = get_conn.return_value.spreadsheets.return_value.values.return_value.append
        execute_method = append_method.return_value.execute
        execute_method.return_value = API_RESPONSE
        result = self.hook.append_values(
            spreadsheet_id=SPREADSHEET_ID,
            range_=RANGE_,
            values=VALUES,
            major_dimension=MAJOR_DIMENSION,
            value_input_option=VALUE_INPUT_OPTION,
            insert_data_option=INSERT_DATA_OPTION,
            include_values_in_response=INCLUDE_VALUES_IN_RESPONSE,
            value_render_option=VALUE_RENDER_OPTION,
            date_time_render_option=DATE_TIME_RENDER_OPTION,
        )
        body = {"range": RANGE_, "majorDimension": MAJOR_DIMENSION, "values": VALUES}
        assert result is API_RESPONSE
        execute_method.assert_called_once_with(num_retries=NUM_RETRIES)
        append_method.assert_called_once_with(
            spreadsheetId=SPREADSHEET_ID,
            range=RANGE_,
            valueInputOption=VALUE_INPUT_OPTION,
            insertDataOption=INSERT_DATA_OPTION,
            includeValuesInResponse=INCLUDE_VALUES_IN_RESPONSE,
            responseValueRenderOption=VALUE_RENDER_OPTION,
            responseDateTimeRenderOption=DATE_TIME_RENDER_OPTION,
            body=body,
        )

    @mock.patch("airflow.providers.google.suite.hooks.sheets.GSheetsHook.get_conn")
    def test_clear_values(self, get_conn):
        clear_method = get_conn.return_value.spreadsheets.return_value.values.return_value.clear
        execute_method = clear_method.return_value.execute
        execute_method.return_value = API_RESPONSE
        result = self.hook.clear(spreadsheet_id=SPREADSHEET_ID, range_=RANGE_)

        assert result is API_RESPONSE
        execute_method.assert_called_once_with(num_retries=NUM_RETRIES)
        clear_method.assert_called_once_with(spreadsheetId=SPREADSHEET_ID, range=RANGE_)

    @mock.patch("airflow.providers.google.suite.hooks.sheets.GSheetsHook.get_conn")
    def test_batch_clear_values(self, get_conn):
        batch_clear_method = get_conn.return_value.spreadsheets.return_value.values.return_value.batchClear
        execute_method = batch_clear_method.return_value.execute
        execute_method.return_value = API_RESPONSE
        result = self.hook.batch_clear(spreadsheet_id=SPREADSHEET_ID, ranges=RANGES)
        body = {"ranges": RANGES}
        assert result is API_RESPONSE
        execute_method.assert_called_once_with(num_retries=NUM_RETRIES)
        batch_clear_method.assert_called_once_with(spreadsheetId=SPREADSHEET_ID, body=body)

    @mock.patch("airflow.providers.google.suite.hooks.sheets.GSheetsHook.get_conn")
    def test_get_spreadsheet(self, mock_get_conn):
        get_mock = mock_get_conn.return_value.spreadsheets.return_value.get
        get_mock.return_value.execute.return_value = API_RESPONSE

        result = self.hook.get_spreadsheet(spreadsheet_id=SPREADSHEET_ID)

        get_mock.assert_called_once_with(spreadsheetId=SPREADSHEET_ID)
        assert result == API_RESPONSE

    @mock.patch("airflow.providers.google.suite.hooks.sheets.GSheetsHook.get_spreadsheet")
    def test_get_sheet_titles(self, mock_get_spreadsheet):
        sheet1 = {"properties": {"title": "title1"}}
        sheet2 = {"properties": {"title": "title2"}}
        mock_get_spreadsheet.return_value = {"sheets": [sheet1, sheet2]}

        result = self.hook.get_sheet_titles(spreadsheet_id=SPREADSHEET_ID)
        mock_get_spreadsheet.assert_called_once_with(spreadsheet_id=SPREADSHEET_ID)
        assert result == ["title1", "title2"]

        result = self.hook.get_sheet_titles(spreadsheet_id=SPREADSHEET_ID, sheet_filter=["title1"])
        assert result == ["title1"]

    @mock.patch("airflow.providers.google.suite.hooks.sheets.GSheetsHook.get_conn")
    def test_create_spreadsheet(self, mock_get_conn):
        spreadsheet = mock.MagicMock()

        create_mock = mock_get_conn.return_value.spreadsheets.return_value.create
        create_mock.return_value.execute.return_value = API_RESPONSE

        result = self.hook.create_spreadsheet(spreadsheet=spreadsheet)

        create_mock.assert_called_once_with(body=spreadsheet)
        assert result == API_RESPONSE
