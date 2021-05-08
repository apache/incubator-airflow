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
# pylint: disable=no-member

import unittest
from collections import OrderedDict
from unittest.mock import Mock, PropertyMock, patch

from airflow import PY38

if PY38:
    MsSqlToHiveTransferOperator: None = None
else:
    from airflow.providers.apache.hive.transfers.mssql_to_hive import MsSqlToHiveOperator

try:
    import pymssql
except ImportError:
    pymssql = None


@unittest.skipIf(PY38, "Mssql package not available when Python >= 3.8.")
@unittest.skipIf(pymssql is None, 'pymssql package not present')
class TestMsSqlToHiveTransfer(unittest.TestCase):
    def setUp(self):
        self.kwargs = dict(sql='sql', hive_table='table', task_id='test_mssql_to_hive', dag=None)

    def test_type_map_binary(self):
        # pylint: disable=c-extension-no-member, no-member
        mapped_type = MsSqlToHiveOperator(**self.kwargs).type_map(pymssql.BINARY.value)

        assert mapped_type == 'INT'

    def test_type_map_decimal(self):
        # pylint: disable=c-extension-no-member, no-member
        mapped_type = MsSqlToHiveOperator(**self.kwargs).type_map(pymssql.DECIMAL.value)

        assert mapped_type == 'FLOAT'

    def test_type_map_number(self):
        # pylint: disable=c-extension-no-member, no-member
        mapped_type = MsSqlToHiveOperator(**self.kwargs).type_map(pymssql.NUMBER.value)

        assert mapped_type == 'INT'

    def test_type_map_string(self):
        mapped_type = MsSqlToHiveOperator(**self.kwargs).type_map(None)

        assert mapped_type == 'STRING'

    @patch('airflow.providers.apache.hive.transfers.mssql_to_hive.csv')
    @patch('airflow.providers.apache.hive.transfers.mssql_to_hive.NamedTemporaryFile')
    @patch('airflow.providers.apache.hive.transfers.mssql_to_hive.MsSqlHook')
    @patch('airflow.providers.apache.hive.transfers.mssql_to_hive.HiveCliHook')
    def test_execute(self, mock_hive_hook, mock_mssql_hook, mock_tmp_file, mock_csv):
        type(mock_tmp_file).name = PropertyMock(return_value='tmp_file')
        mock_tmp_file.return_value.__enter__ = Mock(return_value=mock_tmp_file)
        mock_mssql_hook_get_conn = mock_mssql_hook.return_value.get_conn.return_value.__enter__
        mock_mssql_hook_cursor = mock_mssql_hook_get_conn.return_value.cursor.return_value.__enter__
        mock_mssql_hook_cursor.return_value.description = [('te', 'st')]

        mssql_to_hive_transfer = MsSqlToHiveOperator(**self.kwargs)
        mssql_to_hive_transfer.execute(context={})

        mock_mssql_hook_cursor.return_value.execute.assert_called_once_with(mssql_to_hive_transfer.sql)
        mock_csv.writer.assert_called_once_with(
            mock_tmp_file, delimiter=mssql_to_hive_transfer.delimiter, encoding='utf-8'
        )
        field_dict = OrderedDict()
        for field in mock_mssql_hook_cursor.return_value.description:
            field_dict[field[0]] = mssql_to_hive_transfer.type_map(field[1])
        mock_csv.writer.return_value.writerows.assert_called_once_with(mock_mssql_hook_cursor.return_value)
        mock_hive_hook.return_value.load_file.assert_called_once_with(
            mock_tmp_file.name,
            mssql_to_hive_transfer.hive_table,
            field_dict=field_dict,
            create=mssql_to_hive_transfer.create,
            partition=mssql_to_hive_transfer.partition,
            delimiter=mssql_to_hive_transfer.delimiter,
            recreate=mssql_to_hive_transfer.recreate,
            tblproperties=mssql_to_hive_transfer.tblproperties,
        )

    @patch('airflow.providers.apache.hive.transfers.mssql_to_hive.csv')
    @patch('airflow.providers.apache.hive.transfers.mssql_to_hive.NamedTemporaryFile')
    @patch('airflow.providers.apache.hive.transfers.mssql_to_hive.MsSqlHook')
    @patch('airflow.providers.apache.hive.transfers.mssql_to_hive.HiveCliHook')
    def test_execute_empty_description_field(self, mock_hive_hook, mock_mssql_hook, mock_tmp_file, mock_csv):
        type(mock_tmp_file).name = PropertyMock(return_value='tmp_file')
        mock_tmp_file.return_value.__enter__ = Mock(return_value=mock_tmp_file)
        mock_mssql_hook_get_conn = mock_mssql_hook.return_value.get_conn.return_value.__enter__
        mock_mssql_hook_cursor = mock_mssql_hook_get_conn.return_value.cursor.return_value.__enter__
        mock_mssql_hook_cursor.return_value.description = [('', '')]

        mssql_to_hive_transfer = MsSqlToHiveOperator(**self.kwargs)
        mssql_to_hive_transfer.execute(context={})

        field_dict = OrderedDict()
        col_count = 0
        for field in mock_mssql_hook_cursor.return_value.description:
            col_count += 1
            col_position = f"Column{col_count}"
            field_dict[col_position] = mssql_to_hive_transfer.type_map(field[1])
        mock_hive_hook.return_value.load_file.assert_called_once_with(
            mock_tmp_file.name,
            mssql_to_hive_transfer.hive_table,
            field_dict=field_dict,
            create=mssql_to_hive_transfer.create,
            partition=mssql_to_hive_transfer.partition,
            delimiter=mssql_to_hive_transfer.delimiter,
            recreate=mssql_to_hive_transfer.recreate,
            tblproperties=mssql_to_hive_transfer.tblproperties,
        )
