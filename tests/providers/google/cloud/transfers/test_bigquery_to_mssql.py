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
import unittest
from unittest import mock

from airflow.providers.google.cloud.transfers.bigquery_to_mssql import BigQueryToMsSqlOperator

TASK_ID = 'test-bq-create-table-operator'
TEST_DATASET = 'test-dataset'
TEST_TABLE_ID = 'test-table-id'
TEST_DAG_ID = 'test-bigquery-operators'


class TestBigQueryToMsSqlOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.transfers.bigquery_to_mssql.BigQueryHook')
    def test_execute_good_request_to_bq(self, mock_hook):
        destination_table = 'table'
        operator = BigQueryToMsSqlOperator(
            task_id=TASK_ID,
            dataset_table=f'{TEST_DATASET}.{TEST_TABLE_ID}',
            mssql_table=destination_table,
            replace=False,
        )

        operator.execute(None)
        # fmt: off
        mock_hook.return_value.get_conn.return_value.cursor.return_value.get_tabledata\
            .assert_called_once_with(
                dataset_id=TEST_DATASET,
                table_id=TEST_TABLE_ID,
                max_results=1000,
                selected_fields=None,
                start_index=0,
            )
        # fmt: on
