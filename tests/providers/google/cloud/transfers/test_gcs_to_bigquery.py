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

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

TASK_ID = 'test-gcs-to-bq-operator'
TEST_EXPLICIT_DEST = 'test-project.dataset.table'
TEST_BUCKET = 'test-bucket'
MAX_ID_KEY = 'id'
TEST_SOURCE_OBJECTS = ['test/objects/*']
LABELS = {'k1': 'v1'}
DESCRIPTION = "Test Description"


class TestGoogleCloudStorageToBigQueryOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.transfers.gcs_to_bigquery.BigQueryHook')
    def test_execute_explicit_project_legacy(self, bq_hook):
        operator = GCSToBigQueryOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            max_id_key=MAX_ID_KEY,
        )

        # using legacy SQL
        bq_hook.return_value.get_conn.return_value.cursor.return_value.use_legacy_sql = True

        operator.execute(None)

        bq_hook.return_value.get_conn.return_value.cursor.return_value.execute.assert_called_once_with(
            "SELECT MAX(id) FROM [test-project.dataset.table]"
        )

    @mock.patch('airflow.providers.google.cloud.transfers.gcs_to_bigquery.BigQueryHook')
    def test_execute_explicit_project(self, bq_hook):
        operator = GCSToBigQueryOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            max_id_key=MAX_ID_KEY,
        )

        # using non-legacy SQL
        bq_hook.return_value.get_conn.return_value.cursor.return_value.use_legacy_sql = False

        operator.execute(None)

        bq_hook.return_value.get_conn.return_value.cursor.return_value.execute.assert_called_once_with(
            "SELECT MAX(id) FROM `test-project.dataset.table`"
        )

    @mock.patch('airflow.providers.google.cloud.transfers.gcs_to_bigquery.BigQueryHook')
    def test_labels(self, bq_hook):

        operator = GCSToBigQueryOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            labels=LABELS,
        )

        operator.execute(None)

        bq_hook.return_value.get_conn.return_value.cursor.return_value.run_load.assert_called_once_with(
            destination_project_dataset_table=mock.ANY,
            schema_fields=mock.ANY,
            source_uris=mock.ANY,
            source_format=mock.ANY,
            autodetect=mock.ANY,
            create_disposition=mock.ANY,
            skip_leading_rows=mock.ANY,
            write_disposition=mock.ANY,
            field_delimiter=mock.ANY,
            max_bad_records=mock.ANY,
            quote_character=mock.ANY,
            ignore_unknown_values=mock.ANY,
            allow_quoted_newlines=mock.ANY,
            allow_jagged_rows=mock.ANY,
            encoding=mock.ANY,
            schema_update_options=mock.ANY,
            src_fmt_configs=mock.ANY,
            time_partitioning=mock.ANY,
            cluster_fields=mock.ANY,
            encryption_configuration=mock.ANY,
            labels=LABELS,
            description=mock.ANY,
        )

    @mock.patch('airflow.providers.google.cloud.transfers.gcs_to_bigquery.BigQueryHook')
    def test_description(self, bq_hook):

        operator = GCSToBigQueryOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            description=DESCRIPTION,
        )

        operator.execute(None)

        bq_hook.return_value.get_conn.return_value.cursor.return_value.run_load.assert_called_once_with(
            destination_project_dataset_table=mock.ANY,
            schema_fields=mock.ANY,
            source_uris=mock.ANY,
            source_format=mock.ANY,
            autodetect=mock.ANY,
            create_disposition=mock.ANY,
            skip_leading_rows=mock.ANY,
            write_disposition=mock.ANY,
            field_delimiter=mock.ANY,
            max_bad_records=mock.ANY,
            quote_character=mock.ANY,
            ignore_unknown_values=mock.ANY,
            allow_quoted_newlines=mock.ANY,
            allow_jagged_rows=mock.ANY,
            encoding=mock.ANY,
            schema_update_options=mock.ANY,
            src_fmt_configs=mock.ANY,
            time_partitioning=mock.ANY,
            cluster_fields=mock.ANY,
            encryption_configuration=mock.ANY,
            labels=mock.ANY,
            description=DESCRIPTION,
        )

    @mock.patch('airflow.providers.google.cloud.transfers.gcs_to_bigquery.BigQueryHook')
    def test_labels_external_table(self, bq_hook):

        operator = GCSToBigQueryOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            labels=LABELS,
            external_table=True,
        )

        operator.execute(None)
        # fmt: off
        bq_hook.return_value.get_conn.return_value.cursor.return_value.create_external_table. \
            assert_called_once_with(
                external_project_dataset_table=mock.ANY,
                schema_fields=mock.ANY,
                source_uris=mock.ANY,
                source_format=mock.ANY,
                compression=mock.ANY,
                skip_leading_rows=mock.ANY,
                field_delimiter=mock.ANY,
                max_bad_records=mock.ANY,
                quote_character=mock.ANY,
                ignore_unknown_values=mock.ANY,
                allow_quoted_newlines=mock.ANY,
                allow_jagged_rows=mock.ANY,
                encoding=mock.ANY,
                src_fmt_configs=mock.ANY,
                encryption_configuration=mock.ANY,
                labels=LABELS,
                description=mock.ANY,
            )
        # fmt: on

    @mock.patch('airflow.providers.google.cloud.transfers.gcs_to_bigquery.BigQueryHook')
    def test_description_external_table(self, bq_hook):

        operator = GCSToBigQueryOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            description=DESCRIPTION,
            external_table=True,
        )

        operator.execute(None)
        # fmt: off
        bq_hook.return_value.get_conn.return_value.cursor.return_value.create_external_table. \
            assert_called_once_with(
                external_project_dataset_table=mock.ANY,
                schema_fields=mock.ANY,
                source_uris=mock.ANY,
                source_format=mock.ANY,
                compression=mock.ANY,
                skip_leading_rows=mock.ANY,
                field_delimiter=mock.ANY,
                max_bad_records=mock.ANY,
                quote_character=mock.ANY,
                ignore_unknown_values=mock.ANY,
                allow_quoted_newlines=mock.ANY,
                allow_jagged_rows=mock.ANY,
                encoding=mock.ANY,
                src_fmt_configs=mock.ANY,
                encryption_configuration=mock.ANY,
                labels=mock.ANY,
                description=DESCRIPTION,
            )
        # fmt: on
