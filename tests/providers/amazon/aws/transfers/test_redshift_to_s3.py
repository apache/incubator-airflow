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
from unittest import mock

from boto3.session import Session
from parameterized import parameterized

from airflow.providers.amazon.aws.transfers.redshift_to_s3 import RedshiftToS3Operator
from airflow.providers.amazon.aws.utils.redshift import build_credentials_block
from tests.test_utils.asserts import assert_equal_ignore_multiple_spaces


class TestRedshiftToS3Transfer(unittest.TestCase):
    @parameterized.expand(
        [
            [True, "key/table_"],
            [False, "key"],
        ]
    )
    @mock.patch("boto3.session.Session")
    @mock.patch("airflow.providers.postgres.hooks.postgres.PostgresHook.run")
    def test_table_unloading(
        self,
        table_as_file_name,
        expected_s3_key,
        mock_run,
        mock_session,
    ):
        access_key = "aws_access_key_id"
        secret_key = "aws_secret_access_key"
        mock_session.return_value = Session(access_key, secret_key)
        mock_session.return_value.access_key = access_key
        mock_session.return_value.secret_key = secret_key
        mock_session.return_value.token = None
        schema = "schema"
        table = "table"
        s3_bucket = "bucket"
        s3_key = "key"
        unload_options = [
            'HEADER',
        ]

        op = RedshiftToS3Operator(
            schema=schema,
            table=table,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            unload_options=unload_options,
            include_header=True,
            redshift_conn_id="redshift_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
            table_as_file_name=table_as_file_name,
            dag=None,
        )

        op.execute(None)

        unload_options = '\n\t\t\t'.join(unload_options)
        select_query = f"SELECT * FROM {schema}.{table}"
        credentials_block = build_credentials_block(mock_session.return_value)

        unload_query = op._build_unload_query(
            credentials_block, select_query, expected_s3_key, unload_options
        )

        assert mock_run.call_count == 1
        assert access_key in unload_query
        assert secret_key in unload_query
        assert_equal_ignore_multiple_spaces(self, mock_run.call_args[0][0], unload_query)

    @parameterized.expand(
        [
            [True, "key/table_"],
            [False, "key"],
        ]
    )
    @mock.patch("boto3.session.Session")
    @mock.patch("airflow.providers.postgres.hooks.postgres.PostgresHook.run")
    def test_execute_sts_token(
        self,
        table_as_file_name,
        expected_s3_key,
        mock_run,
        mock_session,
    ):
        access_key = "ASIA_aws_access_key_id"
        secret_key = "aws_secret_access_key"
        token = "token"
        mock_session.return_value = Session(access_key, secret_key, token)
        mock_session.return_value.access_key = access_key
        mock_session.return_value.secret_key = secret_key
        mock_session.return_value.token = token
        schema = "schema"
        table = "table"
        s3_bucket = "bucket"
        s3_key = "key"
        unload_options = [
            'HEADER',
        ]

        op = RedshiftToS3Operator(
            schema=schema,
            table=table,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            unload_options=unload_options,
            include_header=True,
            redshift_conn_id="redshift_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
            table_as_file_name=table_as_file_name,
            dag=None,
        )

        op.execute(None)

        unload_options = '\n\t\t\t'.join(unload_options)
        select_query = f"SELECT * FROM {schema}.{table}"
        credentials_block = build_credentials_block(mock_session.return_value)

        unload_query = op._build_unload_query(
            credentials_block, select_query, expected_s3_key, unload_options
        )

        assert mock_run.call_count == 1
        assert access_key in unload_query
        assert secret_key in unload_query
        assert token in unload_query
        assert_equal_ignore_multiple_spaces(self, mock_run.call_args[0][0], unload_query)

    @parameterized.expand(
        [
            ["table", True, "key/table_"],
            ["table", False, "key"],
            [None, False, "key"],
            [None, True, "key"],
        ]
    )
    @mock.patch("boto3.session.Session")
    @mock.patch("airflow.providers.postgres.hooks.postgres.PostgresHook.run")
    def test_custom_select_query_unloading(
        self,
        table,
        table_as_file_name,
        expected_s3_key,
        mock_run,
        mock_session,
    ):
        access_key = "aws_access_key_id"
        secret_key = "aws_secret_access_key"
        mock_session.return_value = Session(access_key, secret_key)
        mock_session.return_value.access_key = access_key
        mock_session.return_value.secret_key = secret_key
        mock_session.return_value.token = None
        s3_bucket = "bucket"
        s3_key = "key"
        unload_options = [
            'HEADER',
        ]
        select_query = "select column from table"

        op = RedshiftToS3Operator(
            select_query=select_query,
            table=table,
            table_as_file_name=table_as_file_name,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            unload_options=unload_options,
            include_header=True,
            redshift_conn_id="redshift_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
            dag=None,
        )

        op.execute(None)

        unload_options = '\n\t\t\t'.join(unload_options)
        credentials_block = build_credentials_block(mock_session.return_value)

        unload_query = op._build_unload_query(
            credentials_block, select_query, expected_s3_key, unload_options
        )

        assert mock_run.call_count == 1
        assert access_key in unload_query
        assert secret_key in unload_query
        assert_equal_ignore_multiple_spaces(self, mock_run.call_args[0][0], unload_query)

    def test_template_fields_overrides(self):
        assert RedshiftToS3Operator.template_fields == (
            's3_bucket',
            's3_key',
            'schema',
            'table',
            'unload_options',
        )
