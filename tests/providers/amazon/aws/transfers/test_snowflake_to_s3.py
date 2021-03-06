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

from airflow.providers.amazon.aws.transfers.snowflake_to_s3 import SnowflakeToS3Operator
from tests.test_utils.asserts import assert_equal_ignore_multiple_spaces


class TestSnowflakeToS3Transfer(unittest.TestCase):
    @mock.patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook.run")
    def test_execute(
        self,
        mock_run,
    ):

        query_or_table = "schema.table"
        s3_bucket = "bucket"
        s3_key = "key"
        unload_options = [
            'OVERWRITE = TRUE',
        ]
        file_format = "file_format"

        SnowflakeToS3Operator(
            query_or_table=query_or_table,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            file_format="file_format",
            unload_options=unload_options,
            include_header=True,
            task_id="task_id",
            dag=None,
        ).execute(None)

        unload_options = '\n\t'.join(unload_options)
        unload_query = f"""
        COPY INTO 's3://{s3_bucket}/{s3_key}'
        FROM ({query_or_table})
        STORAGE_INTEGRATION = S3
        FILE_FORMAT = ({file_format})
        {unload_options}
        HEADER = True;
        """

        assert mock_run.call_count == 1
        assert_equal_ignore_multiple_spaces(self, mock_run.call_args[0][0], unload_query)
