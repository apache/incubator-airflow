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
"""Transfers data from Snowflake into a S3 Bucket."""
from typing import List, Optional

from airflow.models import BaseOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.decorators import apply_defaults


class SnowflakeToS3Operator(BaseOperator):
    """
    UNLOAD a query or a table from Snowflake to S3. It is not necessary a conn to S3 because
    Snowflake handles it. You have to follow one of the setups in:
    https://docs.snowflake.com/en/user-guide/data-load-s3-config.html

    :param stage: Reference to a specific Snowflake stage to copy the data into it. This allows you to use
        the method 'Configuring AWS IAM User Credentials' for unloading data to s3. If this is passed,
        s3_bucket can't be used and file_format is not necessary
    :type stage: str
    :param warehouse: reference to a specific snowflake warehouse to override the one in the conn
    :type warehouse: str
    :param database: reference to a specific snowflake database to override the one in the conn
    :type warehouse: str
    :param s3_bucket: reference to a specific S3 bucket where the data will be saved. For using it, you
        should have done the one-time setup 'Configuring a Snowflake Storage Integration'
    :type s3_bucket: str
    :param s3_key: reference to a specific S3 key within a bucket or stage.
    :type s3_key: str
    :param file_format: can be either a previous file format created in Snowflake
        or hardcoded one like ``type = csv field_delimiter = ',' skip_header = 1``
    :type file_format: str
    :param query_or_table: query or full table to unload. If table, it must include the schema like
        `schema.table`
    :type query_or_table: str
    :param snowflake_conn_id: reference to a specific snowflake connection
    :type snowflake_conn_id: str
    :param unload_options: reference to a list of UNLOAD options (SINGLE, MAX_FILE_SIZE,
        OVERWRITE etc). Each element of the list has to be a string
    :type unload_options: list
    :param role: name of role (will overwrite any role defined in
        connection's extra JSON)
    :type role: str
    :param authenticator: authenticator for Snowflake.
        'snowflake' (default) to use the internal Snowflake authenticator
        'externalbrowser' to authenticate using your web browser and
        Okta, ADFS or any other SAML 2.0-compliant identify provider
        (IdP) that has been defined for your account
        'https://<your_okta_account_name>.okta.com' to authenticate
        through native Okta.
    :type authenticator: str
    :param session_parameters: You can set session-level parameters at
        the time you connect to Snowflake
    :type session_parameters: dict
    """

    template_fields = (
        's3_key',
        's3_bucket',
        'table_or_query',
        'snowflake_conn_id',
    )
    template_ext = ('.sql',)
    ui_color = '#ffebb2'

    @apply_defaults
    def __init__(
        self,
        *,
        stage: Optional[str] = None,
        s3_bucket: Optional[str],
        s3_key: str,
        query_or_table: str,
        file_format: Optional[str],
        database: Optional[str] = None,
        warehouse: Optional[str] = None,
        snowflake_conn_id: str = "snowflake_default",
        unload_options: Optional[list] = None,
        autocommit: bool = True,
        include_header: bool = False,
        role: Optional[str] = None,
        authenticator: Optional[str] = None,
        session_parameters: Optional[dict] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.stage = stage
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_format = file_format
        self.warehouse = warehouse
        self.database = database
        self.snowflake_conn_id = snowflake_conn_id
        self.query_or_table = query_or_table
        self.unload_options = unload_options or []  # type: List
        self.autocommit = autocommit
        self.include_header = include_header
        self.role = role
        self.authenticator = authenticator
        self.session_parameters = session_parameters

    def execute(self, context):
        snowflake_hook = SnowflakeHook(
            snowflake_conn_id=self.snowflake_conn_id,
            warehouse=self.warehouse,
            database=self.database,
            role=self.role,
            authenticator=self.authenticator,
            session_parameters=self.session_parameters,
        )
        unload_options = '\n\t'.join(self.unload_options)

        if self.stage:
            unload_query = f"""
            COPY INTO @{self.stage}/{self.s3_key}
            FROM ({self.query_or_table})
            """

            if self.file_format:
                unload_query += f'FILE_FORMAT = ({self.file_format})'

        else:
            unload_query = f"""
            COPY INTO 's3://{self.s3_bucket}/{self.s3_key}'
            FROM ({self.query_or_table})
            STORAGE_INTEGRATION = S3
            FILE_FORMAT = ({self.file_format})
            """

        unload_query += f"""{unload_options}
                            HEADER = {self.include_header};"""

        self.log.info('Executing UNLOAD command...')
        snowflake_hook.run(unload_query, self.autocommit)
        self.log.info("UNLOAD command complete...")
