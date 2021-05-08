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
"""MySQL to GCS operator."""

import base64
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Dict

from MySQLdb.constants import FIELD_TYPE

from airflow.providers.google.cloud.transfers.sql_to_gcs import BaseSQLToGCSOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook


class MySQLToGCSOperator(BaseSQLToGCSOperator):
    """Copy data from MySQL to Google Cloud Storage in JSON or CSV format.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:MySQLToGCSOperator`

    :param mysql_conn_id: Reference to :ref:`mysql connection id <howto/connection:mysql>`.
    :type mysql_conn_id: str
    :param ensure_utc: Ensure TIMESTAMP columns exported as UTC. If set to
        `False`, TIMESTAMP columns will be exported using the MySQL server's
        default timezone.
    :type ensure_utc: bool
    """

    ui_color = '#a0e08c'

    type_map = {
        FIELD_TYPE.BIT: 'INTEGER',
        FIELD_TYPE.DATETIME: 'TIMESTAMP',
        FIELD_TYPE.DATE: 'TIMESTAMP',
        FIELD_TYPE.DECIMAL: 'FLOAT',
        FIELD_TYPE.NEWDECIMAL: 'FLOAT',
        FIELD_TYPE.DOUBLE: 'FLOAT',
        FIELD_TYPE.FLOAT: 'FLOAT',
        FIELD_TYPE.INT24: 'INTEGER',
        FIELD_TYPE.LONG: 'INTEGER',
        FIELD_TYPE.LONGLONG: 'INTEGER',
        FIELD_TYPE.SHORT: 'INTEGER',
        FIELD_TYPE.TIME: 'TIME',
        FIELD_TYPE.TIMESTAMP: 'TIMESTAMP',
        FIELD_TYPE.TINY: 'INTEGER',
        FIELD_TYPE.YEAR: 'INTEGER',
    }

    def __init__(self, *, mysql_conn_id='mysql_default', ensure_utc=False, **kwargs):
        super().__init__(**kwargs)
        self.mysql_conn_id = mysql_conn_id
        self.ensure_utc = ensure_utc

    def query(self):
        """Queries mysql and returns a cursor to the results."""
        mysql = MySqlHook(mysql_conn_id=self.mysql_conn_id)
        conn = mysql.get_conn()
        cursor = conn.cursor()
        if self.ensure_utc:
            # Ensure TIMESTAMP results are in UTC
            tz_query = "SET time_zone = '+00:00'"
            self.log.info('Executing: %s', tz_query)
            cursor.execute(tz_query)
        self.log.info('Executing: %s', self.sql)
        cursor.execute(self.sql)
        return cursor

    def field_to_bigquery(self, field) -> Dict[str, str]:
        field_type = self.type_map.get(field[1], "STRING")
        # Always allow TIMESTAMP to be nullable. MySQLdb returns None types
        # for required fields because some MySQL timestamps can't be
        # represented by Python's datetime (e.g. 0000-00-00 00:00:00).
        field_mode = "NULLABLE" if field[6] or field_type == "TIMESTAMP" else "REQUIRED"
        return {
            'name': field[0],
            'type': field_type,
            'mode': field_mode,
        }

    def convert_type(self, value, schema_type: str):
        """
        Takes a value from MySQLdb, and converts it to a value that's safe for
        JSON/Google Cloud Storage/BigQuery.

        * Datetimes are converted to `str(value)` (`datetime.isoformat(' ')`)
          strings.
        * Times are converted to `str((datetime.min + value).time())` strings.
        * Decimals are converted to floats.
        * Dates are converted to ISO formatted strings if given schema_type is
          DATE, or `datetime.isoformat(' ')` strings otherwise.
        * Binary type fields are converted to integer if given schema_type is
          INTEGER, or encoded with base64 otherwise. Imported BYTES data must
          be base64-encoded according to BigQuery documentation:
          https://cloud.google.com/bigquery/data-types

        :param value: MySQLdb column value
        :type value: Any
        :param schema_type: BigQuery data type
        :type schema_type: str
        """
        if value is None:
            return value
        if isinstance(value, datetime):
            value = str(value)
        elif isinstance(value, timedelta):
            value = str((datetime.min + value).time())
        elif isinstance(value, Decimal):
            value = float(value)
        elif isinstance(value, date):
            if schema_type == "DATE":
                value = value.isoformat()
            else:
                value = str(datetime.combine(value, time.min))
        elif isinstance(value, bytes):
            if schema_type == "INTEGER":
                value = int.from_bytes(value, "big")
            else:
                value = base64.standard_b64encode(value).decode('ascii')
        return value
