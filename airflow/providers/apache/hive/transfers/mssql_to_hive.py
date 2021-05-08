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
"""This module contains operator to move data from MSSQL to Hive."""

from collections import OrderedDict
from tempfile import NamedTemporaryFile
from typing import Dict, Optional

import pymssql
import unicodecsv as csv

from airflow.models import BaseOperator
from airflow.providers.apache.hive.hooks.hive import HiveCliHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


class MsSqlToHiveOperator(BaseOperator):
    """
    Moves data from Microsoft SQL Server to Hive. The operator runs
    your query against Microsoft SQL Server, stores the file locally
    before loading it into a Hive table. If the ``create`` or
    ``recreate`` arguments are set to ``True``,
    a ``CREATE TABLE`` and ``DROP TABLE`` statements are generated.
    Hive data types are inferred from the cursor's metadata.
    Note that the table generated in Hive uses ``STORED AS textfile``
    which isn't the most efficient serialization format. If a
    large amount of data is loaded and/or if the table gets
    queried considerably, you may want to use this operator only to
    stage the data into a temporary table before loading it into its
    final destination using a ``HiveOperator``.

    :param sql: SQL query to execute against the Microsoft SQL Server
        database. (templated)
    :type sql: str
    :param hive_table: target Hive table, use dot notation to target a specific
        database. (templated)
    :type hive_table: str
    :param create: whether to create the table if it doesn't exist
    :type create: bool
    :param recreate: whether to drop and recreate the table at every execution
    :type recreate: bool
    :param partition: target partition as a dict of partition columns and
        values. (templated)
    :type partition: dict
    :param delimiter: field delimiter in the file
    :type delimiter: str
    :param mssql_conn_id: source Microsoft SQL Server connection
    :type mssql_conn_id: str
    :param hive_cli_conn_id: Reference to the
        :ref:`Hive CLI connection id <howto/connection:hive_cli>`.
    :type hive_cli_conn_id: str
    :param tblproperties: TBLPROPERTIES of the hive table being created
    :type tblproperties: dict
    """

    template_fields = ('sql', 'partition', 'hive_table')
    template_ext = ('.sql',)
    ui_color = '#a0e08c'

    def __init__(
        self,
        *,
        sql: str,
        hive_table: str,
        create: bool = True,
        recreate: bool = False,
        partition: Optional[Dict] = None,
        delimiter: str = chr(1),
        mssql_conn_id: str = 'mssql_default',
        hive_cli_conn_id: str = 'hive_cli_default',
        tblproperties: Optional[Dict] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.hive_table = hive_table
        self.partition = partition
        self.create = create
        self.recreate = recreate
        self.delimiter = delimiter
        self.mssql_conn_id = mssql_conn_id
        self.hive_cli_conn_id = hive_cli_conn_id
        self.partition = partition or {}
        self.tblproperties = tblproperties

    @classmethod
    # pylint: disable=c-extension-no-member,no-member
    def type_map(cls, mssql_type: int) -> str:
        """Maps MsSQL type to Hive type."""
        map_dict = {
            pymssql.BINARY.value: 'INT',
            pymssql.DECIMAL.value: 'FLOAT',
            pymssql.NUMBER.value: 'INT',
        }
        return map_dict.get(mssql_type, 'STRING')

    def execute(self, context: Dict[str, str]):
        mssql = MsSqlHook(mssql_conn_id=self.mssql_conn_id)
        self.log.info("Dumping Microsoft SQL Server query results to local file")
        with mssql.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(self.sql)
                with NamedTemporaryFile("w") as tmp_file:
                    csv_writer = csv.writer(tmp_file, delimiter=self.delimiter, encoding='utf-8')
                    field_dict = OrderedDict()
                    col_count = 0
                    for field in cursor.description:
                        col_count += 1
                        col_position = f"Column{col_count}"
                        field_dict[col_position if field[0] == '' else field[0]] = self.type_map(field[1])
                    csv_writer.writerows(cursor)
                    tmp_file.flush()

            hive = HiveCliHook(hive_cli_conn_id=self.hive_cli_conn_id)
            self.log.info("Loading file into Hive")
            hive.load_file(
                tmp_file.name,
                self.hive_table,
                field_dict=field_dict,
                create=self.create,
                partition=self.partition,
                delimiter=self.delimiter,
                recreate=self.recreate,
                tblproperties=self.tblproperties,
            )
