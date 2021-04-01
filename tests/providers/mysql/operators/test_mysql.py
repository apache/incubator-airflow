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
from contextlib import closing

import pytest
from parameterized import parameterized

from airflow.models.dag import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.utils import timezone
from tests.providers.mysql.hooks.test_mysql import MySqlContext

DEFAULT_DATE = timezone.datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]
TEST_DAG_ID = 'unit_test_dag'


@pytest.mark.backend("mysql")
class TestMySql(unittest.TestCase):
    def setUp(self):
        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        dag = DAG(TEST_DAG_ID, default_args=args)
        self.dag = dag

    def tearDown(self):
        drop_tables = {'test_mysql_to_mysql', 'test_airflow'}
        with closing(MySqlHook().get_conn()) as conn:
            with closing(conn.cursor()) as cursor:
                for table in drop_tables:
                    cursor.execute(f"DROP TABLE IF EXISTS {table}")

    @parameterized.expand(
        [
            ("mysqlclient",),
            ("mysql-connector-python",),
        ]
    )
    def test_mysql_operator_test(self, client):
        with MySqlContext(client):
            sql = """
            CREATE TABLE IF NOT EXISTS test_airflow (
                dummy VARCHAR(50)
            );
            """
            op = MySqlOperator(task_id='basic_mysql', sql=sql, dag=self.dag)
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    @parameterized.expand(
        [
            ("mysqlclient",),
            ("mysql-connector-python",),
        ]
    )
    def test_mysql_operator_test_multi(self, client):
        with MySqlContext(client):
            sql = [
                "CREATE TABLE IF NOT EXISTS test_airflow (dummy VARCHAR(50))",
                "TRUNCATE TABLE test_airflow",
                "INSERT INTO test_airflow VALUES ('X')",
            ]
            op = MySqlOperator(
                task_id='mysql_operator_test_multi',
                sql=sql,
                dag=self.dag,
            )
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    @parameterized.expand(
        [
            ("mysqlclient",),
            ("mysql-connector-python",),
        ]
    )
    def test_overwrite_schema(self, client):
        """
        Verifies option to overwrite connection schema
        """
        with MySqlContext(client):
            sql = "SELECT 1;"
            op = MySqlOperator(
                task_id='test_mysql_operator_test_schema_overwrite',
                sql=sql,
                dag=self.dag,
                database="foobar",
            )

            from MySQLdb import OperationalError  # pylint: disable=no-name-in-module

            try:
                op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
            except OperationalError as e:
                assert "Unknown database 'foobar'" in str(e)
