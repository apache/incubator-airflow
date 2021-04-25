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
from collections import OrderedDict
from contextlib import closing
from os import path
from unittest import mock

import pytest

from airflow.models.dag import DAG
from airflow.providers.apache.hive.transfers.mysql_to_hive import MySqlToHiveOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils import timezone
from tests.test_utils.mock_hooks import MockHiveServer2Hook
from tests.test_utils.mock_process import MockConnectionCursor, MockSubProcess

DEFAULT_DATE = timezone.datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]
TEST_DAG_ID = 'unit_test_dag'


class HiveopTempFile:
    """
    Make sure temp file path is in the format of "/tmp/airflow_hiveop_t_78lpye/tmpour2_kig",
    """

    def __eq__(self, other: str) -> bool:
        (head, tail) = path.split(other)
        (head, tail) = path.split(head)
        return tail.startswith("airflow_hiveop_")


class HiveopTempDir:
    """
    Make sure temp dir path is in the format of "/tmp/airflow_hiveop_t_78lpye",
    """

    def __eq__(self, other: str) -> bool:
        (_, tail) = path.split(other)
        return tail.startswith("airflow_hiveop_")


@pytest.mark.backend("mysql")
class TestTransfer(unittest.TestCase):
    def setUp(self):
        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        dag = DAG(TEST_DAG_ID, default_args=args)
        self.dag = dag

        rows = [
            (1880, "John", 0.081541, "boy"),
            (1880, "William", 0.080511, "boy"),
            (1880, "James", 0.050057, "boy"),
            (1880, "Charles", 0.045167, "boy"),
            (1880, "George", 0.043292, "boy"),
            (1880, "Frank", 0.02738, "boy"),
            (1880, "Joseph", 0.022229, "boy"),
            (1880, "Thomas", 0.021401, "boy"),
            (1880, "Henry", 0.020641, "boy"),
            (1880, "Robert", 0.020404, "boy"),
            (1880, "Edward", 0.019965, "boy"),
            (1880, "Harry", 0.018175, "boy"),
            (1880, "Walter", 0.014822, "boy"),
            (1880, "Arthur", 0.013504, "boy"),
            (1880, "Fred", 0.013251, "boy"),
            (1880, "Albert", 0.012609, "boy"),
            (1880, "Samuel", 0.008648, "boy"),
            (1880, "David", 0.007339, "boy"),
            (1880, "Louis", 0.006993, "boy"),
            (1880, "Joe", 0.006174, "boy"),
            (1880, "Charlie", 0.006165, "boy"),
            (1880, "Clarence", 0.006165, "boy"),
            (1880, "Richard", 0.006148, "boy"),
            (1880, "Andrew", 0.005439, "boy"),
            (1880, "Daniel", 0.00543, "boy"),
            (1880, "Ernest", 0.005194, "boy"),
            (1880, "Will", 0.004966, "boy"),
            (1880, "Jesse", 0.004805, "boy"),
            (1880, "Oscar", 0.004594, "boy"),
            (1880, "Lewis", 0.004366, "boy"),
            (1880, "Peter", 0.004189, "boy"),
            (1880, "Benjamin", 0.004138, "boy"),
            (1880, "Frederick", 0.004079, "boy"),
            (1880, "Willie", 0.00402, "boy"),
            (1880, "Alfred", 0.003961, "boy"),
            (1880, "Sam", 0.00386, "boy"),
            (1880, "Roy", 0.003716, "boy"),
            (1880, "Herbert", 0.003581, "boy"),
            (1880, "Jacob", 0.003412, "boy"),
            (1880, "Tom", 0.00337, "boy"),
            (1880, "Elmer", 0.00315, "boy"),
            (1880, "Carl", 0.003142, "boy"),
            (1880, "Lee", 0.003049, "boy"),
            (1880, "Howard", 0.003015, "boy"),
            (1880, "Martin", 0.003015, "boy"),
            (1880, "Michael", 0.00299, "boy"),
            (1880, "Bert", 0.002939, "boy"),
            (1880, "Herman", 0.002931, "boy"),
            (1880, "Jim", 0.002914, "boy"),
            (1880, "Francis", 0.002905, "boy"),
            (1880, "Harvey", 0.002905, "boy"),
            (1880, "Earl", 0.002829, "boy"),
            (1880, "Eugene", 0.00277, "boy"),
        ]

        self.env_vars = {
            'AIRFLOW_CTX_DAG_ID': 'test_dag_id',
            'AIRFLOW_CTX_TASK_ID': 'test_task_id',
            'AIRFLOW_CTX_EXECUTION_DATE': '2015-01-01T00:00:00+00:00',
            'AIRFLOW_CTX_DAG_RUN_ID': '55',
            'AIRFLOW_CTX_DAG_OWNER': 'airflow',
            'AIRFLOW_CTX_DAG_EMAIL': 'test@airflow.com',
        }

        with closing(MySqlHook().get_conn()) as conn:
            with closing(conn.cursor()) as cur:
                cur.execute(
                    '''
                CREATE TABLE IF NOT EXISTS baby_names (
                  org_year integer(4),
                  baby_name VARCHAR(25),
                  rate FLOAT(7,6),
                  sex VARCHAR(4)
                )
                '''
                )
                for row in rows:
                    cur.execute("INSERT INTO baby_names VALUES(%s, %s, %s, %s);", row)

    def tearDown(self):
        with closing(MySqlHook().get_conn()) as conn:
            with closing(conn.cursor()) as cur:
                cur.execute("DROP TABLE IF EXISTS baby_names CASCADE;")

    @mock.patch('subprocess.Popen')
    def test_mysql_to_hive(self, mock_popen):
        mock_subprocess = MockSubProcess()
        mock_popen.return_value = mock_subprocess

        with mock.patch.dict('os.environ', self.env_vars):
            sql = "SELECT * FROM baby_names LIMIT 1000;"
            op = MySqlToHiveOperator(
                task_id='test_m2h',
                hive_cli_conn_id='hive_cli_default',
                sql=sql,
                hive_table='test_mysql_to_hive',
                recreate=True,
                delimiter=",",
                dag=self.dag,
            )
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        hive_cmd = [
            'beeline',
            '-u',
            '"jdbc:hive2://localhost:10000/default"',
            '-hiveconf',
            'airflow.ctx.dag_id=unit_test_dag',
            '-hiveconf',
            'airflow.ctx.task_id=test_m2h',
            '-hiveconf',
            'airflow.ctx.execution_date=2015-01-01T00:00:00+00:00',
            '-hiveconf',
            'airflow.ctx.dag_run_id=55',
            '-hiveconf',
            'airflow.ctx.dag_owner=airflow',
            '-hiveconf',
            'airflow.ctx.dag_email=test@airflow.com',
            '-hiveconf',
            'mapreduce.job.queuename=airflow',
            '-hiveconf',
            'mapred.job.queue.name=airflow',
            '-hiveconf',
            'tez.queue.name=airflow',
            '-f',
            HiveopTempFile(),
        ]

        mock_popen.assert_called_with(
            hive_cmd,
            stdout=mock_subprocess.PIPE,
            stderr=mock_subprocess.STDOUT,
            cwd=HiveopTempDir(),
            close_fds=True,
        )

    @mock.patch('subprocess.Popen')
    def test_mysql_to_hive_partition(self, mock_popen):
        mock_subprocess = MockSubProcess()
        mock_popen.return_value = mock_subprocess

        with mock.patch.dict('os.environ', self.env_vars):
            sql = "SELECT * FROM baby_names LIMIT 1000;"
            op = MySqlToHiveOperator(
                task_id='test_m2h',
                hive_cli_conn_id='hive_cli_default',
                sql=sql,
                hive_table='test_mysql_to_hive_part',
                partition={'ds': DEFAULT_DATE_DS},
                recreate=False,
                create=True,
                delimiter=",",
                dag=self.dag,
            )
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        hive_cmd = [
            'beeline',
            '-u',
            '"jdbc:hive2://localhost:10000/default"',
            '-hiveconf',
            'airflow.ctx.dag_id=unit_test_dag',
            '-hiveconf',
            'airflow.ctx.task_id=test_m2h',
            '-hiveconf',
            'airflow.ctx.execution_date=2015-01-01T00:00:00+00:00',
            '-hiveconf',
            'airflow.ctx.dag_run_id=55',
            '-hiveconf',
            'airflow.ctx.dag_owner=airflow',
            '-hiveconf',
            'airflow.ctx.dag_email=test@airflow.com',
            '-hiveconf',
            'mapreduce.job.queuename=airflow',
            '-hiveconf',
            'mapred.job.queue.name=airflow',
            '-hiveconf',
            'tez.queue.name=airflow',
            '-f',
            HiveopTempFile(),
        ]

        mock_popen.assert_called_with(
            hive_cmd,
            stdout=mock_subprocess.PIPE,
            stderr=mock_subprocess.STDOUT,
            cwd=HiveopTempDir(),
            close_fds=True,
        )

    @mock.patch('subprocess.Popen')
    def test_mysql_to_hive_tblproperties(self, mock_popen):
        mock_subprocess = MockSubProcess()
        mock_popen.return_value = mock_subprocess

        with mock.patch.dict('os.environ', self.env_vars):
            sql = "SELECT * FROM baby_names LIMIT 1000;"
            op = MySqlToHiveOperator(
                task_id='test_m2h',
                hive_cli_conn_id='hive_cli_default',
                sql=sql,
                hive_table='test_mysql_to_hive',
                recreate=True,
                delimiter=",",
                tblproperties={'test_property': 'test_value'},
                dag=self.dag,
            )
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        hive_cmd = [
            'beeline',
            '-u',
            '"jdbc:hive2://localhost:10000/default"',
            '-hiveconf',
            'airflow.ctx.dag_id=unit_test_dag',
            '-hiveconf',
            'airflow.ctx.task_id=test_m2h',
            '-hiveconf',
            'airflow.ctx.execution_date=2015-01-01T00:00:00+00:00',
            '-hiveconf',
            'airflow.ctx.dag_run_id=55',
            '-hiveconf',
            'airflow.ctx.dag_owner=airflow',
            '-hiveconf',
            'airflow.ctx.dag_email=test@airflow.com',
            '-hiveconf',
            'mapreduce.job.queuename=airflow',
            '-hiveconf',
            'mapred.job.queue.name=airflow',
            '-hiveconf',
            'tez.queue.name=airflow',
            '-f',
            HiveopTempFile(),
        ]

        mock_popen.assert_called_with(
            hive_cmd,
            stdout=mock_subprocess.PIPE,
            stderr=mock_subprocess.STDOUT,
            cwd=HiveopTempDir(),
            close_fds=True,
        )

    @mock.patch('airflow.providers.apache.hive.hooks.hive.HiveCliHook.load_file')
    def test_mysql_to_hive_type_conversion(self, mock_load_file):
        mysql_table = 'test_mysql_to_hive'

        hook = MySqlHook()

        try:
            with closing(hook.get_conn()) as conn:
                with closing(conn.cursor()) as cursor:
                    cursor.execute(f"DROP TABLE IF EXISTS {mysql_table}")
                    cursor.execute(
                        f"""
                        CREATE TABLE {mysql_table} (
                            c0 TINYINT,
                            c1 SMALLINT,
                            c2 MEDIUMINT,
                            c3 INT,
                            c4 BIGINT,
                            c5 TIMESTAMP
                        )
                    """
                    )

            op = MySqlToHiveOperator(
                task_id='test_m2h',
                hive_cli_conn_id='hive_cli_default',
                sql=f"SELECT * FROM {mysql_table}",
                hive_table='test_mysql_to_hive',
                dag=self.dag,
            )
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

            assert mock_load_file.call_count == 1
            ordered_dict = OrderedDict()
            ordered_dict["c0"] = "SMALLINT"
            ordered_dict["c1"] = "INT"
            ordered_dict["c2"] = "INT"
            ordered_dict["c3"] = "BIGINT"
            ordered_dict["c4"] = "DECIMAL(38,0)"
            ordered_dict["c5"] = "TIMESTAMP"
            assert mock_load_file.call_args[1]["field_dict"] == ordered_dict
        finally:
            with closing(hook.get_conn()) as conn:
                with closing(conn.cursor()) as cursor:
                    cursor.execute(f"DROP TABLE IF EXISTS {mysql_table}")

    @mock.patch('subprocess.Popen')
    def test_mysql_to_hive_verify_csv_special_char(self, mock_popen):
        mock_subprocess = MockSubProcess()
        mock_popen.return_value = mock_subprocess

        mysql_table = 'test_mysql_to_hive'
        hive_table = 'test_mysql_to_hive'

        hook = MySqlHook()

        try:
            db_record = ('c0', '["true"]')
            with closing(hook.get_conn()) as conn:
                with closing(conn.cursor()) as cursor:
                    cursor.execute(f"DROP TABLE IF EXISTS {mysql_table}")
                    cursor.execute(
                        f"""
                        CREATE TABLE {mysql_table} (
                            c0 VARCHAR(25),
                            c1 VARCHAR(25)
                        )
                    """
                    )
                    cursor.execute(
                        """
                        INSERT INTO {} VALUES (
                            '{}', '{}'
                        )
                    """.format(
                            mysql_table, *db_record
                        )
                    )

            with mock.patch.dict('os.environ', self.env_vars):
                import unicodecsv as csv

                op = MySqlToHiveOperator(
                    task_id='test_m2h',
                    hive_cli_conn_id='hive_cli_default',
                    sql=f"SELECT * FROM {mysql_table}",
                    hive_table=hive_table,
                    recreate=True,
                    delimiter=",",
                    quoting=csv.QUOTE_NONE,
                    quotechar='',
                    escapechar='@',
                    dag=self.dag,
                )
                op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

                mock_cursor = MockConnectionCursor()
                mock_cursor.iterable = [('c0', '["true"]'), (2, 2)]
                hive_hook = MockHiveServer2Hook(connection_cursor=mock_cursor)

                result = hive_hook.get_records(f"SELECT * FROM {hive_table}")
            assert result[0] == db_record

            hive_cmd = [
                'beeline',
                '-u',
                '"jdbc:hive2://localhost:10000/default"',
                '-hiveconf',
                'airflow.ctx.dag_id=unit_test_dag',
                '-hiveconf',
                'airflow.ctx.task_id=test_m2h',
                '-hiveconf',
                'airflow.ctx.execution_date=2015-01-01T00:00:00+00:00',
                '-hiveconf',
                'airflow.ctx.dag_run_id=55',
                '-hiveconf',
                'airflow.ctx.dag_owner=airflow',
                '-hiveconf',
                'airflow.ctx.dag_email=test@airflow.com',
                '-hiveconf',
                'mapreduce.job.queuename=airflow',
                '-hiveconf',
                'mapred.job.queue.name=airflow',
                '-hiveconf',
                'tez.queue.name=airflow',
                '-f',
                HiveopTempFile(),
            ]

            mock_popen.assert_called_with(
                hive_cmd,
                stdout=mock_subprocess.PIPE,
                stderr=mock_subprocess.STDOUT,
                cwd=HiveopTempDir(),
                close_fds=True,
            )
        finally:
            with closing(hook.get_conn()) as conn:
                with closing(conn.cursor()) as cursor:
                    cursor.execute(f"DROP TABLE IF EXISTS {mysql_table}")

    @mock.patch('subprocess.Popen')
    def test_mysql_to_hive_verify_loaded_values(self, mock_popen):
        mock_subprocess = MockSubProcess()
        mock_popen.return_value = mock_subprocess

        mysql_table = 'test_mysql_to_hive'
        hive_table = 'test_mysql_to_hive'

        hook = MySqlHook()

        try:
            minmax = (
                255,
                65535,
                16777215,
                4294967295,
                18446744073709551615,
                -128,
                -32768,
                -8388608,
                -2147483648,
                -9223372036854775808,
            )

            with closing(hook.get_conn()) as conn:
                with closing(conn.cursor()) as cursor:
                    cursor.execute(f"DROP TABLE IF EXISTS {mysql_table}")
                    cursor.execute(
                        f"""
                        CREATE TABLE {mysql_table} (
                            c0 TINYINT   UNSIGNED,
                            c1 SMALLINT  UNSIGNED,
                            c2 MEDIUMINT UNSIGNED,
                            c3 INT       UNSIGNED,
                            c4 BIGINT    UNSIGNED,
                            c5 TINYINT,
                            c6 SMALLINT,
                            c7 MEDIUMINT,
                            c8 INT,
                            c9 BIGINT
                        )
                    """
                    )
                    cursor.execute(
                        """
                        INSERT INTO {} VALUES (
                            {}, {}, {}, {}, {}, {}, {}, {}, {}, {}
                        )
                    """.format(
                            mysql_table, *minmax
                        )
                    )

            with mock.patch.dict('os.environ', self.env_vars):
                op = MySqlToHiveOperator(
                    task_id='test_m2h',
                    hive_cli_conn_id='hive_cli_default',
                    sql=f"SELECT * FROM {mysql_table}",
                    hive_table=hive_table,
                    recreate=True,
                    delimiter=",",
                    dag=self.dag,
                )
                op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

                mock_cursor = MockConnectionCursor()
                mock_cursor.iterable = [minmax]
                hive_hook = MockHiveServer2Hook(connection_cursor=mock_cursor)

                result = hive_hook.get_records(f"SELECT * FROM {hive_table}")
                assert result[0] == minmax

                hive_cmd = [
                    'beeline',
                    '-u',
                    '"jdbc:hive2://localhost:10000/default"',
                    '-hiveconf',
                    'airflow.ctx.dag_id=unit_test_dag',
                    '-hiveconf',
                    'airflow.ctx.task_id=test_m2h',
                    '-hiveconf',
                    'airflow.ctx.execution_date=2015-01-01T00:00:00+00:00',
                    '-hiveconf',
                    'airflow.ctx.dag_run_id=55',
                    '-hiveconf',
                    'airflow.ctx.dag_owner=airflow',
                    '-hiveconf',
                    'airflow.ctx.dag_email=test@airflow.com',
                    '-hiveconf',
                    'mapreduce.job.queuename=airflow',
                    '-hiveconf',
                    'mapred.job.queue.name=airflow',
                    '-hiveconf',
                    'tez.queue.name=airflow',
                    '-f',
                    HiveopTempFile(),
                ]

                mock_popen.assert_called_with(
                    hive_cmd,
                    stdout=mock_subprocess.PIPE,
                    stderr=mock_subprocess.STDOUT,
                    cwd=HiveopTempDir(),
                    close_fds=True,
                )

        finally:
            with closing(hook.get_conn()) as conn:
                with closing(conn.cursor()) as cursor:
                    cursor.execute(f"DROP TABLE IF EXISTS {mysql_table}")
