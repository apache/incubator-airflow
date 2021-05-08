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

from unittest import mock, TestCase

from qds_sdk.commands import PrestoCommand

from airflow import settings
from airflow.models import DAG, Connection
from airflow.models.taskinstance import TaskInstance
from airflow.providers.qubole.hooks.qubole import QuboleHook
from airflow.providers.qubole.operators.qubole import QDSLink, QuboleOperator
from airflow.serialization.serialized_objects import SerializedDAG
from airflow.utils import db
from airflow.utils.timezone import datetime

DAG_ID = "qubole_test_dag"
TASK_ID = "test_task"
DEFAULT_CONN = "qubole_default"
TEMPLATE_CONN = "my_conn_id"
TEST_CONN = "qubole_test_conn"
DEFAULT_DATE = datetime(2017, 1, 1)

RESULTS_WITH_HEADER = 'header1\theader2\nval1\tval2'
RESULTS_WITH_NO_HEADER = 'val1\tval2'


def get_result_mock(fp, arguments):
    if arguments[0] == 'true':
        fp.write(bytearray(RESULTS_WITH_HEADER, 'utf-8'))
    else:
        fp.write(bytearray(RESULTS_WITH_NO_HEADER, 'utf-8'))


class TestQuboleOperator(TestCase):
    def setUp(self):
        db.merge_conn(Connection(conn_id=DEFAULT_CONN, conn_type='HTTP'))
        db.merge_conn(Connection(conn_id=TEST_CONN, conn_type='HTTP', host='http://localhost/api'))

    def tearDown(self):
        session = settings.Session()
        session.query(Connection).filter(Connection.conn_id == TEST_CONN).delete()
        session.commit()
        session.close()

    def test_init_with_default_connection(self):
        op = QuboleOperator(task_id=TASK_ID)
        assert op.task_id == TASK_ID
        assert op.qubole_conn_id == DEFAULT_CONN

    def test_init_with_template_connection(self):
        with DAG(DAG_ID, start_date=DEFAULT_DATE):
            task = QuboleOperator(task_id=TASK_ID, qubole_conn_id="{{ qubole_conn_id }}")

        task.render_template_fields({'qubole_conn_id': TEMPLATE_CONN})
        assert task.task_id == TASK_ID
        assert task.qubole_conn_id == TEMPLATE_CONN

    def test_init_with_template_cluster_label(self):
        dag = DAG(DAG_ID, start_date=DEFAULT_DATE)
        task = QuboleOperator(
            task_id=TASK_ID,
            dag=dag,
            cluster_label='{{ params.cluster_label }}',
            params={'cluster_label': 'default'},
        )

        ti = TaskInstance(task, DEFAULT_DATE)
        ti.render_templates()

        assert task.cluster_label == 'default'

    def test_get_hook(self):
        dag = DAG(DAG_ID, start_date=DEFAULT_DATE)

        with dag:
            task = QuboleOperator(task_id=TASK_ID, command_type='hivecmd', dag=dag)

        hook = task.get_hook()
        assert hook.__class__ == QuboleHook

    def test_hyphen_args_note_id(self):
        dag = DAG(DAG_ID, start_date=DEFAULT_DATE)

        with dag:
            task = QuboleOperator(task_id=TASK_ID, command_type='sparkcmd', note_id="123", dag=dag)

        assert task.get_hook().create_cmd_args({'run_id': 'dummy'})[0] == "--note-id=123"

    def test_notify(self):
        dag = DAG(DAG_ID, start_date=DEFAULT_DATE)

        with dag:
            task = QuboleOperator(task_id=TASK_ID, command_type='sparkcmd', notify=True, dag=dag)

        assert task.get_hook().create_cmd_args({'run_id': 'dummy'})[0] == "--notify"

    def test_position_args_parameters(self):
        dag = DAG(DAG_ID, start_date=DEFAULT_DATE)

        with dag:
            task = QuboleOperator(
                task_id=TASK_ID, command_type='pigcmd', parameters="key1=value1 key2=value2", dag=dag
            )

        assert task.get_hook().create_cmd_args({'run_id': 'dummy'})[1] == "key1=value1"
        assert task.get_hook().create_cmd_args({'run_id': 'dummy'})[2] == "key2=value2"

        cmd = "s3distcp --src s3n://airflow/source_hadoopcmd --dest s3n://airflow/destination_hadoopcmd"
        task = QuboleOperator(task_id=TASK_ID + "_1", command_type='hadoopcmd', dag=dag, sub_command=cmd)

        assert task.get_hook().create_cmd_args({'run_id': 'dummy'})[1] == "s3distcp"
        assert task.get_hook().create_cmd_args({'run_id': 'dummy'})[2] == "--src"
        assert task.get_hook().create_cmd_args({'run_id': 'dummy'})[3] == "s3n://airflow/source_hadoopcmd"
        assert task.get_hook().create_cmd_args({'run_id': 'dummy'})[4] == "--dest"
        assert (
            task.get_hook().create_cmd_args({'run_id': 'dummy'})[5] == "s3n://airflow/destination_hadoopcmd"
        )

    def test_get_redirect_url(self):
        dag = DAG(DAG_ID, start_date=DEFAULT_DATE)

        with dag:
            task = QuboleOperator(
                task_id=TASK_ID,
                qubole_conn_id=TEST_CONN,
                command_type='shellcmd',
                parameters="param1 param2",
                dag=dag,
            )

        ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)
        ti.xcom_push('qbol_cmd_id', 12345)

        # check for positive case
        url = task.get_extra_links(DEFAULT_DATE, 'Go to QDS')
        assert url == 'http://localhost/v2/analyze?command_id=12345'

        # check for negative case
        url2 = task.get_extra_links(datetime(2017, 1, 2), 'Go to QDS')
        assert url2 == ''

    def test_extra_serialized_field(self):
        dag = DAG(DAG_ID, start_date=DEFAULT_DATE)
        with dag:
            QuboleOperator(
                task_id=TASK_ID,
                command_type='shellcmd',
                qubole_conn_id=TEST_CONN,
            )

        serialized_dag = SerializedDAG.to_dict(dag)
        assert "qubole_conn_id" in serialized_dag["dag"]["tasks"][0]

        dag = SerializedDAG.from_dict(serialized_dag)
        simple_task = dag.task_dict[TASK_ID]
        assert getattr(simple_task, "qubole_conn_id") == TEST_CONN

        #########################################################
        # Verify Operator Links work with Serialized Operator
        #########################################################
        assert isinstance(list(simple_task.operator_extra_links)[0], QDSLink)

        ti = TaskInstance(task=simple_task, execution_date=DEFAULT_DATE)
        ti.xcom_push('qbol_cmd_id', 12345)

        # check for positive case
        url = simple_task.get_extra_links(DEFAULT_DATE, 'Go to QDS')
        assert url == 'http://localhost/v2/analyze?command_id=12345'

        # check for negative case
        url2 = simple_task.get_extra_links(datetime(2017, 1, 2), 'Go to QDS')
        assert url2 == ''

    def test_parameter_pool_passed(self):
        test_pool = 'test_pool'
        op = QuboleOperator(task_id=TASK_ID, pool=test_pool)
        assert op.pool == test_pool

    @mock.patch('qds_sdk.commands.Command.get_results', new=get_result_mock)
    def test_get_results_with_headers(self):
        dag = DAG(DAG_ID, start_date=DEFAULT_DATE)

        task = QuboleOperator(task_id=TASK_ID, command_type='prestocmd', dag=dag)

        with mock.patch.object(task, 'xcom_pull', return_value='test_command_id'):
            with mock.patch('qds_sdk.resource.Resource.find', return_value=PrestoCommand):
                results = open(task.get_results(ti=task, include_headers=True), 'r').read()
                assert results == RESULTS_WITH_HEADER
                results = open(task.get_results(ti=task, include_headers=False), 'r').read()
                assert results == RESULTS_WITH_NO_HEADER

    @mock.patch('qds_sdk.commands.Command.get_results', new=get_result_mock)
    def test_get_results_without_headers(self):
        dag = DAG(DAG_ID, start_date=DEFAULT_DATE)

        task = QuboleOperator(task_id=TASK_ID, command_type='prestocmd', dag=dag)

        with mock.patch.object(task, 'xcom_pull', return_value='test_command_id'):
            with mock.patch('qds_sdk.resource.Resource.find', return_value=PrestoCommand):
                results = open(task.get_results(ti=task, include_headers=False), 'r').read()
                assert results == RESULTS_WITH_NO_HEADER
