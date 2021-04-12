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

from datetime import datetime, timedelta

import pytest

from airflow import DAG
from airflow.models.xcom_arg import XComArg
from airflow.operators.python import PythonOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.task_group import TaskGroup

DEFAULT_ARGS = {
    "owner": "test",
    "depends_on_past": True,
    "start_date": datetime.today(),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@pytest.fixture
def test_dag():
    """Creates a test DAG with a few operators to test on."""

    def f(task_id):
        return f"OP:{task_id}"

    with DAG(dag_id="test_xcom_dag", default_args=DEFAULT_ARGS) as dag:
        operators = [PythonOperator(python_callable=f, task_id="test_op_%i" % i) for i in range(4)]
        return dag, operators


@pytest.fixture
def test_taskgroup_dag():
    """Creates a test DAG with a few operators to test on, with some in a task group."""

    def f(task_id):
        return f"OP:{task_id}"

    with DAG(dag_id="test_xcom_dag", default_args=DEFAULT_ARGS) as dag:
        op1 = PythonOperator(python_callable=f, task_id="test_op_1")
        op4 = PythonOperator(python_callable=f, task_id="test_op_4")
        with TaskGroup("group_1") as group:
            op2 = PythonOperator(python_callable=f, task_id="test_op_2")
            op3 = PythonOperator(python_callable=f, task_id="test_op_3")
            return dag, group, (op1, op2, op3, op4)


class TestEdgeModifierBuilding:
    """
    Tests that EdgeModifiers work when composed with Tasks (either via >>
    or set_upstream styles)
    """

    def test_operator_set(self, test_dag):
        """Tests the set_upstream/downstream style with a plain operator"""
        # Unpack the fixture
        dag, (op1, op2, op3, op4) = test_dag
        # Arrange the operators with a Label in the middle
        op1.set_downstream(op2, Label("Label 1"))
        op3.set_upstream(op2, Label("Label 2"))
        op4.set_upstream(op2)
        # Check that the DAG has the right edge info
        assert dag.get_edge_info(op1.task_id, op2.task_id) == {"label": "Label 1"}
        assert dag.get_edge_info(op2.task_id, op3.task_id) == {"label": "Label 2"}
        assert dag.get_edge_info(op2.task_id, op4.task_id) == {}

    def test_tasklist_set(self, test_dag):
        """Tests the set_upstream/downstream style with a list of operators"""
        # Unpack the fixture
        dag, (op1, op2, op3, op4) = test_dag
        # Arrange the operators with a Label in the middle
        op1.set_downstream([op2, op3], Label("Label 1"))
        op4.set_upstream(op2)
        # Check that the DAG has the right edge info
        assert dag.get_edge_info(op1.task_id, op2.task_id) == {"label": "Label 1"}
        assert dag.get_edge_info(op1.task_id, op3.task_id) == {"label": "Label 1"}
        assert dag.get_edge_info(op2.task_id, op4.task_id) == {}

    def test_xcomarg_set(self, test_dag):
        """Tests the set_upstream/downstream style with an XComArg"""
        # Unpack the fixture
        dag, (op1, op2, op3, op4) = test_dag
        # Arrange the operators with a Label in the middle
        op1_arg = XComArg(op1, "test_key")
        op1_arg.set_downstream(op2, Label("Label 1"))
        op1.set_downstream([op3, op4])
        # Check that the DAG has the right edge info
        assert dag.get_edge_info(op1.task_id, op2.task_id) == {"label": "Label 1"}
        assert dag.get_edge_info(op1.task_id, op4.task_id) == {}

    def test_taskgroup_set(self, test_taskgroup_dag):
        """Tests the set_upstream/downstream style with a TaskGroup"""
        # Unpack the fixture
        dag, group, (op1, op2, op3, op4) = test_taskgroup_dag
        # Arrange them with a Label in the middle
        op1.set_downstream(group, Label("Group label"))
        group.set_downstream(op4)
        # Check that the DAG has the right edge info
        assert dag.get_edge_info(op1.task_id, op2.task_id) == {"label": "Group label"}
        assert dag.get_edge_info(op1.task_id, op3.task_id) == {"label": "Group label"}
        assert dag.get_edge_info(op3.task_id, op4.task_id) == {}

    def test_operator_shift(self, test_dag):
        """Tests the >> / << style with a plain operator"""
        # Unpack the fixture
        dag, (op1, op2, op3, op4) = test_dag
        # Arrange the operators with a Label in the middle
        op1 >> Label("Label 1") >> op2  # pylint: disable=W0106
        op3 << Label("Label 2") << op2 >> op4  # pylint: disable=W0106
        # Check that the DAG has the right edge info
        assert dag.get_edge_info(op1.task_id, op2.task_id) == {"label": "Label 1"}
        assert dag.get_edge_info(op2.task_id, op3.task_id) == {"label": "Label 2"}
        assert dag.get_edge_info(op2.task_id, op4.task_id) == {}

    def test_tasklist_shift(self, test_dag):
        """Tests the >> / << style with a list of operators"""
        # Unpack the fixture
        dag, (op1, op2, op3, op4) = test_dag
        # Arrange the operators with a Label in the middle
        op1 >> Label("Label 1") >> [op2, op3] << Label("Label 2") << op4  # pylint: disable=W0106
        # Check that the DAG has the right edge info
        assert dag.get_edge_info(op1.task_id, op2.task_id) == {"label": "Label 1"}
        assert dag.get_edge_info(op1.task_id, op3.task_id) == {"label": "Label 1"}
        assert dag.get_edge_info(op4.task_id, op2.task_id) == {"label": "Label 2"}

    def test_xcomarg_shift(self, test_dag):
        """Tests the >> / << style with an XComArg"""
        # Unpack the fixture
        dag, (op1, op2, op3, op4) = test_dag
        # Arrange the operators with a Label in the middle
        op1_arg = XComArg(op1, "test_key")
        op1_arg >> Label("Label 1") >> [op2, op3]  # pylint: disable=W0106
        op1_arg >> op4
        # Check that the DAG has the right edge info
        assert dag.get_edge_info(op1.task_id, op2.task_id) == {"label": "Label 1"}
        assert dag.get_edge_info(op1.task_id, op4.task_id) == {}

    def test_taskgroup_shift(self, test_taskgroup_dag):
        """Tests the set_upstream/downstream style with a TaskGroup"""
        # Unpack the fixture
        dag, group, (op1, op2, op3, op4) = test_taskgroup_dag
        # Arrange them with a Label in the middle
        op1 >> Label("Group label") >> group >> op4  # pylint: disable=W0106
        # Check that the DAG has the right edge info
        assert dag.get_edge_info(op1.task_id, op2.task_id) == {"label": "Group label"}
        assert dag.get_edge_info(op1.task_id, op3.task_id) == {"label": "Group label"}
        assert dag.get_edge_info(op3.task_id, op4.task_id) == {}
