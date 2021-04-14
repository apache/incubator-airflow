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

import pendulum

from airflow.models import DAG, DagRun, TaskInstance
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.deps.not_previously_skipped_dep import NotPreviouslySkippedDep
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType


def test_no_parent():
    """
    A simple DAG with a single task. NotPreviouslySkippedDep is met.
    """
    start_date = pendulum.datetime(2020, 1, 1)
    dag = DAG("test_test_no_parent_dag", schedule_interval=None, start_date=start_date)
    op1 = DummyOperator(task_id="op1", dag=dag)

    ti1 = TaskInstance(op1, start_date)

    with create_session() as session:
        dep = NotPreviouslySkippedDep()
        assert len(list(dep.get_dep_statuses(ti1, session, DepContext()))) == 0
        assert dep.is_met(ti1, session)
        assert ti1.state != State.SKIPPED


def test_no_skipmixin_parent():
    """
    A simple DAG with no branching. Both op1 and op2 are DummyOperator. NotPreviouslySkippedDep is met.
    """
    start_date = pendulum.datetime(2020, 1, 1)
    dag = DAG("test_no_skipmixin_parent_dag", schedule_interval=None, start_date=start_date)
    op1 = DummyOperator(task_id="op1", dag=dag)
    op2 = DummyOperator(task_id="op2", dag=dag)
    op1 >> op2

    ti2 = TaskInstance(op2, start_date)

    with create_session() as session:
        dep = NotPreviouslySkippedDep()
        assert len(list(dep.get_dep_statuses(ti2, session, DepContext()))) == 0
        assert dep.is_met(ti2, session)
        assert ti2.state != State.SKIPPED


def test_parent_follow_branch():
    """
    A simple DAG with a BranchPythonOperator that follows op2. NotPreviouslySkippedDep is met.
    """
    start_date = pendulum.datetime(2020, 1, 1)
    dag = DAG("test_parent_follow_branch_dag", schedule_interval=None, start_date=start_date)
    dag.create_dagrun(run_type=DagRunType.MANUAL, state=State.RUNNING, execution_date=start_date)
    op1 = BranchPythonOperator(task_id="op1", python_callable=lambda: "op2", dag=dag)
    op2 = DummyOperator(task_id="op2", dag=dag)
    op1 >> op2
    TaskInstance(op1, start_date).run()
    ti2 = TaskInstance(op2, start_date)

    with create_session() as session:
        dep = NotPreviouslySkippedDep()
        assert len(list(dep.get_dep_statuses(ti2, session, DepContext()))) == 0
        assert dep.is_met(ti2, session)
        assert ti2.state != State.SKIPPED


def test_parent_skip_branch():
    """
    A simple DAG with a BranchPythonOperator that does not follow op2. NotPreviouslySkippedDep is not met.
    """
    with create_session() as session:
        session.query(DagRun).delete()
        session.query(TaskInstance).delete()
        start_date = pendulum.datetime(2020, 1, 1)
        dag = DAG("test_parent_skip_branch_dag", schedule_interval=None, start_date=start_date)
        dag.create_dagrun(run_type=DagRunType.MANUAL, state=State.RUNNING, execution_date=start_date)
        op1 = BranchPythonOperator(task_id="op1", python_callable=lambda: "op3", dag=dag)
        op2 = DummyOperator(task_id="op2", dag=dag)
        op3 = DummyOperator(task_id="op3", dag=dag)
        op1 >> [op2, op3]
        TaskInstance(op1, start_date).run()
        ti2 = TaskInstance(op2, start_date)
        dep = NotPreviouslySkippedDep()

        assert len(list(dep.get_dep_statuses(ti2, session, DepContext()))) == 1
        session.commit()
        assert not dep.is_met(ti2, session)
        assert ti2.state == State.SKIPPED


def test_parent_not_executed():
    """
    A simple DAG with a BranchPythonOperator that does not follow op2. Parent task is not yet
    executed (no xcom data). NotPreviouslySkippedDep is met (no decision).
    """
    start_date = pendulum.datetime(2020, 1, 1)
    dag = DAG("test_parent_not_executed_dag", schedule_interval=None, start_date=start_date)
    op1 = BranchPythonOperator(task_id="op1", python_callable=lambda: "op3", dag=dag)
    op2 = DummyOperator(task_id="op2", dag=dag)
    op3 = DummyOperator(task_id="op3", dag=dag)
    op1 >> [op2, op3]

    ti2 = TaskInstance(op2, start_date)

    with create_session() as session:
        dep = NotPreviouslySkippedDep()
        assert len(list(dep.get_dep_statuses(ti2, session, DepContext()))) == 0
        assert dep.is_met(ti2, session)
        assert ti2.state == State.NONE
