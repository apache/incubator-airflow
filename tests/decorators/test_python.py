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
import unittest.mock
from collections import namedtuple
from datetime import date, timedelta
from typing import Dict, Tuple

import pytest

from airflow.decorators import task as task_decorator
from airflow.exceptions import AirflowException
from airflow.models import DAG, DagRun, TaskInstance as TI
from airflow.models.xcom_arg import XComArg
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.task_group import TaskGroup
from airflow.utils.types import DagRunType

DEFAULT_DATE = timezone.datetime(2016, 1, 1)
END_DATE = timezone.datetime(2016, 1, 2)
INTERVAL = timedelta(hours=12)
FROZEN_NOW = timezone.datetime(2016, 1, 2, 12, 1, 1)

TI_CONTEXT_ENV_VARS = [
    'AIRFLOW_CTX_DAG_ID',
    'AIRFLOW_CTX_TASK_ID',
    'AIRFLOW_CTX_EXECUTION_DATE',
    'AIRFLOW_CTX_DAG_RUN_ID',
]


class Call:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


def build_recording_function(calls_collection):
    """
    We can not use a Mock instance as a PythonOperator callable function or some tests fail with a
    TypeError: Object of type Mock is not JSON serializable
    Then using this custom function recording custom Call objects for further testing
    (replacing Mock.assert_called_with assertion method)
    """

    def recording_function(*args, **kwargs):
        calls_collection.append(Call(*args, **kwargs))

    return recording_function


class TestPythonBase(unittest.TestCase):
    """Base test class for TestPythonOperator and TestPythonSensor classes"""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def setUp(self):
        super().setUp()
        self.dag = DAG('test_dag', default_args={'owner': 'airflow', 'start_date': DEFAULT_DATE})
        self.addCleanup(self.dag.clear)
        self.clear_run()
        self.addCleanup(self.clear_run)

    def tearDown(self):
        super().tearDown()

        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def clear_run(self):
        self.run = False

    def _assert_calls_equal(self, first, second):
        assert isinstance(first, Call)
        assert isinstance(second, Call)
        assert first.args == second.args
        # eliminate context (conf, dag_run, task_instance, etc.)
        test_args = ["an_int", "a_date", "a_templated_string"]
        first.kwargs = {key: value for (key, value) in first.kwargs.items() if key in test_args}
        second.kwargs = {key: value for (key, value) in second.kwargs.items() if key in test_args}
        assert first.kwargs == second.kwargs


class TestAirflowTaskDecorator(TestPythonBase):
    def test_python_operator_python_callable_is_callable(self):
        """Tests that @task will only instantiate if
        the python_callable argument is callable."""
        not_callable = {}
        with pytest.raises(AirflowException):
            task_decorator(not_callable, dag=self.dag)

    def test_infer_multiple_outputs_using_typing(self):
        @task_decorator
        def identity_dict(x: int, y: int) -> Dict[str, int]:
            return {"x": x, "y": y}

        assert identity_dict(5, 5).operator.multiple_outputs is True  # pylint: disable=maybe-no-member

        @task_decorator
        def identity_tuple(x: int, y: int) -> Tuple[int, int]:
            return x, y

        assert identity_tuple(5, 5).operator.multiple_outputs is False  # pylint: disable=maybe-no-member

        @task_decorator
        def identity_int(x: int) -> int:
            return x

        assert identity_int(5).operator.multiple_outputs is False  # pylint: disable=maybe-no-member

        @task_decorator
        def identity_notyping(x: int):
            return x

        assert identity_notyping(5).operator.multiple_outputs is False  # pylint: disable=maybe-no-member

    def test_manual_multiple_outputs_false_with_typings(self):
        @task_decorator(multiple_outputs=False)
        def identity2(x: int, y: int) -> Dict[int, int]:
            return (x, y)

        with self.dag:
            res = identity2(8, 4)

        dr = self.dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        res.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)  # pylint: disable=maybe-no-member

        ti = dr.get_task_instances()[0]

        assert res.operator.multiple_outputs is False  # pylint: disable=maybe-no-member
        assert ti.xcom_pull() == [8, 4]  # pylint: disable=maybe-no-member
        assert ti.xcom_pull(key="return_value_0") is None
        assert ti.xcom_pull(key="return_value_1") is None

    def test_multiple_outputs_ignore_typing(self):
        @task_decorator
        def identity_tuple(x: int, y: int) -> Tuple[int, int]:
            return x, y

        with self.dag:
            ident = identity_tuple(35, 36)

        dr = self.dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        ident.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)  # pylint: disable=maybe-no-member

        ti = dr.get_task_instances()[0]

        assert not ident.operator.multiple_outputs  # pylint: disable=maybe-no-member
        assert ti.xcom_pull() == [35, 36]
        assert ti.xcom_pull(key="return_value_0") is None
        assert ti.xcom_pull(key="return_value_1") is None

    def test_fails_bad_signature(self):
        """Tests that @task will fail if signature is not binding."""

        @task_decorator
        def add_number(num: int) -> int:
            return num + 2

        with pytest.raises(TypeError):
            add_number(2, 3)  # pylint: disable=too-many-function-args
        with pytest.raises(TypeError):
            add_number()  # pylint: disable=no-value-for-parameter
        add_number('test')  # pylint: disable=no-value-for-parameter

    def test_fail_method(self):
        """Tests that @task will fail if signature is not binding."""

        with pytest.raises(AirflowException):

            class Test:
                num = 2

                @task_decorator
                def add_number(self, num: int) -> int:
                    return self.num + num

            Test().add_number(2)

    def test_fail_multiple_outputs_key_type(self):
        @task_decorator(multiple_outputs=True)
        def add_number(num: int):
            return {2: num}

        with self.dag:
            ret = add_number(2)
        self.dag.create_dagrun(
            run_id=DagRunType.MANUAL,
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        with pytest.raises(AirflowException):
            # pylint: disable=maybe-no-member
            ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_fail_multiple_outputs_no_dict(self):
        @task_decorator(multiple_outputs=True)
        def add_number(num: int):
            return num

        with self.dag:
            ret = add_number(2)
        self.dag.create_dagrun(
            run_id=DagRunType.MANUAL,
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        with pytest.raises(AirflowException):
            # pylint: disable=maybe-no-member
            ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_python_callable_arguments_are_templatized(self):
        """Test @task op_args are templatized"""
        recorded_calls = []

        # Create a named tuple and ensure it is still preserved
        # after the rendering is done
        Named = namedtuple('Named', ['var1', 'var2'])
        named_tuple = Named('{{ ds }}', 'unchanged')

        task = task_decorator(
            # a Mock instance cannot be used as a callable function or test fails with a
            # TypeError: Object of type Mock is not JSON serializable
            build_recording_function(recorded_calls),
            dag=self.dag,
        )
        ret = task(4, date(2019, 1, 1), "dag {{dag.dag_id}} ran on {{ds}}.", named_tuple)

        self.dag.create_dagrun(
            run_id=DagRunType.MANUAL,
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)  # pylint: disable=maybe-no-member

        ds_templated = DEFAULT_DATE.date().isoformat()
        assert len(recorded_calls) == 1
        self._assert_calls_equal(
            recorded_calls[0],
            Call(
                4,
                date(2019, 1, 1),
                f"dag {self.dag.dag_id} ran on {ds_templated}.",
                Named(ds_templated, 'unchanged'),
            ),
        )

    def test_python_callable_keyword_arguments_are_templatized(self):
        """Test PythonOperator op_kwargs are templatized"""
        recorded_calls = []

        task = task_decorator(
            # a Mock instance cannot be used as a callable function or test fails with a
            # TypeError: Object of type Mock is not JSON serializable
            build_recording_function(recorded_calls),
            dag=self.dag,
        )
        ret = task(an_int=4, a_date=date(2019, 1, 1), a_templated_string="dag {{dag.dag_id}} ran on {{ds}}.")
        self.dag.create_dagrun(
            run_id=DagRunType.MANUAL,
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)  # pylint: disable=maybe-no-member

        assert len(recorded_calls) == 1
        self._assert_calls_equal(
            recorded_calls[0],
            Call(
                an_int=4,
                a_date=date(2019, 1, 1),
                a_templated_string="dag {} ran on {}.".format(
                    self.dag.dag_id, DEFAULT_DATE.date().isoformat()
                ),
            ),
        )

    def test_manual_task_id(self):
        """Test manually setting task_id"""

        @task_decorator(task_id='some_name')
        def do_run():
            return 4

        with self.dag:
            do_run()
            assert ['some_name'] == self.dag.task_ids

    def test_multiple_calls(self):
        """Test calling task multiple times in a DAG"""

        @task_decorator
        def do_run():
            return 4

        with self.dag:
            do_run()
            assert ['do_run'] == self.dag.task_ids
            do_run_1 = do_run()
            do_run_2 = do_run()
            assert ['do_run', 'do_run__1', 'do_run__2'] == self.dag.task_ids

        assert do_run_1.operator.task_id == 'do_run__1'  # pylint: disable=maybe-no-member
        assert do_run_2.operator.task_id == 'do_run__2'  # pylint: disable=maybe-no-member

    def test_multiple_calls_in_task_group(self):
        """Test calling task multiple times in a TaskGroup"""

        @task_decorator
        def do_run():
            return 4

        group_id = "KnightsOfNii"
        with self.dag:
            with TaskGroup(group_id=group_id):
                do_run()
                assert [f"{group_id}.do_run"] == self.dag.task_ids
                do_run()
                assert [f"{group_id}.do_run", f"{group_id}.do_run__1"] == self.dag.task_ids

        assert len(self.dag.task_ids) == 2

    def test_call_20(self):
        """Test calling decorated function 21 times in a DAG"""

        @task_decorator
        def __do_run():
            return 4

        with self.dag:
            __do_run()
            for _ in range(20):
                __do_run()

        assert self.dag.task_ids[-1] == '__do_run__20'

    def test_multiple_outputs(self):
        """Tests pushing multiple outputs as a dictionary"""

        @task_decorator(multiple_outputs=True)
        def return_dict(number: int):
            return {'number': number + 1, '43': 43}

        test_number = 10
        with self.dag:
            ret = return_dict(test_number)

        dr = self.dag.create_dagrun(
            run_id=DagRunType.MANUAL,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)  # pylint: disable=maybe-no-member

        ti = dr.get_task_instances()[0]
        assert ti.xcom_pull(key='number') == test_number + 1
        assert ti.xcom_pull(key='43') == 43
        assert ti.xcom_pull() == {'number': test_number + 1, '43': 43}

    def test_default_args(self):
        """Test that default_args are captured when calling the function correctly"""

        @task_decorator
        def do_run():
            return 4

        with self.dag:
            ret = do_run()
        assert ret.operator.owner == 'airflow'  # pylint: disable=maybe-no-member

    def test_xcom_arg(self):
        """Tests that returned key in XComArg is returned correctly"""

        @task_decorator
        def add_2(number: int):
            return number + 2

        @task_decorator
        def add_num(number: int, num2: int = 2):
            return number + num2

        test_number = 10

        with self.dag:
            bigger_number = add_2(test_number)
            ret = add_num(bigger_number, XComArg(bigger_number.operator))  # pylint: disable=maybe-no-member

        dr = self.dag.create_dagrun(
            run_id=DagRunType.MANUAL,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        bigger_number.operator.run(  # pylint: disable=maybe-no-member
            start_date=DEFAULT_DATE, end_date=DEFAULT_DATE
        )
        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)  # pylint: disable=maybe-no-member
        ti_add_num = [ti for ti in dr.get_task_instances() if ti.task_id == 'add_num'][0]
        assert ti_add_num.xcom_pull(key=ret.key) == (test_number + 2) * 2  # pylint: disable=maybe-no-member

    def test_dag_task(self):
        """Tests dag.task property to generate task"""

        @self.dag.task
        def add_2(number: int):
            return number + 2

        test_number = 10
        res = add_2(test_number)
        add_2(res)

        assert 'add_2' in self.dag.task_ids

    def test_dag_task_multiple_outputs(self):
        """Tests dag.task property to generate task with multiple outputs"""

        @self.dag.task(multiple_outputs=True)
        def add_2(number: int):
            return {'1': number + 2, '2': 42}

        test_number = 10
        add_2(test_number)
        add_2(test_number)

        assert 'add_2' in self.dag.task_ids

    def test_task_documentation(self):
        """Tests that task_decorator loads doc_md from function doc"""

        @task_decorator
        def add_2(number: int):
            """
            Adds 2 to number.
            """
            return number + 2

        test_number = 10
        with self.dag:
            ret = add_2(test_number)

        assert ret.operator.doc_md.strip(), "Adds 2 to number."  # pylint: disable=maybe-no-member
