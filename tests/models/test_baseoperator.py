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
import uuid
from datetime import date, datetime
from unittest import mock

import jinja2
import pendulum
import pytest
from parameterized import parameterized

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.lineage.entities import File
from airflow.models import DAG
from airflow.models.baseoperator import chain, cross_downstream
from airflow.operators.dummy import DummyOperator
from airflow.utils.decorators import apply_defaults
from tests.models import DEFAULT_DATE
from tests.test_utils.mock_operators import DeprecatedOperator, MockNamedTuple, MockOperator


class ClassWithCustomAttributes:
    """Class for testing purpose: allows to create objects with custom attributes in one single statement."""

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __str__(self):
        return f"{ClassWithCustomAttributes.__name__}({str(self.__dict__)})"

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not self.__eq__(other)


# Objects with circular references (for testing purpose)
object1 = ClassWithCustomAttributes(attr="{{ foo }}_1", template_fields=["ref"])
object2 = ClassWithCustomAttributes(attr="{{ foo }}_2", ref=object1, template_fields=["ref"])
setattr(object1, 'ref', object2)


class TestBaseOperator(unittest.TestCase):
    @parameterized.expand(
        [
            ("{{ foo }}", {"foo": "bar"}, "bar"),
            (["{{ foo }}_1", "{{ foo }}_2"], {"foo": "bar"}, ["bar_1", "bar_2"]),
            (("{{ foo }}_1", "{{ foo }}_2"), {"foo": "bar"}, ("bar_1", "bar_2")),
            (
                {"key1": "{{ foo }}_1", "key2": "{{ foo }}_2"},
                {"foo": "bar"},
                {"key1": "bar_1", "key2": "bar_2"},
            ),
            (
                {"key_{{ foo }}_1": 1, "key_2": "{{ foo }}_2"},
                {"foo": "bar"},
                {"key_{{ foo }}_1": 1, "key_2": "bar_2"},
            ),
            (date(2018, 12, 6), {"foo": "bar"}, date(2018, 12, 6)),
            (datetime(2018, 12, 6, 10, 55), {"foo": "bar"}, datetime(2018, 12, 6, 10, 55)),
            (MockNamedTuple("{{ foo }}_1", "{{ foo }}_2"), {"foo": "bar"}, MockNamedTuple("bar_1", "bar_2")),
            ({"{{ foo }}_1", "{{ foo }}_2"}, {"foo": "bar"}, {"bar_1", "bar_2"}),
            (None, {}, None),
            ([], {}, []),
            ({}, {}, {}),
            (
                # check nested fields can be templated
                ClassWithCustomAttributes(att1="{{ foo }}_1", att2="{{ foo }}_2", template_fields=["att1"]),
                {"foo": "bar"},
                ClassWithCustomAttributes(att1="bar_1", att2="{{ foo }}_2", template_fields=["att1"]),
            ),
            (
                # check deep nested fields can be templated
                ClassWithCustomAttributes(
                    nested1=ClassWithCustomAttributes(
                        att1="{{ foo }}_1", att2="{{ foo }}_2", template_fields=["att1"]
                    ),
                    nested2=ClassWithCustomAttributes(
                        att3="{{ foo }}_3", att4="{{ foo }}_4", template_fields=["att3"]
                    ),
                    template_fields=["nested1"],
                ),
                {"foo": "bar"},
                ClassWithCustomAttributes(
                    nested1=ClassWithCustomAttributes(
                        att1="bar_1", att2="{{ foo }}_2", template_fields=["att1"]
                    ),
                    nested2=ClassWithCustomAttributes(
                        att3="{{ foo }}_3", att4="{{ foo }}_4", template_fields=["att3"]
                    ),
                    template_fields=["nested1"],
                ),
            ),
            (
                # check null value on nested template field
                ClassWithCustomAttributes(att1=None, template_fields=["att1"]),
                {},
                ClassWithCustomAttributes(att1=None, template_fields=["att1"]),
            ),
            (
                # check there is no RecursionError on circular references
                object1,
                {"foo": "bar"},
                object1,
            ),
            # By default, Jinja2 drops one (single) trailing newline
            ("{{ foo }}\n\n", {"foo": "bar"}, "bar\n"),
        ]
    )
    def test_render_template(self, content, context, expected_output):
        """Test render_template given various input types."""
        with DAG("test-dag", start_date=DEFAULT_DATE):
            task = DummyOperator(task_id="op1")

        result = task.render_template(content, context)
        assert result == expected_output

    def test_render_template_fields(self):
        """Verify if operator attributes are correctly templated."""
        with DAG("test-dag", start_date=DEFAULT_DATE):
            task = MockOperator(task_id="op1", arg1="{{ foo }}", arg2="{{ bar }}")

        # Assert nothing is templated yet
        assert task.arg1 == "{{ foo }}"
        assert task.arg2 == "{{ bar }}"

        # Trigger templating and verify if attributes are templated correctly
        task.render_template_fields(context={"foo": "footemplated", "bar": "bartemplated"})
        assert task.arg1 == "footemplated"
        assert task.arg2 == "bartemplated"

    @parameterized.expand(
        [
            ({"user_defined_macros": {"foo": "bar"}}, "{{ foo }}", {}, "bar"),
            ({"user_defined_macros": {"foo": "bar"}}, 1, {}, 1),
            (
                {"user_defined_filters": {"hello": lambda name: f"Hello {name}"}},
                "{{ 'world' | hello }}",
                {},
                "Hello world",
            ),
        ]
    )
    def test_render_template_fields_with_dag_settings(self, dag_kwargs, content, context, expected_output):
        """Test render_template with additional DAG settings."""
        with DAG("test-dag", start_date=DEFAULT_DATE, **dag_kwargs):
            task = DummyOperator(task_id="op1")

        result = task.render_template(content, context)
        assert result == expected_output

    @parameterized.expand([(object(),), (uuid.uuid4(),)])
    def test_render_template_fields_no_change(self, content):
        """Tests if non-templatable types remain unchanged."""
        with DAG("test-dag", start_date=DEFAULT_DATE):
            task = DummyOperator(task_id="op1")

        result = task.render_template(content, {"foo": "bar"})
        assert content == result

    def test_render_template_field_undefined_default(self):
        """Test render_template with template_undefined unchanged."""
        with DAG("test-dag", start_date=DEFAULT_DATE):
            task = DummyOperator(task_id="op1")

        with pytest.raises(jinja2.UndefinedError):
            task.render_template("{{ foo }}", {})

    def test_render_template_field_undefined_strict(self):
        """Test render_template with template_undefined configured."""
        with DAG("test-dag", start_date=DEFAULT_DATE, template_undefined=jinja2.StrictUndefined):
            task = DummyOperator(task_id="op1")

        with pytest.raises(jinja2.UndefinedError):
            task.render_template("{{ foo }}", {})

    def test_render_template_field_undefined_not_strict(self):
        """Test render_template with template_undefined configured to silently error."""
        with DAG("test-dag", start_date=DEFAULT_DATE, template_undefined=jinja2.Undefined):
            task = DummyOperator(task_id="op1")

        assert task.render_template("{{ foo }}", {}) == ""

    def test_nested_template_fields_declared_must_exist(self):
        """Test render_template when a nested template field is missing."""
        with DAG("test-dag", start_date=DEFAULT_DATE):
            task = DummyOperator(task_id="op1")

        with pytest.raises(AttributeError) as ctx:
            task.render_template(ClassWithCustomAttributes(template_fields=["missing_field"]), {})

        assert "'ClassWithCustomAttributes' object has no attribute 'missing_field'" == str(ctx.value)

    def test_jinja_invalid_expression_is_just_propagated(self):
        """Test render_template propagates Jinja invalid expression errors."""
        with DAG("test-dag", start_date=DEFAULT_DATE):
            task = DummyOperator(task_id="op1")

        with pytest.raises(jinja2.exceptions.TemplateSyntaxError):
            task.render_template("{{ invalid expression }}", {})

    @mock.patch("jinja2.Environment", autospec=True)
    def test_jinja_env_creation(self, mock_jinja_env):
        """Verify if a Jinja environment is created only once when templating."""
        with DAG("test-dag", start_date=DEFAULT_DATE):
            task = MockOperator(task_id="op1", arg1="{{ foo }}", arg2="{{ bar }}")

        task.render_template_fields(context={"foo": "whatever", "bar": "whatever"})
        assert mock_jinja_env.call_count == 1

    def test_set_jinja_env_additional_option(self):
        """Test render_template given various input types."""
        with DAG(
            "test-dag", start_date=DEFAULT_DATE, jinja_environment_kwargs={'keep_trailing_newline': True}
        ):
            task = DummyOperator(task_id="op1")

        result = task.render_template("{{ foo }}\n\n", {"foo": "bar"})
        assert result == "bar\n\n"

    def test_override_jinja_env_option(self):
        """Test render_template given various input types."""
        with DAG("test-dag", start_date=DEFAULT_DATE, jinja_environment_kwargs={'cache_size': 50}):
            task = DummyOperator(task_id="op1")

        result = task.render_template("{{ foo }}", {"foo": "bar"})
        assert result == "bar"

    def test_default_resources(self):
        task = DummyOperator(task_id="default-resources")
        assert task.resources is None

    def test_custom_resources(self):
        task = DummyOperator(task_id="custom-resources", resources={"cpus": 1, "ram": 1024})
        assert task.resources.cpus.qty == 1
        assert task.resources.ram.qty == 1024

    def test_default_email_on_actions(self):
        test_task = DummyOperator(task_id='test_default_email_on_actions')
        assert test_task.email_on_retry is True
        assert test_task.email_on_failure is True

    def test_email_on_actions(self):
        test_task = DummyOperator(
            task_id='test_default_email_on_actions', email_on_retry=False, email_on_failure=True
        )
        assert test_task.email_on_retry is False
        assert test_task.email_on_failure is True

    @parameterized.expand([(True,), (False,)])
    def test_latest_only_enabled(self, latest_only_enabled):
        """
        When enabled should skip if outside latest interval and run otherwise.
        When disabled should not skip.
        """

        class MockDag:
            @staticmethod
            def following_schedule(execution_date):
                return execution_date.add(days=1)

        class MockDagRun:
            external_trigger = False

        op = DummyOperator(task_id='hello', latest_only=latest_only_enabled)

        execution_date = pendulum.now().add(days=-1)
        outside_of_latest = execution_date.add(hours=-36)
        in_latest = execution_date.add(hours=-12)
        base_context = {'dag': MockDag(), 'dag_run': MockDagRun()}
        if latest_only_enabled:
            with pytest.raises(AirflowSkipException):
                op.pre_execute({**base_context, 'execution_date': outside_of_latest})
            op.pre_execute({**base_context, 'execution_date': in_latest})
        else:
            op.pre_execute({**base_context, 'execution_date': outside_of_latest})

    def test_latest_only_no_check(self):
        """
        Should not skip under these circumstances, even when not latest and latest_only enabled:
            - the context dictionary has no dag run
            - the context dictionary has no dag
            - the ``context`` dictionary is ``None`` or empty
            - the dag run is externally triggered
        """

        class MockDag:
            @staticmethod
            def following_schedule(execution_date):
                return execution_date.add(days=1)

        class MockDagRun:
            def __init__(self, external_trigger=False):
                self.external_trigger = external_trigger

        op = DummyOperator(task_id='hello', latest_only=True)

        execution_date = pendulum.now().add(days=-1)
        outside_of_latest = execution_date.add(hours=-36)

        for context in [
            {
                'dag': MockDag(),
                'dag_run': MockDagRun(True),
                'execution_date': outside_of_latest,
            },  # external trigger
            {'dag_run': MockDagRun(), 'execution_date': outside_of_latest},  # missing dag
            {'dag': MockDag(), 'execution_date': outside_of_latest},  # missing dag_run
            {},  # empty
            None,  # None
        ]:
            op.pre_execute(context)  # should not skip


class TestBaseOperatorMethods(unittest.TestCase):
    def test_cross_downstream(self):
        """Test if all dependencies between tasks are all set correctly."""
        dag = DAG(dag_id="test_dag", start_date=datetime.now())
        start_tasks = [DummyOperator(task_id=f"t{i}", dag=dag) for i in range(1, 4)]
        end_tasks = [DummyOperator(task_id=f"t{i}", dag=dag) for i in range(4, 7)]
        cross_downstream(from_tasks=start_tasks, to_tasks=end_tasks)

        for start_task in start_tasks:
            assert set(start_task.get_direct_relatives(upstream=False)) == set(end_tasks)

    def test_chain(self):
        dag = DAG(dag_id='test_chain', start_date=datetime.now())
        [op1, op2, op3, op4, op5, op6] = [DummyOperator(task_id=f't{i}', dag=dag) for i in range(1, 7)]
        chain(op1, [op2, op3], [op4, op5], op6)

        assert {op2, op3} == set(op1.get_direct_relatives(upstream=False))
        assert [op4] == op2.get_direct_relatives(upstream=False)
        assert [op5] == op3.get_direct_relatives(upstream=False)
        assert {op4, op5} == set(op6.get_direct_relatives(upstream=True))

    def test_chain_not_support_type(self):
        dag = DAG(dag_id='test_chain', start_date=datetime.now())
        [op1, op2] = [DummyOperator(task_id=f't{i}', dag=dag) for i in range(1, 3)]
        with pytest.raises(TypeError):
            chain([op1, op2], 1)  # noqa

    def test_chain_different_length_iterable(self):
        dag = DAG(dag_id='test_chain', start_date=datetime.now())
        [op1, op2, op3, op4, op5] = [DummyOperator(task_id=f't{i}', dag=dag) for i in range(1, 6)]
        with pytest.raises(AirflowException):
            chain([op1, op2], [op3, op4, op5])

    def test_lineage_composition(self):
        """
        Test composition with lineage
        """
        inlet = File(url="in")
        outlet = File(url="out")
        dag = DAG("test-dag", start_date=DEFAULT_DATE)
        task1 = DummyOperator(task_id="op1", dag=dag)
        task2 = DummyOperator(task_id="op2", dag=dag)

        # mock
        task1.supports_lineage = True

        # note: operator precedence still applies
        inlet > task1 | (task2 > outlet)

        assert task1.get_inlet_defs() == [inlet]
        assert task2.get_inlet_defs() == [task1.task_id]
        assert task2.get_outlet_defs() == [outlet]

        fail = ClassWithCustomAttributes()
        with pytest.raises(TypeError):
            fail > task1
        with pytest.raises(TypeError):
            task1 > fail
        with pytest.raises(TypeError):
            fail | task1
        with pytest.raises(TypeError):
            task1 | fail

        task3 = DummyOperator(task_id="op3", dag=dag)
        extra = File(url="extra")
        [inlet, extra] > task3

        assert task3.get_inlet_defs() == [inlet, extra]

        task1.supports_lineage = False
        with pytest.raises(ValueError):
            task1 | task3

        assert task2.supports_lineage is False
        task2 | task3
        assert len(task3.get_inlet_defs()) == 3

        task4 = DummyOperator(task_id="op4", dag=dag)
        task4 > [inlet, outlet, extra]
        assert task4.get_outlet_defs() == [inlet, outlet, extra]

    def test_warnings_are_properly_propagated(self):
        with pytest.warns(DeprecationWarning) as warnings:
            DeprecatedOperator(task_id="test")
            assert len(warnings) == 1
            warning = warnings[0]
            # Here we check that the trace points to the place
            # where the deprecated class was used
            assert warning.filename == __file__


class CustomOp(DummyOperator):
    template_fields = ("field", "field2")

    @apply_defaults
    def __init__(self, field=None, field2=None, **kwargs):
        super().__init__(**kwargs)
        self.field = field
        self.field2 = field2

    def execute(self, context):
        self.field = None


class TestXComArgsRelationsAreResolved:
    def test_setattr_performs_no_custom_action_at_execute_time(self):
        op = CustomOp(task_id="test_task")
        op_copy = op.prepare_for_execution()

        with mock.patch("airflow.models.baseoperator.BaseOperator.set_xcomargs_dependencies") as method_mock:
            op_copy.execute({})
        assert method_mock.call_count == 0

    def test_upstream_is_set_when_template_field_is_xcomarg(self):
        with DAG("xcomargs_test", default_args={"start_date": datetime.today()}):
            op1 = DummyOperator(task_id="op1")
            op2 = CustomOp(task_id="op2", field=op1.output)

        assert op1 in op2.upstream_list
        assert op2 in op1.downstream_list

    def test_set_xcomargs_dependencies_works_recursively(self):
        with DAG("xcomargs_test", default_args={"start_date": datetime.today()}):
            op1 = DummyOperator(task_id="op1")
            op2 = DummyOperator(task_id="op2")
            op3 = CustomOp(task_id="op3", field=[op1.output, op2.output])
            op4 = CustomOp(task_id="op4", field={"op1": op1.output, "op2": op2.output})

        assert op1 in op3.upstream_list
        assert op2 in op3.upstream_list
        assert op1 in op4.upstream_list
        assert op2 in op4.upstream_list

    def test_set_xcomargs_dependencies_works_when_set_after_init(self):
        with DAG(dag_id='xcomargs_test', default_args={"start_date": datetime.today()}):
            op1 = DummyOperator(task_id="op1")
            op2 = CustomOp(task_id="op2")
            op2.field = op1.output  # value is set after init

        assert op1 in op2.upstream_list

    def test_set_xcomargs_dependencies_error_when_outside_dag(self):
        with pytest.raises(AirflowException):
            op1 = DummyOperator(task_id="op1")
            CustomOp(task_id="op2", field=op1.output)
