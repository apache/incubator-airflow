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

"""Unit tests for stringified DAGs."""

import importlib
import importlib.util
import multiprocessing
import os
import unittest
from datetime import datetime, timedelta, timezone
from glob import glob
from unittest import mock

import pytest
from dateutil.relativedelta import FR, relativedelta
from kubernetes.client import models as k8s
from parameterized import parameterized

from airflow.hooks.base import BaseHook
from airflow.kubernetes.pod_generator import PodGenerator
from airflow.models import DAG, Connection, DagBag, TaskInstance
from airflow.models.baseoperator import BaseOperator, BaseOperatorLink
from airflow.operators.bash import BashOperator
from airflow.security import permissions
from airflow.serialization.json_schema import load_dag_schema_dict
from airflow.serialization.serialized_objects import SerializedBaseOperator, SerializedDAG
from tests.test_utils.mock_operators import CustomOperator, CustomOpLink, GoogleLink

executor_config_pod = k8s.V1Pod(
    metadata=k8s.V1ObjectMeta(name="my-name"),
    spec=k8s.V1PodSpec(
        containers=[
            k8s.V1Container(name="base", volume_mounts=[k8s.V1VolumeMount(name="my-vol", mount_path="/vol/")])
        ]
    ),
)

serialized_simple_dag_ground_truth = {
    "__version": 1,
    "dag": {
        "default_args": {
            "__type": "dict",
            "__var": {
                "depends_on_past": False,
                "retries": 1,
                "retry_delay": {"__type": "timedelta", "__var": 300.0},
                "max_retry_delay": {"__type": "timedelta", "__var": 600.0},
                "sla": {"__type": "timedelta", "__var": 100.0},
            },
        },
        "start_date": 1564617600.0,
        '_task_group': {
            '_group_id': None,
            'prefix_group_id': True,
            'children': {'bash_task': ('operator', 'bash_task'), 'custom_task': ('operator', 'custom_task')},
            'tooltip': '',
            'ui_color': 'CornflowerBlue',
            'ui_fgcolor': '#000',
            'upstream_group_ids': [],
            'downstream_group_ids': [],
            'upstream_task_ids': [],
            'downstream_task_ids': [],
        },
        "is_paused_upon_creation": False,
        "_dag_id": "simple_dag",
        "fileloc": None,
        "tasks": [
            {
                "task_id": "bash_task",
                "owner": "airflow",
                "retries": 1,
                "retry_delay": 300.0,
                "max_retry_delay": 600.0,
                "sla": 100.0,
                "_downstream_task_ids": [],
                "_inlets": [],
                "_is_dummy": False,
                "_outlets": [],
                "ui_color": "#f0ede4",
                "ui_fgcolor": "#000",
                "template_fields": ['bash_command', 'env'],
                "template_fields_renderers": {'bash_command': 'bash', 'env': 'json'},
                "bash_command": "echo {{ task.task_id }}",
                'label': 'bash_task',
                "_task_type": "BashOperator",
                "_task_module": "airflow.operators.bash",
                "pool": "default_pool",
                "executor_config": {
                    '__type': 'dict',
                    '__var': {
                        "pod_override": {
                            '__type': 'k8s.V1Pod',
                            '__var': PodGenerator.serialize_pod(executor_config_pod),
                        }
                    },
                },
            },
            {
                "task_id": "custom_task",
                "retries": 1,
                "retry_delay": 300.0,
                "max_retry_delay": 600.0,
                "sla": 100.0,
                "_downstream_task_ids": [],
                "_inlets": [],
                "_is_dummy": False,
                "_outlets": [],
                "_operator_extra_links": [{"tests.test_utils.mock_operators.CustomOpLink": {}}],
                "ui_color": "#fff",
                "ui_fgcolor": "#000",
                "template_fields": ['bash_command'],
                "template_fields_renderers": {},
                "_task_type": "CustomOperator",
                "_task_module": "tests.test_utils.mock_operators",
                "pool": "default_pool",
                'label': 'custom_task',
            },
        ],
        "timezone": "UTC",
        "_access_control": {
            "__type": "dict",
            "__var": {
                "test_role": {
                    "__type": "set",
                    "__var": [permissions.ACTION_CAN_READ, permissions.ACTION_CAN_EDIT],
                }
            },
        },
    },
}

ROOT_FOLDER = os.path.realpath(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, os.pardir)
)


def make_example_dags(module_path):
    """Loads DAGs from a module for test."""
    dagbag = DagBag(module_path)
    return dagbag.dags


def make_simple_dag():
    """Make very simple DAG to verify serialization result."""
    with DAG(
        dag_id='simple_dag',
        default_args={
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "max_retry_delay": timedelta(minutes=10),
            "depends_on_past": False,
            "sla": timedelta(seconds=100),
        },
        start_date=datetime(2019, 8, 1),
        is_paused_upon_creation=False,
        access_control={"test_role": {permissions.ACTION_CAN_READ, permissions.ACTION_CAN_EDIT}},
    ) as dag:
        CustomOperator(task_id='custom_task')
        BashOperator(
            task_id='bash_task',
            bash_command='echo {{ task.task_id }}',
            owner='airflow',
            executor_config={"pod_override": executor_config_pod},
        )
        return {'simple_dag': dag}


def make_user_defined_macro_filter_dag():
    """Make DAGs with user defined macros and filters using locally defined methods.

    For Webserver, we do not include ``user_defined_macros`` & ``user_defined_filters``.

    The examples here test:
        (1) functions can be successfully displayed on UI;
        (2) templates with function macros have been rendered before serialization.
    """

    def compute_next_execution_date(dag, execution_date):
        return dag.following_schedule(execution_date)

    default_args = {'start_date': datetime(2019, 7, 10)}
    dag = DAG(
        'user_defined_macro_filter_dag',
        default_args=default_args,
        user_defined_macros={
            'next_execution_date': compute_next_execution_date,
        },
        user_defined_filters={'hello': lambda name: f'Hello {name}'},
        catchup=False,
    )
    BashOperator(
        task_id='echo',
        bash_command='echo "{{ next_execution_date(dag, execution_date) }}"',
        dag=dag,
    )
    return {dag.dag_id: dag}


def collect_dags(dag_folder=None):
    """Collects DAGs to test."""
    dags = {}
    dags.update(make_simple_dag())
    dags.update(make_user_defined_macro_filter_dag())

    if dag_folder:
        if isinstance(dag_folder, (list, tuple)):
            patterns = dag_folder
        else:
            patterns = [dag_folder]
    else:
        patterns = [
            "airflow/example_dags",
            "airflow/providers/*/example_dags",
            "airflow/providers/*/*/example_dags",
        ]
    for pattern in patterns:
        for directory in glob(f"{ROOT_FOLDER}/{pattern}"):
            dags.update(make_example_dags(directory))

    # Filter subdags as they are stored in same row in Serialized Dag table
    dags = {dag_id: dag for dag_id, dag in dags.items() if not dag.is_subdag}
    return dags


def serialize_subprocess(queue, dag_folder):
    """Validate pickle in a subprocess."""
    dags = collect_dags(dag_folder)
    for dag in dags.values():
        queue.put(SerializedDAG.to_json(dag))
    queue.put(None)


class TestStringifiedDAGs(unittest.TestCase):
    """Unit tests for stringified DAGs."""

    def setUp(self):
        super().setUp()
        BaseHook.get_connection = mock.Mock(
            return_value=Connection(
                extra=(
                    '{'
                    '"project_id": "mock", '
                    '"location": "mock", '
                    '"instance": "mock", '
                    '"database_type": "postgres", '
                    '"use_proxy": "False", '
                    '"use_ssl": "False"'
                    '}'
                )
            )
        )
        self.maxDiff = None  # pylint: disable=invalid-name

    def test_serialization(self):
        """Serialization and deserialization should work for every DAG and Operator."""
        dags = collect_dags()
        serialized_dags = {}
        for _, v in dags.items():
            dag = SerializedDAG.to_dict(v)
            SerializedDAG.validate_schema(dag)
            serialized_dags[v.dag_id] = dag

        # Compares with the ground truth of JSON string.
        self.validate_serialized_dag(serialized_dags['simple_dag'], serialized_simple_dag_ground_truth)

    def validate_serialized_dag(self, json_dag, ground_truth_dag):
        """Verify serialized DAGs match the ground truth."""
        assert json_dag['dag']['fileloc'].split('/')[-1] == 'test_dag_serialization.py'
        json_dag['dag']['fileloc'] = None

        def sorted_serialized_dag(dag_dict: dict):
            """
            Sorts the "tasks" list and "access_control" permissions in the
            serialised dag python dictionary. This is needed as the order of
            items should not matter but assertEqual would fail if the order of
            items changes in the dag dictionary
            """
            dag_dict["dag"]["tasks"] = sorted(dag_dict["dag"]["tasks"], key=lambda x: sorted(x.keys()))
            dag_dict["dag"]["_access_control"]["__var"]["test_role"]["__var"] = sorted(
                dag_dict["dag"]["_access_control"]["__var"]["test_role"]["__var"]
            )
            return dag_dict

        assert sorted_serialized_dag(ground_truth_dag) == sorted_serialized_dag(json_dag)

    @pytest.mark.quarantined
    def test_deserialization_across_process(self):
        """A serialized DAG can be deserialized in another process."""

        # Since we need to parse the dags twice here (once in the subprocess,
        # and once here to get a DAG to compare to) we don't want to load all
        # dags.
        queue = multiprocessing.Queue()
        proc = multiprocessing.Process(target=serialize_subprocess, args=(queue, "airflow/example_dags"))
        proc.daemon = True
        proc.start()

        stringified_dags = {}
        while True:
            v = queue.get()
            if v is None:
                break
            dag = SerializedDAG.from_json(v)
            assert isinstance(dag, DAG)
            stringified_dags[dag.dag_id] = dag

        dags = collect_dags("airflow/example_dags")
        assert set(stringified_dags.keys()) == set(dags.keys())

        # Verify deserialized DAGs.
        for dag_id in stringified_dags:
            self.validate_deserialized_dag(stringified_dags[dag_id], dags[dag_id])

    def test_roundtrip_provider_example_dags(self):
        dags = collect_dags(
            [
                "airflow/providers/*/example_dags",
                "airflow/providers/*/*/example_dags",
            ]
        )

        # Verify deserialized DAGs.
        for dag in dags.values():
            serialized_dag = SerializedDAG.from_json(SerializedDAG.to_json(dag))
            self.validate_deserialized_dag(serialized_dag, dag)

    def validate_deserialized_dag(self, serialized_dag, dag):
        """
        Verify that all example DAGs work with DAG Serialization by
        checking fields between Serialized Dags & non-Serialized Dags
        """
        fields_to_check = dag.get_serialized_fields() - {
            # Doesn't implement __eq__ properly. Check manually
            'timezone',
            # Need to check fields in it, to exclude functions
            'default_args',
            "_task_group",
        }
        for field in fields_to_check:
            dag_field = getattr(dag, field)
            if isinstance(dag_field, list):
                dag_field = sorted(dag_field)
            assert getattr(serialized_dag, field) == dag_field, f'{dag.dag_id}.{field} does not match'

        if dag.default_args:
            for k, v in dag.default_args.items():
                if callable(v):
                    # Check we stored _something_.
                    assert k in serialized_dag.default_args
                else:
                    assert (
                        v == serialized_dag.default_args[k]
                    ), f'{dag.dag_id}.default_args[{k}] does not match'

        assert serialized_dag.timezone.name == dag.timezone.name

        for task_id in dag.task_ids:
            self.validate_deserialized_task(serialized_dag.get_task(task_id), dag.get_task(task_id))

        # Verify that the DAG object has 'full_filepath' attribute
        # and is equal to fileloc
        assert serialized_dag.full_filepath == dag.fileloc

    def validate_deserialized_task(
        self,
        serialized_task,
        task,
    ):
        """Verify non-airflow operators are casted to BaseOperator."""
        assert isinstance(serialized_task, SerializedBaseOperator)
        assert not isinstance(task, SerializedBaseOperator)
        assert isinstance(task, BaseOperator)

        fields_to_check = task.get_serialized_fields() - {
            # Checked separately
            '_task_type',
            'subdag',
            # Type is excluded, so don't check it
            '_log',
            # List vs tuple. Check separately
            'template_fields',
            # We store the string, real dag has the actual code
            'on_failure_callback',
            'on_success_callback',
            'on_retry_callback',
            # Checked separately
            'resources',
        }

        assert serialized_task.task_type == task.task_type
        assert set(serialized_task.template_fields) == set(task.template_fields)

        assert serialized_task.upstream_task_ids == task.upstream_task_ids
        assert serialized_task.downstream_task_ids == task.downstream_task_ids

        for field in fields_to_check:
            assert getattr(serialized_task, field) == getattr(
                task, field
            ), f'{task.dag.dag_id}.{task.task_id}.{field} does not match'

        if serialized_task.resources is None:
            assert task.resources is None or task.resources == []
        else:
            assert serialized_task.resources == task.resources

        # Check that for Deserialised task, task.subdag is None for all other Operators
        # except for the SubDagOperator where task.subdag is an instance of DAG object
        if task.task_type == "SubDagOperator":
            assert serialized_task.subdag is not None
            assert isinstance(serialized_task.subdag, DAG)
        else:
            assert serialized_task.subdag is None

    @parameterized.expand(
        [
            (datetime(2019, 8, 1, tzinfo=timezone.utc), None, datetime(2019, 8, 1, tzinfo=timezone.utc)),
            (
                datetime(2019, 8, 1, tzinfo=timezone.utc),
                datetime(2019, 8, 2, tzinfo=timezone.utc),
                datetime(2019, 8, 2, tzinfo=timezone.utc),
            ),
            (
                datetime(2019, 8, 1, tzinfo=timezone.utc),
                datetime(2019, 7, 30, tzinfo=timezone.utc),
                datetime(2019, 8, 1, tzinfo=timezone.utc),
            ),
        ]
    )
    def test_deserialization_start_date(self, dag_start_date, task_start_date, expected_task_start_date):
        dag = DAG(dag_id='simple_dag', start_date=dag_start_date)
        BaseOperator(task_id='simple_task', dag=dag, start_date=task_start_date)

        serialized_dag = SerializedDAG.to_dict(dag)
        if not task_start_date or dag_start_date >= task_start_date:
            # If dag.start_date > task.start_date -> task.start_date=dag.start_date
            # because of the logic in dag.add_task()
            assert "start_date" not in serialized_dag["dag"]["tasks"][0]
        else:
            assert "start_date" in serialized_dag["dag"]["tasks"][0]

        dag = SerializedDAG.from_dict(serialized_dag)
        simple_task = dag.task_dict["simple_task"]
        assert simple_task.start_date == expected_task_start_date

    def test_deserialization_with_dag_context(self):
        with DAG(dag_id='simple_dag', start_date=datetime(2019, 8, 1, tzinfo=timezone.utc)) as dag:
            BaseOperator(task_id='simple_task')
            # should not raise RuntimeError: dictionary changed size during iteration
            SerializedDAG.to_dict(dag)

    @parameterized.expand(
        [
            (datetime(2019, 8, 1, tzinfo=timezone.utc), None, datetime(2019, 8, 1, tzinfo=timezone.utc)),
            (
                datetime(2019, 8, 1, tzinfo=timezone.utc),
                datetime(2019, 8, 2, tzinfo=timezone.utc),
                datetime(2019, 8, 1, tzinfo=timezone.utc),
            ),
            (
                datetime(2019, 8, 1, tzinfo=timezone.utc),
                datetime(2019, 7, 30, tzinfo=timezone.utc),
                datetime(2019, 7, 30, tzinfo=timezone.utc),
            ),
        ]
    )
    def test_deserialization_end_date(self, dag_end_date, task_end_date, expected_task_end_date):
        dag = DAG(dag_id='simple_dag', start_date=datetime(2019, 8, 1), end_date=dag_end_date)
        BaseOperator(task_id='simple_task', dag=dag, end_date=task_end_date)

        serialized_dag = SerializedDAG.to_dict(dag)
        if not task_end_date or dag_end_date <= task_end_date:
            # If dag.end_date < task.end_date -> task.end_date=dag.end_date
            # because of the logic in dag.add_task()
            assert "end_date" not in serialized_dag["dag"]["tasks"][0]
        else:
            assert "end_date" in serialized_dag["dag"]["tasks"][0]

        dag = SerializedDAG.from_dict(serialized_dag)
        simple_task = dag.task_dict["simple_task"]
        assert simple_task.end_date == expected_task_end_date

    @parameterized.expand(
        [
            (None, None, None),
            ("@weekly", "@weekly", "0 0 * * 0"),
            ("@once", "@once", None),
            ({"__type": "timedelta", "__var": 86400.0}, timedelta(days=1), timedelta(days=1)),
        ]
    )
    def test_deserialization_schedule_interval(
        self, serialized_schedule_interval, expected_schedule_interval, expected_n_schedule_interval
    ):
        serialized = {
            "__version": 1,
            "dag": {
                "default_args": {"__type": "dict", "__var": {}},
                "_dag_id": "simple_dag",
                "fileloc": __file__,
                "tasks": [],
                "timezone": "UTC",
                "schedule_interval": serialized_schedule_interval,
            },
        }

        SerializedDAG.validate_schema(serialized)

        dag = SerializedDAG.from_dict(serialized)

        assert dag.schedule_interval == expected_schedule_interval
        assert dag.normalized_schedule_interval == expected_n_schedule_interval

    @parameterized.expand(
        [
            (relativedelta(days=-1), {"__type": "relativedelta", "__var": {"days": -1}}),
            (relativedelta(month=1, days=-1), {"__type": "relativedelta", "__var": {"month": 1, "days": -1}}),
            # Every friday
            (relativedelta(weekday=FR), {"__type": "relativedelta", "__var": {"weekday": [4]}}),
            # Every second friday
            (relativedelta(weekday=FR(2)), {"__type": "relativedelta", "__var": {"weekday": [4, 2]}}),
        ]
    )
    def test_roundtrip_relativedelta(self, val, expected):
        serialized = SerializedDAG._serialize(val)
        assert serialized == expected

        round_tripped = SerializedDAG._deserialize(serialized)
        assert val == round_tripped

    @parameterized.expand(
        [
            (None, {}),
            ({"param_1": "value_1"}, {"param_1": "value_1"}),
        ]
    )
    def test_dag_params_roundtrip(self, val, expected_val):
        """
        Test that params work both on Serialized DAGs & Tasks
        """
        dag = DAG(dag_id='simple_dag', params=val)
        BaseOperator(task_id='simple_task', dag=dag, start_date=datetime(2019, 8, 1))

        serialized_dag = SerializedDAG.to_dict(dag)
        if val:
            assert "params" in serialized_dag["dag"]
        else:
            assert "params" not in serialized_dag["dag"]

        deserialized_dag = SerializedDAG.from_dict(serialized_dag)
        deserialized_simple_task = deserialized_dag.task_dict["simple_task"]
        assert expected_val == deserialized_dag.params
        assert expected_val == deserialized_simple_task.params

    @parameterized.expand(
        [
            (None, {}),
            ({"param_1": "value_1"}, {"param_1": "value_1"}),
        ]
    )
    def test_task_params_roundtrip(self, val, expected_val):
        """
        Test that params work both on Serialized DAGs & Tasks
        """
        dag = DAG(dag_id='simple_dag')
        BaseOperator(task_id='simple_task', dag=dag, params=val, start_date=datetime(2019, 8, 1))

        serialized_dag = SerializedDAG.to_dict(dag)
        if val:
            assert "params" in serialized_dag["dag"]["tasks"][0]
        else:
            assert "params" not in serialized_dag["dag"]["tasks"][0]

        deserialized_dag = SerializedDAG.from_dict(serialized_dag)
        deserialized_simple_task = deserialized_dag.task_dict["simple_task"]
        assert expected_val == deserialized_simple_task.params

    def test_extra_serialized_field_and_operator_links(self):
        """
        Assert extra field exists & OperatorLinks defined in Plugins and inbuilt Operator Links.

        This tests also depends on GoogleLink() registered as a plugin
        in tests/plugins/test_plugin.py

        The function tests that if extra operator links are registered in plugin
        in ``operator_extra_links`` and the same is also defined in
        the Operator in ``BaseOperator.operator_extra_links``, it has the correct
        extra link.
        """
        test_date = datetime(2019, 8, 1)
        dag = DAG(dag_id='simple_dag', start_date=test_date)
        CustomOperator(task_id='simple_task', dag=dag, bash_command="true")

        serialized_dag = SerializedDAG.to_dict(dag)
        assert "bash_command" in serialized_dag["dag"]["tasks"][0]

        dag = SerializedDAG.from_dict(serialized_dag)
        simple_task = dag.task_dict["simple_task"]
        assert getattr(simple_task, "bash_command") == "true"

        #########################################################
        # Verify Operator Links work with Serialized Operator
        #########################################################
        # Check Serialized version of operator link only contains the inbuilt Op Link
        assert serialized_dag["dag"]["tasks"][0]["_operator_extra_links"] == [
            {'tests.test_utils.mock_operators.CustomOpLink': {}}
        ]

        # Test all the extra_links are set
        assert set(simple_task.extra_links) == {'Google Custom', 'airflow', 'github', 'google'}

        ti = TaskInstance(task=simple_task, execution_date=test_date)
        ti.xcom_push('search_query', "dummy_value_1")

        # Test Deserialized inbuilt link
        custom_inbuilt_link = simple_task.get_extra_links(test_date, CustomOpLink.name)
        assert 'http://google.com/custom_base_link?search=dummy_value_1' == custom_inbuilt_link

        # Test Deserialized link registered via Airflow Plugin
        google_link_from_plugin = simple_task.get_extra_links(test_date, GoogleLink.name)
        assert "https://www.google.com" == google_link_from_plugin

    def test_extra_operator_links_logs_error_for_non_registered_extra_links(self):
        """
        Assert OperatorLinks not registered via Plugins and if it is not an inbuilt Operator Link,
        it can still deserialize the DAG (does not error) but just logs an error
        """

        class TaskStateLink(BaseOperatorLink):
            """OperatorLink not registered via Plugins nor a built-in OperatorLink"""

            name = 'My Link'

            def get_link(self, operator, dttm):
                return 'https://www.google.com'

        class MyOperator(BaseOperator):
            """Just a DummyOperator using above defined Extra Operator Link"""

            operator_extra_links = [TaskStateLink()]

            def execute(self, context):
                pass

        with DAG(dag_id='simple_dag', start_date=datetime(2019, 8, 1)) as dag:
            MyOperator(task_id='blah')

        serialized_dag = SerializedDAG.to_dict(dag)

        with self.assertLogs("airflow.serialization.serialized_objects", level="ERROR") as log_output:
            SerializedDAG.from_dict(serialized_dag)
            received_logs = log_output.output[0]
            expected_err_msg = (
                "Operator Link class 'tests.serialization.test_dag_serialization.TaskStateLink' "
                "not registered"
            )
            assert expected_err_msg in received_logs

    def test_extra_serialized_field_and_multiple_operator_links(self):
        """
        Assert extra field exists & OperatorLinks defined in Plugins and inbuilt Operator Links.

        This tests also depends on GoogleLink() registered as a plugin
        in tests/plugins/test_plugin.py

        The function tests that if extra operator links are registered in plugin
        in ``operator_extra_links`` and the same is also defined in
        the Operator in ``BaseOperator.operator_extra_links``, it has the correct
        extra link.
        """
        test_date = datetime(2019, 8, 1)
        dag = DAG(dag_id='simple_dag', start_date=test_date)
        CustomOperator(task_id='simple_task', dag=dag, bash_command=["echo", "true"])

        serialized_dag = SerializedDAG.to_dict(dag)
        assert "bash_command" in serialized_dag["dag"]["tasks"][0]

        dag = SerializedDAG.from_dict(serialized_dag)
        simple_task = dag.task_dict["simple_task"]
        assert getattr(simple_task, "bash_command") == ["echo", "true"]

        #########################################################
        # Verify Operator Links work with Serialized Operator
        #########################################################
        # Check Serialized version of operator link only contains the inbuilt Op Link
        assert serialized_dag["dag"]["tasks"][0]["_operator_extra_links"] == [
            {'tests.test_utils.mock_operators.CustomBaseIndexOpLink': {'index': 0}},
            {'tests.test_utils.mock_operators.CustomBaseIndexOpLink': {'index': 1}},
        ]

        # Test all the extra_links are set
        assert set(simple_task.extra_links) == {
            'BigQuery Console #1',
            'BigQuery Console #2',
            'airflow',
            'github',
            'google',
        }

        ti = TaskInstance(task=simple_task, execution_date=test_date)
        ti.xcom_push('search_query', ["dummy_value_1", "dummy_value_2"])

        # Test Deserialized inbuilt link #1
        custom_inbuilt_link = simple_task.get_extra_links(test_date, "BigQuery Console #1")
        assert 'https://console.cloud.google.com/bigquery?j=dummy_value_1' == custom_inbuilt_link

        # Test Deserialized inbuilt link #2
        custom_inbuilt_link = simple_task.get_extra_links(test_date, "BigQuery Console #2")
        assert 'https://console.cloud.google.com/bigquery?j=dummy_value_2' == custom_inbuilt_link

        # Test Deserialized link registered via Airflow Plugin
        google_link_from_plugin = simple_task.get_extra_links(test_date, GoogleLink.name)
        assert "https://www.google.com" == google_link_from_plugin

    class ClassWithCustomAttributes:
        """
        Class for testing purpose: allows to create objects with custom attributes in one single statement.
        """

        def __init__(self, **kwargs):
            for key, value in kwargs.items():
                setattr(self, key, value)

        def __str__(self):
            return f"{self.__class__.__name__}({str(self.__dict__)})"

        def __repr__(self):
            return self.__str__()

        def __eq__(self, other):
            return self.__dict__ == other.__dict__

        def __ne__(self, other):
            return not self.__eq__(other)

    @parameterized.expand(
        [
            (None, None),
            ([], []),
            ({}, {}),
            ("{{ task.task_id }}", "{{ task.task_id }}"),
            (["{{ task.task_id }}", "{{ task.task_id }}"]),
            ({"foo": "{{ task.task_id }}"}, {"foo": "{{ task.task_id }}"}),
            ({"foo": {"bar": "{{ task.task_id }}"}}, {"foo": {"bar": "{{ task.task_id }}"}}),
            (
                [{"foo1": {"bar": "{{ task.task_id }}"}}, {"foo2": {"bar": "{{ task.task_id }}"}}],
                [{"foo1": {"bar": "{{ task.task_id }}"}}, {"foo2": {"bar": "{{ task.task_id }}"}}],
            ),
            (
                {"foo": {"bar": {"{{ task.task_id }}": ["sar"]}}},
                {"foo": {"bar": {"{{ task.task_id }}": ["sar"]}}},
            ),
            (
                ClassWithCustomAttributes(
                    att1="{{ task.task_id }}", att2="{{ task.task_id }}", template_fields=["att1"]
                ),
                "ClassWithCustomAttributes("
                "{'att1': '{{ task.task_id }}', 'att2': '{{ task.task_id }}', 'template_fields': ['att1']})",
            ),
            (
                ClassWithCustomAttributes(
                    nested1=ClassWithCustomAttributes(
                        att1="{{ task.task_id }}", att2="{{ task.task_id }}", template_fields=["att1"]
                    ),
                    nested2=ClassWithCustomAttributes(
                        att3="{{ task.task_id }}", att4="{{ task.task_id }}", template_fields=["att3"]
                    ),
                    template_fields=["nested1"],
                ),
                "ClassWithCustomAttributes("
                "{'nested1': ClassWithCustomAttributes({'att1': '{{ task.task_id }}', "
                "'att2': '{{ task.task_id }}', 'template_fields': ['att1']}), "
                "'nested2': ClassWithCustomAttributes({'att3': '{{ task.task_id }}', 'att4': "
                "'{{ task.task_id }}', 'template_fields': ['att3']}), 'template_fields': ['nested1']})",
            ),
        ]
    )
    def test_templated_fields_exist_in_serialized_dag(self, templated_field, expected_field):
        """
        Test that templated_fields exists for all Operators in Serialized DAG

        Since we don't want to inflate arbitrary python objects (it poses a RCE/security risk etc.)
        we want check that non-"basic" objects are turned in to strings after deserializing.
        """

        dag = DAG("test_serialized_template_fields", start_date=datetime(2019, 8, 1))
        with dag:
            BashOperator(task_id="test", bash_command=templated_field)

        serialized_dag = SerializedDAG.to_dict(dag)
        deserialized_dag = SerializedDAG.from_dict(serialized_dag)
        deserialized_test_task = deserialized_dag.task_dict["test"]
        assert expected_field == getattr(deserialized_test_task, "bash_command")

    def test_dag_serialized_fields_with_schema(self):
        """
        Additional Properties are disabled on DAGs. This test verifies that all the
        keys in DAG.get_serialized_fields are listed in Schema definition.
        """
        dag_schema: dict = load_dag_schema_dict()["definitions"]["dag"]["properties"]

        # The parameters we add manually in Serialization needs to be ignored
        ignored_keys: set = {"is_subdag", "tasks", "has_on_success_callback", "has_on_failure_callback"}
        dag_params: set = set(dag_schema.keys()) - ignored_keys
        assert set(DAG.get_serialized_fields()) == dag_params

    def test_operator_subclass_changing_base_defaults(self):
        assert (
            BaseOperator(task_id='dummy').do_xcom_push is True
        ), "Precondition check! If this fails the test won't make sense"

        class MyOperator(BaseOperator):
            def __init__(self, do_xcom_push=False, **kwargs):
                super().__init__(**kwargs)
                self.do_xcom_push = do_xcom_push

        op = MyOperator(task_id='dummy')
        assert op.do_xcom_push is False

        blob = SerializedBaseOperator.serialize_operator(op)
        serialized_op = SerializedBaseOperator.deserialize_operator(blob)

        assert serialized_op.do_xcom_push is False

    def test_no_new_fields_added_to_base_operator(self):
        """
        This test verifies that there are no new fields added to BaseOperator. And reminds that
        tests should be added for it.
        """
        base_operator = BaseOperator(task_id="10")
        fields = base_operator.__dict__
        assert {
            '_BaseOperator__instantiated': True,
            '_dag': None,
            '_downstream_task_ids': set(),
            '_inlets': [],
            '_log': base_operator.log,
            '_outlets': [],
            '_upstream_task_ids': set(),
            'depends_on_past': False,
            'do_xcom_push': True,
            'email': None,
            'email_on_failure': True,
            'email_on_retry': True,
            'end_date': None,
            'execution_timeout': None,
            'executor_config': {},
            'inlets': [],
            'label': '10',
            'max_retry_delay': None,
            'on_execute_callback': None,
            'on_failure_callback': None,
            'on_retry_callback': None,
            'on_success_callback': None,
            'outlets': [],
            'owner': 'airflow',
            'params': {},
            'pool': 'default_pool',
            'pool_slots': 1,
            'priority_weight': 1,
            'queue': 'default',
            'resources': None,
            'retries': 0,
            'retry_delay': timedelta(0, 300),
            'retry_exponential_backoff': False,
            'run_as_user': None,
            'sla': None,
            'start_date': None,
            'subdag': None,
            'task_concurrency': None,
            'task_id': '10',
            'trigger_rule': 'all_success',
            'wait_for_downstream': False,
            'weight_rule': 'downstream',
        } == fields, """
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

     ACTION NEEDED! PLEASE READ THIS CAREFULLY AND CORRECT TESTS CAREFULLY

 Some fields were added to the BaseOperator! Please add them to the list above and make sure that
 you add support for DAG serialization - you should add the field to
 `airflow/serialization/schema.json` - they should have correct type defined there.

 Note that we do not support versioning yet so you should only add optional fields to BaseOperator.

!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                         """

    def test_task_group_serialization(self):
        """
        Test TaskGroup serialization/deserialization.
        """
        from airflow.operators.dummy import DummyOperator
        from airflow.utils.task_group import TaskGroup

        execution_date = datetime(2020, 1, 1)
        with DAG("test_task_group_serialization", start_date=execution_date) as dag:
            task1 = DummyOperator(task_id="task1")
            with TaskGroup("group234") as group234:
                _ = DummyOperator(task_id="task2")

                with TaskGroup("group34") as group34:
                    _ = DummyOperator(task_id="task3")
                    _ = DummyOperator(task_id="task4")

            task5 = DummyOperator(task_id="task5")
            task1 >> group234
            group34 >> task5

        dag_dict = SerializedDAG.to_dict(dag)
        SerializedDAG.validate_schema(dag_dict)
        json_dag = SerializedDAG.from_json(SerializedDAG.to_json(dag))
        self.validate_deserialized_dag(json_dag, dag)

        serialized_dag = SerializedDAG.deserialize_dag(SerializedDAG.serialize_dag(dag))

        assert serialized_dag.task_group.children
        assert serialized_dag.task_group.children.keys() == dag.task_group.children.keys()

        def check_task_group(node):
            try:
                children = node.children.values()
            except AttributeError:
                # Round-trip serialization and check the result
                expected_serialized = SerializedBaseOperator.serialize_operator(dag.get_task(node.task_id))
                expected_deserialized = SerializedBaseOperator.deserialize_operator(expected_serialized)
                expected_dict = SerializedBaseOperator.serialize_operator(expected_deserialized)
                assert node
                assert SerializedBaseOperator.serialize_operator(node) == expected_dict
                return

            for child in children:
                check_task_group(child)

        check_task_group(serialized_dag.task_group)

    @parameterized.expand(
        [
            ("poke", False),
            ("reschedule", True),
        ]
    )
    def test_serialize_sensor(self, mode, expect_custom_deps):
        from airflow.sensors.base import BaseSensorOperator

        class DummySensor(BaseSensorOperator):
            def poke(self, context):
                return False

        op = DummySensor(task_id='dummy', mode=mode, poke_interval=23)

        blob = SerializedBaseOperator.serialize_operator(op)

        if expect_custom_deps:
            assert "deps" in blob
        else:
            assert "deps" not in blob

        serialized_op = SerializedBaseOperator.deserialize_operator(blob)

        assert op.deps == serialized_op.deps

    @parameterized.expand(
        [
            ({"on_success_callback": lambda x: print("hi")}, True),
            ({}, False),
        ]
    )
    def test_dag_on_success_callback_roundtrip(self, passed_success_callback, expected_value):
        """
        Test that when on_success_callback is passed to the DAG, has_on_success_callback is stored
        in Serialized JSON blob. And when it is de-serialized dag.has_on_success_callback is set to True.

        When the callback is not set, has_on_success_callback should not be stored in Serialized blob
        and so default to False on de-serialization
        """
        dag = DAG(dag_id='test_dag_on_success_callback_roundtrip', **passed_success_callback)
        BaseOperator(task_id='simple_task', dag=dag, start_date=datetime(2019, 8, 1))

        serialized_dag = SerializedDAG.to_dict(dag)
        if expected_value:
            assert "has_on_success_callback" in serialized_dag["dag"]
        else:
            assert "has_on_success_callback" not in serialized_dag["dag"]

        deserialized_dag = SerializedDAG.from_dict(serialized_dag)

        assert deserialized_dag.has_on_success_callback is expected_value

    @parameterized.expand(
        [
            ({"on_failure_callback": lambda x: print("hi")}, True),
            ({}, False),
        ]
    )
    def test_dag_on_failure_callback_roundtrip(self, passed_failure_callback, expected_value):
        """
        Test that when on_failure_callback is passed to the DAG, has_on_failure_callback is stored
        in Serialized JSON blob. And when it is de-serialized dag.has_on_failure_callback is set to True.

        When the callback is not set, has_on_failure_callback should not be stored in Serialized blob
        and so default to False on de-serialization
        """
        dag = DAG(dag_id='test_dag_on_failure_callback_roundtrip', **passed_failure_callback)
        BaseOperator(task_id='simple_task', dag=dag, start_date=datetime(2019, 8, 1))

        serialized_dag = SerializedDAG.to_dict(dag)
        if expected_value:
            assert "has_on_failure_callback" in serialized_dag["dag"]
        else:
            assert "has_on_failure_callback" not in serialized_dag["dag"]

        deserialized_dag = SerializedDAG.from_dict(serialized_dag)

        assert deserialized_dag.has_on_failure_callback is expected_value

    @parameterized.expand(
        [
            (
                ['task_1', 'task_5', 'task_2', 'task_4'],
                ['task_1', 'task_2', 'task_4', 'task_5'],
            ),
            (
                {'task_1', 'task_5', 'task_2', 'task_4'},
                ['task_1', 'task_2', 'task_4', 'task_5'],
            ),
            (
                ('task_1', 'task_5', 'task_2', 'task_4'),
                ['task_1', 'task_2', 'task_4', 'task_5'],
            ),
            (
                {"task3": "test3", "task2": "test2", "task1": "test1"},
                {"task1": "test1", "task2": "test2", "task3": "test3"},
            ),
        ]
    )
    def test_serialized_objects_are_sorted(self, object_to_serialized, expected_output):
        """Test Serialized Lists, Sets and Tuples are sorted"""
        serialized_obj = SerializedDAG._serialize(object_to_serialized)
        if isinstance(serialized_obj, dict) and "__type" in serialized_obj:
            serialized_obj = serialized_obj["__var"]
        assert serialized_obj == expected_output


def test_kubernetes_optional():
    """Serialisation / deserialisation continues to work without kubernetes installed"""

    def mock__import__(name, globals_=None, locals_=None, fromlist=(), level=0):
        if level == 0 and name.partition('.')[0] == 'kubernetes':
            raise ImportError("No module named 'kubernetes'")
        return importlib.__import__(name, globals=globals_, locals=locals_, fromlist=fromlist, level=level)

    with mock.patch('builtins.__import__', side_effect=mock__import__) as import_mock:
        # load module from scratch, this does not replace any already imported
        # airflow.serialization.serialized_objects module in sys.modules
        spec = importlib.util.find_spec("airflow.serialization.serialized_objects")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        # if we got this far, the module did not try to load kubernetes, but
        # did it try to access airflow.kubernetes.*?
        imported_airflow = {
            c.args[0].split('.', 2)[1] for c in import_mock.call_args_list if c.args[0].startswith("airflow.")
        }
        assert "kubernetes" not in imported_airflow

        # pod loading is not supported when kubernetes is not available
        pod_override = {
            '__type': 'k8s.V1Pod',
            '__var': PodGenerator.serialize_pod(executor_config_pod),
        }

        with pytest.raises(RuntimeError):
            module.BaseSerialization.from_dict(pod_override)

        # basic serialization should succeed
        module.SerializedDAG.to_dict(make_simple_dag()["simple_dag"])
