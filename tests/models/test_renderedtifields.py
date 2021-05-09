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

"""Unit tests for RenderedTaskInstanceFields."""

import os
import unittest
from datetime import date, timedelta
from unittest import mock

from parameterized import parameterized

from airflow import settings
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.models.renderedtifields import RenderedTaskInstanceFields as RTIF
from airflow.models.taskinstance import TaskInstance as TI
from airflow.operators.bash import BashOperator
from airflow.utils.session import create_session
from airflow.utils.timezone import datetime
from airflow.version import version
from tests.test_utils.asserts import assert_queries_count
from tests.test_utils.db import clear_rendered_ti_fields

TEST_DAG = DAG("example_rendered_ti_field", schedule_interval=None)
START_DATE = datetime(2018, 1, 1)
EXECUTION_DATE = datetime(2019, 1, 1)


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


class TestRenderedTaskInstanceFields(unittest.TestCase):
    """Unit tests for RenderedTaskInstanceFields."""

    def setUp(self):
        clear_rendered_ti_fields()

    def tearDown(self):
        clear_rendered_ti_fields()

    @parameterized.expand(
        [
            (None, None),
            ([], []),
            ({}, {}),
            ("test-string", "test-string"),
            ({"foo": "bar"}, {"foo": "bar"}),
            ("{{ task.task_id }}", "test"),
            (date(2018, 12, 6), "2018-12-06"),
            (datetime(2018, 12, 6, 10, 55), "2018-12-06 10:55:00+00:00"),
            (
                ClassWithCustomAttributes(
                    att1="{{ task.task_id }}", att2="{{ task.task_id }}", template_fields=["att1"]
                ),
                "ClassWithCustomAttributes({'att1': 'test', 'att2': '{{ task.task_id }}', "
                "'template_fields': ['att1']})",
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
                "ClassWithCustomAttributes({'nested1': ClassWithCustomAttributes("
                "{'att1': 'test', 'att2': '{{ task.task_id }}', 'template_fields': ['att1']}), "
                "'nested2': ClassWithCustomAttributes("
                "{'att3': '{{ task.task_id }}', 'att4': '{{ task.task_id }}', 'template_fields': ['att3']}), "
                "'template_fields': ['nested1']})",
            ),
        ]
    )
    def test_get_templated_fields(self, templated_field, expected_rendered_field):
        """
        Test that template_fields are rendered correctly, stored in the Database,
        and are correctly fetched using RTIF.get_templated_fields
        """
        dag = DAG("test_serialized_rendered_fields", start_date=START_DATE)
        with dag:
            task = BashOperator(task_id="test", bash_command=templated_field)

        ti = TI(task=task, execution_date=EXECUTION_DATE)
        rtif = RTIF(ti=ti)
        assert ti.dag_id == rtif.dag_id
        assert ti.task_id == rtif.task_id
        assert ti.execution_date == rtif.execution_date
        assert expected_rendered_field == rtif.rendered_fields.get("bash_command")

        with create_session() as session:
            session.add(rtif)

        assert {"bash_command": expected_rendered_field, "env": None} == RTIF.get_templated_fields(ti=ti)

        # Test the else part of get_templated_fields
        # i.e. for the TIs that are not stored in RTIF table
        # Fetching them will return None
        with dag:
            task_2 = BashOperator(task_id="test2", bash_command=templated_field)

        ti2 = TI(task_2, EXECUTION_DATE)
        assert RTIF.get_templated_fields(ti=ti2) is None

    @parameterized.expand(
        [
            (0, 1, 0, 1),
            (1, 1, 1, 1),
            (1, 0, 1, 0),
            (3, 1, 1, 1),
            (4, 2, 2, 1),
            (5, 2, 2, 1),
        ]
    )
    def test_delete_old_records(self, rtif_num, num_to_keep, remaining_rtifs, expected_query_count):
        """
        Test that old records are deleted from rendered_task_instance_fields table
        for a given task_id and dag_id.
        """
        session = settings.Session()
        dag = DAG("test_delete_old_records", start_date=START_DATE)
        with dag:
            task = BashOperator(task_id="test", bash_command="echo {{ ds }}")

        rtif_list = [
            RTIF(TI(task=task, execution_date=EXECUTION_DATE + timedelta(days=num)))
            for num in range(rtif_num)
        ]

        session.add_all(rtif_list)
        session.commit()

        result = session.query(RTIF).filter(RTIF.dag_id == dag.dag_id, RTIF.task_id == task.task_id).all()

        for rtif in rtif_list:
            assert rtif in result

        assert rtif_num == len(result)

        # Verify old records are deleted and only 'num_to_keep' records are kept
        # For other DBs,an extra query is fired in RenderedTaskInstanceFields.delete_old_records
        expected_query_count_based_on_db = (
            expected_query_count + 1
            if session.bind.dialect.name == "mssql" and expected_query_count != 0
            else expected_query_count
        )

        with assert_queries_count(expected_query_count_based_on_db):
            RTIF.delete_old_records(task_id=task.task_id, dag_id=task.dag_id, num_to_keep=num_to_keep)
        result = session.query(RTIF).filter(RTIF.dag_id == dag.dag_id, RTIF.task_id == task.task_id).all()
        assert remaining_rtifs == len(result)

    def test_write(self):
        """
        Test records can be written and overwritten
        """
        Variable.set(key="test_key", value="test_val")

        session = settings.Session()
        result = session.query(RTIF).all()
        assert [] == result

        with DAG("test_write", start_date=START_DATE):
            task = BashOperator(task_id="test", bash_command="echo {{ var.value.test_key }}")

        rtif = RTIF(TI(task=task, execution_date=EXECUTION_DATE))
        rtif.write()
        result = (
            session.query(RTIF.dag_id, RTIF.task_id, RTIF.rendered_fields)
            .filter(
                RTIF.dag_id == rtif.dag_id,
                RTIF.task_id == rtif.task_id,
                RTIF.execution_date == rtif.execution_date,
            )
            .first()
        )
        assert ('test_write', 'test', {'bash_command': 'echo test_val', 'env': None}) == result

        # Test that overwrite saves new values to the DB
        Variable.delete("test_key")
        Variable.set(key="test_key", value="test_val_updated")

        with DAG("test_write", start_date=START_DATE):
            updated_task = BashOperator(task_id="test", bash_command="echo {{ var.value.test_key }}")

        rtif_updated = RTIF(TI(task=updated_task, execution_date=EXECUTION_DATE))
        rtif_updated.write()

        result_updated = (
            session.query(RTIF.dag_id, RTIF.task_id, RTIF.rendered_fields)
            .filter(
                RTIF.dag_id == rtif_updated.dag_id,
                RTIF.task_id == rtif_updated.task_id,
                RTIF.execution_date == rtif_updated.execution_date,
            )
            .first()
        )
        assert (
            'test_write',
            'test',
            {'bash_command': 'echo test_val_updated', 'env': None},
        ) == result_updated

    @mock.patch.dict(os.environ, {"AIRFLOW_IS_K8S_EXECUTOR_POD": "True"})
    @mock.patch('airflow.utils.log.secrets_masker.redact', autospec=True, side_effect=lambda d, _=None: d)
    @mock.patch("airflow.settings.pod_mutation_hook")
    def test_get_k8s_pod_yaml(self, mock_pod_mutation_hook, redact):
        """
        Test that k8s_pod_yaml is rendered correctly, stored in the Database,
        and are correctly fetched using RTIF.get_k8s_pod_yaml
        """
        dag = DAG("test_get_k8s_pod_yaml", start_date=START_DATE)
        with dag:
            task = BashOperator(task_id="test", bash_command="echo hi")

        ti = TI(task=task, execution_date=EXECUTION_DATE)
        rtif = RTIF(ti=ti)

        # Test that pod_mutation_hook is called
        mock_pod_mutation_hook.assert_called_once_with(mock.ANY)

        assert ti.dag_id == rtif.dag_id
        assert ti.task_id == rtif.task_id
        assert ti.execution_date == rtif.execution_date

        expected_pod_yaml = {
            'metadata': {
                'annotations': {
                    'dag_id': 'test_get_k8s_pod_yaml',
                    'execution_date': '2019-01-01T00:00:00+00:00',
                    'task_id': 'test',
                    'try_number': '1',
                },
                'labels': {
                    'airflow-worker': 'worker-config',
                    'airflow_version': version,
                    'dag_id': 'test_get_k8s_pod_yaml',
                    'execution_date': '2019-01-01T00_00_00_plus_00_00',
                    'kubernetes_executor': 'True',
                    'task_id': 'test',
                    'try_number': '1',
                },
                'name': mock.ANY,
                'namespace': 'default',
            },
            'spec': {
                'containers': [
                    {
                        'args': [
                            'airflow',
                            'tasks',
                            'run',
                            'test_get_k8s_pod_yaml',
                            'test',
                            '2019-01-01T00:00:00+00:00',
                        ],
                        'image': ':',
                        'name': 'base',
                        'env': [{'name': 'AIRFLOW_IS_K8S_EXECUTOR_POD', 'value': 'True'}],
                    }
                ]
            },
        }

        assert expected_pod_yaml == rtif.k8s_pod_yaml
        # K8s pod spec dict was passed to redact
        redact.assert_any_call(rtif.k8s_pod_yaml)

        with create_session() as session:
            session.add(rtif)

        assert expected_pod_yaml == RTIF.get_k8s_pod_yaml(ti=ti)

        # Test the else part of get_k8s_pod_yaml
        # i.e. for the TIs that are not stored in RTIF table
        # Fetching them will return None
        with dag:
            task_2 = BashOperator(task_id="test2", bash_command="echo hello")

        ti2 = TI(task_2, EXECUTION_DATE)
        assert RTIF.get_k8s_pod_yaml(ti=ti2) is None

    @mock.patch.dict(os.environ, {"AIRFLOW_VAR_API_KEY": "secret"})
    @mock.patch('airflow.utils.log.secrets_masker.redact', autospec=True)
    def test_redact(self, redact):
        dag = DAG("test_ritf_redact", start_date=START_DATE)
        with dag:
            task = BashOperator(
                task_id="test",
                bash_command="echo {{ var.value.api_key }}",
                env={'foo': 'secret', 'other_api_key': 'masked based on key name'},
            )

        redact.side_effect = [
            'val 1',
            'val 2',
        ]

        ti = TI(task=task, execution_date=EXECUTION_DATE)
        rtif = RTIF(ti=ti)
        assert rtif.rendered_fields == {
            'bash_command': 'val 1',
            'env': 'val 2',
        }
