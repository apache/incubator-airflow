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
from datetime import datetime

import pytest

from airflow.models import TaskInstance
from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils import helpers
from airflow.utils.helpers import build_airflow_url_with_query, merge_dicts
from tests.test_utils.config import conf_vars


class TestHelpers(unittest.TestCase):
    def test_render_log_filename(self):
        try_number = 1
        dag_id = 'test_render_log_filename_dag'
        task_id = 'test_render_log_filename_task'
        execution_date = datetime(2016, 1, 1)

        dag = DAG(dag_id, start_date=execution_date)
        task = DummyOperator(task_id=task_id, dag=dag)
        ti = TaskInstance(task=task, execution_date=execution_date)

        filename_template = "{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts }}/{{ try_number }}.log"

        ts = ti.get_template_context()['ts']
        expected_filename = f"{dag_id}/{task_id}/{ts}/{try_number}.log"

        rendered_filename = helpers.render_log_filename(ti, try_number, filename_template)

        assert rendered_filename == expected_filename

    def test_chunks(self):
        with pytest.raises(ValueError):
            list(helpers.chunks([1, 2, 3], 0))

        with pytest.raises(ValueError):
            list(helpers.chunks([1, 2, 3], -3))

        assert list(helpers.chunks([], 5)) == []
        assert list(helpers.chunks([1], 1)) == [[1]]
        assert list(helpers.chunks([1, 2, 3], 2)) == [[1, 2], [3]]

    def test_reduce_in_chunks(self):
        assert helpers.reduce_in_chunks(lambda x, y: x + [y], [1, 2, 3, 4, 5], []) == [[1, 2, 3, 4, 5]]

        assert helpers.reduce_in_chunks(lambda x, y: x + [y], [1, 2, 3, 4, 5], [], 2) == [[1, 2], [3, 4], [5]]

        assert helpers.reduce_in_chunks(lambda x, y: x + y[0] * y[1], [1, 2, 3, 4], 0, 2) == 14

    def test_is_container(self):
        assert not helpers.is_container("a string is not a container")
        assert helpers.is_container(["a", "list", "is", "a", "container"])

        assert helpers.is_container(['test_list'])
        assert not helpers.is_container('test_str_not_iterable')
        # Pass an object that is not iter nor a string.
        assert not helpers.is_container(10)

    def test_as_tuple(self):
        assert helpers.as_tuple("a string is not a container") == ("a string is not a container",)

        assert helpers.as_tuple(["a", "list", "is", "a", "container"]) == (
            "a",
            "list",
            "is",
            "a",
            "container",
        )

    def test_as_tuple_iter(self):
        test_list = ['test_str']
        as_tup = helpers.as_tuple(test_list)
        assert tuple(test_list) == as_tup

    def test_as_tuple_no_iter(self):
        test_str = 'test_str'
        as_tup = helpers.as_tuple(test_str)
        assert (test_str,) == as_tup

    def test_convert_camel_to_snake(self):
        assert helpers.convert_camel_to_snake('LocalTaskJob') == 'local_task_job'
        assert helpers.convert_camel_to_snake('somethingVeryRandom') == 'something_very_random'

    def test_merge_dicts(self):
        """
        Test _merge method from JSONFormatter
        """
        dict1 = {'a': 1, 'b': 2, 'c': 3}
        dict2 = {'a': 1, 'b': 3, 'd': 42}
        merged = merge_dicts(dict1, dict2)
        assert merged == {'a': 1, 'b': 3, 'c': 3, 'd': 42}

    def test_merge_dicts_recursive_overlap_l1(self):
        """
        Test merge_dicts with recursive dict; one level of nesting
        """
        dict1 = {'a': 1, 'r': {'a': 1, 'b': 2}}
        dict2 = {'a': 1, 'r': {'c': 3, 'b': 0}}
        merged = merge_dicts(dict1, dict2)
        assert merged == {'a': 1, 'r': {'a': 1, 'b': 0, 'c': 3}}

    def test_merge_dicts_recursive_overlap_l2(self):
        """
        Test merge_dicts with recursive dict; two levels of nesting
        """

        dict1 = {'a': 1, 'r': {'a': 1, 'b': {'a': 1}}}
        dict2 = {'a': 1, 'r': {'c': 3, 'b': {'b': 1}}}
        merged = merge_dicts(dict1, dict2)
        assert merged == {'a': 1, 'r': {'a': 1, 'b': {'a': 1, 'b': 1}, 'c': 3}}

    def test_merge_dicts_recursive_right_only(self):
        """
        Test merge_dicts with recursive when dict1 doesn't have any nested dict
        """
        dict1 = {'a': 1}
        dict2 = {'a': 1, 'r': {'c': 3, 'b': 0}}
        merged = merge_dicts(dict1, dict2)
        assert merged == {'a': 1, 'r': {'b': 0, 'c': 3}}

    @conf_vars(
        {
            ("webserver", "dag_default_view"): "graph",
        }
    )
    def test_build_airflow_url_with_query(self):
        """
        Test query generated with dag_id and params
        """
        query = {"dag_id": "test_dag", "param": "key/to.encode"}
        expected_url = "/graph?dag_id=test_dag&param=key%2Fto.encode"

        from airflow.www.app import cached_app

        with cached_app(testing=True).test_request_context():
            assert build_airflow_url_with_query(query) == expected_url
