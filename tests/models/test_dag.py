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

import datetime
import io
import logging
import os
import pickle
import re
import unittest
from contextlib import redirect_stdout
from datetime import timedelta
from tempfile import NamedTemporaryFile
from typing import Optional
from unittest import mock
from unittest.mock import patch

import pendulum
import pytest
from dateutil.relativedelta import relativedelta
from freezegun import freeze_time
from parameterized import parameterized

from airflow import models, settings
from airflow.configuration import conf
from airflow.decorators import task as task_decorator
from airflow.exceptions import AirflowException, DuplicateTaskIdFound
from airflow.models import DAG, DagModel, DagRun, DagTag, TaskFail, TaskInstance as TI
from airflow.models.baseoperator import BaseOperator
from airflow.models.dag import dag as dag_decorator
from airflow.models.dagparam import DagParam
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.security import permissions
from airflow.utils import timezone
from airflow.utils.file import list_py_file_paths
from airflow.utils.session import create_session, provide_session
from airflow.utils.state import State
from airflow.utils.timezone import datetime as datetime_tz
from airflow.utils.types import DagRunType
from airflow.utils.weight_rule import WeightRule
from tests.models import DEFAULT_DATE
from tests.test_utils.asserts import assert_queries_count
from tests.test_utils.db import clear_db_dags, clear_db_runs

TEST_DATE = datetime_tz(2015, 1, 2, 0, 0)


class TestDag(unittest.TestCase):
    def setUp(self) -> None:
        clear_db_runs()
        clear_db_dags()
        self.patcher_dag_code = patch.object(settings, "STORE_DAG_CODE", False)
        self.patcher_dag_code.start()

    def tearDown(self) -> None:
        clear_db_runs()
        clear_db_dags()
        self.patcher_dag_code.stop()

    @staticmethod
    def _clean_up(dag_id: str):
        with create_session() as session:
            session.query(DagRun).filter(DagRun.dag_id == dag_id).delete(synchronize_session=False)
            session.query(TI).filter(TI.dag_id == dag_id).delete(synchronize_session=False)
            session.query(TaskFail).filter(TaskFail.dag_id == dag_id).delete(synchronize_session=False)

    @staticmethod
    def _occur_before(a, b, list_):
        """
        Assert that a occurs before b in the list.
        """
        a_index = -1
        b_index = -1
        for i, e in enumerate(list_):
            if e.task_id == a:
                a_index = i
            if e.task_id == b:
                b_index = i
        return 0 <= a_index < b_index

    def test_params_not_passed_is_empty_dict(self):
        """
        Test that when 'params' is _not_ passed to a new Dag, that the params
        attribute is set to an empty dictionary.
        """
        dag = models.DAG('test-dag')

        assert isinstance(dag.params, dict)
        assert 0 == len(dag.params)

    def test_params_passed_and_params_in_default_args_no_override(self):
        """
        Test that when 'params' exists as a key passed to the default_args dict
        in addition to params being passed explicitly as an argument to the
        dag, that the 'params' key of the default_args dict is merged with the
        dict of the params argument.
        """
        params1 = {'parameter1': 1}
        params2 = {'parameter2': 2}

        dag = models.DAG('test-dag', default_args={'params': params1}, params=params2)

        params_combined = params1.copy()
        params_combined.update(params2)
        assert params_combined == dag.params

    def test_dag_invalid_default_view(self):
        """
        Test invalid `default_view` of DAG initialization
        """
        with pytest.raises(AirflowException, match='Invalid values of dag.default_view: only support'):
            models.DAG(dag_id='test-invalid-default_view', default_view='airflow')

    def test_dag_default_view_default_value(self):
        """
        Test `default_view` default value of DAG initialization
        """
        dag = models.DAG(dag_id='test-default_default_view')
        assert conf.get('webserver', 'dag_default_view').lower() == dag.default_view

    def test_dag_invalid_orientation(self):
        """
        Test invalid `orientation` of DAG initialization
        """
        with pytest.raises(AirflowException, match='Invalid values of dag.orientation: only support'):
            models.DAG(dag_id='test-invalid-orientation', orientation='airflow')

    def test_dag_orientation_default_value(self):
        """
        Test `orientation` default value of DAG initialization
        """
        dag = models.DAG(dag_id='test-default_orientation')
        assert conf.get('webserver', 'dag_orientation') == dag.orientation

    def test_dag_as_context_manager(self):
        """
        Test DAG as a context manager.
        When used as a context manager, Operators are automatically added to
        the DAG (unless they specify a different DAG)
        """
        dag = DAG('dag', start_date=DEFAULT_DATE, default_args={'owner': 'owner1'})
        dag2 = DAG('dag2', start_date=DEFAULT_DATE, default_args={'owner': 'owner2'})

        with dag:
            op1 = DummyOperator(task_id='op1')
            op2 = DummyOperator(task_id='op2', dag=dag2)

        assert op1.dag is dag
        assert op1.owner == 'owner1'
        assert op2.dag is dag2
        assert op2.owner == 'owner2'

        with dag2:
            op3 = DummyOperator(task_id='op3')

        assert op3.dag is dag2
        assert op3.owner == 'owner2'

        with dag:
            with dag2:
                op4 = DummyOperator(task_id='op4')
            op5 = DummyOperator(task_id='op5')

        assert op4.dag is dag2
        assert op5.dag is dag
        assert op4.owner == 'owner2'
        assert op5.owner == 'owner1'

        with DAG('creating_dag_in_cm', start_date=DEFAULT_DATE) as dag:
            DummyOperator(task_id='op6')

        assert dag.dag_id == 'creating_dag_in_cm'
        assert dag.tasks[0].task_id == 'op6'

        with dag:
            with dag:
                op7 = DummyOperator(task_id='op7')
            op8 = DummyOperator(task_id='op8')
        op9 = DummyOperator(task_id='op8')
        op9.dag = dag2

        assert op7.dag == dag
        assert op8.dag == dag
        assert op9.dag == dag2

    def test_dag_topological_sort_include_subdag_tasks(self):
        child_dag = DAG(
            'parent_dag.child_dag',
            schedule_interval='@daily',
            start_date=DEFAULT_DATE,
        )

        with child_dag:
            DummyOperator(task_id='a_child')
            DummyOperator(task_id='b_child')

        parent_dag = DAG(
            'parent_dag',
            schedule_interval='@daily',
            start_date=DEFAULT_DATE,
        )

        # a_parent -> child_dag -> (a_child | b_child) -> b_parent
        with parent_dag:
            op1 = DummyOperator(task_id='a_parent')
            op2 = SubDagOperator(task_id='child_dag', subdag=child_dag)
            op3 = DummyOperator(task_id='b_parent')

            op1 >> op2 >> op3

        topological_list = parent_dag.topological_sort(include_subdag_tasks=True)

        assert self._occur_before('a_parent', 'child_dag', topological_list)
        assert self._occur_before('child_dag', 'a_child', topological_list)
        assert self._occur_before('child_dag', 'b_child', topological_list)
        assert self._occur_before('a_child', 'b_parent', topological_list)
        assert self._occur_before('b_child', 'b_parent', topological_list)

    def test_dag_topological_sort1(self):
        dag = DAG('dag', start_date=DEFAULT_DATE, default_args={'owner': 'owner1'})

        # A -> B
        # A -> C -> D
        # ordered: B, D, C, A or D, B, C, A or D, C, B, A
        with dag:
            op1 = DummyOperator(task_id='A')
            op2 = DummyOperator(task_id='B')
            op3 = DummyOperator(task_id='C')
            op4 = DummyOperator(task_id='D')
            op1.set_upstream([op2, op3])
            op3.set_upstream(op4)

        topological_list = dag.topological_sort()
        logging.info(topological_list)

        tasks = [op2, op3, op4]
        assert topological_list[0] in tasks
        tasks.remove(topological_list[0])
        assert topological_list[1] in tasks
        tasks.remove(topological_list[1])
        assert topological_list[2] in tasks
        tasks.remove(topological_list[2])
        assert topological_list[3] == op1

    def test_dag_topological_sort2(self):
        dag = DAG('dag', start_date=DEFAULT_DATE, default_args={'owner': 'owner1'})

        # C -> (A u B) -> D
        # C -> E
        # ordered: E | D, A | B, C
        with dag:
            op1 = DummyOperator(task_id='A')
            op2 = DummyOperator(task_id='B')
            op3 = DummyOperator(task_id='C')
            op4 = DummyOperator(task_id='D')
            op5 = DummyOperator(task_id='E')
            op1.set_downstream(op3)
            op2.set_downstream(op3)
            op1.set_upstream(op4)
            op2.set_upstream(op4)
            op5.set_downstream(op3)

        topological_list = dag.topological_sort()
        logging.info(topological_list)

        set1 = [op4, op5]
        assert topological_list[0] in set1
        set1.remove(topological_list[0])

        set2 = [op1, op2]
        set2.extend(set1)
        assert topological_list[1] in set2
        set2.remove(topological_list[1])

        assert topological_list[2] in set2
        set2.remove(topological_list[2])

        assert topological_list[3] in set2

        assert topological_list[4] == op3

    def test_dag_topological_sort_dag_without_tasks(self):
        dag = DAG('dag', start_date=DEFAULT_DATE, default_args={'owner': 'owner1'})

        assert () == dag.topological_sort()

    def test_dag_naive_start_date_string(self):
        DAG('DAG', default_args={'start_date': '2019-06-01'})

    def test_dag_naive_start_end_dates_strings(self):
        DAG('DAG', default_args={'start_date': '2019-06-01', 'end_date': '2019-06-05'})

    def test_dag_start_date_propagates_to_end_date(self):
        """
        Tests that a start_date string with a timezone and an end_date string without a timezone
        are accepted and that the timezone from the start carries over the end

        This test is a little indirect, it works by setting start and end equal except for the
        timezone and then testing for equality after the DAG construction.  They'll be equal
        only if the same timezone was applied to both.

        An explicit check the `tzinfo` attributes for both are the same is an extra check.
        """
        dag = DAG(
            'DAG', default_args={'start_date': '2019-06-05T00:00:00+05:00', 'end_date': '2019-06-05T00:00:00'}
        )
        assert dag.default_args['start_date'] == dag.default_args['end_date']
        assert dag.default_args['start_date'].tzinfo == dag.default_args['end_date'].tzinfo

    def test_dag_naive_default_args_start_date(self):
        dag = DAG('DAG', default_args={'start_date': datetime.datetime(2018, 1, 1)})
        assert dag.timezone == settings.TIMEZONE
        dag = DAG('DAG', start_date=datetime.datetime(2018, 1, 1))
        assert dag.timezone == settings.TIMEZONE

    def test_dag_none_default_args_start_date(self):
        """
        Tests if a start_date of None in default_args
        works.
        """
        dag = DAG('DAG', default_args={'start_date': None})
        assert dag.timezone == settings.TIMEZONE

    def test_dag_task_priority_weight_total(self):
        width = 5
        depth = 5
        weight = 5
        pattern = re.compile('stage(\\d*).(\\d*)')
        # Fully connected parallel tasks. i.e. every task at each parallel
        # stage is dependent on every task in the previous stage.
        # Default weight should be calculated using downstream descendants
        with DAG('dag', start_date=DEFAULT_DATE, default_args={'owner': 'owner1'}) as dag:
            pipeline = [
                [DummyOperator(task_id=f'stage{i}.{j}', priority_weight=weight) for j in range(0, width)]
                for i in range(0, depth)
            ]
            for i, stage in enumerate(pipeline):
                if i == 0:
                    continue
                for current_task in stage:
                    for prev_task in pipeline[i - 1]:
                        current_task.set_upstream(prev_task)

            for task in dag.task_dict.values():
                match = pattern.match(task.task_id)
                task_depth = int(match.group(1))
                # the sum of each stages after this task + itself
                correct_weight = ((depth - (task_depth + 1)) * width + 1) * weight

                calculated_weight = task.priority_weight_total
                assert calculated_weight == correct_weight

    def test_dag_task_priority_weight_total_using_upstream(self):
        # Same test as above except use 'upstream' for weight calculation
        weight = 3
        width = 5
        depth = 5
        pattern = re.compile('stage(\\d*).(\\d*)')
        with DAG('dag', start_date=DEFAULT_DATE, default_args={'owner': 'owner1'}) as dag:
            pipeline = [
                [
                    DummyOperator(
                        task_id=f'stage{i}.{j}',
                        priority_weight=weight,
                        weight_rule=WeightRule.UPSTREAM,
                    )
                    for j in range(0, width)
                ]
                for i in range(0, depth)
            ]
            for i, stage in enumerate(pipeline):
                if i == 0:
                    continue
                for current_task in stage:
                    for prev_task in pipeline[i - 1]:
                        current_task.set_upstream(prev_task)

            for task in dag.task_dict.values():
                match = pattern.match(task.task_id)
                task_depth = int(match.group(1))
                # the sum of each stages after this task + itself
                correct_weight = (task_depth * width + 1) * weight

                calculated_weight = task.priority_weight_total
                assert calculated_weight == correct_weight

    def test_dag_task_priority_weight_total_using_absolute(self):
        # Same test as above except use 'absolute' for weight calculation
        weight = 10
        width = 5
        depth = 5
        with DAG('dag', start_date=DEFAULT_DATE, default_args={'owner': 'owner1'}) as dag:
            pipeline = [
                [
                    DummyOperator(
                        task_id=f'stage{i}.{j}',
                        priority_weight=weight,
                        weight_rule=WeightRule.ABSOLUTE,
                    )
                    for j in range(0, width)
                ]
                for i in range(0, depth)
            ]
            for i, stage in enumerate(pipeline):
                if i == 0:
                    continue
                for current_task in stage:
                    for prev_task in pipeline[i - 1]:
                        current_task.set_upstream(prev_task)

            for task in dag.task_dict.values():
                # the sum of each stages after this task + itself
                correct_weight = weight
                calculated_weight = task.priority_weight_total
                assert calculated_weight == correct_weight

    def test_dag_task_invalid_weight_rule(self):
        # Test if we enter an invalid weight rule
        with DAG('dag', start_date=DEFAULT_DATE, default_args={'owner': 'owner1'}):
            with pytest.raises(AirflowException):
                DummyOperator(task_id='should_fail', weight_rule='no rule')

    def test_get_num_task_instances(self):
        test_dag_id = 'test_get_num_task_instances_dag'
        test_task_id = 'task_1'

        test_dag = DAG(dag_id=test_dag_id, start_date=DEFAULT_DATE)
        test_task = DummyOperator(task_id=test_task_id, dag=test_dag)

        ti1 = TI(task=test_task, execution_date=DEFAULT_DATE)
        ti1.state = None
        ti2 = TI(task=test_task, execution_date=DEFAULT_DATE + datetime.timedelta(days=1))
        ti2.state = State.RUNNING
        ti3 = TI(task=test_task, execution_date=DEFAULT_DATE + datetime.timedelta(days=2))
        ti3.state = State.QUEUED
        ti4 = TI(task=test_task, execution_date=DEFAULT_DATE + datetime.timedelta(days=3))
        ti4.state = State.RUNNING
        session = settings.Session()
        session.merge(ti1)
        session.merge(ti2)
        session.merge(ti3)
        session.merge(ti4)
        session.commit()

        assert 0 == DAG.get_num_task_instances(test_dag_id, ['fakename'], session=session)
        assert 4 == DAG.get_num_task_instances(test_dag_id, [test_task_id], session=session)
        assert 4 == DAG.get_num_task_instances(test_dag_id, ['fakename', test_task_id], session=session)
        assert 1 == DAG.get_num_task_instances(test_dag_id, [test_task_id], states=[None], session=session)
        assert 2 == DAG.get_num_task_instances(
            test_dag_id, [test_task_id], states=[State.RUNNING], session=session
        )
        assert 3 == DAG.get_num_task_instances(
            test_dag_id, [test_task_id], states=[None, State.RUNNING], session=session
        )
        assert 4 == DAG.get_num_task_instances(
            test_dag_id, [test_task_id], states=[None, State.QUEUED, State.RUNNING], session=session
        )
        session.close()

    def test_user_defined_filters(self):
        def jinja_udf(name):
            return f'Hello {name}'

        dag = models.DAG('test-dag', start_date=DEFAULT_DATE, user_defined_filters={"hello": jinja_udf})
        jinja_env = dag.get_template_env()

        assert 'hello' in jinja_env.filters
        assert jinja_env.filters['hello'] == jinja_udf  # pylint: disable=comparison-with-callable

    def test_resolve_template_files_value(self):

        with NamedTemporaryFile(suffix='.template') as f:
            f.write(b'{{ ds }}')
            f.flush()
            template_dir = os.path.dirname(f.name)
            template_file = os.path.basename(f.name)

            with DAG('test-dag', start_date=DEFAULT_DATE, template_searchpath=template_dir):
                task = DummyOperator(task_id='op1')

            task.test_field = template_file
            task.template_fields = ('test_field',)
            task.template_ext = ('.template',)
            task.resolve_template_files()

        assert task.test_field == '{{ ds }}'

    def test_resolve_template_files_list(self):

        with NamedTemporaryFile(suffix='.template') as f:
            f.write(b'{{ ds }}')
            f.flush()
            template_dir = os.path.dirname(f.name)
            template_file = os.path.basename(f.name)

            with DAG('test-dag', start_date=DEFAULT_DATE, template_searchpath=template_dir):
                task = DummyOperator(task_id='op1')

            task.test_field = [template_file, 'some_string']
            task.template_fields = ('test_field',)
            task.template_ext = ('.template',)
            task.resolve_template_files()

        assert task.test_field == ['{{ ds }}', 'some_string']

    def test_following_previous_schedule(self):
        """
        Make sure DST transitions are properly observed
        """
        local_tz = pendulum.timezone('Europe/Zurich')
        start = local_tz.convert(datetime.datetime(2018, 10, 28, 2, 55), dst_rule=pendulum.PRE_TRANSITION)
        assert start.isoformat() == "2018-10-28T02:55:00+02:00", "Pre-condition: start date is in DST"

        utc = timezone.convert_to_utc(start)

        dag = DAG('tz_dag', start_date=start, schedule_interval='*/5 * * * *')
        _next = dag.following_schedule(utc)
        next_local = local_tz.convert(_next)

        assert _next.isoformat() == "2018-10-28T01:00:00+00:00"
        assert next_local.isoformat() == "2018-10-28T02:00:00+01:00"

        prev = dag.previous_schedule(utc)
        prev_local = local_tz.convert(prev)

        assert prev_local.isoformat() == "2018-10-28T02:50:00+02:00"

        prev = dag.previous_schedule(_next)
        prev_local = local_tz.convert(prev)

        assert prev_local.isoformat() == "2018-10-28T02:55:00+02:00"
        assert prev == utc

    def test_following_previous_schedule_daily_dag_cest_to_cet(self):
        """
        Make sure DST transitions are properly observed
        """
        local_tz = pendulum.timezone('Europe/Zurich')
        start = local_tz.convert(datetime.datetime(2018, 10, 27, 3), dst_rule=pendulum.PRE_TRANSITION)

        utc = timezone.convert_to_utc(start)

        dag = DAG('tz_dag', start_date=start, schedule_interval='0 3 * * *')

        prev = dag.previous_schedule(utc)
        prev_local = local_tz.convert(prev)

        assert prev_local.isoformat() == "2018-10-26T03:00:00+02:00"
        assert prev.isoformat() == "2018-10-26T01:00:00+00:00"

        _next = dag.following_schedule(utc)
        next_local = local_tz.convert(_next)

        assert next_local.isoformat() == "2018-10-28T03:00:00+01:00"
        assert _next.isoformat() == "2018-10-28T02:00:00+00:00"

        prev = dag.previous_schedule(_next)
        prev_local = local_tz.convert(prev)

        assert prev_local.isoformat() == "2018-10-27T03:00:00+02:00"
        assert prev.isoformat() == "2018-10-27T01:00:00+00:00"

    def test_following_previous_schedule_daily_dag_cet_to_cest(self):
        """
        Make sure DST transitions are properly observed
        """
        local_tz = pendulum.timezone('Europe/Zurich')
        start = local_tz.convert(datetime.datetime(2018, 3, 25, 2), dst_rule=pendulum.PRE_TRANSITION)

        utc = timezone.convert_to_utc(start)

        dag = DAG('tz_dag', start_date=start, schedule_interval='0 3 * * *')

        prev = dag.previous_schedule(utc)
        prev_local = local_tz.convert(prev)

        assert prev_local.isoformat() == "2018-03-24T03:00:00+01:00"
        assert prev.isoformat() == "2018-03-24T02:00:00+00:00"

        _next = dag.following_schedule(utc)
        next_local = local_tz.convert(_next)

        assert next_local.isoformat() == "2018-03-25T03:00:00+02:00"
        assert _next.isoformat() == "2018-03-25T01:00:00+00:00"

        prev = dag.previous_schedule(_next)
        prev_local = local_tz.convert(prev)

        assert prev_local.isoformat() == "2018-03-24T03:00:00+01:00"
        assert prev.isoformat() == "2018-03-24T02:00:00+00:00"

    def test_following_schedule_relativedelta(self):
        """
        Tests following_schedule a dag with a relativedelta schedule_interval
        """
        dag_id = "test_schedule_dag_relativedelta"
        delta = relativedelta(hours=+1)
        dag = DAG(dag_id=dag_id, schedule_interval=delta)
        dag.add_task(BaseOperator(task_id="faketastic", owner='Also fake', start_date=TEST_DATE))

        _next = dag.following_schedule(TEST_DATE)
        assert _next.isoformat() == "2015-01-02T01:00:00+00:00"

        _next = dag.following_schedule(_next)
        assert _next.isoformat() == "2015-01-02T02:00:00+00:00"

    def test_dagtag_repr(self):
        clear_db_dags()
        dag = DAG('dag-test-dagtag', start_date=DEFAULT_DATE, tags=['tag-1', 'tag-2'])
        dag.sync_to_db()
        with create_session() as session:
            assert {'tag-1', 'tag-2'} == {
                repr(t) for t in session.query(DagTag).filter(DagTag.dag_id == 'dag-test-dagtag').all()
            }

    def test_bulk_write_to_db(self):
        clear_db_dags()
        dags = [DAG(f'dag-bulk-sync-{i}', start_date=DEFAULT_DATE, tags=["test-dag"]) for i in range(0, 4)]

        with assert_queries_count(5):
            DAG.bulk_write_to_db(dags)
        with create_session() as session:
            assert {'dag-bulk-sync-0', 'dag-bulk-sync-1', 'dag-bulk-sync-2', 'dag-bulk-sync-3'} == {
                row[0] for row in session.query(DagModel.dag_id).all()
            }
            assert {
                ('dag-bulk-sync-0', 'test-dag'),
                ('dag-bulk-sync-1', 'test-dag'),
                ('dag-bulk-sync-2', 'test-dag'),
                ('dag-bulk-sync-3', 'test-dag'),
            } == set(session.query(DagTag.dag_id, DagTag.name).all())

            for row in session.query(DagModel.last_parsed_time).all():
                assert row[0] is not None

        # Re-sync should do fewer queries
        with assert_queries_count(4):
            DAG.bulk_write_to_db(dags)
        with assert_queries_count(4):
            DAG.bulk_write_to_db(dags)
        # Adding tags
        for dag in dags:
            dag.tags.append("test-dag2")
        with assert_queries_count(5):
            DAG.bulk_write_to_db(dags)
        with create_session() as session:
            assert {'dag-bulk-sync-0', 'dag-bulk-sync-1', 'dag-bulk-sync-2', 'dag-bulk-sync-3'} == {
                row[0] for row in session.query(DagModel.dag_id).all()
            }
            assert {
                ('dag-bulk-sync-0', 'test-dag'),
                ('dag-bulk-sync-0', 'test-dag2'),
                ('dag-bulk-sync-1', 'test-dag'),
                ('dag-bulk-sync-1', 'test-dag2'),
                ('dag-bulk-sync-2', 'test-dag'),
                ('dag-bulk-sync-2', 'test-dag2'),
                ('dag-bulk-sync-3', 'test-dag'),
                ('dag-bulk-sync-3', 'test-dag2'),
            } == set(session.query(DagTag.dag_id, DagTag.name).all())
        # Removing tags
        for dag in dags:
            dag.tags.remove("test-dag")
        with assert_queries_count(5):
            DAG.bulk_write_to_db(dags)
        with create_session() as session:
            assert {'dag-bulk-sync-0', 'dag-bulk-sync-1', 'dag-bulk-sync-2', 'dag-bulk-sync-3'} == {
                row[0] for row in session.query(DagModel.dag_id).all()
            }
            assert {
                ('dag-bulk-sync-0', 'test-dag2'),
                ('dag-bulk-sync-1', 'test-dag2'),
                ('dag-bulk-sync-2', 'test-dag2'),
                ('dag-bulk-sync-3', 'test-dag2'),
            } == set(session.query(DagTag.dag_id, DagTag.name).all())

            for row in session.query(DagModel.last_parsed_time).all():
                assert row[0] is not None

    def test_bulk_write_to_db_max_active_runs(self):
        """
        Test that DagModel.next_dagrun_create_after is set to NULL when the dag cannot be created due to max
        active runs being hit.
        """
        dag = DAG(dag_id='test_scheduler_verify_max_active_runs', start_date=DEFAULT_DATE)
        dag.max_active_runs = 1

        DummyOperator(task_id='dummy', dag=dag, owner='airflow')

        session = settings.Session()
        dag.clear()
        DAG.bulk_write_to_db([dag], session)

        model = session.query(DagModel).get((dag.dag_id,))

        period_end = dag.following_schedule(DEFAULT_DATE)
        assert model.next_dagrun == DEFAULT_DATE
        assert model.next_dagrun_create_after == period_end

        dr = dag.create_dagrun(
            state=State.RUNNING,
            execution_date=model.next_dagrun,
            run_type=DagRunType.SCHEDULED,
            session=session,
        )
        assert dr is not None
        DAG.bulk_write_to_db([dag])

        model = session.query(DagModel).get((dag.dag_id,))
        assert model.next_dagrun == period_end
        # We signal "at max active runs" by saying this run is never eligible to be created
        assert model.next_dagrun_create_after is None

    def test_sync_to_db(self):
        dag = DAG(
            'dag',
            start_date=DEFAULT_DATE,
        )
        with dag:
            DummyOperator(task_id='task', owner='owner1')
            subdag = DAG(
                'dag.subtask',
                start_date=DEFAULT_DATE,
            )
            # parent_dag and is_subdag was set by DagBag. We don't use DagBag, so this value is not set.
            subdag.parent_dag = dag
            subdag.is_subdag = True
            SubDagOperator(task_id='subtask', owner='owner2', subdag=subdag)
        session = settings.Session()
        dag.sync_to_db(session=session)

        orm_dag = session.query(DagModel).filter(DagModel.dag_id == 'dag').one()
        assert set(orm_dag.owners.split(', ')) == {'owner1', 'owner2'}
        assert orm_dag.is_active
        assert orm_dag.default_view is not None
        assert orm_dag.default_view == conf.get('webserver', 'dag_default_view').lower()
        assert orm_dag.safe_dag_id == 'dag'

        orm_subdag = session.query(DagModel).filter(DagModel.dag_id == 'dag.subtask').one()
        assert set(orm_subdag.owners.split(', ')) == {'owner1', 'owner2'}
        assert orm_subdag.is_active
        assert orm_subdag.safe_dag_id == 'dag__dot__subtask'
        assert orm_subdag.fileloc == orm_dag.fileloc
        session.close()

    def test_sync_to_db_default_view(self):
        dag = DAG(
            'dag',
            start_date=DEFAULT_DATE,
            default_view="graph",
        )
        with dag:
            DummyOperator(task_id='task', owner='owner1')
            SubDagOperator(
                task_id='subtask',
                owner='owner2',
                subdag=DAG(
                    'dag.subtask',
                    start_date=DEFAULT_DATE,
                ),
            )
        session = settings.Session()
        dag.sync_to_db(session=session)

        orm_dag = session.query(DagModel).filter(DagModel.dag_id == 'dag').one()
        assert orm_dag.default_view is not None
        assert orm_dag.default_view == "graph"
        session.close()

    @provide_session
    def test_is_paused_subdag(self, session):
        subdag_id = 'dag.subdag'
        subdag = DAG(
            subdag_id,
            start_date=DEFAULT_DATE,
        )
        with subdag:
            DummyOperator(
                task_id='dummy_task',
            )

        dag_id = 'dag'
        dag = DAG(
            dag_id,
            start_date=DEFAULT_DATE,
        )

        with dag:
            SubDagOperator(task_id='subdag', subdag=subdag)

        # parent_dag and is_subdag was set by DagBag. We don't use DagBag, so this value is not set.
        subdag.parent_dag = dag
        subdag.is_subdag = True

        session.query(DagModel).filter(DagModel.dag_id.in_([subdag_id, dag_id])).delete(
            synchronize_session=False
        )

        dag.sync_to_db(session=session)

        unpaused_dags = (
            session.query(DagModel.dag_id, DagModel.is_paused)
            .filter(
                DagModel.dag_id.in_([subdag_id, dag_id]),
            )
            .all()
        )

        assert {
            (dag_id, False),
            (subdag_id, False),
        } == set(unpaused_dags)

        DagModel.get_dagmodel(dag.dag_id).set_is_paused(is_paused=True, including_subdags=False)

        paused_dags = (
            session.query(DagModel.dag_id, DagModel.is_paused)
            .filter(
                DagModel.dag_id.in_([subdag_id, dag_id]),
            )
            .all()
        )

        assert {
            (dag_id, True),
            (subdag_id, False),
        } == set(paused_dags)

        DagModel.get_dagmodel(dag.dag_id).set_is_paused(is_paused=True)

        paused_dags = (
            session.query(DagModel.dag_id, DagModel.is_paused)
            .filter(
                DagModel.dag_id.in_([subdag_id, dag_id]),
            )
            .all()
        )

        assert {
            (dag_id, True),
            (subdag_id, True),
        } == set(paused_dags)

    def test_existing_dag_is_paused_upon_creation(self):
        dag = DAG('dag_paused')
        dag.sync_to_db()
        assert not dag.get_is_paused()

        dag = DAG('dag_paused', is_paused_upon_creation=True)
        dag.sync_to_db()
        # Since the dag existed before, it should not follow the pause flag upon creation
        assert not dag.get_is_paused()

    def test_new_dag_is_paused_upon_creation(self):
        dag = DAG('new_nonexisting_dag', is_paused_upon_creation=True)
        session = settings.Session()
        dag.sync_to_db(session=session)

        orm_dag = session.query(DagModel).filter(DagModel.dag_id == 'new_nonexisting_dag').one()
        # Since the dag didn't exist before, it should follow the pause flag upon creation
        assert orm_dag.is_paused
        session.close()

    def test_existing_dag_default_view(self):

        with create_session() as session:
            session.add(DagModel(dag_id='dag_default_view_old', default_view=None))
            session.commit()
            orm_dag = session.query(DagModel).filter(DagModel.dag_id == 'dag_default_view_old').one()
        assert orm_dag.default_view is None
        assert orm_dag.get_default_view() == conf.get('webserver', 'dag_default_view').lower()

    def test_dag_is_deactivated_upon_dagfile_deletion(self):
        dag_id = 'old_existing_dag'
        dag_fileloc = "/usr/local/airflow/dags/non_existing_path.py"
        dag = DAG(
            dag_id,
            is_paused_upon_creation=True,
        )
        dag.fileloc = dag_fileloc
        session = settings.Session()
        with mock.patch.object(settings, "STORE_DAG_CODE", False):
            dag.sync_to_db(session=session)

        orm_dag = session.query(DagModel).filter(DagModel.dag_id == dag_id).one()

        assert orm_dag.is_active
        assert orm_dag.fileloc == dag_fileloc

        DagModel.deactivate_deleted_dags(list_py_file_paths(settings.DAGS_FOLDER))

        orm_dag = session.query(DagModel).filter(DagModel.dag_id == dag_id).one()
        assert not orm_dag.is_active

        # pylint: disable=no-member
        session.execute(DagModel.__table__.delete().where(DagModel.dag_id == dag_id))
        session.close()

    def test_dag_naive_default_args_start_date_with_timezone(self):
        local_tz = pendulum.timezone('Europe/Zurich')
        default_args = {'start_date': datetime.datetime(2018, 1, 1, tzinfo=local_tz)}

        dag = DAG('DAG', default_args=default_args)
        assert dag.timezone.name == local_tz.name

        dag = DAG('DAG', default_args=default_args)
        assert dag.timezone.name == local_tz.name

    def test_roots(self):
        """Verify if dag.roots returns the root tasks of a DAG."""
        with DAG("test_dag", start_date=DEFAULT_DATE) as dag:
            op1 = DummyOperator(task_id="t1")
            op2 = DummyOperator(task_id="t2")
            op3 = DummyOperator(task_id="t3")
            op4 = DummyOperator(task_id="t4")
            op5 = DummyOperator(task_id="t5")
            [op1, op2] >> op3 >> [op4, op5]

            assert set(dag.roots) == {op1, op2}

    def test_leaves(self):
        """Verify if dag.leaves returns the leaf tasks of a DAG."""
        with DAG("test_dag", start_date=DEFAULT_DATE) as dag:
            op1 = DummyOperator(task_id="t1")
            op2 = DummyOperator(task_id="t2")
            op3 = DummyOperator(task_id="t3")
            op4 = DummyOperator(task_id="t4")
            op5 = DummyOperator(task_id="t5")
            [op1, op2] >> op3 >> [op4, op5]

            assert set(dag.leaves) == {op4, op5}

    def test_tree_view(self):
        """Verify correctness of dag.tree_view()."""
        with DAG("test_dag", start_date=DEFAULT_DATE) as dag:
            op1 = DummyOperator(task_id="t1")
            op2 = DummyOperator(task_id="t2")
            op3 = DummyOperator(task_id="t3")
            op1 >> op2 >> op3

            with redirect_stdout(io.StringIO()) as stdout:
                dag.tree_view()
                stdout = stdout.getvalue()

            stdout_lines = stdout.split("\n")
            assert 't1' in stdout_lines[0]
            assert 't2' in stdout_lines[1]
            assert 't3' in stdout_lines[2]

    def test_duplicate_task_ids_not_allowed_with_dag_context_manager(self):
        """Verify tasks with Duplicate task_id raises error"""
        with pytest.raises(DuplicateTaskIdFound, match="Task id 't1' has already been added to the DAG"):
            with DAG("test_dag", start_date=DEFAULT_DATE) as dag:
                op1 = DummyOperator(task_id="t1")
                op2 = BashOperator(task_id="t1", bash_command="sleep 1")
                op1 >> op2

        assert dag.task_dict == {op1.task_id: op1}

    def test_duplicate_task_ids_not_allowed_without_dag_context_manager(self):
        """Verify tasks with Duplicate task_id raises error"""
        with pytest.raises(DuplicateTaskIdFound, match="Task id 't1' has already been added to the DAG"):
            dag = DAG("test_dag", start_date=DEFAULT_DATE)
            op1 = DummyOperator(task_id="t1", dag=dag)
            op2 = DummyOperator(task_id="t1", dag=dag)
            op1 >> op2

        assert dag.task_dict == {op1.task_id: op1}

    def test_duplicate_task_ids_for_same_task_is_allowed(self):
        """Verify that same tasks with Duplicate task_id do not raise error"""
        with DAG("test_dag", start_date=DEFAULT_DATE) as dag:
            op1 = op2 = DummyOperator(task_id="t1")
            op3 = DummyOperator(task_id="t3")
            op1 >> op3
            op2 >> op3

        assert op1 == op2
        assert dag.task_dict == {op1.task_id: op1, op3.task_id: op3}
        assert dag.task_dict == {op2.task_id: op2, op3.task_id: op3}

    def test_sub_dag_updates_all_references_while_deepcopy(self):
        with DAG("test_dag", start_date=DEFAULT_DATE) as dag:
            op1 = DummyOperator(task_id='t1')
            op2 = DummyOperator(task_id='t2')
            op3 = DummyOperator(task_id='t3')
            op1 >> op2
            op2 >> op3

        sub_dag = dag.sub_dag('t2', include_upstream=True, include_downstream=False)
        assert id(sub_dag.task_dict['t1'].downstream_list[0].dag) == id(sub_dag)

    def test_schedule_dag_no_previous_runs(self):
        """
        Tests scheduling a dag with no previous runs
        """
        dag_id = "test_schedule_dag_no_previous_runs"
        dag = DAG(dag_id=dag_id)
        dag.add_task(BaseOperator(task_id="faketastic", owner='Also fake', start_date=TEST_DATE))

        dag_run = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=TEST_DATE,
            state=State.RUNNING,
        )
        assert dag_run is not None
        assert dag.dag_id == dag_run.dag_id
        assert dag_run.run_id is not None
        assert '' != dag_run.run_id
        assert (
            TEST_DATE == dag_run.execution_date
        ), f'dag_run.execution_date did not match expectation: {dag_run.execution_date}'
        assert State.RUNNING == dag_run.state
        assert not dag_run.external_trigger
        dag.clear()
        self._clean_up(dag_id)

    @patch('airflow.models.dag.Stats')
    def test_dag_handle_callback_crash(self, mock_stats):
        """
        Tests avoid crashes from calling dag callbacks exceptions
        """
        dag_id = "test_dag_callback_crash"
        mock_callback_with_exception = mock.MagicMock()
        mock_callback_with_exception.side_effect = Exception
        dag = DAG(
            dag_id=dag_id,
            # callback with invalid signature should not cause crashes
            on_success_callback=lambda: 1,
            on_failure_callback=mock_callback_with_exception,
        )
        when = TEST_DATE
        dag.add_task(BaseOperator(task_id="faketastic", owner='Also fake', start_date=when))

        dag_run = dag.create_dagrun(State.RUNNING, when, run_type=DagRunType.MANUAL)
        # should not raise any exception
        dag.handle_callback(dag_run, success=False)
        dag.handle_callback(dag_run, success=True)

        mock_stats.incr.assert_called_with("dag.callback_exceptions")

        dag.clear()
        self._clean_up(dag_id)

    def test_next_dagrun_after_fake_scheduled_previous(self):
        """
        Test scheduling a dag where there is a prior DagRun
        which has the same run_id as the next run should have
        """
        delta = datetime.timedelta(hours=1)
        dag_id = "test_schedule_dag_fake_scheduled_previous"
        dag = DAG(dag_id=dag_id, schedule_interval=delta, start_date=DEFAULT_DATE)
        dag.add_task(BaseOperator(task_id="faketastic", owner='Also fake', start_date=DEFAULT_DATE))

        dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
            state=State.SUCCESS,
            external_trigger=True,
        )
        dag.sync_to_db()
        with create_session() as session:
            model = session.query(DagModel).get((dag.dag_id,))

        # Even though there is a run for this date already, it is marked as manual/external, so we should
        # create a scheduled one anyway!
        assert model.next_dagrun == DEFAULT_DATE
        assert model.next_dagrun_create_after == dag.following_schedule(DEFAULT_DATE)

        self._clean_up(dag_id)

    def test_schedule_dag_once(self):
        """
        Tests scheduling a dag scheduled for @once - should be scheduled the first time
        it is called, and not scheduled the second.
        """
        dag_id = "test_schedule_dag_once"
        dag = DAG(dag_id=dag_id)
        dag.schedule_interval = '@once'
        assert dag.normalized_schedule_interval is None
        dag.add_task(BaseOperator(task_id="faketastic", owner='Also fake', start_date=TEST_DATE))

        # Sync once to create the DagModel
        dag.sync_to_db()

        dag.create_dagrun(run_type=DagRunType.SCHEDULED, execution_date=TEST_DATE, state=State.SUCCESS)

        # Then sync again after creating the dag run -- this should update next_dagrun
        dag.sync_to_db()
        with create_session() as session:
            model = session.query(DagModel).get((dag.dag_id,))

        assert model.next_dagrun is None
        assert model.next_dagrun_create_after is None
        self._clean_up(dag_id)

    def test_fractional_seconds(self):
        """
        Tests if fractional seconds are stored in the database
        """
        dag_id = "test_fractional_seconds"
        dag = DAG(dag_id=dag_id)
        dag.schedule_interval = '@once'
        dag.add_task(BaseOperator(task_id="faketastic", owner='Also fake', start_date=TEST_DATE))

        start_date = timezone.utcnow()

        run = dag.create_dagrun(
            run_id='test_' + start_date.isoformat(),
            execution_date=start_date,
            start_date=start_date,
            state=State.RUNNING,
            external_trigger=False,
        )

        run.refresh_from_db()

        assert start_date == run.execution_date, "dag run execution_date loses precision"
        assert start_date == run.start_date, "dag run start_date loses precision "
        self._clean_up(dag_id)

    def test_pickling(self):
        test_dag_id = 'test_pickling'
        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        dag = DAG(test_dag_id, default_args=args)
        dag_pickle = dag.pickle()
        assert dag_pickle.pickle.dag_id == dag.dag_id

    def test_rich_comparison_ops(self):
        test_dag_id = 'test_rich_comparison_ops'

        class DAGsubclass(DAG):
            pass

        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        dag = DAG(test_dag_id, default_args=args)

        dag_eq = DAG(test_dag_id, default_args=args)

        dag_diff_load_time = DAG(test_dag_id, default_args=args)
        dag_diff_name = DAG(test_dag_id + '_neq', default_args=args)

        dag_subclass = DAGsubclass(test_dag_id, default_args=args)
        dag_subclass_diff_name = DAGsubclass(test_dag_id + '2', default_args=args)

        for dag_ in [dag_eq, dag_diff_name, dag_subclass, dag_subclass_diff_name]:
            dag_.last_loaded = dag.last_loaded

        # test identity equality
        assert dag == dag  # pylint: disable=comparison-with-itself

        # test dag (in)equality based on _comps
        assert dag_eq == dag
        assert dag_diff_name != dag
        assert dag_diff_load_time != dag

        # test dag inequality based on type even if _comps happen to match
        assert dag_subclass != dag

        # a dag should equal an unpickled version of itself
        dump = pickle.dumps(dag)
        assert pickle.loads(dump) == dag

        # dags are ordered based on dag_id no matter what the type is
        assert dag < dag_diff_name
        assert dag > dag_diff_load_time
        assert dag < dag_subclass_diff_name

        # greater than should have been created automatically by functools
        assert dag_diff_name > dag

        # hashes are non-random and match equality
        assert hash(dag) == hash(dag)
        assert hash(dag_eq) == hash(dag)
        assert hash(dag_diff_name) != hash(dag)
        assert hash(dag_subclass) != hash(dag)

    def test_get_paused_dag_ids(self):
        dag_id = "test_get_paused_dag_ids"
        dag = DAG(dag_id, is_paused_upon_creation=True)
        dag.sync_to_db()
        assert DagModel.get_dagmodel(dag_id) is not None

        paused_dag_ids = DagModel.get_paused_dag_ids([dag_id])
        assert paused_dag_ids == {dag_id}

        with create_session() as session:
            session.query(DagModel).filter(DagModel.dag_id == dag_id).delete(synchronize_session=False)

    @parameterized.expand(
        [
            (None, None),
            ("@daily", "0 0 * * *"),
            ("@weekly", "0 0 * * 0"),
            ("@monthly", "0 0 1 * *"),
            ("@quarterly", "0 0 1 */3 *"),
            ("@yearly", "0 0 1 1 *"),
            ("@once", None),
            (datetime.timedelta(days=1), datetime.timedelta(days=1)),
        ]
    )
    def test_normalized_schedule_interval(self, schedule_interval, expected_n_schedule_interval):
        dag = DAG("test_schedule_interval", schedule_interval=schedule_interval)

        assert dag.normalized_schedule_interval == expected_n_schedule_interval
        assert dag.schedule_interval == schedule_interval

    def test_set_dag_runs_state(self):
        clear_db_runs()
        dag_id = "test_set_dag_runs_state"
        dag = DAG(dag_id=dag_id)

        for i in range(3):
            dag.create_dagrun(run_id=f"test{i}", state=State.RUNNING)

        dag.set_dag_runs_state(state=State.NONE)
        drs = DagRun.find(dag_id=dag_id)

        assert len(drs) == 3
        assert all(dr.state == State.NONE for dr in drs)

    def test_create_dagrun_run_id_is_generated(self):
        dag = DAG(dag_id="run_id_is_generated")
        dr = dag.create_dagrun(run_type=DagRunType.MANUAL, execution_date=DEFAULT_DATE, state=State.NONE)
        assert dr.run_id == f"manual__{DEFAULT_DATE.isoformat()}"

    def test_create_dagrun_run_type_is_obtained_from_run_id(self):
        dag = DAG(dag_id="run_type_is_obtained_from_run_id")
        dr = dag.create_dagrun(run_id="scheduled__", state=State.NONE)
        assert dr.run_type == DagRunType.SCHEDULED

        dr = dag.create_dagrun(run_id="custom_is_set_to_manual", state=State.NONE)
        assert dr.run_type == DagRunType.MANUAL

    def test_create_dagrun_job_id_is_set(self):
        job_id = 42
        dag = DAG(dag_id="test_create_dagrun_job_id_is_set")
        dr = dag.create_dagrun(
            run_id="test_create_dagrun_job_id_is_set", state=State.NONE, creating_job_id=job_id
        )
        assert dr.creating_job_id == job_id

    @parameterized.expand(
        [
            (State.NONE,),
            (State.RUNNING,),
        ]
    )
    def test_clear_set_dagrun_state(self, dag_run_state):
        dag_id = 'test_clear_set_dagrun_state'
        self._clean_up(dag_id)
        task_id = 't1'
        dag = DAG(dag_id, start_date=DEFAULT_DATE, max_active_runs=1)
        t_1 = DummyOperator(task_id=task_id, dag=dag)

        session = settings.Session()
        dagrun_1 = dag.create_dagrun(
            run_type=DagRunType.BACKFILL_JOB,
            state=State.FAILED,
            start_date=DEFAULT_DATE,
            execution_date=DEFAULT_DATE,
        )
        session.merge(dagrun_1)

        task_instance_1 = TI(t_1, execution_date=DEFAULT_DATE, state=State.RUNNING)
        session.merge(task_instance_1)
        session.commit()

        dag.clear(
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=1),
            dag_run_state=dag_run_state,
            include_subdags=False,
            include_parentdag=False,
            session=session,
        )

        dagruns = (
            session.query(
                DagRun,
            )
            .filter(
                DagRun.dag_id == dag_id,
            )
            .all()
        )

        assert len(dagruns) == 1
        dagrun = dagruns[0]  # type: DagRun
        assert dagrun.state == dag_run_state

    @parameterized.expand(
        [
            (State.NONE,),
            (State.RUNNING,),
        ]
    )
    def test_clear_set_dagrun_state_for_subdag(self, dag_run_state):
        dag_id = 'test_clear_set_dagrun_state_subdag'
        self._clean_up(dag_id)
        task_id = 't1'
        dag = DAG(dag_id, start_date=DEFAULT_DATE, max_active_runs=1)
        t_1 = DummyOperator(task_id=task_id, dag=dag)
        subdag = DAG(dag_id + '.test', start_date=DEFAULT_DATE, max_active_runs=1)
        SubDagOperator(task_id='test', subdag=subdag, dag=dag)
        t_2 = DummyOperator(task_id='task', dag=subdag)

        session = settings.Session()
        dagrun_1 = dag.create_dagrun(
            run_type=DagRunType.BACKFILL_JOB,
            state=State.FAILED,
            start_date=DEFAULT_DATE,
            execution_date=DEFAULT_DATE,
        )
        dagrun_2 = subdag.create_dagrun(
            run_type=DagRunType.BACKFILL_JOB,
            state=State.FAILED,
            start_date=DEFAULT_DATE,
            execution_date=DEFAULT_DATE,
        )
        session.merge(dagrun_1)
        session.merge(dagrun_2)
        task_instance_1 = TI(t_1, execution_date=DEFAULT_DATE, state=State.RUNNING)
        task_instance_2 = TI(t_2, execution_date=DEFAULT_DATE, state=State.RUNNING)
        session.merge(task_instance_1)
        session.merge(task_instance_2)
        session.commit()

        dag.clear(
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=1),
            dag_run_state=dag_run_state,
            include_subdags=True,
            include_parentdag=False,
            session=session,
        )

        dagrun = (
            session.query(
                DagRun,
            )
            .filter(DagRun.dag_id == subdag.dag_id)
            .one()
        )
        assert dagrun.state == dag_run_state

    @parameterized.expand(
        [(state, State.NONE) for state in State.task_states if state != State.RUNNING]
        + [(State.RUNNING, State.SHUTDOWN)]
    )  # type: ignore
    def test_clear_dag(self, ti_state_begin, ti_state_end: Optional[str]):
        dag_id = 'test_clear_dag'
        self._clean_up(dag_id)
        task_id = 't1'
        dag = DAG(dag_id, start_date=DEFAULT_DATE, max_active_runs=1)
        t_1 = DummyOperator(task_id=task_id, dag=dag)

        session = settings.Session()  # type: ignore
        dagrun_1 = dag.create_dagrun(
            run_type=DagRunType.BACKFILL_JOB,
            state=State.RUNNING,
            start_date=DEFAULT_DATE,
            execution_date=DEFAULT_DATE,
        )
        session.merge(dagrun_1)

        task_instance_1 = TI(t_1, execution_date=DEFAULT_DATE, state=ti_state_begin)
        task_instance_1.job_id = 123
        session.merge(task_instance_1)
        session.commit()

        dag.clear(
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=1),
            session=session,
        )

        task_instances = (
            session.query(
                TI,
            )
            .filter(
                TI.dag_id == dag_id,
            )
            .all()
        )

        assert len(task_instances) == 1
        task_instance = task_instances[0]  # type: TI
        assert task_instance.state == ti_state_end
        self._clean_up(dag_id)

    def test_next_dagrun_after_date_once(self):
        dag = DAG(
            'test_scheduler_dagrun_once', start_date=timezone.datetime(2015, 1, 1), schedule_interval="@once"
        )

        next_date = dag.next_dagrun_after_date(None)

        assert next_date == timezone.datetime(2015, 1, 1)

        next_date = dag.next_dagrun_after_date(next_date)
        assert next_date is None

    def test_next_dagrun_after_date_start_end_dates(self):
        """
        Tests that an attempt to schedule a task after the Dag's end_date
        does not succeed.
        """
        delta = datetime.timedelta(hours=1)
        runs = 3
        start_date = DEFAULT_DATE
        end_date = start_date + (runs - 1) * delta
        dag_id = "test_schedule_dag_start_end_dates"
        dag = DAG(dag_id=dag_id, start_date=start_date, end_date=end_date, schedule_interval=delta)
        dag.add_task(BaseOperator(task_id='faketastic', owner='Also fake'))

        # Create and schedule the dag runs
        dates = []
        date = None
        for _ in range(runs):
            date = dag.next_dagrun_after_date(date)
            dates.append(date)

        for date in dates:
            assert date is not None

        assert dates[-1] == end_date

        assert dag.next_dagrun_after_date(date) is None

    def test_next_dagrun_after_date_catcup(self):
        """
        Test to check that a DAG with catchup = False only schedules beginning now, not back to the start date
        """

        def make_dag(dag_id, schedule_interval, start_date, catchup):
            default_args = {
                'owner': 'airflow',
                'depends_on_past': False,
            }
            dag = DAG(
                dag_id,
                schedule_interval=schedule_interval,
                start_date=start_date,
                catchup=catchup,
                default_args=default_args,
            )

            op1 = DummyOperator(task_id='t1', dag=dag)
            op2 = DummyOperator(task_id='t2', dag=dag)
            op3 = DummyOperator(task_id='t3', dag=dag)
            op1 >> op2 >> op3

            return dag

        now = timezone.utcnow()
        six_hours_ago_to_the_hour = (now - datetime.timedelta(hours=6)).replace(
            minute=0, second=0, microsecond=0
        )
        half_an_hour_ago = now - datetime.timedelta(minutes=30)
        two_hours_ago = now - datetime.timedelta(hours=2)

        dag1 = make_dag(
            dag_id='dag_without_catchup_ten_minute',
            schedule_interval='*/10 * * * *',
            start_date=six_hours_ago_to_the_hour,
            catchup=False,
        )
        next_date = dag1.next_dagrun_after_date(None)
        # The DR should be scheduled in the last half an hour, not 6 hours ago
        assert next_date > half_an_hour_ago
        assert next_date < timezone.utcnow()

        dag2 = make_dag(
            dag_id='dag_without_catchup_hourly',
            schedule_interval='@hourly',
            start_date=six_hours_ago_to_the_hour,
            catchup=False,
        )

        next_date = dag2.next_dagrun_after_date(None)
        # The DR should be scheduled in the last 2 hours, not 6 hours ago
        assert next_date > two_hours_ago
        # The DR should be scheduled BEFORE now
        assert next_date < timezone.utcnow()

        dag3 = make_dag(
            dag_id='dag_without_catchup_once',
            schedule_interval='@once',
            start_date=six_hours_ago_to_the_hour,
            catchup=False,
        )

        next_date = dag3.next_dagrun_after_date(None)
        # The DR should be scheduled in the last 2 hours, not 6 hours ago
        assert next_date == six_hours_ago_to_the_hour

    @freeze_time(timezone.datetime(2020, 1, 5))
    def test_next_dagrun_after_date_timedelta_schedule_and_catchup_false(self):
        """
        Test that the dag file processor does not create multiple dagruns
        if a dag is scheduled with 'timedelta' and catchup=False
        """
        dag = DAG(
            'test_scheduler_dagrun_once_with_timedelta_and_catchup_false',
            start_date=timezone.datetime(2015, 1, 1),
            schedule_interval=timedelta(days=1),
            catchup=False,
        )

        next_date = dag.next_dagrun_after_date(None)
        assert next_date == timezone.datetime(2020, 1, 4)

        # The date to create is in the future, this is handled by "DagModel.dags_needing_dagruns"
        next_date = dag.next_dagrun_after_date(next_date)
        assert next_date == timezone.datetime(2020, 1, 5)

    @freeze_time(timezone.datetime(2020, 5, 4))
    def test_next_dagrun_after_date_timedelta_schedule_and_catchup_true(self):
        """
        Test that the dag file processor creates multiple dagruns
        if a dag is scheduled with 'timedelta' and catchup=True
        """
        dag = DAG(
            'test_scheduler_dagrun_once_with_timedelta_and_catchup_true',
            start_date=timezone.datetime(2020, 5, 1),
            schedule_interval=timedelta(days=1),
            catchup=True,
        )

        next_date = dag.next_dagrun_after_date(None)
        assert next_date == timezone.datetime(2020, 5, 1)

        next_date = dag.next_dagrun_after_date(next_date)
        assert next_date == timezone.datetime(2020, 5, 2)

        next_date = dag.next_dagrun_after_date(next_date)
        assert next_date == timezone.datetime(2020, 5, 3)

        # The date to create is in the future, this is handled by "DagModel.dags_needing_dagruns"
        next_date = dag.next_dagrun_after_date(next_date)
        assert next_date == timezone.datetime(2020, 5, 4)

    def test_next_dagrun_after_auto_align(self):
        """
        Test if the schedule_interval will be auto aligned with the start_date
        such that if the start_date coincides with the schedule the first
        execution_date will be start_date, otherwise it will be start_date +
        interval.
        """
        dag = DAG(
            dag_id='test_scheduler_auto_align_1',
            start_date=timezone.datetime(2016, 1, 1, 10, 10, 0),
            schedule_interval="4 5 * * *",
        )
        DummyOperator(task_id='dummy', dag=dag, owner='airflow')

        next_date = dag.next_dagrun_after_date(None)
        assert next_date == timezone.datetime(2016, 1, 2, 5, 4)

        dag = DAG(
            dag_id='test_scheduler_auto_align_2',
            start_date=timezone.datetime(2016, 1, 1, 10, 10, 0),
            schedule_interval="10 10 * * *",
        )
        DummyOperator(task_id='dummy', dag=dag, owner='airflow')

        next_date = dag.next_dagrun_after_date(None)
        assert next_date == timezone.datetime(2016, 1, 1, 10, 10)

    def test_next_dagrun_after_not_for_subdags(self):
        """
        Test the subdags are never marked to have dagruns created, as they are
        handled by the SubDagOperator, not the scheduler
        """

        def subdag(parent_dag_name, child_dag_name, args):
            """
            Create a subdag.
            """
            dag_subdag = DAG(
                dag_id=f'{parent_dag_name}.{child_dag_name}',
                schedule_interval="@daily",
                default_args=args,
            )

            for i in range(2):
                DummyOperator(task_id=f'{child_dag_name}-task-{i + 1}', dag=dag_subdag)

            return dag_subdag

        with DAG(
            dag_id='test_subdag_operator',
            start_date=datetime.datetime(2019, 1, 1),
            max_active_runs=1,
            schedule_interval=timedelta(minutes=1),
        ) as dag:
            section_1 = SubDagOperator(
                task_id='section-1',
                subdag=subdag(dag.dag_id, 'section-1', {'start_date': dag.start_date}),
            )

        subdag = section_1.subdag
        # parent_dag and is_subdag was set by DagBag. We don't use DagBag, so this value is not set.
        subdag.parent_dag = dag
        subdag.is_subdag = True

        next_date = dag.next_dagrun_after_date(None)
        assert next_date == timezone.datetime(2019, 1, 1, 0, 0)

        next_subdag_date = subdag.next_dagrun_after_date(None)
        assert next_subdag_date is None, "SubDags should never have DagRuns created by the scheduler"

    def test_replace_outdated_access_control_actions(self):
        outdated_permissions = {
            'role1': {permissions.ACTION_CAN_READ, permissions.ACTION_CAN_EDIT},
            'role2': {permissions.DEPRECATED_ACTION_CAN_DAG_READ, permissions.DEPRECATED_ACTION_CAN_DAG_EDIT},
        }
        updated_permissions = {
            'role1': {permissions.ACTION_CAN_READ, permissions.ACTION_CAN_EDIT},
            'role2': {permissions.ACTION_CAN_READ, permissions.ACTION_CAN_EDIT},
        }

        with pytest.warns(DeprecationWarning):
            dag = DAG(dag_id='dag_with_outdated_perms', access_control=outdated_permissions)
        assert dag.access_control == updated_permissions

        with pytest.warns(DeprecationWarning):
            dag.access_control = outdated_permissions
        assert dag.access_control == updated_permissions


class TestDagModel:
    def test_dags_needing_dagruns_not_too_early(self):
        dag = DAG(dag_id='far_future_dag', start_date=timezone.datetime(2038, 1, 1))
        DummyOperator(task_id='dummy', dag=dag, owner='airflow')

        session = settings.Session()
        orm_dag = DagModel(
            dag_id=dag.dag_id,
            concurrency=1,
            has_task_concurrency_limits=False,
            next_dagrun=dag.start_date,
            next_dagrun_create_after=timezone.datetime(2038, 1, 2),
            is_active=True,
        )
        session.add(orm_dag)
        session.flush()

        dag_models = DagModel.dags_needing_dagruns(session).all()
        assert dag_models == []

        session.rollback()
        session.close()

    def test_dags_needing_dagruns_only_unpaused(self):
        """
        We should never create dagruns for unpaused DAGs
        """
        dag = DAG(dag_id='test_dags', start_date=DEFAULT_DATE)
        DummyOperator(task_id='dummy', dag=dag, owner='airflow')

        session = settings.Session()
        orm_dag = DagModel(
            dag_id=dag.dag_id,
            has_task_concurrency_limits=False,
            next_dagrun=dag.start_date,
            next_dagrun_create_after=dag.following_schedule(DEFAULT_DATE),
            is_active=True,
        )
        session.add(orm_dag)
        session.flush()

        needed = DagModel.dags_needing_dagruns(session).all()
        assert needed == [orm_dag]

        orm_dag.is_paused = True
        session.flush()

        dag_models = DagModel.dags_needing_dagruns(session).all()
        assert dag_models == []

        session.rollback()
        session.close()


class TestQueries(unittest.TestCase):
    def setUp(self) -> None:
        clear_db_runs()

    def tearDown(self) -> None:
        clear_db_runs()

    @parameterized.expand(
        [
            (3,),
            (12,),
        ]
    )
    def test_count_number_queries(self, tasks_count):
        dag = DAG('test_dagrun_query_count', start_date=DEFAULT_DATE)
        for i in range(tasks_count):
            DummyOperator(task_id=f'dummy_task_{i}', owner='test', dag=dag)
        with assert_queries_count(2):
            dag.create_dagrun(
                run_id="test_dagrun_query_count",
                state=State.RUNNING,
                execution_date=TEST_DATE,
            )


class TestDagDecorator(unittest.TestCase):
    DEFAULT_ARGS = {
        "owner": "test",
        "depends_on_past": True,
        "start_date": timezone.utcnow(),
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    }
    DEFAULT_DATE = timezone.datetime(2016, 1, 1)
    VALUE = 42

    def setUp(self):
        super().setUp()
        self.operator = None

    def tearDown(self):
        super().tearDown()
        clear_db_runs()

    def test_set_dag_id(self):
        """Test that checks you can set dag_id from decorator."""

        @dag_decorator('test', default_args=self.DEFAULT_ARGS)
        def noop_pipeline():
            @task_decorator
            def return_num(num):
                return num

            return_num(4)

        dag = noop_pipeline()
        assert isinstance(dag, DAG)
        assert dag.dag_id, 'test'

    def test_default_dag_id(self):
        """Test that @dag uses function name as default dag id."""

        @dag_decorator(default_args=self.DEFAULT_ARGS)
        def noop_pipeline():
            @task_decorator
            def return_num(num):
                return num

            return_num(4)

        dag = noop_pipeline()
        assert isinstance(dag, DAG)
        assert dag.dag_id, 'noop_pipeline'

    def test_documentation_added(self):
        """Test that @dag uses function docs as doc_md for DAG object"""

        @dag_decorator(default_args=self.DEFAULT_ARGS)
        def noop_pipeline():
            """
            Regular DAG documentation
            """

            @task_decorator
            def return_num(num):
                return num

            return_num(4)

        dag = noop_pipeline()
        assert isinstance(dag, DAG)
        assert dag.dag_id, 'test'
        assert dag.doc_md.strip(), "Regular DAG documentation"

    def test_fails_if_arg_not_set(self):
        """Test that @dag decorated function fails if positional argument is not set"""

        @dag_decorator(default_args=self.DEFAULT_ARGS)
        def noop_pipeline(value):
            @task_decorator
            def return_num(num):
                return num

            return_num(value)

        # Test that if arg is not passed it raises a type error as expected.
        with pytest.raises(TypeError):
            noop_pipeline()  # pylint: disable=no-value-for-parameter

    def test_dag_param_resolves(self):
        """Test that dag param is correctly resolved by operator"""

        @dag_decorator(default_args=self.DEFAULT_ARGS)
        def xcom_pass_to_op(value=self.VALUE):
            @task_decorator
            def return_num(num):
                return num

            xcom_arg = return_num(value)
            self.operator = xcom_arg.operator  # pylint: disable=maybe-no-member

        dag = xcom_pass_to_op()

        dr = dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=self.DEFAULT_DATE,
            state=State.RUNNING,
        )

        self.operator.run(start_date=self.DEFAULT_DATE, end_date=self.DEFAULT_DATE)
        ti = dr.get_task_instances()[0]
        assert ti.xcom_pull() == self.VALUE

    def test_dag_param_dagrun_parameterized(self):
        """Test that dag param is correctly overwritten when set in dag run"""

        @dag_decorator(default_args=self.DEFAULT_ARGS)
        def xcom_pass_to_op(value=self.VALUE):
            @task_decorator
            def return_num(num):
                return num

            assert isinstance(value, DagParam)

            xcom_arg = return_num(value)
            self.operator = xcom_arg.operator  # pylint: disable=maybe-no-member

        dag = xcom_pass_to_op()
        new_value = 52
        dr = dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=self.DEFAULT_DATE,
            state=State.RUNNING,
            conf={'value': new_value},
        )

        self.operator.run(start_date=self.DEFAULT_DATE, end_date=self.DEFAULT_DATE)
        ti = dr.get_task_instances()[0]
        assert ti.xcom_pull(), new_value

    def test_set_params_for_dag(self):
        """Test that dag param is correctly set when using dag decorator"""

        @dag_decorator(default_args=self.DEFAULT_ARGS)
        def xcom_pass_to_op(value=self.VALUE):
            @task_decorator
            def return_num(num):
                return num

            xcom_arg = return_num(value)
            self.operator = xcom_arg.operator  # pylint: disable=maybe-no-member

        dag = xcom_pass_to_op()
        assert dag.params['value'] == self.VALUE
