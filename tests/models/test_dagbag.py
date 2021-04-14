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
import inspect
import os
import shutil
import textwrap
import unittest
from datetime import datetime, timezone
from tempfile import NamedTemporaryFile, mkdtemp
from unittest import mock
from unittest.mock import patch

from freezegun import freeze_time
from sqlalchemy import func
from sqlalchemy.exc import OperationalError

import airflow.example_dags
from airflow import models
from airflow.exceptions import SerializationError
from airflow.models import DagBag, DagModel
from airflow.models.serialized_dag import SerializedDagModel
from airflow.utils.dates import timezone as tz
from airflow.utils.session import create_session
from tests import cluster_policies
from tests.models import TEST_DAGS_FOLDER
from tests.test_utils import db
from tests.test_utils.asserts import assert_queries_count
from tests.test_utils.config import conf_vars


class TestDagBag(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.empty_dir = mkdtemp()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.empty_dir)

    def setUp(self) -> None:
        db.clear_db_dags()
        db.clear_db_serialized_dags()

    def tearDown(self) -> None:
        db.clear_db_dags()
        db.clear_db_serialized_dags()

    def test_get_existing_dag(self):
        """
        Test that we're able to parse some example DAGs and retrieve them
        """
        dagbag = models.DagBag(dag_folder=self.empty_dir, include_examples=True)

        some_expected_dag_ids = ["example_bash_operator", "example_branch_operator"]

        for dag_id in some_expected_dag_ids:
            dag = dagbag.get_dag(dag_id)

            assert dag is not None
            assert dag_id == dag.dag_id

        assert dagbag.size() >= 7

    def test_get_non_existing_dag(self):
        """
        test that retrieving a non existing dag id returns None without crashing
        """
        dagbag = models.DagBag(dag_folder=self.empty_dir, include_examples=False)

        non_existing_dag_id = "non_existing_dag_id"
        assert dagbag.get_dag(non_existing_dag_id) is None

    def test_dont_load_example(self):
        """
        test that the example are not loaded
        """
        dagbag = models.DagBag(dag_folder=self.empty_dir, include_examples=False)

        assert dagbag.size() == 0

    def test_safe_mode_heuristic_match(self):
        """With safe mode enabled, a file matching the discovery heuristics
        should be discovered.
        """
        with NamedTemporaryFile(dir=self.empty_dir, suffix=".py") as f:
            f.write(b"# airflow")
            f.write(b"# DAG")
            f.flush()

            with conf_vars({('core', 'dags_folder'): self.empty_dir}):
                dagbag = models.DagBag(include_examples=False, safe_mode=True)

            assert len(dagbag.dagbag_stats) == 1
            assert dagbag.dagbag_stats[0].file == f"/{os.path.basename(f.name)}"

    def test_safe_mode_heuristic_mismatch(self):
        """With safe mode enabled, a file not matching the discovery heuristics
        should not be discovered.
        """
        with NamedTemporaryFile(dir=self.empty_dir, suffix=".py"):
            with conf_vars({('core', 'dags_folder'): self.empty_dir}):
                dagbag = models.DagBag(include_examples=False, safe_mode=True)
            assert len(dagbag.dagbag_stats) == 0

    def test_safe_mode_disabled(self):
        """With safe mode disabled, an empty python file should be discovered."""
        with NamedTemporaryFile(dir=self.empty_dir, suffix=".py") as f:
            with conf_vars({('core', 'dags_folder'): self.empty_dir}):
                dagbag = models.DagBag(include_examples=False, safe_mode=False)
            assert len(dagbag.dagbag_stats) == 1
            assert dagbag.dagbag_stats[0].file == f"/{os.path.basename(f.name)}"

    def test_process_file_that_contains_multi_bytes_char(self):
        """
        test that we're able to parse file that contains multi-byte char
        """
        f = NamedTemporaryFile()
        f.write('\u3042'.encode())  # write multi-byte char (hiragana)
        f.flush()

        dagbag = models.DagBag(dag_folder=self.empty_dir, include_examples=False)
        assert [] == dagbag.process_file(f.name)

    def test_zip_skip_log(self):
        """
        test the loading of a DAG from within a zip file that skips another file because
        it doesn't have "airflow" and "DAG"
        """
        with self.assertLogs() as cm:
            test_zip_path = os.path.join(TEST_DAGS_FOLDER, "test_zip.zip")
            dagbag = models.DagBag(dag_folder=test_zip_path, include_examples=False)

            assert dagbag.has_logged
            assert (
                f'INFO:airflow.models.dagbag.DagBag:File {test_zip_path}:file_no_airflow_dag.py '
                'assumed to contain no DAGs. Skipping.' in cm.output
            )

    def test_zip(self):
        """
        test the loading of a DAG within a zip file that includes dependencies
        """
        dagbag = models.DagBag(dag_folder=self.empty_dir, include_examples=False)
        dagbag.process_file(os.path.join(TEST_DAGS_FOLDER, "test_zip.zip"))
        assert dagbag.get_dag("test_zip_dag")

    def test_process_file_cron_validity_check(self):
        """
        test if an invalid cron expression
        as schedule interval can be identified
        """
        invalid_dag_files = ["test_invalid_cron.py", "test_zip_invalid_cron.zip"]
        dagbag = models.DagBag(dag_folder=self.empty_dir, include_examples=False)

        assert len(dagbag.import_errors) == 0
        for file in invalid_dag_files:
            dagbag.process_file(os.path.join(TEST_DAGS_FOLDER, file))
        assert len(dagbag.import_errors) == len(invalid_dag_files)
        assert len(dagbag.dags) == 0

    @patch.object(DagModel, 'get_current')
    def test_get_dag_without_refresh(self, mock_dagmodel):
        """
        Test that, once a DAG is loaded, it doesn't get refreshed again if it
        hasn't been expired.
        """
        dag_id = 'example_bash_operator'

        mock_dagmodel.return_value = DagModel()
        mock_dagmodel.return_value.last_expired = None
        mock_dagmodel.return_value.fileloc = 'foo'

        class _TestDagBag(models.DagBag):
            process_file_calls = 0

            def process_file(self, filepath, only_if_updated=True, safe_mode=True):
                if os.path.basename(filepath) == 'example_bash_operator.py':
                    _TestDagBag.process_file_calls += 1
                super().process_file(filepath, only_if_updated, safe_mode)

        dagbag = _TestDagBag(include_examples=True)
        dagbag.process_file_calls

        # Should not call process_file again, since it's already loaded during init.
        assert 1 == dagbag.process_file_calls
        assert dagbag.get_dag(dag_id) is not None
        assert 1 == dagbag.process_file_calls

    def test_get_dag_fileloc(self):
        """
        Test that fileloc is correctly set when we load example DAGs,
        specifically SubDAGs and packaged DAGs.
        """
        dagbag = models.DagBag(dag_folder=self.empty_dir, include_examples=True)
        dagbag.process_file(os.path.join(TEST_DAGS_FOLDER, "test_zip.zip"))

        expected = {
            'example_bash_operator': 'airflow/example_dags/example_bash_operator.py',
            'example_subdag_operator': 'airflow/example_dags/example_subdag_operator.py',
            'example_subdag_operator.section-1': 'airflow/example_dags/subdags/subdag.py',
            'test_zip_dag': 'dags/test_zip.zip/test_zip.py',
        }

        for dag_id, path in expected.items():
            dag = dagbag.get_dag(dag_id)
            assert dag.fileloc.endswith(path)

    @patch.object(DagModel, "get_current")
    def test_refresh_py_dag(self, mock_dagmodel):
        """
        Test that we can refresh an ordinary .py DAG
        """
        example_dags_folder = airflow.example_dags.__path__[0]

        dag_id = "example_bash_operator"
        fileloc = os.path.realpath(os.path.join(example_dags_folder, "example_bash_operator.py"))

        mock_dagmodel.return_value = DagModel()
        mock_dagmodel.return_value.last_expired = datetime.max.replace(tzinfo=timezone.utc)
        mock_dagmodel.return_value.fileloc = fileloc

        class _TestDagBag(DagBag):
            process_file_calls = 0

            def process_file(self, filepath, only_if_updated=True, safe_mode=True):
                if filepath == fileloc:
                    _TestDagBag.process_file_calls += 1
                return super().process_file(filepath, only_if_updated, safe_mode)

        dagbag = _TestDagBag(dag_folder=self.empty_dir, include_examples=True)

        assert 1 == dagbag.process_file_calls
        dag = dagbag.get_dag(dag_id)
        assert dag is not None
        assert dag_id == dag.dag_id
        assert 2 == dagbag.process_file_calls

    @patch.object(DagModel, "get_current")
    def test_refresh_packaged_dag(self, mock_dagmodel):
        """
        Test that we can refresh a packaged DAG
        """
        dag_id = "test_zip_dag"
        fileloc = os.path.realpath(os.path.join(TEST_DAGS_FOLDER, "test_zip.zip/test_zip.py"))

        mock_dagmodel.return_value = DagModel()
        mock_dagmodel.return_value.last_expired = datetime.max.replace(tzinfo=timezone.utc)
        mock_dagmodel.return_value.fileloc = fileloc

        class _TestDagBag(DagBag):
            process_file_calls = 0

            def process_file(self, filepath, only_if_updated=True, safe_mode=True):
                if filepath in fileloc:
                    _TestDagBag.process_file_calls += 1
                return super().process_file(filepath, only_if_updated, safe_mode)

        dagbag = _TestDagBag(dag_folder=os.path.realpath(TEST_DAGS_FOLDER), include_examples=False)

        assert 1 == dagbag.process_file_calls
        dag = dagbag.get_dag(dag_id)
        assert dag is not None
        assert dag_id == dag.dag_id
        assert 2 == dagbag.process_file_calls

    def process_dag(self, create_dag):
        """
        Helper method to process a file generated from the input create_dag function.
        """
        # write source to file
        source = textwrap.dedent(''.join(inspect.getsource(create_dag).splitlines(True)[1:-1]))
        f = NamedTemporaryFile()
        f.write(source.encode('utf8'))
        f.flush()

        dagbag = models.DagBag(dag_folder=self.empty_dir, include_examples=False)
        found_dags = dagbag.process_file(f.name)
        return dagbag, found_dags, f.name

    def validate_dags(self, expected_parent_dag, actual_found_dags, actual_dagbag, should_be_found=True):
        expected_dag_ids = list(map(lambda dag: dag.dag_id, expected_parent_dag.subdags))
        expected_dag_ids.append(expected_parent_dag.dag_id)

        actual_found_dag_ids = list(map(lambda dag: dag.dag_id, actual_found_dags))

        for dag_id in expected_dag_ids:
            actual_dagbag.log.info(f'validating {dag_id}')
            assert (
                dag_id in actual_found_dag_ids
            ) == should_be_found, 'dag "{}" should {}have been found after processing dag "{}"'.format(
                dag_id,
                '' if should_be_found else 'not ',
                expected_parent_dag.dag_id,
            )
            assert (
                dag_id in actual_dagbag.dags
            ) == should_be_found, 'dag "{}" should {}be in dagbag.dags after processing dag "{}"'.format(
                dag_id,
                '' if should_be_found else 'not ',
                expected_parent_dag.dag_id,
            )

    def test_load_subdags(self):
        # Define Dag to load
        def standard_subdag():
            import datetime  # pylint: disable=redefined-outer-name,reimported

            from airflow.models import DAG
            from airflow.operators.dummy import DummyOperator
            from airflow.operators.subdag import SubDagOperator

            dag_name = 'parent'
            default_args = {'owner': 'owner1', 'start_date': datetime.datetime(2016, 1, 1)}
            dag = DAG(dag_name, default_args=default_args)

            # parent:
            #     A -> opSubDag_0
            #          parent.opsubdag_0:
            #              -> subdag_0.task
            #     A -> opSubDag_1
            #          parent.opsubdag_1:
            #              -> subdag_1.task

            with dag:

                def subdag_0():
                    subdag_0 = DAG('parent.op_subdag_0', default_args=default_args)
                    DummyOperator(task_id='subdag_0.task', dag=subdag_0)
                    return subdag_0

                def subdag_1():
                    subdag_1 = DAG('parent.op_subdag_1', default_args=default_args)
                    DummyOperator(task_id='subdag_1.task', dag=subdag_1)
                    return subdag_1

                op_subdag_0 = SubDagOperator(task_id='op_subdag_0', dag=dag, subdag=subdag_0())
                op_subdag_1 = SubDagOperator(task_id='op_subdag_1', dag=dag, subdag=subdag_1())

                op_a = DummyOperator(task_id='A')
                op_a.set_downstream(op_subdag_0)
                op_a.set_downstream(op_subdag_1)
            return dag

        test_dag = standard_subdag()
        # sanity check to make sure DAG.subdag is still functioning properly
        assert len(test_dag.subdags) == 2

        # Perform processing dag
        dagbag, found_dags, _ = self.process_dag(standard_subdag)

        # Validate correctness
        # all dags from test_dag should be listed
        self.validate_dags(test_dag, found_dags, dagbag)

        # Define Dag to load
        def nested_subdags():
            import datetime  # pylint: disable=redefined-outer-name,reimported

            from airflow.models import DAG
            from airflow.operators.dummy import DummyOperator
            from airflow.operators.subdag import SubDagOperator

            dag_name = 'parent'
            default_args = {'owner': 'owner1', 'start_date': datetime.datetime(2016, 1, 1)}
            dag = DAG(dag_name, default_args=default_args)

            # parent:
            #     A -> op_subdag_0
            #          parent.op_subdag_0:
            #              -> opSubDag_A
            #                 parent.op_subdag_0.opSubdag_A:
            #                     -> subdag_a.task
            #              -> opSubdag_B
            #                 parent.op_subdag_0.opSubdag_B:
            #                     -> subdag_b.task
            #     A -> op_subdag_1
            #          parent.op_subdag_1:
            #              -> opSubdag_C
            #                 parent.op_subdag_1.opSubdag_C:
            #                     -> subdag_c.task
            #              -> opSubDag_D
            #                 parent.op_subdag_1.opSubdag_D:
            #                     -> subdag_d.task

            with dag:

                def subdag_a():
                    subdag_a = DAG('parent.op_subdag_0.opSubdag_A', default_args=default_args)
                    DummyOperator(task_id='subdag_a.task', dag=subdag_a)
                    return subdag_a

                def subdag_b():
                    subdag_b = DAG('parent.op_subdag_0.opSubdag_B', default_args=default_args)
                    DummyOperator(task_id='subdag_b.task', dag=subdag_b)
                    return subdag_b

                def subdag_c():
                    subdag_c = DAG('parent.op_subdag_1.opSubdag_C', default_args=default_args)
                    DummyOperator(task_id='subdag_c.task', dag=subdag_c)
                    return subdag_c

                def subdag_d():
                    subdag_d = DAG('parent.op_subdag_1.opSubdag_D', default_args=default_args)
                    DummyOperator(task_id='subdag_d.task', dag=subdag_d)
                    return subdag_d

                def subdag_0():
                    subdag_0 = DAG('parent.op_subdag_0', default_args=default_args)
                    SubDagOperator(task_id='opSubdag_A', dag=subdag_0, subdag=subdag_a())
                    SubDagOperator(task_id='opSubdag_B', dag=subdag_0, subdag=subdag_b())
                    return subdag_0

                def subdag_1():
                    subdag_1 = DAG('parent.op_subdag_1', default_args=default_args)
                    SubDagOperator(task_id='opSubdag_C', dag=subdag_1, subdag=subdag_c())
                    SubDagOperator(task_id='opSubdag_D', dag=subdag_1, subdag=subdag_d())
                    return subdag_1

                op_subdag_0 = SubDagOperator(task_id='op_subdag_0', dag=dag, subdag=subdag_0())
                op_subdag_1 = SubDagOperator(task_id='op_subdag_1', dag=dag, subdag=subdag_1())

                op_a = DummyOperator(task_id='A')
                op_a.set_downstream(op_subdag_0)
                op_a.set_downstream(op_subdag_1)

            return dag

        test_dag = nested_subdags()
        # sanity check to make sure DAG.subdag is still functioning properly
        assert len(test_dag.subdags) == 6

        # Perform processing dag
        dagbag, found_dags, _ = self.process_dag(nested_subdags)

        # Validate correctness
        # all dags from test_dag should be listed
        self.validate_dags(test_dag, found_dags, dagbag)

    def test_skip_cycle_dags(self):
        """
        Don't crash when loading an invalid (contains a cycle) DAG file.
        Don't load the dag into the DagBag either
        """

        # Define Dag to load
        def basic_cycle():
            import datetime  # pylint: disable=redefined-outer-name,reimported

            from airflow.models import DAG
            from airflow.operators.dummy import DummyOperator

            dag_name = 'cycle_dag'
            default_args = {'owner': 'owner1', 'start_date': datetime.datetime(2016, 1, 1)}
            dag = DAG(dag_name, default_args=default_args)

            # A -> A
            with dag:
                op_a = DummyOperator(task_id='A')
                op_a.set_downstream(op_a)

            return dag

        test_dag = basic_cycle()
        # sanity check to make sure DAG.subdag is still functioning properly
        assert len(test_dag.subdags) == 0

        # Perform processing dag
        dagbag, found_dags, file_path = self.process_dag(basic_cycle)

        # #Validate correctness
        # None of the dags should be found
        self.validate_dags(test_dag, found_dags, dagbag, should_be_found=False)
        assert file_path in dagbag.import_errors

        # Define Dag to load
        def nested_subdag_cycle():
            import datetime  # pylint: disable=redefined-outer-name,reimported

            from airflow.models import DAG
            from airflow.operators.dummy import DummyOperator
            from airflow.operators.subdag import SubDagOperator

            dag_name = 'nested_cycle'
            default_args = {'owner': 'owner1', 'start_date': datetime.datetime(2016, 1, 1)}
            dag = DAG(dag_name, default_args=default_args)

            # cycle:
            #     A -> op_subdag_0
            #          cycle.op_subdag_0:
            #              -> opSubDag_A
            #                 cycle.op_subdag_0.opSubdag_A:
            #                     -> subdag_a.task
            #              -> opSubdag_B
            #                 cycle.op_subdag_0.opSubdag_B:
            #                     -> subdag_b.task
            #     A -> op_subdag_1
            #          cycle.op_subdag_1:
            #              -> opSubdag_C
            #                 cycle.op_subdag_1.opSubdag_C:
            #                     -> subdag_c.task -> subdag_c.task  >Invalid Loop<
            #              -> opSubDag_D
            #                 cycle.op_subdag_1.opSubdag_D:
            #                     -> subdag_d.task

            with dag:

                def subdag_a():
                    subdag_a = DAG('nested_cycle.op_subdag_0.opSubdag_A', default_args=default_args)
                    DummyOperator(task_id='subdag_a.task', dag=subdag_a)
                    return subdag_a

                def subdag_b():
                    subdag_b = DAG('nested_cycle.op_subdag_0.opSubdag_B', default_args=default_args)
                    DummyOperator(task_id='subdag_b.task', dag=subdag_b)
                    return subdag_b

                def subdag_c():
                    subdag_c = DAG('nested_cycle.op_subdag_1.opSubdag_C', default_args=default_args)
                    op_subdag_c_task = DummyOperator(task_id='subdag_c.task', dag=subdag_c)
                    # introduce a loop in opSubdag_C
                    op_subdag_c_task.set_downstream(op_subdag_c_task)
                    return subdag_c

                def subdag_d():
                    subdag_d = DAG('nested_cycle.op_subdag_1.opSubdag_D', default_args=default_args)
                    DummyOperator(task_id='subdag_d.task', dag=subdag_d)
                    return subdag_d

                def subdag_0():
                    subdag_0 = DAG('nested_cycle.op_subdag_0', default_args=default_args)
                    SubDagOperator(task_id='opSubdag_A', dag=subdag_0, subdag=subdag_a())
                    SubDagOperator(task_id='opSubdag_B', dag=subdag_0, subdag=subdag_b())
                    return subdag_0

                def subdag_1():
                    subdag_1 = DAG('nested_cycle.op_subdag_1', default_args=default_args)
                    SubDagOperator(task_id='opSubdag_C', dag=subdag_1, subdag=subdag_c())
                    SubDagOperator(task_id='opSubdag_D', dag=subdag_1, subdag=subdag_d())
                    return subdag_1

                op_subdag_0 = SubDagOperator(task_id='op_subdag_0', dag=dag, subdag=subdag_0())
                op_subdag_1 = SubDagOperator(task_id='op_subdag_1', dag=dag, subdag=subdag_1())

                op_a = DummyOperator(task_id='A')
                op_a.set_downstream(op_subdag_0)
                op_a.set_downstream(op_subdag_1)

            return dag

        test_dag = nested_subdag_cycle()
        # sanity check to make sure DAG.subdag is still functioning properly
        assert len(test_dag.subdags) == 6

        # Perform processing dag
        dagbag, found_dags, file_path = self.process_dag(nested_subdag_cycle)

        # Validate correctness
        # None of the dags should be found
        self.validate_dags(test_dag, found_dags, dagbag, should_be_found=False)
        assert file_path in dagbag.import_errors

    def test_process_file_with_none(self):
        """
        test that process_file can handle Nones
        """
        dagbag = models.DagBag(dag_folder=self.empty_dir, include_examples=False)

        assert [] == dagbag.process_file(None)

    def test_deactivate_unknown_dags(self):
        """
        Test that dag_ids not passed into deactivate_unknown_dags
        are deactivated when function is invoked
        """
        dagbag = DagBag(include_examples=True)
        dag_id = "test_deactivate_unknown_dags"
        expected_active_dags = dagbag.dags.keys()

        model_before = DagModel(dag_id=dag_id, is_active=True)
        with create_session() as session:
            session.merge(model_before)

        models.DAG.deactivate_unknown_dags(expected_active_dags)

        after_model = DagModel.get_dagmodel(dag_id)
        assert model_before.is_active
        assert not after_model.is_active

        # clean up
        with create_session() as session:
            session.query(DagModel).filter(DagModel.dag_id == 'test_deactivate_unknown_dags').delete()

    def test_serialized_dags_are_written_to_db_on_sync(self):
        """
        Test that when dagbag.sync_to_db is called the DAGs are Serialized and written to DB
        even when dagbag.read_dags_from_db is False
        """
        with create_session() as session:
            serialized_dags_count = session.query(func.count(SerializedDagModel.dag_id)).scalar()
            assert serialized_dags_count == 0

            dagbag = DagBag(
                dag_folder=os.path.join(TEST_DAGS_FOLDER, "test_example_bash_operator.py"),
                include_examples=False,
            )
            dagbag.sync_to_db()

            assert not dagbag.read_dags_from_db

            new_serialized_dags_count = session.query(func.count(SerializedDagModel.dag_id)).scalar()
            assert new_serialized_dags_count == 1

    @patch("airflow.models.serialized_dag.SerializedDagModel.write_dag")
    def test_serialized_dag_errors_are_import_errors(self, mock_serialize):
        """
        Test that errors serializing a DAG are recorded as import_errors in the DB
        """
        mock_serialize.side_effect = SerializationError

        with create_session() as session:
            path = os.path.join(TEST_DAGS_FOLDER, "test_example_bash_operator.py")

            dagbag = DagBag(
                dag_folder=path,
                include_examples=False,
            )
            assert dagbag.import_errors == {}

            dagbag.sync_to_db(session=session)

            assert path in dagbag.import_errors
            err = dagbag.import_errors[path]
            assert "SerializationError" in err
            session.rollback()

    @patch("airflow.models.dagbag.DagBag.collect_dags")
    @patch("airflow.models.serialized_dag.SerializedDagModel.write_dag")
    @patch("airflow.models.dag.DAG.bulk_write_to_db")
    def test_sync_to_db_is_retried(self, mock_bulk_write_to_db, mock_s10n_write_dag, mock_collect_dags):
        """Test that dagbag.sync_to_db is retried on OperationalError"""

        dagbag = DagBag("/dev/null")
        mock_dag = mock.MagicMock(spec=models.DAG)
        mock_dag.is_subdag = False
        dagbag.dags['mock_dag'] = mock_dag

        op_error = OperationalError(statement=mock.ANY, params=mock.ANY, orig=mock.ANY)

        # Mock error for the first 2 tries and a successful third try
        side_effect = [op_error, op_error, mock.ANY]

        mock_bulk_write_to_db.side_effect = side_effect

        mock_session = mock.MagicMock()
        dagbag.sync_to_db(session=mock_session)

        # Test that 3 attempts were made to run 'DAG.bulk_write_to_db' successfully
        mock_bulk_write_to_db.assert_has_calls(
            [
                mock.call(mock.ANY, session=mock.ANY),
                mock.call(mock.ANY, session=mock.ANY),
                mock.call(mock.ANY, session=mock.ANY),
            ]
        )
        # Assert that rollback is called twice (i.e. whenever OperationalError occurs)
        mock_session.rollback.assert_has_calls([mock.call(), mock.call()])
        # Check that 'SerializedDagModel.write_dag' is also called
        # Only called once since the other two times the 'DAG.bulk_write_to_db' error'd
        # and the session was roll-backed before even reaching 'SerializedDagModel.write_dag'
        mock_s10n_write_dag.assert_has_calls(
            [
                mock.call(mock_dag, min_update_interval=mock.ANY, session=mock_session),
            ]
        )

    @patch("airflow.models.dagbag.settings.MIN_SERIALIZED_DAG_UPDATE_INTERVAL", 5)
    @patch("airflow.models.dagbag.settings.MIN_SERIALIZED_DAG_FETCH_INTERVAL", 5)
    def test_get_dag_with_dag_serialization(self):
        """
        Test that Serialized DAG is updated in DagBag when it is updated in
        Serialized DAG table after 'min_serialized_dag_fetch_interval' seconds are passed.
        """

        with freeze_time(tz.datetime(2020, 1, 5, 0, 0, 0)):
            example_bash_op_dag = DagBag(include_examples=True).dags.get("example_bash_operator")
            SerializedDagModel.write_dag(dag=example_bash_op_dag)

            dag_bag = DagBag(read_dags_from_db=True)
            ser_dag_1 = dag_bag.get_dag("example_bash_operator")
            ser_dag_1_update_time = dag_bag.dags_last_fetched["example_bash_operator"]
            assert example_bash_op_dag.tags == ser_dag_1.tags
            assert ser_dag_1_update_time == tz.datetime(2020, 1, 5, 0, 0, 0)

        # Check that if min_serialized_dag_fetch_interval has not passed we do not fetch the DAG
        # from DB
        with freeze_time(tz.datetime(2020, 1, 5, 0, 0, 4)):
            with assert_queries_count(0):
                assert dag_bag.get_dag("example_bash_operator").tags == ["example", "example2"]

        # Make a change in the DAG and write Serialized DAG to the DB
        with freeze_time(tz.datetime(2020, 1, 5, 0, 0, 6)):
            example_bash_op_dag.tags += ["new_tag"]
            SerializedDagModel.write_dag(dag=example_bash_op_dag)

        # Since min_serialized_dag_fetch_interval is passed verify that calling 'dag_bag.get_dag'
        # fetches the Serialized DAG from DB
        with freeze_time(tz.datetime(2020, 1, 5, 0, 0, 8)):
            with assert_queries_count(2):
                updated_ser_dag_1 = dag_bag.get_dag("example_bash_operator")
                updated_ser_dag_1_update_time = dag_bag.dags_last_fetched["example_bash_operator"]

        assert set(updated_ser_dag_1.tags) == {"example", "example2", "new_tag"}
        assert updated_ser_dag_1_update_time > ser_dag_1_update_time

    def test_collect_dags_from_db(self):
        """DAGs are collected from Database"""
        example_dags_folder = airflow.example_dags.__path__[0]
        dagbag = DagBag(example_dags_folder)

        example_dags = dagbag.dags
        for dag in example_dags.values():
            SerializedDagModel.write_dag(dag)

        new_dagbag = DagBag(read_dags_from_db=True)
        assert len(new_dagbag.dags) == 0
        new_dagbag.collect_dags_from_db()
        new_dags = new_dagbag.dags
        assert len(example_dags) == len(new_dags)
        for dag_id, dag in example_dags.items():
            serialized_dag = new_dags[dag_id]

            assert serialized_dag.dag_id == dag.dag_id
            assert set(serialized_dag.task_dict) == set(dag.task_dict)

    @patch("airflow.settings.task_policy", cluster_policies.cluster_policy)
    def test_task_cluster_policy_violation(self):
        """
        test that file processing results in import error when task does not
        obey cluster policy.
        """
        dag_file = os.path.join(TEST_DAGS_FOLDER, "test_missing_owner.py")

        dagbag = DagBag(dag_folder=dag_file, include_smart_sensor=False, include_examples=False)
        assert set() == set(dagbag.dag_ids)
        expected_import_errors = {
            dag_file: (
                f"""DAG policy violation (DAG ID: test_missing_owner, Path: {dag_file}):\n"""
                """Notices:\n"""
                """ * Task must have non-None non-default owner. Current value: airflow"""
            )
        }
        assert expected_import_errors == dagbag.import_errors

    @patch("airflow.settings.task_policy", cluster_policies.cluster_policy)
    def test_task_cluster_policy_obeyed(self):
        """
        test that dag successfully imported without import errors when tasks
        obey cluster policy.
        """
        dag_file = os.path.join(TEST_DAGS_FOLDER, "test_with_non_default_owner.py")

        dagbag = DagBag(dag_folder=dag_file, include_examples=False, include_smart_sensor=False)
        assert {"test_with_non_default_owner"} == set(dagbag.dag_ids)

        assert {} == dagbag.import_errors

    @patch("airflow.settings.dag_policy", cluster_policies.dag_policy)
    def test_dag_cluster_policy_obeyed(self):
        dag_file = os.path.join(TEST_DAGS_FOLDER, "test_dag_with_no_tags.py")

        dagbag = DagBag(dag_folder=dag_file, include_examples=False, include_smart_sensor=False)
        assert len(dagbag.dag_ids) == 0
        assert "has no tags" in dagbag.import_errors[dag_file]
