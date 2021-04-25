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
import copy
import unittest
from copy import deepcopy
from unittest import mock

from airflow.providers.google.cloud.operators.dataflow import (
    CheckJobRunning,
    DataflowCreateJavaJobOperator,
    DataflowCreatePythonJobOperator,
    DataflowStartFlexTemplateOperator,
    DataflowStartSqlJobOperator,
    DataflowTemplatedJobStartOperator,
)
from airflow.version import version

TASK_ID = 'test-dataflow-operator'
JOB_ID = 'test-dataflow-pipeline-id'
JOB_NAME = 'test-dataflow-pipeline-name'
TEMPLATE = 'gs://dataflow-templates/wordcount/template_file'
PARAMETERS = {
    'inputFile': 'gs://dataflow-samples/shakespeare/kinglear.txt',
    'output': 'gs://test/output/my_output',
}
PY_FILE = 'gs://my-bucket/my-object.py'
PY_INTERPRETER = 'python3'
JAR_FILE = 'gs://my-bucket/example/test.jar'
JOB_CLASS = 'com.test.NotMain'
PY_OPTIONS = ['-m']
DEFAULT_OPTIONS_PYTHON = DEFAULT_OPTIONS_JAVA = {
    'project': 'test',
    'stagingLocation': 'gs://test/staging',
}
DEFAULT_OPTIONS_TEMPLATE = {
    'project': 'test',
    'stagingLocation': 'gs://test/staging',
    'tempLocation': 'gs://test/temp',
    'zone': 'us-central1-f',
}
ADDITIONAL_OPTIONS = {'output': 'gs://test/output', 'labels': {'foo': 'bar'}}
TEST_VERSION = f"v{version.replace('.', '-').replace('+', '-')}"
EXPECTED_ADDITIONAL_OPTIONS = {
    'output': 'gs://test/output',
    'labels': {'foo': 'bar', 'airflow-version': TEST_VERSION},
}
POLL_SLEEP = 30
GCS_HOOK_STRING = 'airflow.providers.google.cloud.operators.dataflow.{}'
TEST_FLEX_PARAMETERS = {
    "containerSpecGcsPath": "gs://test-bucket/test-file",
    "jobName": 'test-job-name',
    "parameters": {
        "inputSubscription": 'test-subscription',
        "outputTable": "test-project:test-dataset.streaming_beam_sql",
    },
}
TEST_LOCATION = 'custom-location'
TEST_PROJECT = "test-project"
TEST_SQL_JOB_NAME = 'test-sql-job-name'
TEST_DATASET = 'test-dataset'
TEST_SQL_OPTIONS = {
    "bigquery-project": TEST_PROJECT,
    "bigquery-dataset": TEST_DATASET,
    "bigquery-table": "beam_output",
    'bigquery-write-disposition': "write-truncate",
}
TEST_SQL_QUERY = """
SELECT
    sales_region as sales_region,
    count(state_id) as count_state
FROM
    bigquery.table.test-project.beam_samples.beam_table
GROUP BY sales_region;
"""
TEST_SQL_JOB_ID = 'test-job-id'


class TestDataflowPythonOperator(unittest.TestCase):
    def setUp(self):
        self.dataflow = DataflowCreatePythonJobOperator(
            task_id=TASK_ID,
            py_file=PY_FILE,
            job_name=JOB_NAME,
            py_options=PY_OPTIONS,
            dataflow_default_options=DEFAULT_OPTIONS_PYTHON,
            options=ADDITIONAL_OPTIONS,
            poll_sleep=POLL_SLEEP,
            location=TEST_LOCATION,
        )

    def test_init(self):
        """Test DataFlowPythonOperator instance is properly initialized."""
        assert self.dataflow.task_id == TASK_ID
        assert self.dataflow.job_name == JOB_NAME
        assert self.dataflow.py_file == PY_FILE
        assert self.dataflow.py_options == PY_OPTIONS
        assert self.dataflow.py_interpreter == PY_INTERPRETER
        assert self.dataflow.poll_sleep == POLL_SLEEP
        assert self.dataflow.dataflow_default_options == DEFAULT_OPTIONS_PYTHON
        assert self.dataflow.options == EXPECTED_ADDITIONAL_OPTIONS

    @mock.patch(
        'airflow.providers.google.cloud.operators.dataflow.process_line_and_extract_dataflow_job_id_callback'
    )
    @mock.patch('airflow.providers.google.cloud.operators.dataflow.BeamHook')
    @mock.patch('airflow.providers.google.cloud.operators.dataflow.DataflowHook')
    @mock.patch('airflow.providers.google.cloud.operators.dataflow.GCSHook')
    def test_exec(self, gcs_hook, dataflow_hook_mock, beam_hook_mock, mock_callback_on_job_id):
        """Test DataflowHook is created and the right args are passed to
        start_python_workflow.

        """
        start_python_mock = beam_hook_mock.return_value.start_python_pipeline
        gcs_provide_file = gcs_hook.return_value.provide_file
        job_name = dataflow_hook_mock.return_value.build_dataflow_job_name.return_value
        self.dataflow.execute(None)
        beam_hook_mock.assert_called_once_with(runner="DataflowRunner")
        self.assertTrue(self.dataflow.py_file.startswith('/tmp/dataflow'))
        gcs_provide_file.assert_called_once_with(object_url=PY_FILE)
        mock_callback_on_job_id.assert_called_once_with(on_new_job_id_callback=mock.ANY)
        dataflow_hook_mock.assert_called_once_with(
            gcp_conn_id="google_cloud_default",
            delegate_to=mock.ANY,
            poll_sleep=POLL_SLEEP,
            impersonation_chain=None,
            drain_pipeline=False,
            cancel_timeout=mock.ANY,
            wait_until_finished=None,
        )
        expected_options = {
            "project": dataflow_hook_mock.return_value.project_id,
            "staging_location": 'gs://test/staging',
            "job_name": job_name,
            "region": TEST_LOCATION,
            'output': 'gs://test/output',
            'labels': {'foo': 'bar', 'airflow-version': 'v2-1-0-dev0'},
        }
        start_python_mock.assert_called_once_with(
            variables=expected_options,
            py_file=gcs_provide_file.return_value.__enter__.return_value.name,
            py_options=PY_OPTIONS,
            py_interpreter=PY_INTERPRETER,
            py_requirements=None,
            py_system_site_packages=False,
            process_line_callback=mock_callback_on_job_id.return_value,
        )
        dataflow_hook_mock.return_value.wait_for_done.assert_called_once_with(
            job_id=mock.ANY,
            job_name=job_name,
            location=TEST_LOCATION,
            multiple_jobs=False,
        )
        assert self.dataflow.py_file.startswith('/tmp/dataflow')


class TestDataflowJavaOperator(unittest.TestCase):
    def setUp(self):
        self.dataflow = DataflowCreateJavaJobOperator(
            task_id=TASK_ID,
            jar=JAR_FILE,
            job_name=JOB_NAME,
            job_class=JOB_CLASS,
            dataflow_default_options=DEFAULT_OPTIONS_JAVA,
            options=ADDITIONAL_OPTIONS,
            poll_sleep=POLL_SLEEP,
            location=TEST_LOCATION,
        )

    def test_init(self):
        """Test DataflowTemplateOperator instance is properly initialized."""
        assert self.dataflow.task_id == TASK_ID
        assert self.dataflow.job_name == JOB_NAME
        assert self.dataflow.poll_sleep == POLL_SLEEP
        assert self.dataflow.dataflow_default_options == DEFAULT_OPTIONS_JAVA
        assert self.dataflow.job_class == JOB_CLASS
        assert self.dataflow.jar == JAR_FILE
        assert self.dataflow.options == EXPECTED_ADDITIONAL_OPTIONS
        assert self.dataflow.check_if_running == CheckJobRunning.WaitForRun

    @mock.patch(
        'airflow.providers.google.cloud.operators.dataflow.process_line_and_extract_dataflow_job_id_callback'
    )
    @mock.patch('airflow.providers.google.cloud.operators.dataflow.BeamHook')
    @mock.patch('airflow.providers.google.cloud.operators.dataflow.DataflowHook')
    @mock.patch('airflow.providers.google.cloud.operators.dataflow.GCSHook')
    def test_exec(self, gcs_hook, dataflow_hook_mock, beam_hook_mock, mock_callback_on_job_id):
        """Test DataflowHook is created and the right args are passed to
        start_java_workflow.

        """
        start_java_mock = beam_hook_mock.return_value.start_java_pipeline
        gcs_provide_file = gcs_hook.return_value.provide_file
        job_name = dataflow_hook_mock.return_value.build_dataflow_job_name.return_value
        self.dataflow.check_if_running = CheckJobRunning.IgnoreJob

        self.dataflow.execute(None)

        mock_callback_on_job_id.assert_called_once_with(on_new_job_id_callback=mock.ANY)
        gcs_provide_file.assert_called_once_with(object_url=JAR_FILE)
        expected_variables = {
            'project': dataflow_hook_mock.return_value.project_id,
            'stagingLocation': 'gs://test/staging',
            'jobName': job_name,
            'region': TEST_LOCATION,
            'output': 'gs://test/output',
            'labels': {'foo': 'bar', 'airflow-version': 'v2-1-0-dev0'},
        }

        start_java_mock.assert_called_once_with(
            variables=expected_variables,
            jar=gcs_provide_file.return_value.__enter__.return_value.name,
            job_class=JOB_CLASS,
            process_line_callback=mock_callback_on_job_id.return_value,
        )
        dataflow_hook_mock.return_value.wait_for_done.assert_called_once_with(
            job_id=mock.ANY,
            job_name=job_name,
            location=TEST_LOCATION,
            multiple_jobs=None,
        )

    @mock.patch('airflow.providers.google.cloud.operators.dataflow.BeamHook')
    @mock.patch('airflow.providers.google.cloud.operators.dataflow.DataflowHook')
    @mock.patch('airflow.providers.google.cloud.operators.dataflow.GCSHook')
    def test_check_job_running_exec(self, gcs_hook, dataflow_mock, beam_hook_mock):
        """Test DataflowHook is created and the right args are passed to
        start_java_workflow.

        """
        dataflow_running = dataflow_mock.return_value.is_job_dataflow_running
        dataflow_running.return_value = True
        start_java_hook = beam_hook_mock.return_value.start_java_pipeline
        gcs_provide_file = gcs_hook.return_value.provide_file
        self.dataflow.check_if_running = True

        self.dataflow.execute(None)

        self.assertTrue(dataflow_mock.called)
        start_java_hook.assert_not_called()
        gcs_provide_file.assert_called_once()
        variables = {
            'project': dataflow_mock.return_value.project_id,
            'stagingLocation': 'gs://test/staging',
            'jobName': JOB_NAME,
            'region': TEST_LOCATION,
            'output': 'gs://test/output',
            'labels': {'foo': 'bar', 'airflow-version': 'v2-1-0-dev0'},
        }
        dataflow_running.assert_called_once_with(name=JOB_NAME, variables=variables)

    @mock.patch(
        'airflow.providers.google.cloud.operators.dataflow.process_line_and_extract_dataflow_job_id_callback'
    )
    @mock.patch('airflow.providers.google.cloud.operators.dataflow.BeamHook')
    @mock.patch('airflow.providers.google.cloud.operators.dataflow.DataflowHook')
    @mock.patch('airflow.providers.google.cloud.operators.dataflow.GCSHook')
    def test_check_job_not_running_exec(
        self, gcs_hook, dataflow_hook_mock, beam_hook_mock, mock_callback_on_job_id
    ):
        """Test DataflowHook is created and the right args are passed to
        start_java_workflow with option to check if job is running
        """
        is_job_dataflow_running_variables = None

        def set_is_job_dataflow_running_variables(*args, **kwargs):
            nonlocal is_job_dataflow_running_variables
            is_job_dataflow_running_variables = copy.deepcopy(kwargs.get("variables"))

        dataflow_running = dataflow_hook_mock.return_value.is_job_dataflow_running
        dataflow_running.side_effect = set_is_job_dataflow_running_variables
        dataflow_running.return_value = False
        start_java_mock = beam_hook_mock.return_value.start_java_pipeline
        gcs_provide_file = gcs_hook.return_value.provide_file
        self.dataflow.check_if_running = True

        self.dataflow.execute(None)

        mock_callback_on_job_id.assert_called_once_with(on_new_job_id_callback=mock.ANY)
        gcs_provide_file.assert_called_once_with(object_url=JAR_FILE)
        expected_variables = {
            'project': dataflow_hook_mock.return_value.project_id,
            'stagingLocation': 'gs://test/staging',
            'jobName': JOB_NAME,
            'region': TEST_LOCATION,
            'output': 'gs://test/output',
            'labels': {'foo': 'bar', 'airflow-version': 'v2-1-0-dev0'},
        }
        self.assertEqual(expected_variables, is_job_dataflow_running_variables)
        job_name = dataflow_hook_mock.return_value.build_dataflow_job_name.return_value
        expected_variables["jobName"] = job_name
        start_java_mock.assert_called_once_with(
            variables=expected_variables,
            jar=gcs_provide_file.return_value.__enter__.return_value.name,
            job_class=JOB_CLASS,
            process_line_callback=mock_callback_on_job_id.return_value,
        )
        dataflow_hook_mock.return_value.wait_for_done.assert_called_once_with(
            job_id=mock.ANY,
            job_name=job_name,
            location=TEST_LOCATION,
            multiple_jobs=None,
        )

    @mock.patch(
        'airflow.providers.google.cloud.operators.dataflow.process_line_and_extract_dataflow_job_id_callback'
    )
    @mock.patch('airflow.providers.google.cloud.operators.dataflow.BeamHook')
    @mock.patch('airflow.providers.google.cloud.operators.dataflow.DataflowHook')
    @mock.patch('airflow.providers.google.cloud.operators.dataflow.GCSHook')
    def test_check_multiple_job_exec(
        self, gcs_hook, dataflow_hook_mock, beam_hook_mock, mock_callback_on_job_id
    ):
        """Test DataflowHook is created and the right args are passed to
        start_java_workflow with option to check if job is running
        """
        is_job_dataflow_running_variables = None

        def set_is_job_dataflow_running_variables(*args, **kwargs):
            nonlocal is_job_dataflow_running_variables
            is_job_dataflow_running_variables = copy.deepcopy(kwargs.get("variables"))

        dataflow_running = dataflow_hook_mock.return_value.is_job_dataflow_running
        dataflow_running.side_effect = set_is_job_dataflow_running_variables
        dataflow_running.return_value = False
        start_java_mock = beam_hook_mock.return_value.start_java_pipeline
        gcs_provide_file = gcs_hook.return_value.provide_file
        self.dataflow.check_if_running = True
        self.dataflow.multiple_jobs = True

        self.dataflow.execute(None)

        mock_callback_on_job_id.assert_called_once_with(on_new_job_id_callback=mock.ANY)
        gcs_provide_file.assert_called_once_with(object_url=JAR_FILE)
        expected_variables = {
            'project': dataflow_hook_mock.return_value.project_id,
            'stagingLocation': 'gs://test/staging',
            'jobName': JOB_NAME,
            'region': TEST_LOCATION,
            'output': 'gs://test/output',
            'labels': {'foo': 'bar', 'airflow-version': 'v2-1-0-dev0'},
        }
        self.assertEqual(expected_variables, is_job_dataflow_running_variables)
        job_name = dataflow_hook_mock.return_value.build_dataflow_job_name.return_value
        expected_variables["jobName"] = job_name
        start_java_mock.assert_called_once_with(
            variables=expected_variables,
            jar=gcs_provide_file.return_value.__enter__.return_value.name,
            job_class=JOB_CLASS,
            process_line_callback=mock_callback_on_job_id.return_value,
        )
        dataflow_hook_mock.return_value.wait_for_done.assert_called_once_with(
            job_id=mock.ANY,
            job_name=job_name,
            location=TEST_LOCATION,
            multiple_jobs=True,
        )


class TestDataflowTemplateOperator(unittest.TestCase):
    def setUp(self):
        self.dataflow = DataflowTemplatedJobStartOperator(
            task_id=TASK_ID,
            template=TEMPLATE,
            job_name=JOB_NAME,
            parameters=PARAMETERS,
            options=DEFAULT_OPTIONS_TEMPLATE,
            dataflow_default_options={"EXTRA_OPTION": "TEST_A"},
            poll_sleep=POLL_SLEEP,
            location=TEST_LOCATION,
            environment={"maxWorkers": 2},
        )

    @mock.patch('airflow.providers.google.cloud.operators.dataflow.DataflowHook')
    def test_exec(self, dataflow_mock):
        """Test DataflowHook is created and the right args are passed to
        start_template_workflow.

        """
        start_template_hook = dataflow_mock.return_value.start_template_dataflow
        self.dataflow.execute(None)
        assert dataflow_mock.called
        expected_options = {
            'project': 'test',
            'stagingLocation': 'gs://test/staging',
            'tempLocation': 'gs://test/temp',
            'zone': 'us-central1-f',
            'EXTRA_OPTION': "TEST_A",
        }
        start_template_hook.assert_called_once_with(
            job_name=JOB_NAME,
            variables=expected_options,
            parameters=PARAMETERS,
            dataflow_template=TEMPLATE,
            on_new_job_id_callback=mock.ANY,
            project_id=None,
            location=TEST_LOCATION,
            environment={'maxWorkers': 2},
        )


class TestDataflowStartFlexTemplateOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.dataflow.DataflowHook')
    def test_execute(self, mock_dataflow):
        start_flex_template = DataflowStartFlexTemplateOperator(
            task_id="start_flex_template_streaming_beam_sql",
            body={"launchParameter": TEST_FLEX_PARAMETERS},
            do_xcom_push=True,
            project_id=TEST_PROJECT,
            location=TEST_LOCATION,
        )
        start_flex_template.execute(mock.MagicMock())
        mock_dataflow.return_value.start_flex_template.assert_called_once_with(
            body={"launchParameter": TEST_FLEX_PARAMETERS},
            location=TEST_LOCATION,
            project_id=TEST_PROJECT,
            on_new_job_id_callback=mock.ANY,
        )

    def test_on_kill(self):
        start_flex_template = DataflowStartFlexTemplateOperator(
            task_id="start_flex_template_streaming_beam_sql",
            body={"launchParameter": TEST_FLEX_PARAMETERS},
            do_xcom_push=True,
            location=TEST_LOCATION,
            project_id=TEST_PROJECT,
        )
        start_flex_template.hook = mock.MagicMock()
        start_flex_template.job_id = JOB_ID
        start_flex_template.on_kill()
        start_flex_template.hook.cancel_job.assert_called_once_with(
            job_id='test-dataflow-pipeline-id', project_id=TEST_PROJECT
        )


class TestDataflowSqlOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.dataflow.DataflowHook')
    def test_execute(self, mock_hook):
        start_sql = DataflowStartSqlJobOperator(
            task_id="start_sql_query",
            job_name=TEST_SQL_JOB_NAME,
            query=TEST_SQL_QUERY,
            options=deepcopy(TEST_SQL_OPTIONS),
            location=TEST_LOCATION,
            do_xcom_push=True,
        )

        start_sql.execute(mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id='google_cloud_default', delegate_to=None, drain_pipeline=False
        )
        mock_hook.return_value.start_sql_job.assert_called_once_with(
            job_name=TEST_SQL_JOB_NAME,
            query=TEST_SQL_QUERY,
            options=TEST_SQL_OPTIONS,
            location=TEST_LOCATION,
            project_id=None,
            on_new_job_id_callback=mock.ANY,
        )
        start_sql.job_id = TEST_SQL_JOB_ID
        start_sql.on_kill()
        mock_hook.return_value.cancel_job.assert_called_once_with(job_id='test-job-id', project_id=None)
