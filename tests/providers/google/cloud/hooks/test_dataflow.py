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
import shlex
import subprocess
import unittest
from typing import Any, Dict
from unittest import mock
from unittest.mock import MagicMock
from uuid import UUID

import pytest
from parameterized import parameterized

from airflow.exceptions import AirflowException
from airflow.providers.apache.beam.hooks.beam import BeamCommandRunner, BeamHook
from airflow.providers.google.cloud.hooks.dataflow import (
    DEFAULT_DATAFLOW_LOCATION,
    DataflowHook,
    DataflowJobStatus,
    DataflowJobType,
    _DataflowJobsController,
    _fallback_to_project_id_from_variables,
    process_line_and_extract_dataflow_job_id_callback,
)

DEFAULT_RUNNER = "DirectRunner"
BEAM_STRING = 'airflow.providers.apache.beam.hooks.beam.{}'

TASK_ID = 'test-dataflow-operator'
JOB_NAME = 'test-dataflow-pipeline'
MOCK_UUID = UUID('cf4a56d2-8101-4217-b027-2af6216feb48')
MOCK_UUID_PREFIX = str(MOCK_UUID)[:8]
UNIQUE_JOB_NAME = f'test-dataflow-pipeline-{MOCK_UUID_PREFIX}'
TEST_TEMPLATE = 'gs://dataflow-templates/wordcount/template_file'
PARAMETERS = {
    'inputFile': 'gs://dataflow-samples/shakespeare/kinglear.txt',
    'output': 'gs://test/output/my_output',
}
PY_FILE = 'apache_beam.examples.wordcount'
JAR_FILE = 'unitest.jar'
JOB_CLASS = 'com.example.UnitTest'
PY_OPTIONS = ['-m']
DATAFLOW_VARIABLES_PY = {'project': 'test', 'staging_location': 'gs://test/staging', 'labels': {'foo': 'bar'}}
DATAFLOW_VARIABLES_JAVA = {
    'project': 'test',
    'stagingLocation': 'gs://test/staging',
    'labels': {'foo': 'bar'},
}
RUNTIME_ENV = {
    'additionalExperiments': ['exp_flag1', 'exp_flag2'],
    'additionalUserLabels': {'name': 'wrench', 'mass': '1.3kg', 'count': '3'},
    'bypassTempDirValidation': {},
    'ipConfiguration': 'WORKER_IP_PRIVATE',
    'kmsKeyName': (
        'projects/TEST_PROJECT_ID/locations/TEST_LOCATIONS/keyRings/TEST_KEYRING/cryptoKeys/TEST_CRYPTOKEYS'
    ),
    'maxWorkers': 10,
    'network': 'default',
    'numWorkers': 2,
    'serviceAccountEmail': 'test@apache.airflow',
    'subnetwork': 'regions/REGION/subnetworks/SUBNETWORK',
    'tempLocation': 'gs://test/temp',
    'workerRegion': "test-region",
    'workerZone': 'test-zone',
    'zone': 'us-central1-f',
    'machineType': 'n1-standard-1',
}
BASE_STRING = 'airflow.providers.google.common.hooks.base_google.{}'
DATAFLOW_STRING = 'airflow.providers.google.cloud.hooks.dataflow.{}'
TEST_PROJECT = 'test-project'
TEST_JOB_ID = 'test-job-id'
TEST_LOCATION = 'custom-location'
DEFAULT_PY_INTERPRETER = 'python3'
TEST_FLEX_PARAMETERS = {
    "containerSpecGcsPath": "gs://test-bucket/test-file",
    "jobName": 'test-job-name',
    "parameters": {
        "inputSubscription": 'test-subscription',
        "outputTable": "test-project:test-dataset.streaming_beam_sql",
    },
}
TEST_PROJECT_ID = 'test-project-id'

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

DEFAULT_CANCEL_TIMEOUT = 5 * 60


class TestFallbackToVariables(unittest.TestCase):
    def test_support_project_id_parameter(self):
        mock_instance = mock.MagicMock()

        class FixtureFallback:
            @_fallback_to_project_id_from_variables
            def test_fn(self, *args, **kwargs):
                mock_instance(*args, **kwargs)

        FixtureFallback().test_fn(project_id="TEST")

        mock_instance.assert_called_once_with(project_id="TEST")

    def test_support_project_id_from_variable_parameter(self):
        mock_instance = mock.MagicMock()

        class FixtureFallback:
            @_fallback_to_project_id_from_variables
            def test_fn(self, *args, **kwargs):
                mock_instance(*args, **kwargs)

        FixtureFallback().test_fn(variables={'project': "TEST"})

        mock_instance.assert_called_once_with(project_id='TEST', variables={})

    def test_raise_exception_on_conflict(self):
        mock_instance = mock.MagicMock()

        class FixtureFallback:
            @_fallback_to_project_id_from_variables
            def test_fn(self, *args, **kwargs):
                mock_instance(*args, **kwargs)

        with pytest.raises(
            AirflowException,
            match="The mutually exclusive parameter `project_id` and `project` key in `variables` parameter "
            "are both present\\. Please remove one\\.",
        ):
            FixtureFallback().test_fn(variables={'project': "TEST"}, project_id="TEST2")

    def test_raise_exception_on_positional_argument(self):
        mock_instance = mock.MagicMock()

        class FixtureFallback:
            @_fallback_to_project_id_from_variables
            def test_fn(self, *args, **kwargs):
                mock_instance(*args, **kwargs)

        with pytest.raises(
            AirflowException, match="You must use keyword arguments in this methods rather than positional"
        ):
            FixtureFallback().test_fn({'project': "TEST"}, "TEST2")


def mock_init(
    self,
    gcp_conn_id,
    delegate_to=None,
    impersonation_chain=None,
):  # pylint: disable=unused-argument
    pass


class TestDataflowHook(unittest.TestCase):
    def setUp(self):
        with mock.patch(BASE_STRING.format('GoogleBaseHook.__init__'), new=mock_init):
            self.dataflow_hook = DataflowHook(gcp_conn_id='test')
            self.dataflow_hook.beam_hook = MagicMock()

    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.DataflowHook._authorize")
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.build")
    def test_dataflow_client_creation(self, mock_build, mock_authorize):
        result = self.dataflow_hook.get_conn()
        mock_build.assert_called_once_with(
            'dataflow', 'v1b3', http=mock_authorize.return_value, cache_discovery=False
        )
        assert mock_build.return_value == result

    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.wait_for_done'))
    @mock.patch(DATAFLOW_STRING.format('process_line_and_extract_dataflow_job_id_callback'))
    def test_start_python_dataflow(self, mock_callback_on_job_id, mock_dataflow_wait_for_done, mock_uuid):
        mock_beam_start_python_pipeline = self.dataflow_hook.beam_hook.start_python_pipeline
        mock_uuid.return_value = MOCK_UUID
        on_new_job_id_callback = MagicMock()
        py_requirements = ["pandas", "numpy"]
        job_name = f"{JOB_NAME}-{MOCK_UUID_PREFIX}"

        with self.assertWarnsRegex(DeprecationWarning, "This method is deprecated"):
            self.dataflow_hook.start_python_dataflow(  # pylint: disable=no-value-for-parameter
                job_name=JOB_NAME,
                variables=DATAFLOW_VARIABLES_PY,
                dataflow=PY_FILE,
                py_options=PY_OPTIONS,
                py_interpreter=DEFAULT_PY_INTERPRETER,
                py_requirements=py_requirements,
                on_new_job_id_callback=on_new_job_id_callback,
            )

        expected_variables = copy.deepcopy(DATAFLOW_VARIABLES_PY)
        expected_variables["job_name"] = job_name
        expected_variables["region"] = DEFAULT_DATAFLOW_LOCATION

        mock_callback_on_job_id.assert_called_once_with(on_new_job_id_callback)
        mock_beam_start_python_pipeline.assert_called_once_with(
            variables=expected_variables,
            py_file=PY_FILE,
            py_interpreter=DEFAULT_PY_INTERPRETER,
            py_options=PY_OPTIONS,
            py_requirements=py_requirements,
            py_system_site_packages=False,
            process_line_callback=mock_callback_on_job_id.return_value,
        )

        mock_dataflow_wait_for_done.assert_called_once_with(
            job_id=mock.ANY, job_name=job_name, location=DEFAULT_DATAFLOW_LOCATION
        )

    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.wait_for_done'))
    @mock.patch(DATAFLOW_STRING.format('process_line_and_extract_dataflow_job_id_callback'))
    def test_start_python_dataflow_with_custom_region_as_variable(
        self, mock_callback_on_job_id, mock_dataflow_wait_for_done, mock_uuid
    ):
        mock_beam_start_python_pipeline = self.dataflow_hook.beam_hook.start_python_pipeline
        mock_uuid.return_value = MOCK_UUID
        on_new_job_id_callback = MagicMock()
        py_requirements = ["pandas", "numpy"]
        job_name = f"{JOB_NAME}-{MOCK_UUID_PREFIX}"

        passed_variables = copy.deepcopy(DATAFLOW_VARIABLES_PY)
        passed_variables["region"] = TEST_LOCATION

        with self.assertWarnsRegex(DeprecationWarning, "This method is deprecated"):
            self.dataflow_hook.start_python_dataflow(  # pylint: disable=no-value-for-parameter
                job_name=JOB_NAME,
                variables=passed_variables,
                dataflow=PY_FILE,
                py_options=PY_OPTIONS,
                py_interpreter=DEFAULT_PY_INTERPRETER,
                py_requirements=py_requirements,
                on_new_job_id_callback=on_new_job_id_callback,
            )

        expected_variables = copy.deepcopy(DATAFLOW_VARIABLES_PY)
        expected_variables["job_name"] = job_name
        expected_variables["region"] = TEST_LOCATION

        mock_callback_on_job_id.assert_called_once_with(on_new_job_id_callback)
        mock_beam_start_python_pipeline.assert_called_once_with(
            variables=expected_variables,
            py_file=PY_FILE,
            py_interpreter=DEFAULT_PY_INTERPRETER,
            py_options=PY_OPTIONS,
            py_requirements=py_requirements,
            py_system_site_packages=False,
            process_line_callback=mock_callback_on_job_id.return_value,
        )

        mock_dataflow_wait_for_done.assert_called_once_with(
            job_id=mock.ANY, job_name=job_name, location=TEST_LOCATION
        )

    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.wait_for_done'))
    @mock.patch(DATAFLOW_STRING.format('process_line_and_extract_dataflow_job_id_callback'))
    def test_start_python_dataflow_with_custom_region_as_parameter(
        self, mock_callback_on_job_id, mock_dataflow_wait_for_done, mock_uuid
    ):
        mock_beam_start_python_pipeline = self.dataflow_hook.beam_hook.start_python_pipeline
        mock_uuid.return_value = MOCK_UUID
        on_new_job_id_callback = MagicMock()
        py_requirements = ["pandas", "numpy"]
        job_name = f"{JOB_NAME}-{MOCK_UUID_PREFIX}"

        passed_variables = copy.deepcopy(DATAFLOW_VARIABLES_PY)

        with self.assertWarnsRegex(DeprecationWarning, "This method is deprecated"):
            self.dataflow_hook.start_python_dataflow(  # pylint: disable=no-value-for-parameter
                job_name=JOB_NAME,
                variables=passed_variables,
                dataflow=PY_FILE,
                py_options=PY_OPTIONS,
                py_interpreter=DEFAULT_PY_INTERPRETER,
                py_requirements=py_requirements,
                on_new_job_id_callback=on_new_job_id_callback,
                location=TEST_LOCATION,
            )

        expected_variables = copy.deepcopy(DATAFLOW_VARIABLES_PY)
        expected_variables["job_name"] = job_name
        expected_variables["region"] = TEST_LOCATION

        mock_callback_on_job_id.assert_called_once_with(on_new_job_id_callback)
        mock_beam_start_python_pipeline.assert_called_once_with(
            variables=expected_variables,
            py_file=PY_FILE,
            py_interpreter=DEFAULT_PY_INTERPRETER,
            py_options=PY_OPTIONS,
            py_requirements=py_requirements,
            py_system_site_packages=False,
            process_line_callback=mock_callback_on_job_id.return_value,
        )

        mock_dataflow_wait_for_done.assert_called_once_with(
            job_id=mock.ANY, job_name=job_name, location=TEST_LOCATION
        )

    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.wait_for_done'))
    @mock.patch(DATAFLOW_STRING.format('process_line_and_extract_dataflow_job_id_callback'))
    def test_start_python_dataflow_with_multiple_extra_packages(
        self, mock_callback_on_job_id, mock_dataflow_wait_for_done, mock_uuid
    ):
        mock_beam_start_python_pipeline = self.dataflow_hook.beam_hook.start_python_pipeline
        mock_uuid.return_value = MOCK_UUID
        on_new_job_id_callback = MagicMock()
        py_requirements = ["pandas", "numpy"]
        job_name = f"{JOB_NAME}-{MOCK_UUID_PREFIX}"

        passed_variables = copy.deepcopy(DATAFLOW_VARIABLES_PY)
        passed_variables['extra-package'] = ['a.whl', 'b.whl']

        with self.assertWarnsRegex(DeprecationWarning, "This method is deprecated"):
            self.dataflow_hook.start_python_dataflow(  # pylint: disable=no-value-for-parameter
                job_name=JOB_NAME,
                variables=passed_variables,
                dataflow=PY_FILE,
                py_options=PY_OPTIONS,
                py_interpreter=DEFAULT_PY_INTERPRETER,
                py_requirements=py_requirements,
                on_new_job_id_callback=on_new_job_id_callback,
            )

        expected_variables = copy.deepcopy(DATAFLOW_VARIABLES_PY)
        expected_variables["job_name"] = job_name
        expected_variables["region"] = DEFAULT_DATAFLOW_LOCATION
        expected_variables['extra-package'] = ['a.whl', 'b.whl']

        mock_callback_on_job_id.assert_called_once_with(on_new_job_id_callback)
        mock_beam_start_python_pipeline.assert_called_once_with(
            variables=expected_variables,
            py_file=PY_FILE,
            py_interpreter=DEFAULT_PY_INTERPRETER,
            py_options=PY_OPTIONS,
            py_requirements=py_requirements,
            py_system_site_packages=False,
            process_line_callback=mock_callback_on_job_id.return_value,
        )

        mock_dataflow_wait_for_done.assert_called_once_with(
            job_id=mock.ANY, job_name=job_name, location=DEFAULT_DATAFLOW_LOCATION
        )

    @parameterized.expand(
        [
            ('python3',),
            ('python2',),
            ('python3',),
            ('python3.6',),
        ]
    )
    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.wait_for_done'))
    @mock.patch(DATAFLOW_STRING.format('process_line_and_extract_dataflow_job_id_callback'))
    def test_start_python_dataflow_with_custom_interpreter(
        self, py_interpreter, mock_callback_on_job_id, mock_dataflow_wait_for_done, mock_uuid
    ):
        mock_beam_start_python_pipeline = self.dataflow_hook.beam_hook.start_python_pipeline
        mock_uuid.return_value = MOCK_UUID
        on_new_job_id_callback = MagicMock()
        job_name = f"{JOB_NAME}-{MOCK_UUID_PREFIX}"

        with self.assertWarnsRegex(DeprecationWarning, "This method is deprecated"):
            self.dataflow_hook.start_python_dataflow(  # pylint: disable=no-value-for-parameter
                job_name=JOB_NAME,
                variables=DATAFLOW_VARIABLES_PY,
                dataflow=PY_FILE,
                py_options=PY_OPTIONS,
                py_interpreter=py_interpreter,
                py_requirements=None,
                on_new_job_id_callback=on_new_job_id_callback,
            )

        expected_variables = copy.deepcopy(DATAFLOW_VARIABLES_PY)
        expected_variables["job_name"] = job_name
        expected_variables["region"] = DEFAULT_DATAFLOW_LOCATION

        mock_callback_on_job_id.assert_called_once_with(on_new_job_id_callback)
        mock_beam_start_python_pipeline.assert_called_once_with(
            variables=expected_variables,
            py_file=PY_FILE,
            py_interpreter=py_interpreter,
            py_options=PY_OPTIONS,
            py_requirements=None,
            py_system_site_packages=False,
            process_line_callback=mock_callback_on_job_id.return_value,
        )

        mock_dataflow_wait_for_done.assert_called_once_with(
            job_id=mock.ANY, job_name=job_name, location=DEFAULT_DATAFLOW_LOCATION
        )

    @parameterized.expand(
        [
            (['foo-bar'], False),
            (['foo-bar'], True),
            ([], True),
        ]
    )
    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.wait_for_done'))
    @mock.patch(DATAFLOW_STRING.format('process_line_and_extract_dataflow_job_id_callback'))
    def test_start_python_dataflow_with_non_empty_py_requirements_and_without_system_packages(
        self,
        current_py_requirements,
        current_py_system_site_packages,
        mock_callback_on_job_id,
        mock_dataflow_wait_for_done,
        mock_uuid,
    ):
        mock_beam_start_python_pipeline = self.dataflow_hook.beam_hook.start_python_pipeline
        mock_uuid.return_value = MOCK_UUID
        on_new_job_id_callback = MagicMock()
        job_name = f"{JOB_NAME}-{MOCK_UUID_PREFIX}"

        with self.assertWarnsRegex(DeprecationWarning, "This method is deprecated"):
            self.dataflow_hook.start_python_dataflow(  # pylint: disable=no-value-for-parameter
                job_name=JOB_NAME,
                variables=DATAFLOW_VARIABLES_PY,
                dataflow=PY_FILE,
                py_options=PY_OPTIONS,
                py_interpreter=DEFAULT_PY_INTERPRETER,
                py_requirements=current_py_requirements,
                py_system_site_packages=current_py_system_site_packages,
                on_new_job_id_callback=on_new_job_id_callback,
            )

        expected_variables = copy.deepcopy(DATAFLOW_VARIABLES_PY)
        expected_variables["job_name"] = job_name
        expected_variables["region"] = DEFAULT_DATAFLOW_LOCATION

        mock_callback_on_job_id.assert_called_once_with(on_new_job_id_callback)
        mock_beam_start_python_pipeline.assert_called_once_with(
            variables=expected_variables,
            py_file=PY_FILE,
            py_interpreter=DEFAULT_PY_INTERPRETER,
            py_options=PY_OPTIONS,
            py_requirements=current_py_requirements,
            py_system_site_packages=current_py_system_site_packages,
            process_line_callback=mock_callback_on_job_id.return_value,
        )

        mock_dataflow_wait_for_done.assert_called_once_with(
            job_id=mock.ANY, job_name=job_name, location=DEFAULT_DATAFLOW_LOCATION
        )

    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.wait_for_done'))
    def test_start_python_dataflow_with_empty_py_requirements_and_without_system_packages(
        self, mock_dataflow_wait_for_done, mock_uuid
    ):
        self.dataflow_hook.beam_hook = BeamHook(runner="DataflowRunner")
        mock_uuid.return_value = MOCK_UUID
        on_new_job_id_callback = MagicMock()

        with self.assertWarnsRegex(DeprecationWarning, "This method is deprecated"), self.assertRaisesRegex(
            AirflowException, "Invalid method invocation."
        ):
            self.dataflow_hook.start_python_dataflow(  # pylint: disable=no-value-for-parameter
                job_name=JOB_NAME,
                variables=DATAFLOW_VARIABLES_PY,
                dataflow=PY_FILE,
                py_options=PY_OPTIONS,
                py_interpreter=DEFAULT_PY_INTERPRETER,
                py_requirements=[],
                on_new_job_id_callback=on_new_job_id_callback,
            )

        mock_dataflow_wait_for_done.assert_not_called()

    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.wait_for_done'))
    @mock.patch(DATAFLOW_STRING.format('process_line_and_extract_dataflow_job_id_callback'))
    def test_start_java_dataflow(self, mock_callback_on_job_id, mock_dataflow_wait_for_done, mock_uuid):
        mock_beam_start_java_pipeline = self.dataflow_hook.beam_hook.start_java_pipeline
        mock_uuid.return_value = MOCK_UUID
        on_new_job_id_callback = MagicMock()
        job_name = f"{JOB_NAME}-{MOCK_UUID_PREFIX}"

        with self.assertWarnsRegex(DeprecationWarning, "This method is deprecated"):
            self.dataflow_hook.start_java_dataflow(  # pylint: disable=no-value-for-parameter
                job_name=JOB_NAME,
                variables=DATAFLOW_VARIABLES_JAVA,
                jar=JAR_FILE,
                job_class=JOB_CLASS,
                on_new_job_id_callback=on_new_job_id_callback,
            )

        expected_variables = copy.deepcopy(DATAFLOW_VARIABLES_JAVA)
        expected_variables["jobName"] = job_name
        expected_variables["region"] = DEFAULT_DATAFLOW_LOCATION
        expected_variables["labels"] = '{"foo":"bar"}'

        mock_callback_on_job_id.assert_called_once_with(on_new_job_id_callback)
        mock_beam_start_java_pipeline.assert_called_once_with(
            variables=expected_variables,
            jar=JAR_FILE,
            job_class=JOB_CLASS,
            process_line_callback=mock_callback_on_job_id.return_value,
        )

        mock_dataflow_wait_for_done.assert_called_once_with(
            job_id=mock.ANY, job_name=job_name, location=DEFAULT_DATAFLOW_LOCATION, multiple_jobs=False
        )

    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.wait_for_done'))
    @mock.patch(DATAFLOW_STRING.format('process_line_and_extract_dataflow_job_id_callback'))
    def test_start_java_dataflow_with_multiple_values_in_variables(
        self, mock_callback_on_job_id, mock_dataflow_wait_for_done, mock_uuid
    ):
        mock_beam_start_java_pipeline = self.dataflow_hook.beam_hook.start_java_pipeline
        mock_uuid.return_value = MOCK_UUID
        on_new_job_id_callback = MagicMock()
        job_name = f"{JOB_NAME}-{MOCK_UUID_PREFIX}"

        passed_variables: Dict[str, Any] = copy.deepcopy(DATAFLOW_VARIABLES_JAVA)
        passed_variables['mock-option'] = ['a.whl', 'b.whl']

        with self.assertWarnsRegex(DeprecationWarning, "This method is deprecated"):
            self.dataflow_hook.start_java_dataflow(  # pylint: disable=no-value-for-parameter
                job_name=JOB_NAME,
                variables=passed_variables,
                jar=JAR_FILE,
                job_class=JOB_CLASS,
                on_new_job_id_callback=on_new_job_id_callback,
            )

        expected_variables = copy.deepcopy(passed_variables)
        expected_variables["jobName"] = job_name
        expected_variables["region"] = DEFAULT_DATAFLOW_LOCATION
        expected_variables["labels"] = '{"foo":"bar"}'

        mock_callback_on_job_id.assert_called_once_with(on_new_job_id_callback)
        mock_beam_start_java_pipeline.assert_called_once_with(
            variables=expected_variables,
            jar=JAR_FILE,
            job_class=JOB_CLASS,
            process_line_callback=mock_callback_on_job_id.return_value,
        )

        mock_dataflow_wait_for_done.assert_called_once_with(
            job_id=mock.ANY, job_name=job_name, location=DEFAULT_DATAFLOW_LOCATION, multiple_jobs=False
        )

    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.wait_for_done'))
    @mock.patch(DATAFLOW_STRING.format('process_line_and_extract_dataflow_job_id_callback'))
    def test_start_java_dataflow_with_custom_region_as_variable(
        self, mock_callback_on_job_id, mock_dataflow_wait_for_done, mock_uuid
    ):
        mock_beam_start_java_pipeline = self.dataflow_hook.beam_hook.start_java_pipeline
        mock_uuid.return_value = MOCK_UUID
        on_new_job_id_callback = MagicMock()
        job_name = f"{JOB_NAME}-{MOCK_UUID_PREFIX}"

        passed_variables: Dict[str, Any] = copy.deepcopy(DATAFLOW_VARIABLES_JAVA)
        passed_variables['region'] = TEST_LOCATION

        with self.assertWarnsRegex(DeprecationWarning, "This method is deprecated"):
            self.dataflow_hook.start_java_dataflow(  # pylint: disable=no-value-for-parameter
                job_name=JOB_NAME,
                variables=passed_variables,
                jar=JAR_FILE,
                job_class=JOB_CLASS,
                on_new_job_id_callback=on_new_job_id_callback,
            )

        expected_variables = copy.deepcopy(DATAFLOW_VARIABLES_JAVA)
        expected_variables["jobName"] = job_name
        expected_variables["region"] = TEST_LOCATION
        expected_variables["labels"] = '{"foo":"bar"}'

        mock_callback_on_job_id.assert_called_once_with(on_new_job_id_callback)
        mock_beam_start_java_pipeline.assert_called_once_with(
            variables=expected_variables,
            jar=JAR_FILE,
            job_class=JOB_CLASS,
            process_line_callback=mock_callback_on_job_id.return_value,
        )

        mock_dataflow_wait_for_done.assert_called_once_with(
            job_id=mock.ANY, job_name=job_name, location=TEST_LOCATION, multiple_jobs=False
        )

    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.wait_for_done'))
    @mock.patch(DATAFLOW_STRING.format('process_line_and_extract_dataflow_job_id_callback'))
    def test_start_java_dataflow_with_custom_region_as_parameter(
        self, mock_callback_on_job_id, mock_dataflow_wait_for_done, mock_uuid
    ):
        mock_beam_start_java_pipeline = self.dataflow_hook.beam_hook.start_java_pipeline
        mock_uuid.return_value = MOCK_UUID
        on_new_job_id_callback = MagicMock()
        job_name = f"{JOB_NAME}-{MOCK_UUID_PREFIX}"

        with self.assertWarnsRegex(DeprecationWarning, "This method is deprecated"):
            self.dataflow_hook.start_java_dataflow(  # pylint: disable=no-value-for-parameter
                job_name=JOB_NAME,
                variables=DATAFLOW_VARIABLES_JAVA,
                jar=JAR_FILE,
                job_class=JOB_CLASS,
                on_new_job_id_callback=on_new_job_id_callback,
                location=TEST_LOCATION,
            )

        expected_variables = copy.deepcopy(DATAFLOW_VARIABLES_JAVA)
        expected_variables["jobName"] = job_name
        expected_variables["region"] = TEST_LOCATION
        expected_variables["labels"] = '{"foo":"bar"}'

        mock_callback_on_job_id.assert_called_once_with(on_new_job_id_callback)
        mock_beam_start_java_pipeline.assert_called_once_with(
            variables=expected_variables,
            jar=JAR_FILE,
            job_class=JOB_CLASS,
            process_line_callback=mock_callback_on_job_id.return_value,
        )

        mock_dataflow_wait_for_done.assert_called_once_with(
            job_id=mock.ANY, job_name=job_name, location=TEST_LOCATION, multiple_jobs=False
        )

    @parameterized.expand(
        [
            (JOB_NAME, JOB_NAME, False),
            ('test-example', 'test_example', False),
            (f'test-dataflow-pipeline-{MOCK_UUID_PREFIX}', JOB_NAME, True),
            (f'test-example-{MOCK_UUID_PREFIX}', 'test_example', True),
            ('df-job-1', 'df-job-1', False),
            ('df-job', 'df-job', False),
            ('dfjob', 'dfjob', False),
            ('dfjob1', 'dfjob1', False),
        ]
    )
    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'), return_value=MOCK_UUID)
    def test_valid_dataflow_job_name(self, expected_result, job_name, append_job_name, mock_uuid4):
        job_name = self.dataflow_hook.build_dataflow_job_name(
            job_name=job_name, append_job_name=append_job_name
        )

        self.assertEqual(expected_result, job_name)

    #
    @parameterized.expand([("1dfjob@",), ("dfjob@",), ("df^jo",)])
    def test_build_dataflow_job_name_with_invalid_value(self, job_name):
        self.assertRaises(
            ValueError, self.dataflow_hook.build_dataflow_job_name, job_name=job_name, append_job_name=False
        )

    #
    @mock.patch(DATAFLOW_STRING.format('_DataflowJobsController'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.get_conn'))
    def test_get_job(self, mock_conn, mock_dataflowjob):
        method_fetch_job_by_id = mock_dataflowjob.return_value.fetch_job_by_id

        self.dataflow_hook.get_job(job_id=TEST_JOB_ID, project_id=TEST_PROJECT_ID, location=TEST_LOCATION)
        mock_conn.assert_called_once()
        mock_dataflowjob.assert_called_once_with(
            dataflow=mock_conn.return_value,
            project_number=TEST_PROJECT_ID,
            location=TEST_LOCATION,
        )
        method_fetch_job_by_id.assert_called_once_with(TEST_JOB_ID)

    #
    @mock.patch(DATAFLOW_STRING.format('_DataflowJobsController'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.get_conn'))
    def test_fetch_job_metrics_by_id(self, mock_conn, mock_dataflowjob):
        method_fetch_job_metrics_by_id = mock_dataflowjob.return_value.fetch_job_metrics_by_id

        self.dataflow_hook.fetch_job_metrics_by_id(
            job_id=TEST_JOB_ID, project_id=TEST_PROJECT_ID, location=TEST_LOCATION
        )
        mock_conn.assert_called_once()
        mock_dataflowjob.assert_called_once_with(
            dataflow=mock_conn.return_value,
            project_number=TEST_PROJECT_ID,
            location=TEST_LOCATION,
        )
        method_fetch_job_metrics_by_id.assert_called_once_with(TEST_JOB_ID)

    @mock.patch(DATAFLOW_STRING.format('DataflowHook.get_conn'))
    def test_fetch_job_metrics_by_id_controller(self, mock_conn):
        method_get_metrics = (
            mock_conn.return_value.projects.return_value.locations.return_value.jobs.return_value.getMetrics
        )
        self.dataflow_hook.fetch_job_metrics_by_id(
            job_id=TEST_JOB_ID, project_id=TEST_PROJECT_ID, location=TEST_LOCATION
        )

        mock_conn.assert_called_once()
        method_get_metrics.return_value.execute.assert_called_once_with(num_retries=0)
        method_get_metrics.assert_called_once_with(
            jobId=TEST_JOB_ID, projectId=TEST_PROJECT_ID, location=TEST_LOCATION
        )

    @mock.patch(DATAFLOW_STRING.format('_DataflowJobsController'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.get_conn'))
    def test_fetch_job_messages_by_id(self, mock_conn, mock_dataflowjob):
        method_fetch_job_messages_by_id = mock_dataflowjob.return_value.fetch_job_messages_by_id

        self.dataflow_hook.fetch_job_messages_by_id(
            job_id=TEST_JOB_ID, project_id=TEST_PROJECT_ID, location=TEST_LOCATION
        )
        mock_conn.assert_called_once()
        mock_dataflowjob.assert_called_once_with(
            dataflow=mock_conn.return_value,
            project_number=TEST_PROJECT_ID,
            location=TEST_LOCATION,
        )
        method_fetch_job_messages_by_id.assert_called_once_with(TEST_JOB_ID)

    @mock.patch(DATAFLOW_STRING.format('_DataflowJobsController'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.get_conn'))
    def test_fetch_job_autoscaling_events_by_id(self, mock_conn, mock_dataflowjob):
        method_fetch_job_autoscaling_events_by_id = (
            mock_dataflowjob.return_value.fetch_job_autoscaling_events_by_id
        )

        self.dataflow_hook.fetch_job_autoscaling_events_by_id(
            job_id=TEST_JOB_ID, project_id=TEST_PROJECT_ID, location=TEST_LOCATION
        )
        mock_conn.assert_called_once()
        mock_dataflowjob.assert_called_once_with(
            dataflow=mock_conn.return_value,
            project_number=TEST_PROJECT_ID,
            location=TEST_LOCATION,
        )
        method_fetch_job_autoscaling_events_by_id.assert_called_once_with(TEST_JOB_ID)

    @mock.patch(DATAFLOW_STRING.format('_DataflowJobsController'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.get_conn'))
    def test_wait_for_done(self, mock_conn, mock_dataflowjob):
        method_wait_for_done = mock_dataflowjob.return_value.wait_for_done

        self.dataflow_hook.wait_for_done(
            job_name="JOB_NAME",
            project_id=TEST_PROJECT_ID,
            job_id=TEST_JOB_ID,
            location=TEST_LOCATION,
            multiple_jobs=False,
        )
        mock_conn.assert_called_once()
        mock_dataflowjob.assert_called_once_with(
            dataflow=mock_conn.return_value,
            project_number=TEST_PROJECT_ID,
            name="JOB_NAME",
            location=TEST_LOCATION,
            poll_sleep=self.dataflow_hook.poll_sleep,
            job_id=TEST_JOB_ID,
            num_retries=self.dataflow_hook.num_retries,
            multiple_jobs=False,
            drain_pipeline=self.dataflow_hook.drain_pipeline,
            cancel_timeout=self.dataflow_hook.cancel_timeout,
            wait_until_finished=self.dataflow_hook.wait_until_finished,
        )
        method_wait_for_done.assert_called_once_with()


class TestDataflowTemplateHook(unittest.TestCase):
    def setUp(self):
        with mock.patch(BASE_STRING.format('GoogleBaseHook.__init__'), new=mock_init):
            self.dataflow_hook = DataflowHook(gcp_conn_id='test')

    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'), return_value=MOCK_UUID)
    @mock.patch(DATAFLOW_STRING.format('_DataflowJobsController'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.get_conn'))
    def test_start_template_dataflow(self, mock_conn, mock_controller, mock_uuid):

        launch_method = (
            mock_conn.return_value.projects.return_value.locations.return_value.templates.return_value.launch
        )
        launch_method.return_value.execute.return_value = {"job": {"id": TEST_JOB_ID}}
        variables = {'zone': 'us-central1-f', 'tempLocation': 'gs://test/temp'}
        self.dataflow_hook.start_template_dataflow(  # pylint: disable=no-value-for-parameter
            job_name=JOB_NAME,
            variables=copy.deepcopy(variables),
            parameters=PARAMETERS,
            dataflow_template=TEST_TEMPLATE,
            project_id=TEST_PROJECT,
        )

        launch_method.assert_called_once_with(
            body={
                'jobName': f'test-dataflow-pipeline-{MOCK_UUID_PREFIX}',
                'parameters': PARAMETERS,
                'environment': variables,
            },
            gcsPath='gs://dataflow-templates/wordcount/template_file',
            projectId=TEST_PROJECT,
            location=DEFAULT_DATAFLOW_LOCATION,
        )

        mock_controller.assert_called_once_with(
            dataflow=mock_conn.return_value,
            job_id='test-job-id',
            name=f'test-dataflow-pipeline-{MOCK_UUID_PREFIX}',
            num_retries=5,
            poll_sleep=10,
            project_number=TEST_PROJECT,
            location=DEFAULT_DATAFLOW_LOCATION,
            drain_pipeline=False,
            cancel_timeout=DEFAULT_CANCEL_TIMEOUT,
        )
        mock_controller.return_value.wait_for_done.assert_called_once()

    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'), return_value=MOCK_UUID)
    @mock.patch(DATAFLOW_STRING.format('_DataflowJobsController'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.get_conn'))
    def test_start_template_dataflow_with_custom_region_as_variable(
        self, mock_conn, mock_controller, mock_uuid
    ):
        launch_method = (
            mock_conn.return_value.projects.return_value.locations.return_value.templates.return_value.launch
        )
        launch_method.return_value.execute.return_value = {"job": {"id": TEST_JOB_ID}}
        self.dataflow_hook.start_template_dataflow(  # pylint: disable=no-value-for-parameter
            job_name=JOB_NAME,
            variables={'region': TEST_LOCATION},
            parameters=PARAMETERS,
            dataflow_template=TEST_TEMPLATE,
            project_id=TEST_PROJECT,
        )

        launch_method.assert_called_once_with(
            projectId=TEST_PROJECT,
            location=TEST_LOCATION,
            gcsPath=TEST_TEMPLATE,
            body=mock.ANY,
        )

        mock_controller.assert_called_once_with(
            dataflow=mock_conn.return_value,
            job_id=TEST_JOB_ID,
            name=UNIQUE_JOB_NAME,
            num_retries=5,
            poll_sleep=10,
            project_number=TEST_PROJECT,
            location=TEST_LOCATION,
            drain_pipeline=False,
            cancel_timeout=DEFAULT_CANCEL_TIMEOUT,
        )
        mock_controller.return_value.wait_for_done.assert_called_once()

    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'), return_value=MOCK_UUID)
    @mock.patch(DATAFLOW_STRING.format('_DataflowJobsController'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.get_conn'))
    def test_start_template_dataflow_with_custom_region_as_parameter(
        self, mock_conn, mock_controller, mock_uuid
    ):
        launch_method = (
            mock_conn.return_value.projects.return_value.locations.return_value.templates.return_value.launch
        )
        launch_method.return_value.execute.return_value = {"job": {"id": TEST_JOB_ID}}

        self.dataflow_hook.start_template_dataflow(  # pylint: disable=no-value-for-parameter
            job_name=JOB_NAME,
            variables={},
            parameters=PARAMETERS,
            dataflow_template=TEST_TEMPLATE,
            location=TEST_LOCATION,
            project_id=TEST_PROJECT,
        )

        launch_method.assert_called_once_with(
            body={'jobName': UNIQUE_JOB_NAME, 'parameters': PARAMETERS, 'environment': {}},
            gcsPath='gs://dataflow-templates/wordcount/template_file',
            projectId=TEST_PROJECT,
            location=TEST_LOCATION,
        )

        mock_controller.assert_called_once_with(
            dataflow=mock_conn.return_value,
            job_id=TEST_JOB_ID,
            name=UNIQUE_JOB_NAME,
            num_retries=5,
            poll_sleep=10,
            project_number=TEST_PROJECT,
            location=TEST_LOCATION,
            drain_pipeline=False,
            cancel_timeout=DEFAULT_CANCEL_TIMEOUT,
        )
        mock_controller.return_value.wait_for_done.assert_called_once()

    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'), return_value=MOCK_UUID)
    @mock.patch(DATAFLOW_STRING.format('_DataflowJobsController'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.get_conn'))
    def test_start_template_dataflow_with_runtime_env(self, mock_conn, mock_dataflowjob, mock_uuid):
        options_with_runtime_env = copy.deepcopy(RUNTIME_ENV)

        dataflowjob_instance = mock_dataflowjob.return_value
        dataflowjob_instance.wait_for_done.return_value = None
        # fmt: off
        method = (mock_conn.return_value
                  .projects.return_value
                  .locations.return_value
                  .templates.return_value
                  .launch)
        # fmt: on
        method.return_value.execute.return_value = {'job': {'id': TEST_JOB_ID}}
        self.dataflow_hook.start_template_dataflow(  # pylint: disable=no-value-for-parameter
            job_name=JOB_NAME,
            variables=options_with_runtime_env,
            parameters=PARAMETERS,
            dataflow_template=TEST_TEMPLATE,
            project_id=TEST_PROJECT,
            environment={"numWorkers": 17},
        )
        body = {"jobName": mock.ANY, "parameters": PARAMETERS, "environment": RUNTIME_ENV}
        method.assert_called_once_with(
            projectId=TEST_PROJECT,
            location=DEFAULT_DATAFLOW_LOCATION,
            gcsPath=TEST_TEMPLATE,
            body=body,
        )
        mock_dataflowjob.assert_called_once_with(
            dataflow=mock_conn.return_value,
            job_id=TEST_JOB_ID,
            location=DEFAULT_DATAFLOW_LOCATION,
            name=f'test-dataflow-pipeline-{MOCK_UUID_PREFIX}',
            num_retries=5,
            poll_sleep=10,
            project_number=TEST_PROJECT,
            drain_pipeline=False,
            cancel_timeout=DEFAULT_CANCEL_TIMEOUT,
        )
        mock_uuid.assert_called_once_with()

    @mock.patch(DATAFLOW_STRING.format('uuid.uuid4'), return_value=MOCK_UUID)
    @mock.patch(DATAFLOW_STRING.format('_DataflowJobsController'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.get_conn'))
    def test_start_template_dataflow_update_runtime_env(self, mock_conn, mock_dataflowjob, mock_uuid):
        options_with_runtime_env = copy.deepcopy(RUNTIME_ENV)
        del options_with_runtime_env["numWorkers"]
        runtime_env = {"numWorkers": 17}
        expected_runtime_env = copy.deepcopy(RUNTIME_ENV)
        expected_runtime_env.update(runtime_env)

        dataflowjob_instance = mock_dataflowjob.return_value
        dataflowjob_instance.wait_for_done.return_value = None
        # fmt: off
        method = (mock_conn.return_value
                  .projects.return_value
                  .locations.return_value
                  .templates.return_value
                  .launch)
        # fmt: on
        method.return_value.execute.return_value = {'job': {'id': TEST_JOB_ID}}
        self.dataflow_hook.start_template_dataflow(  # pylint: disable=no-value-for-parameter
            job_name=JOB_NAME,
            variables=options_with_runtime_env,
            parameters=PARAMETERS,
            dataflow_template=TEST_TEMPLATE,
            project_id=TEST_PROJECT,
            environment=runtime_env,
        )
        body = {"jobName": mock.ANY, "parameters": PARAMETERS, "environment": expected_runtime_env}
        method.assert_called_once_with(
            projectId=TEST_PROJECT,
            location=DEFAULT_DATAFLOW_LOCATION,
            gcsPath=TEST_TEMPLATE,
            body=body,
        )
        mock_dataflowjob.assert_called_once_with(
            dataflow=mock_conn.return_value,
            job_id=TEST_JOB_ID,
            location=DEFAULT_DATAFLOW_LOCATION,
            name=f'test-dataflow-pipeline-{MOCK_UUID_PREFIX}',
            num_retries=5,
            poll_sleep=10,
            project_number=TEST_PROJECT,
            drain_pipeline=False,
            cancel_timeout=DEFAULT_CANCEL_TIMEOUT,
        )
        mock_uuid.assert_called_once_with()

    @mock.patch(DATAFLOW_STRING.format('_DataflowJobsController'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.get_conn'))
    def test_start_flex_template(self, mock_conn, mock_controller):
        mock_locations = mock_conn.return_value.projects.return_value.locations
        launch_method = mock_locations.return_value.flexTemplates.return_value.launch
        launch_method.return_value.execute.return_value = {"job": {"id": TEST_JOB_ID}}
        mock_controller.return_value.get_jobs.return_value = [{"id": TEST_JOB_ID}]

        on_new_job_id_callback = mock.MagicMock()
        result = self.dataflow_hook.start_flex_template(
            body={"launchParameter": TEST_FLEX_PARAMETERS},
            location=TEST_LOCATION,
            project_id=TEST_PROJECT_ID,
            on_new_job_id_callback=on_new_job_id_callback,
        )
        on_new_job_id_callback.assert_called_once_with(TEST_JOB_ID)
        launch_method.assert_called_once_with(
            projectId='test-project-id',
            body={'launchParameter': TEST_FLEX_PARAMETERS},
            location=TEST_LOCATION,
        )
        mock_controller.assert_called_once_with(
            dataflow=mock_conn.return_value,
            project_number=TEST_PROJECT_ID,
            job_id=TEST_JOB_ID,
            location=TEST_LOCATION,
            poll_sleep=self.dataflow_hook.poll_sleep,
            num_retries=self.dataflow_hook.num_retries,
            cancel_timeout=DEFAULT_CANCEL_TIMEOUT,
        )
        mock_controller.return_value.get_jobs.wait_for_done.assrt_called_once_with()
        mock_controller.return_value.get_jobs.assrt_called_once_with()

        assert result == {"id": TEST_JOB_ID}

    @mock.patch(DATAFLOW_STRING.format('_DataflowJobsController'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.get_conn'))
    def test_cancel_job(self, mock_get_conn, jobs_controller):
        self.dataflow_hook.cancel_job(
            job_name=UNIQUE_JOB_NAME, job_id=TEST_JOB_ID, project_id=TEST_PROJECT, location=TEST_LOCATION
        )
        jobs_controller.assert_called_once_with(
            dataflow=mock_get_conn.return_value,
            job_id=TEST_JOB_ID,
            location=TEST_LOCATION,
            name=UNIQUE_JOB_NAME,
            poll_sleep=10,
            project_number=TEST_PROJECT,
            num_retries=5,
            drain_pipeline=False,
            cancel_timeout=DEFAULT_CANCEL_TIMEOUT,
        )
        jobs_controller.cancel()

    @mock.patch(DATAFLOW_STRING.format('_DataflowJobsController'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.provide_authorized_gcloud'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.get_conn'))
    @mock.patch(DATAFLOW_STRING.format('subprocess.run'))
    def test_start_sql_job_failed_to_run(
        self, mock_run, mock_get_conn, mock_provide_authorized_gcloud, mock_controller
    ):
        test_job = {'id': "TEST_JOB_ID"}
        mock_controller.return_value.get_jobs.return_value = [test_job]
        mock_run.return_value = mock.MagicMock(
            stdout=f"{TEST_JOB_ID}\n".encode(), stderr=f"{TEST_JOB_ID}\n".encode(), returncode=0
        )
        on_new_job_id_callback = mock.MagicMock()
        result = self.dataflow_hook.start_sql_job(
            job_name=TEST_SQL_JOB_NAME,
            query=TEST_SQL_QUERY,
            options=TEST_SQL_OPTIONS,
            location=TEST_LOCATION,
            project_id=TEST_PROJECT,
            on_new_job_id_callback=on_new_job_id_callback,
        )
        mock_run.assert_called_once_with(
            [
                'gcloud',
                'dataflow',
                'sql',
                'query',
                TEST_SQL_QUERY,
                '--project=test-project',
                '--format=value(job.id)',
                '--job-name=test-sql-job-name',
                '--region=custom-location',
                '--bigquery-project=test-project',
                '--bigquery-dataset=test-dataset',
                '--bigquery-table=beam_output',
                '--bigquery-write-disposition=write-truncate',
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        mock_controller.assert_called_once_with(
            dataflow=mock_get_conn.return_value,
            job_id=TEST_JOB_ID,
            location=TEST_LOCATION,
            poll_sleep=10,
            project_number=TEST_PROJECT,
            num_retries=5,
            drain_pipeline=False,
        )
        mock_controller.return_value.wait_for_done.assert_called_once()
        assert result == test_job

    @mock.patch(DATAFLOW_STRING.format('DataflowHook.get_conn'))
    @mock.patch(DATAFLOW_STRING.format('DataflowHook.provide_authorized_gcloud'))
    @mock.patch(DATAFLOW_STRING.format('subprocess.run'))
    def test_start_sql_job(self, mock_run, mock_provide_authorized_gcloud, mock_get_conn):
        mock_run.return_value = mock.MagicMock(
            stdout=f"{TEST_JOB_ID}\n".encode(), stderr=f"{TEST_JOB_ID}\n".encode(), returncode=1
        )
        with pytest.raises(AirflowException):
            self.dataflow_hook.start_sql_job(
                job_name=TEST_SQL_JOB_NAME,
                query=TEST_SQL_QUERY,
                options=TEST_SQL_OPTIONS,
                location=TEST_LOCATION,
                project_id=TEST_PROJECT,
                on_new_job_id_callback=mock.MagicMock(),
            )


class TestDataflowJob(unittest.TestCase):
    def setUp(self):
        self.mock_dataflow = MagicMock()

    def test_dataflow_job_init_with_job_id(self):
        mock_jobs = MagicMock()
        self.mock_dataflow.projects.return_value.locations.return_value.jobs.return_value = mock_jobs
        _DataflowJobsController(
            self.mock_dataflow, TEST_PROJECT, TEST_LOCATION, 10, UNIQUE_JOB_NAME, TEST_JOB_ID
        ).get_jobs()
        mock_jobs.get.assert_called_once_with(
            projectId=TEST_PROJECT, location=TEST_LOCATION, jobId=TEST_JOB_ID
        )

    def test_dataflow_job_init_without_job_id(self):
        job = {"id": TEST_JOB_ID, "name": UNIQUE_JOB_NAME, "currentState": DataflowJobStatus.JOB_STATE_DONE}

        mock_list = self.mock_dataflow.projects.return_value.locations.return_value.jobs.return_value.list
        (mock_list.return_value.execute.return_value) = {'jobs': [job]}
        # fmt: off
        (
            self.mock_dataflow.projects.return_value.
            locations.return_value.
            jobs.return_value.
            list_next.return_value
        ) = None
        # fmt: on
        _DataflowJobsController(
            self.mock_dataflow, TEST_PROJECT, TEST_LOCATION, 10, UNIQUE_JOB_NAME
        ).get_jobs()

        mock_list.assert_called_once_with(projectId=TEST_PROJECT, location=TEST_LOCATION)

    def test_dataflow_job_wait_for_multiple_jobs(self):
        job = {
            "id": TEST_JOB_ID,
            "name": UNIQUE_JOB_NAME,
            "type": DataflowJobType.JOB_TYPE_BATCH,
            "currentState": DataflowJobStatus.JOB_STATE_DONE,
        }
        # fmt: off
        (
            self.mock_dataflow.projects.return_value.
            locations.return_value.
            jobs.return_value.
            list.return_value.
            execute.return_value
        ) = {
            "jobs": [job, job]
        }
        (
            self.mock_dataflow.projects.return_value.
            locations.return_value.
            jobs.return_value.
            list_next.return_value
        ) = None
        # fmt: on
        dataflow_job = _DataflowJobsController(
            dataflow=self.mock_dataflow,
            project_number=TEST_PROJECT,
            name=UNIQUE_JOB_NAME,
            location=TEST_LOCATION,
            poll_sleep=10,
            job_id=TEST_JOB_ID,
            num_retries=20,
            multiple_jobs=True,
        )
        dataflow_job.wait_for_done()

        # fmt: off
        self.mock_dataflow.projects.return_value.locations.return_value.jobs.return_value.\
            list.assert_called_once_with(location=TEST_LOCATION, projectId=TEST_PROJECT)

        self.mock_dataflow.projects.return_value.locations.return_value.jobs.return_value.list\
            .return_value.execute.assert_called_once_with(num_retries=20)
        # fmt: on

        assert dataflow_job.get_jobs() == [job, job]

    @parameterized.expand(
        [
            (DataflowJobStatus.JOB_STATE_FAILED, "Google Cloud Dataflow job name-2 has failed\\."),
            (DataflowJobStatus.JOB_STATE_CANCELLED, "Google Cloud Dataflow job name-2 was cancelled\\."),
            (DataflowJobStatus.JOB_STATE_DRAINED, "Google Cloud Dataflow job name-2 was drained\\."),
            (DataflowJobStatus.JOB_STATE_UPDATED, "Google Cloud Dataflow job name-2 was updated\\."),
            (
                DataflowJobStatus.JOB_STATE_UNKNOWN,
                "Google Cloud Dataflow job name-2 was unknown state: JOB_STATE_UNKNOWN",
            ),
        ]
    )
    def test_dataflow_job_wait_for_multiple_jobs_and_one_in_terminal_state(self, state, exception_regex):
        # fmt: off
        (
            self.mock_dataflow.projects.return_value.
            locations.return_value.
            jobs.return_value.
            list.return_value.
            execute.return_value
        ) = {
            "jobs": [
                {
                    "id": "id-1", "name": "name-1",
                    "type": DataflowJobType.JOB_TYPE_BATCH,
                    "currentState": DataflowJobStatus.JOB_STATE_DONE
                },
                {
                    "id": "id-2", "name": "name-2",
                    "type": DataflowJobType.JOB_TYPE_BATCH,
                    "currentState": state
                }
            ]
        }
        (
            self.mock_dataflow.projects.return_value.
            locations.return_value.
            jobs.return_value.
            list_next.return_value
        ) = None
        # fmt: on
        dataflow_job = _DataflowJobsController(
            dataflow=self.mock_dataflow,
            project_number=TEST_PROJECT,
            name="name-",
            location=TEST_LOCATION,
            poll_sleep=0,
            job_id=None,
            num_retries=20,
            multiple_jobs=True,
        )
        with pytest.raises(Exception, match=exception_regex):
            dataflow_job.wait_for_done()

    def test_dataflow_job_wait_for_multiple_jobs_and_streaming_jobs(self):
        # fmt: off
        mock_jobs_list = (
            self.mock_dataflow.projects.return_value.
            locations.return_value.
            jobs.return_value.
            list
        )
        mock_jobs_list.return_value.execute.return_value = {
            "jobs": [
                {
                    "id": "id-2",
                    "name": "name-2",
                    "currentState": DataflowJobStatus.JOB_STATE_RUNNING,
                    "type": DataflowJobType.JOB_TYPE_STREAMING
                }
            ]
        }
        (
            self.mock_dataflow.projects.return_value.
            locations.return_value.
            jobs.return_value.
            list_next.return_value
        ) = None
        # fmt: on
        dataflow_job = _DataflowJobsController(
            dataflow=self.mock_dataflow,
            project_number=TEST_PROJECT,
            name="name-",
            location=TEST_LOCATION,
            poll_sleep=0,
            job_id=None,
            num_retries=20,
            multiple_jobs=True,
        )
        dataflow_job.wait_for_done()

        assert 1 == mock_jobs_list.call_count

    def test_dataflow_job_wait_for_single_jobs(self):
        job = {
            "id": TEST_JOB_ID,
            "name": UNIQUE_JOB_NAME,
            "type": DataflowJobType.JOB_TYPE_BATCH,
            "currentState": DataflowJobStatus.JOB_STATE_DONE,
        }
        # fmt: off
        self.mock_dataflow.projects.return_value.locations.return_value. \
            jobs.return_value.get.return_value.execute.return_value = job

        (
            self.mock_dataflow.projects.return_value.
            locations.return_value.
            jobs.return_value.
            list_next.return_value
        ) = None
        # fmt: on
        dataflow_job = _DataflowJobsController(
            dataflow=self.mock_dataflow,
            project_number=TEST_PROJECT,
            name=UNIQUE_JOB_NAME,
            location=TEST_LOCATION,
            poll_sleep=10,
            job_id=TEST_JOB_ID,
            num_retries=20,
            multiple_jobs=False,
        )
        dataflow_job.wait_for_done()
        # fmt: off
        self.mock_dataflow.projects.return_value.locations.return_value. \
            jobs.return_value.get.assert_called_once_with(
                jobId=TEST_JOB_ID,
                location=TEST_LOCATION,
                projectId=TEST_PROJECT
            )

        self.mock_dataflow.projects.return_value.locations.return_value. \
            jobs.return_value.get.return_value.execute.assert_called_once_with(num_retries=20)
        # fmt: on
        assert dataflow_job.get_jobs() == [job]

    def test_dataflow_job_is_job_running_with_no_job(self):
        # fmt: off
        mock_jobs_list = (
            self.mock_dataflow.projects.return_value.
            locations.return_value.
            jobs.return_value.
            list
        )
        mock_jobs_list.return_value.execute.return_value = {
            "jobs": []
        }
        (
            self.mock_dataflow.projects.return_value.
            locations.return_value.
            jobs.return_value.
            list_next.return_value
        ) = None
        # fmt: on
        dataflow_job = _DataflowJobsController(
            dataflow=self.mock_dataflow,
            project_number=TEST_PROJECT,
            name="name-",
            location=TEST_LOCATION,
            poll_sleep=0,
            job_id=None,
            num_retries=20,
            multiple_jobs=True,
        )
        result = dataflow_job.is_job_running()

        assert result is False

    # fmt: off
    @parameterized.expand([
        # RUNNING
        (DataflowJobType.JOB_TYPE_BATCH, DataflowJobStatus.JOB_STATE_RUNNING, None, False),
        (DataflowJobType.JOB_TYPE_STREAMING, DataflowJobStatus.JOB_STATE_RUNNING, None, True),
        (DataflowJobType.JOB_TYPE_BATCH, DataflowJobStatus.JOB_STATE_RUNNING, True, False),
        (DataflowJobType.JOB_TYPE_STREAMING, DataflowJobStatus.JOB_STATE_RUNNING, True, False),
        (DataflowJobType.JOB_TYPE_BATCH, DataflowJobStatus.JOB_STATE_RUNNING, False, True),
        (DataflowJobType.JOB_TYPE_STREAMING, DataflowJobStatus.JOB_STATE_RUNNING, False, True),
        # AWAITING STATE
        (DataflowJobType.JOB_TYPE_BATCH, DataflowJobStatus.JOB_STATE_PENDING, None, False),
        (DataflowJobType.JOB_TYPE_STREAMING, DataflowJobStatus.JOB_STATE_PENDING, None, False),
        (DataflowJobType.JOB_TYPE_BATCH, DataflowJobStatus.JOB_STATE_PENDING, True, False),
        (DataflowJobType.JOB_TYPE_STREAMING, DataflowJobStatus.JOB_STATE_PENDING, True, False),
        (DataflowJobType.JOB_TYPE_BATCH, DataflowJobStatus.JOB_STATE_PENDING, False, True),
        (DataflowJobType.JOB_TYPE_STREAMING, DataflowJobStatus.JOB_STATE_PENDING, False, True),
    ])
    # fmt: on
    def test_check_dataflow_job_state_wait_until_finished(
        self, job_type, job_state, wait_until_finished, expected_result
    ):
        job = {"id": "id-2", "name": "name-2", "type": job_type, "currentState": job_state}
        dataflow_job = _DataflowJobsController(
            dataflow=self.mock_dataflow,
            project_number=TEST_PROJECT,
            name="name-",
            location=TEST_LOCATION,
            poll_sleep=0,
            job_id=None,
            num_retries=20,
            multiple_jobs=True,
            wait_until_finished=wait_until_finished,
        )
        result = dataflow_job._check_dataflow_job_state(job)
        assert result == expected_result

    # fmt: off
    @parameterized.expand([
        (DataflowJobType.JOB_TYPE_BATCH, DataflowJobStatus.JOB_STATE_FAILED,
            "Google Cloud Dataflow job name-2 has failed\\."),
        (DataflowJobType.JOB_TYPE_STREAMING, DataflowJobStatus.JOB_STATE_FAILED,
         "Google Cloud Dataflow job name-2 has failed\\."),
        (DataflowJobType.JOB_TYPE_STREAMING, DataflowJobStatus.JOB_STATE_UNKNOWN,
         "Google Cloud Dataflow job name-2 was unknown state: JOB_STATE_UNKNOWN"),
        (DataflowJobType.JOB_TYPE_BATCH, DataflowJobStatus.JOB_STATE_UNKNOWN,
         "Google Cloud Dataflow job name-2 was unknown state: JOB_STATE_UNKNOWN"),
        (DataflowJobType.JOB_TYPE_BATCH, DataflowJobStatus.JOB_STATE_CANCELLED,
            "Google Cloud Dataflow job name-2 was cancelled\\."),
        (DataflowJobType.JOB_TYPE_STREAMING, DataflowJobStatus.JOB_STATE_CANCELLED,
         "Google Cloud Dataflow job name-2 was cancelled\\."),
        (DataflowJobType.JOB_TYPE_BATCH, DataflowJobStatus.JOB_STATE_DRAINED,
            "Google Cloud Dataflow job name-2 was drained\\."),
        (DataflowJobType.JOB_TYPE_STREAMING, DataflowJobStatus.JOB_STATE_DRAINED,
         "Google Cloud Dataflow job name-2 was drained\\."),
        (DataflowJobType.JOB_TYPE_BATCH, DataflowJobStatus.JOB_STATE_UPDATED,
            "Google Cloud Dataflow job name-2 was updated\\."),
        (DataflowJobType.JOB_TYPE_STREAMING, DataflowJobStatus.JOB_STATE_UPDATED,
         "Google Cloud Dataflow job name-2 was updated\\."),
    ])
    # fmt: on
    def test_check_dataflow_job_state_terminal_state(self, job_type, job_state, exception_regex):
        job = {"id": "id-2", "name": "name-2", "type": job_type, "currentState": job_state}
        dataflow_job = _DataflowJobsController(
            dataflow=self.mock_dataflow,
            project_number=TEST_PROJECT,
            name="name-",
            location=TEST_LOCATION,
            poll_sleep=0,
            job_id=None,
            num_retries=20,
            multiple_jobs=True,
        )
        with pytest.raises(Exception, match=exception_regex):
            dataflow_job._check_dataflow_job_state(job)

    def test_dataflow_job_cancel_job(self):
        mock_jobs = self.mock_dataflow.projects.return_value.locations.return_value.jobs
        get_method = mock_jobs.return_value.get
        get_method.return_value.execute.side_effect = [
            {"id": TEST_JOB_ID, "name": JOB_NAME, "currentState": DataflowJobStatus.JOB_STATE_RUNNING},
            {"id": TEST_JOB_ID, "name": JOB_NAME, "currentState": DataflowJobStatus.JOB_STATE_PENDING},
            {"id": TEST_JOB_ID, "name": JOB_NAME, "currentState": DataflowJobStatus.JOB_STATE_QUEUED},
            {"id": TEST_JOB_ID, "name": JOB_NAME, "currentState": DataflowJobStatus.JOB_STATE_CANCELLING},
            {"id": TEST_JOB_ID, "name": JOB_NAME, "currentState": DataflowJobStatus.JOB_STATE_DRAINING},
            {"id": TEST_JOB_ID, "name": JOB_NAME, "currentState": DataflowJobStatus.JOB_STATE_STOPPED},
            {"id": TEST_JOB_ID, "name": JOB_NAME, "currentState": DataflowJobStatus.JOB_STATE_CANCELLED},
        ]

        mock_jobs.return_value.list_next.return_value = None
        dataflow_job = _DataflowJobsController(
            dataflow=self.mock_dataflow,
            project_number=TEST_PROJECT,
            name=UNIQUE_JOB_NAME,
            location=TEST_LOCATION,
            poll_sleep=0,
            job_id=TEST_JOB_ID,
            num_retries=20,
            multiple_jobs=False,
        )
        dataflow_job.cancel()

        get_method.assert_called_with(jobId=TEST_JOB_ID, location=TEST_LOCATION, projectId=TEST_PROJECT)
        get_method.return_value.execute.assert_called_with(num_retries=20)

        self.mock_dataflow.new_batch_http_request.assert_called_once_with()
        mock_batch = self.mock_dataflow.new_batch_http_request.return_value
        mock_update = mock_jobs.return_value.update
        mock_update.assert_called_once_with(
            body={'requestedState': 'JOB_STATE_CANCELLED'},
            jobId='test-job-id',
            location=TEST_LOCATION,
            projectId='test-project',
        )
        mock_batch.add.assert_called_once_with(mock_update.return_value)

    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.timeout")
    @mock.patch("time.sleep")
    def test_dataflow_job_cancel_job_cancel_timeout(self, mock_sleep, mock_timeout):
        mock_jobs = self.mock_dataflow.projects.return_value.locations.return_value.jobs
        get_method = mock_jobs.return_value.get
        get_method.return_value.execute.side_effect = [
            {"id": TEST_JOB_ID, "name": JOB_NAME, "currentState": DataflowJobStatus.JOB_STATE_CANCELLING},
            {"id": TEST_JOB_ID, "name": JOB_NAME, "currentState": DataflowJobStatus.JOB_STATE_CANCELLING},
            {"id": TEST_JOB_ID, "name": JOB_NAME, "currentState": DataflowJobStatus.JOB_STATE_CANCELLING},
            {"id": TEST_JOB_ID, "name": JOB_NAME, "currentState": DataflowJobStatus.JOB_STATE_CANCELLING},
            {"id": TEST_JOB_ID, "name": JOB_NAME, "currentState": DataflowJobStatus.JOB_STATE_CANCELLED},
        ]

        mock_jobs.return_value.list_next.return_value = None
        dataflow_job = _DataflowJobsController(
            dataflow=self.mock_dataflow,
            project_number=TEST_PROJECT,
            name=UNIQUE_JOB_NAME,
            location=TEST_LOCATION,
            poll_sleep=4,
            job_id=TEST_JOB_ID,
            num_retries=20,
            multiple_jobs=False,
            cancel_timeout=10,
        )
        dataflow_job.cancel()

        get_method.assert_called_with(jobId=TEST_JOB_ID, location=TEST_LOCATION, projectId=TEST_PROJECT)
        get_method.return_value.execute.assert_called_with(num_retries=20)

        self.mock_dataflow.new_batch_http_request.assert_called_once_with()
        mock_batch = self.mock_dataflow.new_batch_http_request.return_value
        mock_update = mock_jobs.return_value.update
        mock_update.assert_called_once_with(
            body={'requestedState': 'JOB_STATE_CANCELLED'},
            jobId='test-job-id',
            location=TEST_LOCATION,
            projectId='test-project',
        )
        mock_batch.add.assert_called_once_with(mock_update.return_value)
        mock_sleep.assert_has_calls([mock.call(4), mock.call(4), mock.call(4)])
        mock_timeout.assert_called_once_with(
            seconds=10, error_message='Canceling jobs failed due to timeout (10s): test-job-id'
        )

    @parameterized.expand(
        [
            (False, "JOB_TYPE_BATCH", "JOB_STATE_CANCELLED"),
            (False, "JOB_TYPE_STREAMING", "JOB_STATE_CANCELLED"),
            (True, "JOB_TYPE_BATCH", "JOB_STATE_CANCELLED"),
            (True, "JOB_TYPE_STREAMING", "JOB_STATE_DRAINED"),
        ]
    )
    def test_dataflow_job_cancel_or_drain_job(self, drain_pipeline, job_type, requested_state):
        job = {
            "id": TEST_JOB_ID,
            "name": UNIQUE_JOB_NAME,
            "currentState": DataflowJobStatus.JOB_STATE_RUNNING,
            "type": job_type,
        }
        get_method = self.mock_dataflow.projects.return_value.locations.return_value.jobs.return_value.get
        get_method.return_value.execute.return_value = job
        # fmt: off
        job_list_nest_method = (self.mock_dataflow
                                .projects.return_value.
                                locations.return_value.
                                jobs.return_value.list_next)
        job_list_nest_method.return_value = None
        # fmt: on
        dataflow_job = _DataflowJobsController(
            dataflow=self.mock_dataflow,
            project_number=TEST_PROJECT,
            name=UNIQUE_JOB_NAME,
            location=TEST_LOCATION,
            poll_sleep=10,
            job_id=TEST_JOB_ID,
            num_retries=20,
            multiple_jobs=False,
            drain_pipeline=drain_pipeline,
            cancel_timeout=None,
        )
        dataflow_job.cancel()

        get_method.assert_called_once_with(jobId=TEST_JOB_ID, location=TEST_LOCATION, projectId=TEST_PROJECT)

        get_method.return_value.execute.assert_called_once_with(num_retries=20)

        self.mock_dataflow.new_batch_http_request.assert_called_once_with()

        mock_batch = self.mock_dataflow.new_batch_http_request.return_value
        mock_update = self.mock_dataflow.projects.return_value.locations.return_value.jobs.return_value.update
        mock_update.assert_called_once_with(
            body={'requestedState': requested_state},
            jobId='test-job-id',
            location=TEST_LOCATION,
            projectId='test-project',
        )
        mock_batch.add.assert_called_once_with(mock_update.return_value)
        mock_batch.execute.assert_called_once()

    def test_dataflow_job_cancel_job_no_running_jobs(self):
        mock_jobs = self.mock_dataflow.projects.return_value.locations.return_value.jobs
        get_method = mock_jobs.return_value.get
        get_method.return_value.execute.side_effect = [
            {"id": TEST_JOB_ID, "name": JOB_NAME, "currentState": DataflowJobStatus.JOB_STATE_DONE},
            {"id": TEST_JOB_ID, "name": JOB_NAME, "currentState": DataflowJobStatus.JOB_STATE_UPDATED},
            {"id": TEST_JOB_ID, "name": JOB_NAME, "currentState": DataflowJobStatus.JOB_STATE_DRAINED},
            {"id": TEST_JOB_ID, "name": JOB_NAME, "currentState": DataflowJobStatus.JOB_STATE_FAILED},
            {"id": TEST_JOB_ID, "name": JOB_NAME, "currentState": DataflowJobStatus.JOB_STATE_CANCELLED},
        ]

        mock_jobs.return_value.list_next.return_value = None
        dataflow_job = _DataflowJobsController(
            dataflow=self.mock_dataflow,
            project_number=TEST_PROJECT,
            name=UNIQUE_JOB_NAME,
            location=TEST_LOCATION,
            poll_sleep=0,
            job_id=TEST_JOB_ID,
            num_retries=20,
            multiple_jobs=False,
        )
        dataflow_job.cancel()

        get_method.assert_called_with(jobId=TEST_JOB_ID, location=TEST_LOCATION, projectId=TEST_PROJECT)
        get_method.return_value.execute.assert_called_with(num_retries=20)

        self.mock_dataflow.new_batch_http_request.assert_not_called()
        mock_jobs.return_value.update.assert_not_called()

    def test_fetch_list_job_messages_responses(self):
        # fmt: off
        mock_list = (
            self.mock_dataflow
            .projects.return_value
            .locations.return_value
            .jobs.return_value
            .messages.return_value
            .list
        )
        mock_list_next = (
            self.mock_dataflow.
            projects.return_value.
            locations.return_value.
            jobs.return_value
            .messages.return_value
            .list_next
        )
        # fmt: on
        mock_list.return_value.execute.return_value = "response_1"
        mock_list_next.return_value = None

        jobs_controller = _DataflowJobsController(
            dataflow=self.mock_dataflow,
            project_number=TEST_PROJECT,
            location=TEST_LOCATION,
            job_id=TEST_JOB_ID,
        )
        result = list(jobs_controller._fetch_list_job_messages_responses(TEST_JOB_ID))

        mock_list.assert_called_once_with(projectId=TEST_PROJECT, location=TEST_LOCATION, jobId=TEST_JOB_ID)
        mock_list_next.assert_called_once_with(
            previous_request=mock_list.return_value, previous_response="response_1"
        )
        assert result == ["response_1"]

    @mock.patch(DATAFLOW_STRING.format('_DataflowJobsController._fetch_list_job_messages_responses'))
    def test_fetch_job_messages_by_id(self, mock_fetch_responses):
        mock_fetch_responses.return_value = iter(
            [
                {"jobMessages": ["message_1"]},
                {"jobMessages": ["message_2"]},
            ]
        )
        jobs_controller = _DataflowJobsController(
            dataflow=self.mock_dataflow,
            project_number=TEST_PROJECT,
            location=TEST_LOCATION,
            job_id=TEST_JOB_ID,
        )
        result = jobs_controller.fetch_job_messages_by_id(TEST_JOB_ID)
        mock_fetch_responses.assert_called_once_with(job_id=TEST_JOB_ID)
        assert result == ['message_1', 'message_2']

    @mock.patch(DATAFLOW_STRING.format('_DataflowJobsController._fetch_list_job_messages_responses'))
    def test_fetch_job_autoscaling_events_by_id(self, mock_fetch_responses):
        mock_fetch_responses.return_value = iter(
            [
                {"autoscalingEvents": ["event_1"]},
                {"autoscalingEvents": ["event_2"]},
            ]
        )
        jobs_controller = _DataflowJobsController(
            dataflow=self.mock_dataflow,
            project_number=TEST_PROJECT,
            location=TEST_LOCATION,
            job_id=TEST_JOB_ID,
        )
        result = jobs_controller.fetch_job_autoscaling_events_by_id(TEST_JOB_ID)
        mock_fetch_responses.assert_called_once_with(job_id=TEST_JOB_ID)
        assert result == ['event_1', 'event_2']


APACHE_BEAM_V_2_14_0_JAVA_SDK_LOG = f""""\
Dataflow SDK version: 2.14.0
Jun 15, 2020 2:57:28 PM org.apache.beam.runners.dataflow.DataflowRunner run
INFO: To access the Dataflow monitoring console, please navigate to https://console.cloud.google.com/dataflow\
/jobsDetail/locations/europe-west3/jobs/{TEST_JOB_ID}?project=XXX
Submitted job: {TEST_JOB_ID}
Jun 15, 2020 2:57:28 PM org.apache.beam.runners.dataflow.DataflowRunner run
INFO: To cancel the job using the 'gcloud' tool, run:
> gcloud dataflow jobs --project=XXX cancel --region=europe-west3 {TEST_JOB_ID}
"""

APACHE_BEAM_V_2_22_0_JAVA_SDK_LOG = f""""\
INFO: Dataflow SDK version: 2.22.0
Jun 15, 2020 3:09:03 PM org.apache.beam.runners.dataflow.DataflowRunner run
INFO: To access the Dataflow monitoring console, please navigate to https://console.cloud.google.com/dataflow\
/jobs/europe-west3/{TEST_JOB_ID}?project=XXXX
Jun 15, 2020 3:09:03 PM org.apache.beam.runners.dataflow.DataflowRunner run
INFO: Submitted job: {TEST_JOB_ID}
Jun 15, 2020 3:09:03 PM org.apache.beam.runners.dataflow.DataflowRunner run
INFO: To cancel the job using the 'gcloud' tool, run:
> gcloud dataflow jobs --project=XXX cancel --region=europe-west3 {TEST_JOB_ID}
"""

APACHE_BEAM_V_2_14_0_PYTHON_SDK_LOG = f""""\
INFO:root:Completed GCS upload to gs://test-dataflow-example/staging/start-python-job-local-5bcf3d71.\
1592286375.000962/apache_beam-2.14.0-cp37-cp37m-manylinux1_x86_64.whl in 0 seconds.
INFO:root:Create job: <Job
 createTime: '2020-06-16T05:46:20.911857Z'
 currentStateTime: '1970-01-01T00:00:00Z'
 id: '{TEST_JOB_ID}'
 location: 'us-central1'
 name: 'start-python-job-local-5bcf3d71'
 projectId: 'XXX'
 stageStates: []
 startTime: '2020-06-16T05:46:20.911857Z'
 steps: []
 tempFiles: []
 type: TypeValueValuesEnum(JOB_TYPE_BATCH, 1)>
INFO:root:Created job with id: [{TEST_JOB_ID}]
INFO:root:To access the Dataflow monitoring console, please navigate to https://console.cloud.google.com/\
dataflow/jobsDetail/locations/us-central1/jobs/{TEST_JOB_ID}?project=XXX
"""

APACHE_BEAM_V_2_22_0_PYTHON_SDK_LOG = f""""\
INFO:apache_beam.runners.dataflow.internal.apiclient:Completed GCS upload to gs://test-dataflow-example/\
staging/start-python-job-local-5bcf3d71.1592286719.303624/apache_beam-2.22.0-cp37-cp37m-manylinux1_x86_64.whl\
 in 1 seconds.
INFO:apache_beam.runners.dataflow.internal.apiclient:Create job: <Job
 createTime: '2020-06-16T05:52:04.095216Z'
 currentStateTime: '1970-01-01T00:00:00Z'
 id: '{TEST_JOB_ID}'
 location: 'us-central1'
 name: 'start-python-job-local-5bcf3d71'
 projectId: 'XXX'
 stageStates: []
 startTime: '2020-06-16T05:52:04.095216Z'
 steps: []
 tempFiles: []
 type: TypeValueValuesEnum(JOB_TYPE_BATCH, 1)>
INFO:apache_beam.runners.dataflow.internal.apiclient:Created job with id: [{TEST_JOB_ID}]
INFO:apache_beam.runners.dataflow.internal.apiclient:Submitted job: {TEST_JOB_ID}
INFO:apache_beam.runners.dataflow.internal.apiclient:To access the Dataflow monitoring console, please \
navigate to https://console.cloud.google.com/dataflow/jobs/us-central1/{TEST_JOB_ID}?project=XXX
"""


class TestDataflow(unittest.TestCase):
    @parameterized.expand(
        [
            (APACHE_BEAM_V_2_14_0_JAVA_SDK_LOG,),
            (APACHE_BEAM_V_2_22_0_JAVA_SDK_LOG,),
            (APACHE_BEAM_V_2_14_0_PYTHON_SDK_LOG,),
            (APACHE_BEAM_V_2_22_0_PYTHON_SDK_LOG,),
        ],
        name_func=lambda func, num, p: f"{func.__name__}_{num}",
    )
    def test_data_flow_valid_job_id(self, log):
        echos = ";".join([f"echo {shlex.quote(line)}" for line in log.split("\n")])
        cmd = ["bash", "-c", echos]
        found_job_id = None

        def callback(job_id):
            nonlocal found_job_id
            found_job_id = job_id

        BeamCommandRunner(
            cmd, process_line_callback=process_line_and_extract_dataflow_job_id_callback(callback)
        ).wait_for_done()
        self.assertEqual(found_job_id, TEST_JOB_ID)

    def test_data_flow_missing_job_id(self):
        cmd = ['echo', 'unit testing']
        found_job_id = None

        def callback(job_id):
            nonlocal found_job_id
            found_job_id = job_id

        BeamCommandRunner(
            cmd, process_line_callback=process_line_and_extract_dataflow_job_id_callback(callback)
        ).wait_for_done()

        self.assertEqual(found_job_id, None)

    @mock.patch('airflow.providers.apache.beam.hooks.beam.BeamCommandRunner.log')
    @mock.patch('subprocess.Popen')
    @mock.patch('select.select')
    def test_dataflow_wait_for_done_logging(self, mock_select, mock_popen, mock_logging):
        mock_logging.info = MagicMock()
        mock_logging.warning = MagicMock()
        mock_proc = MagicMock()
        mock_proc.stderr = MagicMock()
        mock_proc.stderr.readlines = MagicMock(return_value=['test\n', 'error\n'])
        mock_stderr_fd = MagicMock()
        mock_proc.stderr.fileno = MagicMock(return_value=mock_stderr_fd)
        mock_proc_poll = MagicMock()
        mock_select.return_value = [[mock_stderr_fd]]

        def poll_resp_error():
            mock_proc.return_code = 1
            return True

        mock_proc_poll.side_effect = [None, poll_resp_error]
        mock_proc.poll = mock_proc_poll
        mock_popen.return_value = mock_proc
        dataflow = BeamCommandRunner(['test', 'cmd'])
        mock_logging.info.assert_called_once_with('Running command: %s', 'test cmd')
        self.assertRaises(Exception, dataflow.wait_for_done)
