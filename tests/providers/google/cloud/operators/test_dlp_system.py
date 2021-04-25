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

# pylint: disable=C0111
"""
This module contains various unit tests for
example_gcp_dlp DAG
"""
import pytest

from airflow.providers.google.cloud.example_dags.example_dlp import OUTPUT_BUCKET, OUTPUT_FILENAME
from tests.providers.google.cloud.utils.gcp_authenticator import GCP_DLP_KEY
from tests.test_utils.gcp_system_helpers import CLOUD_DAG_FOLDER, GoogleSystemTest, provide_gcp_context


@pytest.fixture(scope="class")
def helper():
    GoogleSystemTest.create_gcs_bucket(OUTPUT_BUCKET)
    GoogleSystemTest.upload_content_to_gcs("aaaa\nbbbb", OUTPUT_BUCKET, f"tmp/{OUTPUT_FILENAME}")
    yield
    GoogleSystemTest.delete_gcs_bucket(OUTPUT_BUCKET)


@pytest.mark.backend("mysql", "postgres")
@pytest.mark.usefixtures("helper")
@pytest.mark.credential_file(GCP_DLP_KEY)
class GcpDLPExampleDagsSystemTest(GoogleSystemTest):
    @provide_gcp_context(GCP_DLP_KEY)
    def test_run_example_dag(self):
        self.run_dag('example_gcp_dlp', CLOUD_DAG_FOLDER)

    @provide_gcp_context(GCP_DLP_KEY)
    def test_run_example_info_types(self):
        self.run_dag('example_gcp_dlp_info_types', CLOUD_DAG_FOLDER)

    @provide_gcp_context(GCP_DLP_KEY)
    def test_run_example_dlp_job(self):
        self.run_dag('example_gcp_dlp_job', CLOUD_DAG_FOLDER)

    @provide_gcp_context(GCP_DLP_KEY)
    def test_run_example_dlp_deidentify_content(self):
        self.run_dag('example_gcp_dlp_deidentify_content', CLOUD_DAG_FOLDER)
