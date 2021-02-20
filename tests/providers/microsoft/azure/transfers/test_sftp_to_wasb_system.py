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


import os

import pytest

from airflow.providers.microsoft.azure.example_dags.example_sftp_to_wasb import SFTP_SRC_PATH
from tests.test_utils.azure_system_helpers import (
    AZURE_DAG_FOLDER,
    AzureSystemTest,
    provide_wasb_default_connection,
)

CREDENTIALS_DIR = os.environ.get('CREDENTIALS_DIR', '/files/airflow-breeze-config/keys')
WASB_DEFAULT_KEY = 'wasb_key.json'
CREDENTIALS_PATH = os.path.join(CREDENTIALS_DIR, WASB_DEFAULT_KEY)
FILENAME = "TEST.TXT"


@pytest.mark.backend('postgres', 'mysql')
@pytest.mark.credential_file(WASB_DEFAULT_KEY)
class TestSFTPToWasbSystem(AzureSystemTest):
    def setUp(self):
        super().setUp()
        self.create_dummy_file(FILENAME, SFTP_SRC_PATH)

    def tearDown(self):
        os.remove(SFTP_SRC_PATH)
        super().tearDown()

    @provide_wasb_default_connection(CREDENTIALS_PATH)
    def test_run_example_file_to_wasb(self):
        print("ok")
        self.run_dag('example_sftp_to_wasb', AZURE_DAG_FOLDER)
