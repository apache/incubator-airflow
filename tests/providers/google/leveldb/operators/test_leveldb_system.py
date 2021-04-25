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
from contextlib import contextmanager

import pytest

from airflow.models import Connection
from airflow.utils.process_utils import patch_environ
from tests.test_utils.gcp_system_helpers import LEVELDB_DAG_FOLDER, GoogleSystemTest

CONNECTION_TYPE = os.environ.get('CONNECTION_TYPE', 'leveldb')
LEVELDB_CONNECTION_ID = os.environ.get('AIRFLOW_CONN_LEVELDB_DEFAULT', 'leveldb_default')


@contextmanager
def provide_leveldb_connection():
    """Context manager that provides a temporary value of AIRFLOW_CONN_LEVELDB_DEFAULT connection"""
    conn = Connection(conn_id=LEVELDB_CONNECTION_ID, conn_type=CONNECTION_TYPE)
    with patch_environ({f"AIRFLOW_CONN_{conn.conn_id.upper()}": conn.get_uri()}):
        yield


@pytest.mark.system("google.leveldb")
class LevelDBSystemTest(GoogleSystemTest):
    @provide_leveldb_connection()
    def test_run_example_dag(self):
        self.run_dag('example_leveldb', LEVELDB_DAG_FOLDER)
