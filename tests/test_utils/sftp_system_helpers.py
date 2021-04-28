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

import json
import os
from contextlib import contextmanager

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.utils.process_utils import patch_environ

SFTP_CONNECTION_ID = os.environ.get("SFTP_CONNECTION_ID", "sftp_default")


@contextmanager
def provide_sftp_default_connection(key_file_path: str):
    """
    Context manager to provide a temporary value for sftp_default connection

    :param key_file_path: Path to file with sftp_default credentials .json file.
    :type key_file_path: str
    """
    if not key_file_path.endswith(".json"):
        raise AirflowException("Use a JSON key file.")
    with open(key_file_path) as credentials:
        creds = json.load(credentials)
    conn = Connection(
        conn_id=SFTP_CONNECTION_ID,
        conn_type="ssh",
        port=creds.get("port", None),
        host=creds.get("host", None),
        login=creds.get("login", None),
        password=creds.get("password", None),
        extra=json.dumps(creds.get('extra', None)),
    )
    with patch_environ({f"AIRFLOW_CONN_{conn.conn_id.upper()}": conn.get_uri()}):
        yield
