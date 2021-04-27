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
"""Serve logs command"""
from multiprocessing import Process

from airflow.utils import cli as cli_utils
from airflow.utils.serve_logs import serve_logs as serve_logs_utils


@cli_utils.action_logging
def serve_logs(args):
    """Updates permissions for existing roles and DAGs"""
    sub_proc = Process(target=serve_logs_utils)
    sub_proc.start()
