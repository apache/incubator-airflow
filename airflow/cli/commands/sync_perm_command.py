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
"""Sync permission command"""
from airflow.models import DagBag
from airflow.utils import cli as cli_utils
from airflow.www.app import cached_app


@cli_utils.action_logging
def sync_perm(args):
    """Updates permissions for existing roles and DAGs"""
    appbuilder = cached_app().appbuilder  # pylint: disable=no-member
    print('Updating permission, view-menu for all existing roles')
    # Add missing permissions for all the Base Views _before_ syncing/creating roles
    appbuilder.add_permissions(update_perms=True)
    appbuilder.sm.sync_roles()
    print('Updating permission on all DAG views')
    dagbag = DagBag(read_dags_from_db=True)
    dagbag.collect_dags_from_db()
    dags = dagbag.dags.values()
    for dag in dags:
        appbuilder.sm.sync_perm_for_dag(dag.dag_id, dag.access_control)
