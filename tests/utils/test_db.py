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

import inspect
import re
import unittest

from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import MetaData

from airflow.models import Base as airflow_base
from airflow.settings import engine
from airflow.utils.db import create_default_connections


class TestDb(unittest.TestCase):
    def test_database_schema_and_sqlalchemy_model_are_in_sync(self):
        all_meta_data = MetaData()
        for (table_name, table) in airflow_base.metadata.tables.items():
            all_meta_data._add_table(table_name, table.schema, table)

        # create diff between database schema and SQLAlchemy model
        mctx = MigrationContext.configure(engine.connect())
        diff = compare_metadata(mctx, all_meta_data)

        # known diffs to ignore
        ignores = [
            # ignore tables created by celery
            lambda t: (t[0] == 'remove_table' and t[1].name == 'celery_taskmeta'),
            lambda t: (t[0] == 'remove_table' and t[1].name == 'celery_tasksetmeta'),
            # ignore indices created by celery
            lambda t: (t[0] == 'remove_index' and t[1].name == 'task_id'),
            lambda t: (t[0] == 'remove_index' and t[1].name == 'taskset_id'),
            # Ignore all the fab tables
            lambda t: (t[0] == 'remove_table' and t[1].name == 'ab_permission'),
            lambda t: (t[0] == 'remove_table' and t[1].name == 'ab_register_user'),
            lambda t: (t[0] == 'remove_table' and t[1].name == 'ab_role'),
            lambda t: (t[0] == 'remove_table' and t[1].name == 'ab_permission_view'),
            lambda t: (t[0] == 'remove_table' and t[1].name == 'ab_permission_view_role'),
            lambda t: (t[0] == 'remove_table' and t[1].name == 'ab_user_role'),
            lambda t: (t[0] == 'remove_table' and t[1].name == 'ab_user'),
            lambda t: (t[0] == 'remove_table' and t[1].name == 'ab_view_menu'),
            # Ignore all the fab indices
            lambda t: (t[0] == 'remove_index' and t[1].name == 'permission_id'),
            lambda t: (t[0] == 'remove_index' and t[1].name == 'name'),
            lambda t: (t[0] == 'remove_index' and t[1].name == 'user_id'),
            lambda t: (t[0] == 'remove_index' and t[1].name == 'username'),
            lambda t: (t[0] == 'remove_index' and t[1].name == 'field_string'),
            lambda t: (t[0] == 'remove_index' and t[1].name == 'email'),
            lambda t: (t[0] == 'remove_index' and t[1].name == 'permission_view_id'),
            # from test_security unit test
            lambda t: (t[0] == 'remove_table' and t[1].name == 'some_model'),
            # MSSQL default tables
            lambda t: (t[0] == 'remove_table' and t[1].name == 'spt_monitor'),
            lambda t: (t[0] == 'remove_table' and t[1].name == 'spt_fallback_db'),
            lambda t: (t[0] == 'remove_table' and t[1].name == 'spt_fallback_usg'),
            lambda t: (t[0] == 'remove_table' and t[1].name == 'MSreplication_options'),
            lambda t: (t[0] == 'remove_table' and t[1].name == 'spt_fallback_dev'),
        ]
        for ignore in ignores:
            diff = [d for d in diff if not ignore(d)]

        assert not diff, 'Database schema and SQLAlchemy model are not in sync: ' + str(diff)

    def test_only_single_head_revision_in_migrations(self):
        config = Config()
        config.set_main_option("script_location", "airflow:migrations")
        script = ScriptDirectory.from_config(config)

        # This will raise if there are multiple heads
        # To resolve, use the command `alembic merge`
        script.get_current_head()

    def test_default_connections_sort(self):
        pattern = re.compile('conn_id=[\"|\'](.*?)[\"|\']', re.DOTALL)
        source = inspect.getsource(create_default_connections)
        src = pattern.findall(source)
        assert sorted(src) == src
