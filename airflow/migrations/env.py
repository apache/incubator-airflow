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

from logging.config import fileConfig

from alembic import context

from airflow import models, settings
from airflow.models.serialized_dag import SerializedDagModel  # pylint: disable=unused-import # noqa


def include_object(_, name, type_, *args):
    """Filter objects for autogenerating revisions"""
    # Ignore _anything_ to do with Flask AppBuilder's tables
    if type_ == "table" and name.startswith("ab_"):
        return False
    else:
        return True


# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
fileConfig(config.config_file_name, disable_existing_loggers=False)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = models.base.Base.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.

COMPARE_TYPE = False


def run_migrations_offline():
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    context.configure(
        url=settings.SQL_ALCHEMY_CONN,
        target_metadata=target_metadata,
        literal_binds=True,
        compare_type=COMPARE_TYPE,
        render_as_batch=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = settings.engine

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            transaction_per_migration=True,
            target_metadata=target_metadata,
            compare_type=COMPARE_TYPE,
            include_object=include_object,
            render_as_batch=True,
        )

        with context.begin_transaction():
            if connection.dialect.name == 'mysql' and connection.dialect.server_version_info >= (5, 6):
                connection.execute("select GET_LOCK('alembic',1800);")
            if connection.dialect.name == 'postgresql':
                context.get_context()._ensure_version_table()  # pylint: disable=protected-access
                connection.execute("LOCK TABLE alembic_version IN ACCESS EXCLUSIVE MODE")
            context.run_migrations()
            if connection.dialect.name == 'mysql' and connection.dialect.server_version_info >= (5, 6):
                connection.execute("select RELEASE_LOCK('alembic');")
            # for Postgres lock is released when transaction ends


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
