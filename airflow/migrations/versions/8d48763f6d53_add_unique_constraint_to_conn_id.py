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

"""add unique constraint to conn_id

Revision ID: 8d48763f6d53
Revises: 8f966b9c467a
Create Date: 2020-05-03 16:55:01.834231

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = '8d48763f6d53'
down_revision = '8f966b9c467a'
branch_labels = None
depends_on = None


def upgrade():
    """Apply add unique constraint to conn_id and set it as non-nullable"""
    try:
        with op.batch_alter_table('connection') as batch_op:
            batch_op.alter_column("conn_id", nullable=False, existing_type=sa.String(250))
            batch_op.create_unique_constraint(constraint_name="unique_conn_id", columns=["conn_id"])

    except sa.exc.IntegrityError:
        raise Exception("Make sure there are no duplicate connections with the same conn_id or null values")


def downgrade():
    """Unapply add unique constraint to conn_id and set it as non-nullable"""
    with op.batch_alter_table('connection') as batch_op:
        batch_op.drop_constraint(constraint_name="unique_conn_id", type_="unique")

        batch_op.alter_column("conn_id", nullable=True, existing_type=sa.String(250))
