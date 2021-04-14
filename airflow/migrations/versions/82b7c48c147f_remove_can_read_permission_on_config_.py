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

"""Remove can_read permission on config resource for User and Viewer role

Revision ID: 82b7c48c147f
Revises: e959f08ac86c
Create Date: 2021-02-04 12:45:58.138224

"""
import logging

from airflow.security import permissions
from airflow.www.app import create_app

# revision identifiers, used by Alembic.
revision = '82b7c48c147f'
down_revision = 'e959f08ac86c'
branch_labels = None
depends_on = None


def upgrade():
    """Remove can_read permission on config resource for User and Viewer role"""
    log = logging.getLogger()
    handlers = log.handlers[:]

    appbuilder = create_app(config={'FAB_UPDATE_PERMS': False}).appbuilder
    roles_to_modify = [role for role in appbuilder.sm.get_all_roles() if role.name in ["User", "Viewer"]]
    can_read_on_config_perm = appbuilder.sm.find_permission_view_menu(
        permissions.ACTION_CAN_READ, permissions.RESOURCE_CONFIG
    )

    for role in roles_to_modify:
        if appbuilder.sm.exist_permission_on_roles(
            permissions.RESOURCE_CONFIG, permissions.ACTION_CAN_READ, [role.id]
        ):
            appbuilder.sm.del_permission_role(role, can_read_on_config_perm)

    log.handlers = handlers


def downgrade():
    """Add can_read permission on config resource for User and Viewer role"""
    appbuilder = create_app(config={'FAB_UPDATE_PERMS': False}).appbuilder
    roles_to_modify = [role for role in appbuilder.sm.get_all_roles() if role.name in ["User", "Viewer"]]
    can_read_on_config_perm = appbuilder.sm.find_permission_view_menu(
        permissions.ACTION_CAN_READ, permissions.RESOURCE_CONFIG
    )

    for role in roles_to_modify:
        if not appbuilder.sm.exist_permission_on_roles(
            permissions.RESOURCE_CONFIG, permissions.ACTION_CAN_READ, [role.id]
        ):
            appbuilder.sm.add_permission_role(role, can_read_on_config_perm)
