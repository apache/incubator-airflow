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

"""Prefix DAG permissions.

Revision ID: 849da589634d
Revises: 45ba3f1493b9
Create Date: 2020-10-01 17:25:10.006322

"""

from flask_appbuilder import SQLA
from flask_appbuilder.security.sqla.models import Permission, PermissionView, ViewMenu

from airflow import settings
from airflow.security import permissions

# revision identifiers, used by Alembic.
revision = '849da589634d'
down_revision = '45ba3f1493b9'
branch_labels = None
depends_on = None


def prefix_individual_dag_permissions(session):  # noqa: D103
    dag_perms = ['can_dag_read', 'can_dag_edit']
    prefix = "DAG:"
    perms = (
        session.query(PermissionView)
        .join(Permission)
        .filter(Permission.name.in_(dag_perms))
        .join(ViewMenu)
        .filter(ViewMenu.name != 'all_dags')
        .filter(ViewMenu.name.notlike(prefix + '%'))
        .all()
    )
    resource_ids = {permission.view_menu.id for permission in perms}
    vm_query = session.query(ViewMenu).filter(ViewMenu.id.in_(resource_ids))
    vm_query.update({ViewMenu.name: prefix + ViewMenu.name}, synchronize_session=False)
    session.commit()


def get_or_create_dag_resource(session):  # noqa: D103
    dag_resource = get_resource_query(session, permissions.RESOURCE_DAG).first()
    if dag_resource:
        return dag_resource

    dag_resource = ViewMenu()
    dag_resource.name = permissions.RESOURCE_DAG
    session.add(dag_resource)
    session.commit()

    return dag_resource


def get_or_create_action(session, action_name):  # noqa: D103
    action = get_action_query(session, action_name).first()
    if action:
        return action

    action = Permission()
    action.name = action_name
    session.add(action)
    session.commit()

    return action


def get_resource_query(session, resource_name):  # noqa: D103
    return session.query(ViewMenu).filter(ViewMenu.name == resource_name)


def get_action_query(session, action_name):  # noqa: D103
    return session.query(Permission).filter(Permission.name == action_name)


def get_permission_with_action_query(session, action):  # noqa: D103
    return session.query(PermissionView).filter(PermissionView.permission == action)


def get_permission_with_resource_query(session, resource):  # noqa: D103
    return session.query(PermissionView).filter(PermissionView.view_menu_id == resource.id)


def update_permission_action(session, permission_query, action):  # noqa: D103
    permission_query.update({PermissionView.permission_id: action.id}, synchronize_session=False)
    session.commit()


def get_permission(session, resource, action):  # noqa: D103
    return (
        session.query(PermissionView)
        .filter(PermissionView.view_menu == resource)
        .filter(PermissionView.permission == action)
        .first()
    )


def update_permission_resource(session, permission_query, resource):  # noqa: D103
    for permission in permission_query.all():  # noqa: D103
        if not get_permission(session, resource, permission.permission):  # noqa: D103
            permission.view_menu = resource
        else:
            session.delete(permission)

    session.commit()


def migrate_to_new_dag_permissions(db):  # noqa: D103
    # Prefix individual dag perms with `DAG:`
    prefix_individual_dag_permissions(db.session)

    # Update existing permissions to use `can_read` instead of `can_dag_read`
    can_dag_read_action = get_action_query(db.session, 'can_dag_read').first()
    old_can_dag_read_permissions = get_permission_with_action_query(db.session, can_dag_read_action)
    can_read_action = get_or_create_action(db.session, 'can_read')
    update_permission_action(db.session, old_can_dag_read_permissions, can_read_action)

    # Update existing permissions to use `can_edit` instead of `can_dag_edit`
    can_dag_edit_action = get_action_query(db.session, 'can_dag_edit').first()
    old_can_dag_edit_permissions = get_permission_with_action_query(db.session, can_dag_edit_action)
    can_edit_action = get_or_create_action(db.session, 'can_edit')
    update_permission_action(db.session, old_can_dag_edit_permissions, can_edit_action)

    # Update existing permissions for `all_dags` resource to use `DAGs` resource.
    all_dags_resource = get_resource_query(db.session, 'all_dags').first()
    if all_dags_resource:
        old_all_dags_permission = get_permission_with_resource_query(db.session, all_dags_resource)
        dag_resource = get_or_create_dag_resource(db.session)
        update_permission_resource(db.session, old_all_dags_permission, dag_resource)

        # Delete the `all_dags` resource
        db.session.delete(all_dags_resource)

    # Delete `can_dag_read` action
    if can_dag_read_action:
        db.session.delete(can_dag_read_action)

    # Delete `can_dag_edit` action
    if can_dag_edit_action:
        db.session.delete(can_dag_edit_action)

    db.session.commit()


def upgrade():  # noqa: D103
    db = SQLA()
    db.session = settings.Session
    migrate_to_new_dag_permissions(db)
    db.session.commit()
    db.session.close()


def downgrade():  # noqa: D103
    pass
