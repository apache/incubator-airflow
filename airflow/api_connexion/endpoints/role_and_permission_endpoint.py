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
from flask import current_app
from flask_appbuilder.security.sqla.models import Permission, Role
from sqlalchemy import func

from airflow.api_connexion import security
from airflow.api_connexion.exceptions import NotFound
from airflow.api_connexion.parameters import apply_sorting, check_limit, format_parameters
from airflow.api_connexion.schemas.role_and_permission_schema import (
    ActionCollection,
    RoleCollection,
    action_collection_schema,
    role_collection_schema,
    role_schema,
)
from airflow.security import permissions


@security.requires_access([(permissions.ACTION_CAN_SHOW, permissions.RESOURCE_ROLE_MODEL_VIEW)])
def get_role(role_name):
    """Get role"""
    ab_security_manager = current_app.appbuilder.sm
    role = ab_security_manager.find_role(name=role_name)
    if not role:
        raise NotFound(title="Role not found", detail=f"The Role with name `{role_name}` was not found")
    return role_schema.dump(role)


@security.requires_access([(permissions.ACTION_CAN_LIST, permissions.RESOURCE_ROLE_MODEL_VIEW)])
@format_parameters({'limit': check_limit})
def get_roles(limit, order_by='name', offset=None):
    """Get roles"""
    appbuilder = current_app.appbuilder
    session = appbuilder.get_session
    total_entries = session.query(func.count(Role.id)).scalar()
    to_replace = {"role_id": "id"}
    allowed_filter_attrs = ['role_id', 'name']
    query = session.query(Role)
    query = apply_sorting(query, order_by, to_replace, allowed_filter_attrs)
    roles = query.offset(offset).limit(limit).all()

    return role_collection_schema.dump(RoleCollection(roles=roles, total_entries=total_entries))


@security.requires_access([(permissions.ACTION_CAN_LIST, permissions.RESOURCE_PERMISSION_MODEL_VIEW)])
@format_parameters({'limit': check_limit})
def get_permissions(limit=None, offset=None):
    """Get permissions"""
    session = current_app.appbuilder.get_session
    total_entries = session.query(func.count(Permission.id)).scalar()
    query = session.query(Permission)
    actions = query.offset(offset).limit(limit).all()
    return action_collection_schema.dump(ActionCollection(actions=actions, total_entries=total_entries))
