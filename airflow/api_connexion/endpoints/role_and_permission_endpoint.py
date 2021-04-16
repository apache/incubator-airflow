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

from connexion import NoContent
from flask import current_app, request
from flask_appbuilder.security.sqla.models import Permission, Role
from marshmallow import ValidationError
from sqlalchemy import func

from airflow.api_connexion import security
from airflow.api_connexion.exceptions import AlreadyExists, BadRequest, NotFound
from airflow.api_connexion.parameters import apply_sorting, check_limit, format_parameters
from airflow.api_connexion.schemas.role_and_permission_schema import (
    ActionCollection,
    RoleCollection,
    action_collection_schema,
    role_collection_schema,
    role_schema,
)
from airflow.security import permissions


def _check_action_and_resource(sm, perms):
    """
    Checks if the action or resource exists and raise 400 if not

    This function is intended for use in the REST API because it raise 400
    """
    for item in perms:
        if not sm.get_action(item[0]):
            raise BadRequest(detail=f"The specified action: '{item[0]}' was not found")
        if not sm.get_resource(item[1]):
            raise BadRequest(detail=f"The specified resource: '{item[1]}' was not found")


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_ROLE)])
def get_role(role_name):
    """Get role"""
    ab_security_manager = current_app.appbuilder.sm
    role = ab_security_manager.find_role(name=role_name)
    if not role:
        raise NotFound(title="Role not found", detail=f"The Role with name `{role_name}` was not found")
    return role_schema.dump(role)


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_ROLE)])
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


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_PERMISSION)])
@format_parameters({'limit': check_limit})
def get_permissions(limit=None, offset=None):
    """Get permissions"""
    session = current_app.appbuilder.get_session
    total_entries = session.query(func.count(Permission.id)).scalar()
    query = session.query(Permission)
    actions = query.offset(offset).limit(limit).all()
    return action_collection_schema.dump(ActionCollection(actions=actions, total_entries=total_entries))


@security.requires_access([(permissions.ACTION_CAN_DELETE, permissions.RESOURCE_ROLE)])
def delete_role(role_name):
    """Delete a role"""
    ab_security_manager = current_app.appbuilder.sm
    role = ab_security_manager.find_role(name=role_name)
    if not role:
        raise NotFound(title="Role not found", detail=f"The Role with name `{role_name}` was not found")
    ab_security_manager.delete_role(role_name=role_name)
    return NoContent, 204


@security.requires_access([(permissions.ACTION_CAN_EDIT, permissions.RESOURCE_ROLE)])
def patch_role(role_name, update_mask=None):
    """Update a role"""
    appbuilder = current_app.appbuilder
    security_manager = appbuilder.sm
    body = request.json
    try:
        data = role_schema.load(body)
    except ValidationError as err:
        raise BadRequest(detail=str(err.messages))
    role = security_manager.find_role(name=role_name)
    if not role:
        raise NotFound(title="Role not found", detail=f"Role with name: `{role_name} was not found")
    if update_mask:
        update_mask = [i.strip() for i in update_mask]
        data_ = {}
        for field in update_mask:
            if field in data and not field == "permissions":
                data_[field] = data[field]
            elif field == "actions":
                data_["permissions"] = data['permissions']
            else:
                raise BadRequest(detail=f"'{field}' in update_mask is unknown")
        data = data_
    perms = data.get("permissions", [])
    if perms:
        perms = [
            (item['permission']['name'], item['view_menu']['name']) for item in data['permissions'] if item
        ]
        _check_action_and_resource(security_manager, perms)
    security_manager.update_role(pk=role.id, name=data['name'])
    security_manager.init_role(role_name=data['name'], perms=perms or role.permissions)
    return role_schema.dump(role)


@security.requires_access([(permissions.ACTION_CAN_CREATE, permissions.RESOURCE_ROLE)])
def post_role():
    """Create a new role"""
    appbuilder = current_app.appbuilder
    security_manager = appbuilder.sm
    body = request.json
    try:
        data = role_schema.load(body)
    except ValidationError as err:
        raise BadRequest(detail=str(err.messages))
    role = security_manager.find_role(name=data['name'])
    if not role:
        perms = [
            (item['permission']['name'], item['view_menu']['name']) for item in data['permissions'] if item
        ]
        _check_action_and_resource(security_manager, perms)
        security_manager.init_role(role_name=data['name'], perms=perms)
        return role_schema.dump(role)
    raise AlreadyExists(
        detail=f"Role with name `{role.name}` already exist. Please update with patch endpoint"
    )
