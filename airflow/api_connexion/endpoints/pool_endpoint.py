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
from flask import Response, request
from marshmallow import ValidationError
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError

from airflow.api_connexion import security
from airflow.api_connexion.exceptions import AlreadyExists, BadRequest, NotFound
from airflow.api_connexion.parameters import apply_sorting, check_limit, format_parameters
from airflow.api_connexion.schemas.pool_schema import PoolCollection, pool_collection_schema, pool_schema
from airflow.models.pool import Pool
from airflow.security import permissions
from airflow.utils.session import provide_session


@security.requires_access([(permissions.ACTION_CAN_DELETE, permissions.RESOURCE_POOL)])
@provide_session
def delete_pool(pool_name: str, session):
    """Delete a pool"""
    if pool_name == "default_pool":
        raise BadRequest(detail="Default Pool can't be deleted")
    elif session.query(Pool).filter(Pool.pool == pool_name).delete() == 0:
        raise NotFound(detail=f"Pool with name:'{pool_name}' not found")
    else:
        return Response(status=204)


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_POOL)])
@provide_session
def get_pool(pool_name, session):
    """Get a pool"""
    obj = session.query(Pool).filter(Pool.pool == pool_name).one_or_none()
    if obj is None:
        raise NotFound(detail=f"Pool with name:'{pool_name}' not found")
    return pool_schema.dump(obj)


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_POOL)])
@format_parameters({'limit': check_limit})
@provide_session
def get_pools(session, limit, order_by='id', offset=None):
    """Get all pools"""
    to_replace = {"name": "pool"}
    allowed_filter_attrs = ['name', 'slots', "id"]
    total_entries = session.query(func.count(Pool.id)).scalar()
    query = session.query(Pool)
    query = apply_sorting(query, order_by, to_replace, allowed_filter_attrs)
    pools = query.offset(offset).limit(limit).all()
    return pool_collection_schema.dump(PoolCollection(pools=pools, total_entries=total_entries))


@security.requires_access([(permissions.ACTION_CAN_EDIT, permissions.RESOURCE_POOL)])
@provide_session
def patch_pool(pool_name, session, update_mask=None):
    """Update a pool"""
    # Only slots can be modified in 'default_pool'
    try:
        if pool_name == Pool.DEFAULT_POOL_NAME and request.json["name"] != Pool.DEFAULT_POOL_NAME:
            if update_mask and len(update_mask) == 1 and update_mask[0].strip() == "slots":
                pass
            else:
                raise BadRequest(detail="Default Pool's name can't be modified")
    except KeyError:
        pass

    pool = session.query(Pool).filter(Pool.pool == pool_name).first()
    if not pool:
        raise NotFound(detail=f"Pool with name:'{pool_name}' not found")

    try:
        patch_body = pool_schema.load(request.json)
    except ValidationError as err:
        raise BadRequest(detail=str(err.messages))

    if update_mask:
        update_mask = [i.strip() for i in update_mask]
        _patch_body = {}
        try:
            update_mask = [
                pool_schema.declared_fields[field].attribute
                if pool_schema.declared_fields[field].attribute
                else field
                for field in update_mask
            ]
        except KeyError as err:
            raise BadRequest(detail=f"Invalid field: {err.args[0]} in update mask")
        _patch_body = {field: patch_body[field] for field in update_mask}
        patch_body = _patch_body

    else:
        required_fields = {"name", "slots"}
        fields_diff = required_fields - set(request.json.keys())
        if fields_diff:
            raise BadRequest(detail=f"Missing required property(ies): {sorted(fields_diff)}")

    for key, value in patch_body.items():
        setattr(pool, key, value)
    session.commit()
    return pool_schema.dump(pool)


@security.requires_access([(permissions.ACTION_CAN_CREATE, permissions.RESOURCE_POOL)])
@provide_session
def post_pool(session):
    """Create a pool"""
    required_fields = {"name", "slots"}  # Pool would require both fields in the post request
    fields_diff = required_fields - set(request.json.keys())
    if fields_diff:
        raise BadRequest(detail=f"Missing required property(ies): {sorted(fields_diff)}")

    try:
        post_body = pool_schema.load(request.json, session=session)
    except ValidationError as err:
        raise BadRequest(detail=str(err.messages))

    pool = Pool(**post_body)
    try:
        session.add(pool)
        session.commit()
        return pool_schema.dump(pool)
    except IntegrityError:
        raise AlreadyExists(detail=f"Pool: {post_body['pool']} already exists")
