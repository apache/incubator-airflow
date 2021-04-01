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
from flask import request
from marshmallow import ValidationError
from sqlalchemy import func

from airflow.api_connexion import security
from airflow.api_connexion.exceptions import AlreadyExists, BadRequest, NotFound
from airflow.api_connexion.parameters import apply_sorting, check_limit, format_parameters
from airflow.api_connexion.schemas.connection_schema import (
    ConnectionCollection,
    connection_collection_schema,
    connection_schema,
)
from airflow.models import Connection
from airflow.security import permissions
from airflow.utils.session import provide_session


@security.requires_access([(permissions.ACTION_CAN_DELETE, permissions.RESOURCE_CONNECTION)])
@provide_session
def delete_connection(connection_id, session):
    """Delete a connection entry"""
    connection = session.query(Connection).filter_by(conn_id=connection_id).one_or_none()
    if connection is None:
        raise NotFound(
            'Connection not found',
            detail=f"The Connection with connection_id: `{connection_id}` was not found",
        )
    session.delete(connection)
    return NoContent, 204


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONNECTION)])
@provide_session
def get_connection(connection_id, session):
    """Get a connection entry"""
    connection = session.query(Connection).filter(Connection.conn_id == connection_id).one_or_none()
    if connection is None:
        raise NotFound(
            "Connection not found",
            detail=f"The Connection with connection_id: `{connection_id}` was not found",
        )
    return connection_schema.dump(connection)


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONNECTION)])
@format_parameters({'limit': check_limit})
@provide_session
def get_connections(session, limit, offset=0, order_by="id"):
    """Get all connection entries"""
    to_replace = {"connection_id": "conn_id"}
    allowed_filter_attrs = ['connection_id', 'conn_type', 'description', 'host', 'port', 'id']

    total_entries = session.query(func.count(Connection.id)).scalar()
    query = session.query(Connection)
    query = apply_sorting(query, order_by, to_replace, allowed_filter_attrs)
    connections = query.offset(offset).limit(limit).all()
    return connection_collection_schema.dump(
        ConnectionCollection(connections=connections, total_entries=total_entries)
    )


@security.requires_access([(permissions.ACTION_CAN_EDIT, permissions.RESOURCE_CONNECTION)])
@provide_session
def patch_connection(connection_id, session, update_mask=None):
    """Update a connection entry"""
    try:
        data = connection_schema.load(request.json, partial=True)
    except ValidationError as err:
        # If validation get to here, it is extra field validation.
        raise BadRequest(detail=str(err.messages))
    non_update_fields = ['connection_id', 'conn_id']
    connection = session.query(Connection).filter_by(conn_id=connection_id).first()
    if connection is None:
        raise NotFound(
            "Connection not found",
            detail=f"The Connection with connection_id: `{connection_id}` was not found",
        )
    if data.get('conn_id') and connection.conn_id != data['conn_id']:
        raise BadRequest(detail="The connection_id cannot be updated.")
    if update_mask:
        update_mask = [i.strip() for i in update_mask]
        data_ = {}
        for field in update_mask:
            if field in data and field not in non_update_fields:
                data_[field] = data[field]
            else:
                raise BadRequest(detail=f"'{field}' is unknown or cannot be updated.")
        data = data_
    for key in data:
        setattr(connection, key, data[key])
    session.add(connection)
    session.commit()
    return connection_schema.dump(connection)


@security.requires_access([(permissions.ACTION_CAN_CREATE, permissions.RESOURCE_CONNECTION)])
@provide_session
def post_connection(session):
    """Create connection entry"""
    body = request.json
    try:
        data = connection_schema.load(body)
    except ValidationError as err:
        raise BadRequest(detail=str(err.messages))
    conn_id = data['conn_id']
    query = session.query(Connection)
    connection = query.filter_by(conn_id=conn_id).first()
    if not connection:
        connection = Connection(**data)
        session.add(connection)
        session.commit()
        return connection_schema.dump(connection)
    raise AlreadyExists(detail=f"Connection already exist. ID: {conn_id}")
