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
from flask import current_app, g, request
from marshmallow import ValidationError

from airflow import DAG
from airflow.api_connexion import security
from airflow.api_connexion.exceptions import BadRequest, NotFound
from airflow.api_connexion.parameters import check_limit, format_parameters
from airflow.api_connexion.schemas.dag_schema import (
    DAGCollection,
    dag_detail_schema,
    dag_schema,
    dags_collection_schema,
)
from airflow.exceptions import SerializedDagNotFound
from airflow.models.dag import DagModel
from airflow.security import permissions
from airflow.utils.session import provide_session


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG)])
@provide_session
def get_dag(dag_id, session):
    """Get basic information about a DAG."""
    dag = session.query(DagModel).filter(DagModel.dag_id == dag_id).one_or_none()

    if dag is None:
        raise NotFound("DAG not found", detail=f"The DAG with dag_id: {dag_id} was not found")

    return dag_schema.dump(dag)


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG)])
def get_dag_details(dag_id):
    """Get details of DAG."""
    try:
        dag: DAG = current_app.dag_bag.get_dag(dag_id)
    except SerializedDagNotFound:
        raise NotFound("DAG not found", detail=f"The DAG with dag_id: {dag_id} was not found")
    if dag is None:
        raise NotFound("DAG not found", detail=f"The DAG with dag_id: {dag_id} was not found")
    return dag_detail_schema.dump(dag)


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG)])
@format_parameters({'limit': check_limit})
def get_dags(limit, offset=0):
    """Get all DAGs."""
    readable_dags = current_app.appbuilder.sm.get_readable_dags(g.user)
    dags = readable_dags.order_by(DagModel.dag_id).offset(offset).limit(limit).all()
    total_entries = readable_dags.count()

    return dags_collection_schema.dump(DAGCollection(dags=dags, total_entries=total_entries))


@security.requires_access([(permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG)])
@provide_session
def patch_dag(session, dag_id, update_mask=None):
    """Update the specific DAG"""
    dag = session.query(DagModel).filter(DagModel.dag_id == dag_id).one_or_none()
    if not dag:
        raise NotFound(f"Dag with id: '{dag_id}' not found")
    try:
        patch_body = dag_schema.load(request.json, session=session)
    except ValidationError as err:
        raise BadRequest("Invalid Dag schema", detail=str(err.messages))
    if update_mask:
        patch_body_ = {}
        if len(update_mask) > 1:
            raise BadRequest(detail="Only `is_paused` field can be updated through the REST API")
        update_mask = update_mask[0]
        if update_mask != 'is_paused':
            raise BadRequest(detail="Only `is_paused` field can be updated through the REST API")
        patch_body_[update_mask] = patch_body[update_mask]
        patch_body = patch_body_
    setattr(dag, 'is_paused', patch_body['is_paused'])
    session.flush()
    return dag_schema.dump(dag)
