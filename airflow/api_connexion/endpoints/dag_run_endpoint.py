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

from flask import request
from marshmallow.exceptions import ValidationError

from airflow.api_connexion import parameters
from airflow.api_connexion.exceptions import BadRequest, NotFound
from airflow.api_connexion.schemas.dag_run_schema import (
    dagrun_collection_schema, dagrun_schema, list_dag_runs_form_schema,
)
from airflow.models import DagRun
from airflow.utils import timezone
from airflow.utils.session import provide_session


def delete_dag_run():
    """
    Delete a DAG Run
    """
    raise NotImplementedError("Not implemented yet.")


@provide_session
def get_dag_run(dag_id, dag_run_id, session):
    """
    Get a DAG Run.
    """
    query = session.query(DagRun)
    query = query.filter(DagRun.dag_id == dag_id)
    query = query.filter(DagRun.run_id == dag_run_id)
    dag_run = query.one_or_none()
    if dag_run is None:
        raise NotFound("DAGRun not found")
    return dagrun_schema.dump(dag_run)


@provide_session
def get_dag_runs(dag_id, session):
    """
    Get all DAG Runs.
    """
    offset = request.args.get(parameters.page_offset, 0)
    limit = min(int(request.args.get(parameters.page_limit, 100)), 100)
    start_date_gte = request.args.get(parameters.filter_start_date_gte, None)
    start_date_lte = request.args.get(parameters.filter_start_date_lte, None)
    execution_date_gte = request.args.get(parameters.filter_execution_date_gte, None)
    execution_date_lte = request.args.get(parameters.filter_execution_date_lte, None)
    end_date_gte = request.args.get(parameters.filter_end_date_gte, None)
    end_date_lte = request.args.get(parameters.filter_end_date_lte, None)

    query = session.query(DagRun)

    #  This endpoint allows specifying ~ as the dag_id to retrieve DAG Runs for all DAGs.
    if dag_id == '~':
        dag_run = query.all()
        return dagrun_collection_schema.dump(dag_run)

    query = query.filter(DagRun.dag_id == dag_id)

    # Todo: validate date format from args
    # filter start date
    if start_date_gte and start_date_lte:
        query = query.filter(DagRun.start_date <= timezone.parse(start_date_lte),
                             DagRun.start_date >= timezone.parse(start_date_gte))

    elif start_date_gte and not start_date_lte:
        query = query.filter(DagRun.start_date >= timezone.parse(start_date_gte))

    elif start_date_lte and not start_date_gte:
        query = query.filter(DagRun.start_date <= timezone.parse(start_date_lte))

    # filter execution date
    if execution_date_gte and execution_date_lte:
        query = query.filter(DagRun.execution_date <= timezone.parse(execution_date_lte),
                             DagRun.execution_date >= timezone.parse(execution_date_gte))

    elif execution_date_gte and not execution_date_lte:
        query = query.filter(DagRun.execution_date >= timezone.parse(execution_date_gte))

    elif execution_date_lte and not execution_date_gte:
        query = query.filter(DagRun.execution_date <= timezone.parse(execution_date_lte))

    # filter end date
    if end_date_gte and end_date_lte:
        query = query.filter(DagRun.end_date <= timezone.parse(end_date_lte),
                             DagRun.end_date >= timezone.parse(end_date_gte))

    elif end_date_gte and not end_date_lte:
        query = query.filter(DagRun.end_date >= timezone.parse(end_date_gte))

    elif end_date_lte and not end_date_gte:
        query = query.filter(DagRun.end_date <= timezone.parse(end_date_lte))

    # apply offset and limit
    dag_run = query.offset(offset).limit(limit)

    return dagrun_collection_schema.dump(dag_run)


@provide_session
def get_dag_runs_batch(session):
    """
    Get list of DAG Runs
    """
    body = request.get_json()
    try:
        data = list_dag_runs_form_schema.load(body)
    except ValidationError as err:
        raise BadRequest(detail=err.messages)

    offset = data.get("page_offset")
    limit = data.get('page_limit')
    start_date_gte = data.get('start_date_gte', None)
    start_date_lte = data.get('start_date_lte', None)
    execution_date_gte = data.get("execution_date_gte", None)
    execution_date_lte = data.get("execution_date_lte", None)
    end_date_gte = data.get("end_date_gte", None)
    end_date_lte = data.get("end_date_lte", None)

    query = session.query(DagRun)

    # filter start date
    if start_date_gte and start_date_lte:
        query = query.filter(DagRun.start_date <= timezone.parse(start_date_lte),
                             DagRun.start_date >= timezone.parse(start_date_gte))

    elif start_date_gte and not start_date_lte:
        query = query.filter(DagRun.start_date >= timezone.parse(start_date_gte))

    elif start_date_lte and not start_date_gte:
        query = query.filter(DagRun.start_date <= timezone.parse(start_date_lte))

    # filter execution date
    if execution_date_gte and execution_date_lte:
        query = query.filter(DagRun.execution_date <= timezone.parse(execution_date_lte),
                             DagRun.execution_date >= timezone.parse(execution_date_gte))

    elif execution_date_gte and not execution_date_lte:
        query = query.filter(DagRun.execution_date >= timezone.parse(execution_date_gte))

    elif execution_date_lte and not execution_date_gte:
        query = query.filter(DagRun.execution_date <= timezone.parse(execution_date_lte))

    # filter end date
    if end_date_gte and end_date_lte:
        query = query.filter(DagRun.end_date <= timezone.parse(end_date_lte),
                             DagRun.end_date >= timezone.parse(end_date_gte))

    elif end_date_gte and not end_date_lte:
        query = query.filter(DagRun.end_date >= timezone.parse(end_date_gte))

    elif end_date_lte and not end_date_gte:
        query = query.filter(DagRun.end_date <= timezone.parse(end_date_lte))

    # apply offset and limit
    dag_run = query.offset(offset).limit(limit)

    return dagrun_collection_schema.dump(dag_run)


def patch_dag_run():
    """
    Update a DAG Run
    """
    raise NotImplementedError("Not implemented yet.")


def post_dag_run():
    """
    Trigger a DAG.
    """
    raise NotImplementedError("Not implemented yet.")
