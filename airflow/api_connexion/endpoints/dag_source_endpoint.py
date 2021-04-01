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

from flask import Response, current_app, request
from itsdangerous import BadSignature, URLSafeSerializer

from airflow.api_connexion import security
from airflow.api_connexion.exceptions import NotFound
from airflow.api_connexion.schemas.dag_source_schema import dag_source_schema
from airflow.models.dagcode import DagCode
from airflow.security import permissions


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_CODE)])
def get_dag_source(file_token: str):
    """Get source code using file token"""
    secret_key = current_app.config["SECRET_KEY"]
    auth_s = URLSafeSerializer(secret_key)
    try:
        path = auth_s.loads(file_token)
        dag_source = DagCode.code(path)
    except (BadSignature, FileNotFoundError):
        raise NotFound("Dag source not found")

    return_type = request.accept_mimetypes.best_match(['text/plain', 'application/json'])
    if return_type == 'text/plain':
        return Response(dag_source, headers={'Content-Type': return_type})
    if return_type == 'application/json':
        content = dag_source_schema.dumps(dict(content=dag_source))
        return Response(content, headers={'Content-Type': return_type})
    return Response("Not Allowed Accept Header", status=406)
