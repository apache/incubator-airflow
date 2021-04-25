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

from sqlalchemy import func

from airflow.api_connexion import security
from airflow.api_connexion.exceptions import NotFound
from airflow.api_connexion.parameters import apply_sorting, check_limit, format_parameters
from airflow.api_connexion.schemas.error_schema import (
    ImportErrorCollection,
    import_error_collection_schema,
    import_error_schema,
)
from airflow.models.errors import ImportError as ImportErrorModel
from airflow.security import permissions
from airflow.utils.session import provide_session


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_IMPORT_ERROR)])
@provide_session
def get_import_error(import_error_id, session):
    """Get an import error"""
    error = session.query(ImportErrorModel).filter(ImportErrorModel.id == import_error_id).one_or_none()

    if error is None:
        raise NotFound(
            "Import error not found",
            detail=f"The ImportError with import_error_id: `{import_error_id}` was not found",
        )
    return import_error_schema.dump(error)


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_IMPORT_ERROR)])
@format_parameters({'limit': check_limit})
@provide_session
def get_import_errors(session, limit, offset=None, order_by='import_error_id'):
    """Get all import errors"""
    to_replace = {"import_error_id": 'id'}
    allowed_filter_attrs = ['import_error_id', "timestamp", "filename"]
    total_entries = session.query(func.count(ImportErrorModel.id)).scalar()
    query = session.query(ImportErrorModel)
    query = apply_sorting(query, order_by, to_replace, allowed_filter_attrs)
    import_errors = query.offset(offset).limit(limit).all()
    return import_error_collection_schema.dump(
        ImportErrorCollection(import_errors=import_errors, total_entries=total_entries)
    )
