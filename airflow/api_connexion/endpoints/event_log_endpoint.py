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
from airflow.api_connexion.schemas.event_log_schema import (
    EventLogCollection,
    event_log_collection_schema,
    event_log_schema,
)
from airflow.models import Log
from airflow.security import permissions
from airflow.utils.session import provide_session


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_AUDIT_LOG)])
@provide_session
def get_event_log(event_log_id, session):
    """Get a log entry"""
    event_log = session.query(Log).filter(Log.id == event_log_id).one_or_none()
    if event_log is None:
        raise NotFound("Event Log not found")
    return event_log_schema.dump(event_log)


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_AUDIT_LOG)])
@format_parameters({'limit': check_limit})
@provide_session
def get_event_logs(session, limit, offset=None, order_by='event_log_id'):
    """Get all log entries from event log"""
    to_replace = {"event_log_id": "id", "when": "dttm"}
    allowed_filter_attrs = [
        'event_log_id',
        "when",
        "dag_id",
        "task_id",
        "event",
        "execution_date",
        "owner",
        "extra",
    ]
    total_entries = session.query(func.count(Log.id)).scalar()
    query = session.query(Log)
    query = apply_sorting(query, order_by, to_replace, allowed_filter_attrs)
    event_logs = query.offset(offset).limit(limit).all()
    return event_log_collection_schema.dump(
        EventLogCollection(event_logs=event_logs, total_entries=total_entries)
    )
