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

from typing import List, NamedTuple, Optional, Tuple

from marshmallow import Schema, ValidationError, fields, validate, validates_schema
from marshmallow.utils import get_value

from airflow.api_connexion.parameters import validate_istimezone
from airflow.api_connexion.schemas.enum_schemas import TaskInstanceStateField
from airflow.api_connexion.schemas.sla_miss_schema import SlaMissSchema
from airflow.models import SlaMiss, TaskInstance
from airflow.utils.state import State


class TaskInstanceSchema(Schema):
    """Task instance schema"""

    task_id = fields.Str()
    dag_id = fields.Str()
    execution_date = fields.DateTime(validate=validate_istimezone)
    start_date = fields.DateTime(validate=validate_istimezone)
    end_date = fields.DateTime(validate=validate_istimezone)
    duration = fields.Float()
    state = TaskInstanceStateField()
    _try_number = fields.Int(data_key="try_number")
    max_tries = fields.Int()
    hostname = fields.Str()
    unixname = fields.Str()
    pool = fields.Str()
    pool_slots = fields.Int()
    queue = fields.Str()
    priority_weight = fields.Int()
    operator = fields.Str()
    queued_dttm = fields.DateTime(data_key="queued_when")
    pid = fields.Int()
    executor_config = fields.Str()
    sla_miss = fields.Nested(SlaMissSchema, default=None)

    def get_attribute(self, obj, attr, default):
        if attr == "sla_miss":
            # Object is a tuple of task_instance and slamiss
            # and the get_value expects a dict with key, value
            # corresponding to the attr.
            slamiss_instance = {"sla_miss": obj[1]}
            return get_value(slamiss_instance, attr, default)
        return get_value(obj[0], attr, default)


class TaskInstanceCollection(NamedTuple):
    """List of task instances with metadata"""

    task_instances: List[Tuple[TaskInstance, Optional[SlaMiss]]]
    total_entries: int


class TaskInstanceCollectionSchema(Schema):
    """Task instance collection schema"""

    task_instances = fields.List(fields.Nested(TaskInstanceSchema))
    total_entries = fields.Int()


class TaskInstanceBatchFormSchema(Schema):
    """Schema for the request form passed to Task Instance Batch endpoint"""

    page_offset = fields.Int(missing=0, min=0)
    page_limit = fields.Int(missing=100, min=1)
    dag_ids = fields.List(fields.Str(), missing=None)
    execution_date_gte = fields.DateTime(missing=None, validate=validate_istimezone)
    execution_date_lte = fields.DateTime(missing=None, validate=validate_istimezone)
    start_date_gte = fields.DateTime(missing=None, validate=validate_istimezone)
    start_date_lte = fields.DateTime(missing=None, validate=validate_istimezone)
    end_date_gte = fields.DateTime(missing=None, validate=validate_istimezone)
    end_date_lte = fields.DateTime(missing=None, validate=validate_istimezone)
    duration_gte = fields.Int(missing=None)
    duration_lte = fields.Int(missing=None)
    state = fields.List(fields.Str(), missing=None)
    pool = fields.List(fields.Str(), missing=None)
    queue = fields.List(fields.Str(), missing=None)


class ClearTaskInstanceFormSchema(Schema):
    """Schema for handling the request of clearing task instance of a Dag"""

    dry_run = fields.Boolean(default=True)
    start_date = fields.DateTime(missing=None, validate=validate_istimezone)
    end_date = fields.DateTime(missing=None, validate=validate_istimezone)
    only_failed = fields.Boolean(missing=True)
    only_running = fields.Boolean(missing=False)
    include_subdags = fields.Boolean(missing=False)
    include_parentdag = fields.Boolean(missing=False)
    reset_dag_runs = fields.Boolean(missing=False)
    task_ids = fields.List(fields.String(), validate=validate.Length(min=1))

    @validates_schema
    def validate_form(self, data, **kwargs):
        """Validates clear task instance form"""
        if data["only_failed"] and data["only_running"]:
            raise ValidationError("only_failed and only_running both are set to True")
        if data["start_date"] and data["end_date"]:
            if data["start_date"] > data["end_date"]:
                raise ValidationError("end_date is sooner than start_date")


class SetTaskInstanceStateFormSchema(Schema):
    """Schema for handling the request of setting state of task instance of a DAG"""

    dry_run = fields.Boolean(default=True)
    task_id = fields.Str(required=True)
    execution_date = fields.DateTime(required=True, validate=validate_istimezone)
    include_upstream = fields.Boolean(required=True)
    include_downstream = fields.Boolean(required=True)
    include_future = fields.Boolean(required=True)
    include_past = fields.Boolean(required=True)
    new_state = TaskInstanceStateField(required=True, validate=validate.OneOf([State.SUCCESS, State.FAILED]))


class TaskInstanceReferenceSchema(Schema):
    """Schema for the task instance reference schema"""

    task_id = fields.Str()
    dag_run_id = fields.Str()
    dag_id = fields.Str()
    execution_date = fields.DateTime()

    def get_attribute(self, obj, attr, default):
        """Overwritten marshmallow function"""
        task_instance_attr = ['task_id', 'execution_date', 'dag_id']
        if attr in task_instance_attr:
            obj = obj[0]  # As object is a tuple of task_instance and dag_run_id
            return get_value(obj, attr, default)
        return obj[1]


class TaskInstanceReferenceCollection(NamedTuple):
    """List of objects with metadata about taskinstance and dag_run_id"""

    task_instances: List[Tuple[TaskInstance, str]]


class TaskInstanceReferenceCollectionSchema(Schema):
    """Collection schema for task reference"""

    task_instances = fields.List(fields.Nested(TaskInstanceReferenceSchema))


task_instance_schema = TaskInstanceSchema()
task_instance_collection_schema = TaskInstanceCollectionSchema()
task_instance_batch_form = TaskInstanceBatchFormSchema()
clear_task_instance_form = ClearTaskInstanceFormSchema()
set_task_instance_state_form = SetTaskInstanceStateFormSchema()
task_instance_reference_schema = TaskInstanceReferenceSchema()
task_instance_reference_collection_schema = TaskInstanceReferenceCollectionSchema()
