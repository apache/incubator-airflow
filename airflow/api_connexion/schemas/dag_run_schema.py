#
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
import json
from typing import List, NamedTuple

from marshmallow import fields, pre_load
from marshmallow.schema import Schema
from marshmallow_sqlalchemy import SQLAlchemySchema, auto_field
from pendulum.parsing import ParserError

from airflow.api_connexion.exceptions import BadRequest
from airflow.api_connexion.parameters import validate_istimezone
from airflow.api_connexion.schemas.enum_schemas import DagStateField
from airflow.models.dagrun import DagRun
from airflow.utils import timezone
from airflow.utils.types import DagRunType


class ConfObject(fields.Field):
    """The conf field"""

    def _serialize(self, value, attr, obj, **kwargs):
        if not value:
            return {}
        return json.loads(value) if isinstance(value, str) else value

    def _deserialize(self, value, attr, data, **kwargs):
        if isinstance(value, str):
            return json.loads(value)
        return value


class DAGRunSchema(SQLAlchemySchema):
    """Schema for DAGRun"""

    class Meta:
        """Meta"""

        model = DagRun
        dateformat = "iso"

    run_id = auto_field(data_key='dag_run_id')
    dag_id = auto_field(dump_only=True)
    execution_date = auto_field(validate=validate_istimezone)
    start_date = auto_field(dump_only=True)
    end_date = auto_field(dump_only=True)
    state = DagStateField(dump_only=True)
    external_trigger = auto_field(default=True, dump_only=True)
    conf = ConfObject()

    @pre_load
    def autogenerate(self, data, **kwargs):
        """Auto generate run_id and execution_date if they are not loaded"""
        if "execution_date" not in data.keys():
            data["execution_date"] = str(timezone.utcnow())
        if "dag_run_id" not in data.keys():
            try:
                data["dag_run_id"] = DagRun.generate_run_id(
                    DagRunType.MANUAL, timezone.parse(data["execution_date"])
                )
            except (ParserError, TypeError) as err:
                raise BadRequest("Incorrect datetime argument", detail=str(err))
        return data


class DAGRunCollection(NamedTuple):
    """List of DAGRuns with metadata"""

    dag_runs: List[DagRun]
    total_entries: int


class DAGRunCollectionSchema(Schema):
    """DAGRun Collection schema"""

    dag_runs = fields.List(fields.Nested(DAGRunSchema))
    total_entries = fields.Int()


class DagRunsBatchFormSchema(Schema):
    """Schema to validate and deserialize the Form(request payload) submitted to DagRun Batch endpoint"""

    class Meta:
        """Meta"""

        datetimeformat = 'iso'
        strict = True

    order_by = fields.String()
    page_offset = fields.Int(missing=0, min=0)
    page_limit = fields.Int(missing=100, min=1)
    dag_ids = fields.List(fields.Str(), missing=None)
    execution_date_gte = fields.DateTime(missing=None, validate=validate_istimezone)
    execution_date_lte = fields.DateTime(missing=None, validate=validate_istimezone)
    start_date_gte = fields.DateTime(missing=None, validate=validate_istimezone)
    start_date_lte = fields.DateTime(missing=None, validate=validate_istimezone)
    end_date_gte = fields.DateTime(missing=None, validate=validate_istimezone)
    end_date_lte = fields.DateTime(missing=None, validate=validate_istimezone)


dagrun_schema = DAGRunSchema()
dagrun_collection_schema = DAGRunCollectionSchema()
dagruns_batch_form_schema = DagRunsBatchFormSchema()
