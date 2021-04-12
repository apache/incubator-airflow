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
from typing import List, NamedTuple

from flask_appbuilder.security.sqla.models import User
from marshmallow import Schema, fields
from marshmallow_sqlalchemy import SQLAlchemySchema, auto_field

from airflow.api_connexion.parameters import validate_istimezone
from airflow.api_connexion.schemas.role_and_permission_schema import RoleSchema


class UserCollectionItemSchema(SQLAlchemySchema):
    """user collection item schema"""

    class Meta:
        """Meta"""

        model = User
        dateformat = "iso"

    first_name = auto_field()
    last_name = auto_field()
    username = auto_field()
    active = auto_field(dump_only=True)
    email = auto_field()
    last_login = auto_field(dump_only=True)
    login_count = auto_field(dump_only=True)
    fail_login_count = auto_field(dump_only=True)
    roles = fields.List(fields.Nested(RoleSchema, only=('name',)))
    created_on = auto_field(validate=validate_istimezone, dump_only=True)
    changed_on = auto_field(validate=validate_istimezone, dump_only=True)


class UserSchema(UserCollectionItemSchema):
    """User schema"""

    password = auto_field(load_only=True)


class UserCollection(NamedTuple):
    """User collection"""

    users: List[User]
    total_entries: int


class UserCollectionSchema(Schema):
    """User collection schema"""

    users = fields.List(fields.Nested(UserCollectionItemSchema))
    total_entries = fields.Int()


user_collection_item_schema = UserCollectionItemSchema()
user_schema = UserSchema()
user_collection_schema = UserCollectionSchema()
