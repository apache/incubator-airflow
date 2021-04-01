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

from flask_appbuilder.security.sqla.models import Permission, PermissionView, Role, ViewMenu
from marshmallow import Schema, fields
from marshmallow_sqlalchemy import SQLAlchemySchema, auto_field


class ActionSchema(SQLAlchemySchema):
    """Permission Action Schema"""

    class Meta:
        """Meta"""

        model = Permission

    name = auto_field()


class ResourceSchema(SQLAlchemySchema):
    """View menu Schema"""

    class Meta:
        """Meta"""

        model = ViewMenu

    name = auto_field()


class ActionCollection(NamedTuple):
    """Permission Action Collection"""

    actions: List[Permission]
    total_entries: int


class ActionCollectionSchema(Schema):
    """Permissions list schema"""

    actions = fields.List(fields.Nested(ActionSchema))
    total_entries = fields.Int()


class ActionResourceSchema(SQLAlchemySchema):
    """Permission View Schema"""

    class Meta:
        """Meta"""

        model = PermissionView

    permission = fields.Nested(ActionSchema, data_key="action")
    view_menu = fields.Nested(ResourceSchema, data_key="resource")


class RoleSchema(SQLAlchemySchema):
    """Role item schema"""

    class Meta:
        """Meta"""

        model = Role

    name = auto_field()
    permissions = fields.List(fields.Nested(ActionResourceSchema), data_key='actions')


class RoleCollection(NamedTuple):
    """List of roles"""

    roles: List[Role]
    total_entries: int


class RoleCollectionSchema(Schema):
    """List of roles"""

    roles = fields.List(fields.Nested(RoleSchema))
    total_entries = fields.Int()


role_schema = RoleSchema()
role_collection_schema = RoleCollectionSchema()
action_collection_schema = ActionCollectionSchema()
