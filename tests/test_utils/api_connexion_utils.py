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
from airflow.api_connexion.exceptions import EXCEPTIONS_LINK_MAP
from airflow.www.security import EXISTING_ROLES


def create_user(app, username, role_name, permissions=None):
    appbuilder = app.appbuilder

    # Removes user and role so each test has isolated test data.
    delete_user(app, username)
    delete_role(app, role_name)
    role = create_role(app, role_name, permissions)

    appbuilder.sm.add_user(
        username=username,
        first_name=username,
        last_name=username,
        email=f"{username}@fab.org",
        role=role,
        password=username,
    )


def create_role(app, name, permissions=None):
    appbuilder = app.appbuilder
    role = appbuilder.sm.find_role(name)
    if not role:
        role = appbuilder.sm.add_role(name)
    if not permissions:
        permissions = []
    for permission in permissions:
        perm_object = appbuilder.sm.find_permission_view_menu(*permission)
        appbuilder.sm.add_permission_role(role, perm_object)
    return role


def delete_role(app, name):
    if app.appbuilder.sm.find_role(name):
        app.appbuilder.sm.delete_role(name)


def delete_user(app, username):
    appbuilder = app.appbuilder
    for user in appbuilder.sm.get_all_users():
        if user.username == username:
            _ = [
                delete_role(app, role.name) for role in user.roles if role and role.name not in EXISTING_ROLES
            ]
            appbuilder.sm.del_register_user(user)
            break


def assert_401(response):
    assert response.status_code == 401, f"Current code: {response.status_code}"
    assert response.json == {
        'detail': None,
        'status': 401,
        'title': 'Unauthorized',
        'type': EXCEPTIONS_LINK_MAP[401],
    }
