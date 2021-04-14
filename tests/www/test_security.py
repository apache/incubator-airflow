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

import logging
import unittest
from unittest import mock

import pytest
from flask_appbuilder import SQLA, Model, expose, has_access
from flask_appbuilder.security.sqla import models as sqla_models
from flask_appbuilder.views import BaseView, ModelView
from sqlalchemy import Column, Date, Float, Integer, String

from airflow import settings
from airflow.exceptions import AirflowException
from airflow.models import DagModel
from airflow.security import permissions
from airflow.www import app as application
from airflow.www.utils import CustomSQLAInterface
from tests.test_utils import fab_utils
from tests.test_utils.db import clear_db_dags, clear_db_runs
from tests.test_utils.mock_security_manager import MockSecurityManager

READ_WRITE = {permissions.ACTION_CAN_READ, permissions.ACTION_CAN_EDIT}
READ_ONLY = {permissions.ACTION_CAN_READ}

logging.basicConfig(format='%(asctime)s:%(levelname)s:%(name)s:%(message)s')
logging.getLogger().setLevel(logging.DEBUG)
log = logging.getLogger(__name__)


class SomeModel(Model):
    id = Column(Integer, primary_key=True)
    field_string = Column(String(50), unique=True, nullable=False)
    field_integer = Column(Integer())
    field_float = Column(Float())
    field_date = Column(Date())

    def __repr__(self):
        return str(self.field_string)


class SomeModelView(ModelView):
    datamodel = CustomSQLAInterface(SomeModel)
    base_permissions = [
        'can_list',
        'can_show',
        'can_add',
        permissions.ACTION_CAN_EDIT,
        permissions.ACTION_CAN_DELETE,
    ]
    list_columns = ['field_string', 'field_integer', 'field_float', 'field_date']


class SomeBaseView(BaseView):
    route_base = ''

    @expose('/some_action')
    @has_access
    def some_action(self):
        return "action!"


class TestSecurity(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        settings.configure_orm()
        cls.session = settings.Session
        cls.app = application.create_app(testing=True)
        cls.appbuilder = cls.app.appbuilder  # pylint: disable=no-member
        cls.app.config['WTF_CSRF_ENABLED'] = False
        cls.security_manager = cls.appbuilder.sm
        cls.delete_roles()

    def setUp(self):
        clear_db_runs()
        clear_db_dags()
        self.db = SQLA(self.app)
        self.appbuilder.add_view(SomeBaseView, "SomeBaseView", category="BaseViews")
        self.appbuilder.add_view(SomeModelView, "SomeModelView", category="ModelViews")

        log.debug("Complete setup!")

    @classmethod
    def delete_roles(cls):
        for role_name in ['team-a', 'MyRole1', 'MyRole5', 'Test_Role', 'MyRole3', 'MyRole2']:
            fab_utils.delete_role(cls.app, role_name)

    def expect_user_is_in_role(self, user, rolename):
        self.security_manager.init_role(rolename, [])
        role = self.security_manager.find_role(rolename)
        if not role:
            self.security_manager.add_role(rolename)
            role = self.security_manager.find_role(rolename)
        user.roles = [role]
        self.security_manager.update_user(user)

    def assert_user_has_dag_perms(self, perms, dag_id, user=None):
        for perm in perms:
            assert self._has_dag_perm(perm, dag_id, user), f"User should have '{perm}' on DAG '{dag_id}'"

    def assert_user_does_not_have_dag_perms(self, dag_id, perms, user=None):
        for perm in perms:
            assert not self._has_dag_perm(
                perm, dag_id, user
            ), f"User should not have '{perm}' on DAG '{dag_id}'"

    def _has_dag_perm(self, perm, dag_id, user):
        # if not user:
        #     user = self.user
        return self.security_manager.has_access(perm, self.security_manager.prefixed_dag_id(dag_id), user)

    def _create_dag(self, dag_id):
        dag_model = DagModel(dag_id=dag_id)
        self.session.add(dag_model)
        self.session.commit()
        self.security_manager.sync_perm_for_dag(dag_id, access_control=None)

    def tearDown(self):
        clear_db_runs()
        clear_db_dags()
        self.appbuilder = None
        self.app = None
        self.db = None
        log.debug("Complete teardown!")

    def test_init_role_baseview(self):
        role_name = 'MyRole3'
        role_perms = [('can_some_action', 'SomeBaseView')]
        self.security_manager.init_role(role_name, perms=role_perms)
        role = self.appbuilder.sm.find_role(role_name)
        assert role is not None
        assert len(role_perms) == len(role.permissions)

    def test_init_role_modelview(self):
        role_name = 'MyRole2'
        role_perms = [
            ('can_list', 'SomeModelView'),
            ('can_show', 'SomeModelView'),
            ('can_add', 'SomeModelView'),
            (permissions.ACTION_CAN_EDIT, 'SomeModelView'),
            (permissions.ACTION_CAN_DELETE, 'SomeModelView'),
        ]
        self.security_manager.init_role(role_name, role_perms)
        role = self.appbuilder.sm.find_role(role_name)
        assert role is not None
        assert len(role_perms) == len(role.permissions)

    def test_update_and_verify_permission_role(self):
        role_name = 'Test_Role'
        self.security_manager.init_role(role_name, [])
        role = self.security_manager.find_role(role_name)

        perm = self.security_manager.find_permission_view_menu(permissions.ACTION_CAN_EDIT, 'RoleModelView')
        self.security_manager.add_permission_role(role, perm)
        role_perms_len = len(role.permissions)

        self.security_manager.init_role(role_name, [])
        new_role_perms_len = len(role.permissions)

        assert role_perms_len == new_role_perms_len

    def test_verify_public_role_has_no_permissions(self):
        public = self.appbuilder.sm.find_role("Public")

        assert public.permissions == []

    def test_verify_default_anon_user_has_no_accessible_dag_ids(self):
        with self.app.app_context():
            user = mock.MagicMock()
            user.is_anonymous = True
            self.app.config['AUTH_ROLE_PUBLIC'] = 'Public'
            assert self.app.appbuilder.sm.get_user_roles(user) == [self.app.appbuilder.sm.get_public_role()]

            self._create_dag("test_dag_id")
            self.security_manager.sync_roles()

            assert self.security_manager.get_accessible_dag_ids(user) == set()

    def test_verify_default_anon_user_has_no_access_to_specific_dag(self):
        with self.app.app_context():
            user = mock.MagicMock()
            user.is_anonymous = True
            self.app.config['AUTH_ROLE_PUBLIC'] = 'Public'
            assert self.app.appbuilder.sm.get_user_roles(user) == [self.app.appbuilder.sm.get_public_role()]

            dag_id = "test_dag_id"
            self._create_dag(dag_id)
            self.app.appbuilder.sm.sync_roles()

            assert self.app.appbuilder.sm.can_read_dag(dag_id, user) is False
            assert self.app.appbuilder.sm.can_edit_dag(dag_id, user) is False
            assert self._has_dag_perm(permissions.ACTION_CAN_READ, dag_id, user) is False
            assert self._has_dag_perm(permissions.ACTION_CAN_EDIT, dag_id, user) is False

    def test_verify_anon_user_with_admin_role_has_all_dag_access(self):
        with self.app.app_context():
            self.app.config['AUTH_ROLE_PUBLIC'] = 'Admin'
            user = mock.MagicMock()
            user.is_anonymous = True

            assert self.app.appbuilder.sm.get_user_roles(user) == [self.app.appbuilder.sm.get_public_role()]

            test_dag_ids = ["test_dag_id_1", "test_dag_id_2", "test_dag_id_3"]
            for dag_id in test_dag_ids:
                self._create_dag(dag_id)
            self.security_manager.sync_roles()

            assert self.security_manager.get_accessible_dag_ids(user) == set(test_dag_ids)

    def test_verify_anon_user_with_admin_role_has_access_to_each_dag(self):
        with self.app.app_context():
            user = mock.MagicMock()
            user.is_anonymous = True
            self.app.config['AUTH_ROLE_PUBLIC'] = 'Admin'

            # Call `.get_user_roles` bc `user` is a mock and the `user.roles` prop needs to be set.
            user.roles = self.app.appbuilder.sm.get_user_roles(user)
            assert user.roles == [self.app.appbuilder.sm.get_public_role()]

            test_dag_ids = ["test_dag_id_1", "test_dag_id_2", "test_dag_id_3"]

            for dag_id in test_dag_ids:
                self._create_dag(dag_id)
            self.security_manager.sync_roles()

            for dag_id in test_dag_ids:
                assert self.app.appbuilder.sm.can_read_dag(dag_id, user) is True
                assert self.app.appbuilder.sm.can_edit_dag(dag_id, user) is True
                assert self._has_dag_perm(permissions.ACTION_CAN_READ, dag_id, user) is True
                assert self._has_dag_perm(permissions.ACTION_CAN_EDIT, dag_id, user) is True

    def test_get_user_roles(self):
        user = mock.MagicMock()
        user.is_anonymous = False
        roles = self.appbuilder.sm.find_role('Admin')
        user.roles = roles
        assert self.security_manager.get_user_roles(user) == roles

    def test_get_user_roles_for_anonymous_user(self):
        viewer_role_perms = {
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_CODE),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_IMPORT_ERROR),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_AUDIT_LOG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_JOB),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_PLUGIN),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_SLA_MISS),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_LOG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_XCOM),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_BROWSE_MENU),
            (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_DAG_RUN),
            (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_JOB),
            (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_AUDIT_LOG),
            (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_PLUGIN),
            (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_SLA_MISS),
            (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_TASK_INSTANCE),
            (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_DOCS_MENU),
            (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_DOCS_LINKS),
            (permissions.ACTION_CAN_THIS_FORM_GET, permissions.RESOURCE_RESET_MY_PASSWORD_VIEW),
            (permissions.ACTION_CAN_THIS_FORM_POST, permissions.RESOURCE_RESET_MY_PASSWORD_VIEW),
            (permissions.ACTION_RESETMYPASSWORD, permissions.RESOURCE_USER_DB_MODELVIEW),
            (permissions.ACTION_CAN_THIS_FORM_GET, permissions.RESOURCE_USERINFO_EDIT_VIEW),
            (permissions.ACTION_CAN_THIS_FORM_POST, permissions.RESOURCE_USERINFO_EDIT_VIEW),
            (permissions.ACTION_USERINFOEDIT, permissions.RESOURCE_USER_DB_MODELVIEW),
            (permissions.ACTION_CAN_USERINFO, permissions.RESOURCE_USER_DB_MODELVIEW),
            (permissions.ACTION_CAN_USERINFO, permissions.RESOURCE_USER_OID_MODELVIEW),
            (permissions.ACTION_CAN_USERINFO, permissions.RESOURCE_USER_LDAP_MODELVIEW),
            (permissions.ACTION_CAN_USERINFO, permissions.RESOURCE_USER_OAUTH_MODELVIEW),
            (permissions.ACTION_CAN_USERINFO, permissions.RESOURCE_USER_REMOTEUSER_MODELVIEW),
        }
        self.app.config['AUTH_ROLE_PUBLIC'] = 'Viewer'

        with self.app.app_context():
            user = mock.MagicMock()
            user.is_anonymous = True

            perms_views = set()
            for role in self.security_manager.get_user_roles(user):
                perms_views.update(
                    {(perm_view.permission.name, perm_view.view_menu.name) for perm_view in role.permissions}
                )
            assert perms_views == viewer_role_perms

    @mock.patch('airflow.www.security.AirflowSecurityManager.get_user_roles')
    def test_get_current_user_permissions(self, mock_get_user_roles):
        role_name = 'MyRole5'
        role_perm = 'can_some_action'
        role_vm = 'SomeBaseView'
        username = 'get_current_user_permissions'

        with self.app.app_context():
            user = fab_utils.create_user(
                self.app,
                username,
                role_name,
                permissions=[
                    (role_perm, role_vm),
                ],
            )
            role = user.roles[0]
            mock_get_user_roles.return_value = [role]

            assert self.security_manager.get_current_user_permissions() == {(role_perm, role_vm)}

            mock_get_user_roles.return_value = []
            assert len(self.security_manager.get_current_user_permissions()) == 0

    def test_get_accessible_dag_ids(self):
        role_name = 'MyRole1'
        permission_action = [permissions.ACTION_CAN_READ]
        dag_id = 'dag_id'
        username = "ElUser"

        user = fab_utils.create_user(
            self.app,
            username,
            role_name,
            permissions=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            ],
        )

        dag_model = DagModel(dag_id=dag_id, fileloc="/tmp/dag_.py", schedule_interval="2 2 * * *")
        self.session.add(dag_model)
        self.session.commit()

        self.security_manager.sync_perm_for_dag(  # type: ignore  # pylint: disable=no-member
            dag_id, access_control={role_name: permission_action}
        )

        assert self.security_manager.get_accessible_dag_ids(user) == {'dag_id'}

    def test_dont_get_inaccessible_dag_ids_for_dag_resource_permission(self):
        # In this test case,
        # get_readable_dag_ids() don't return DAGs to which the user has CAN_EDIT permission
        username = "Monsieur User"
        role_name = "MyRole1"
        permission_action = [permissions.ACTION_CAN_EDIT]
        dag_id = "dag_id"

        user = fab_utils.create_user(
            self.app,
            username,
            role_name,
            permissions=[
                (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
            ],
        )

        dag_model = DagModel(dag_id=dag_id, fileloc="/tmp/dag_.py", schedule_interval="2 2 * * *")
        self.session.add(dag_model)
        self.session.commit()

        self.security_manager.sync_perm_for_dag(  # type: ignore  # pylint: disable=no-member
            dag_id, access_control={role_name: permission_action}
        )

        assert self.security_manager.get_readable_dag_ids(user) == set()

    @mock.patch('airflow.www.security.AirflowSecurityManager._has_view_access')
    def test_has_access(self, mock_has_view_access):
        user = mock.MagicMock()
        user.is_anonymous = False
        mock_has_view_access.return_value = True
        assert self.security_manager.has_access('perm', 'view', user)

    def test_sync_perm_for_dag_creates_permissions_on_view_menus(self):
        test_dag_id = 'TEST_DAG'
        prefixed_test_dag_id = f'DAG:{test_dag_id}'
        self.security_manager.sync_perm_for_dag(test_dag_id, access_control=None)
        assert (
            self.security_manager.find_permission_view_menu(permissions.ACTION_CAN_READ, prefixed_test_dag_id)
            is not None
        )
        assert (
            self.security_manager.find_permission_view_menu(permissions.ACTION_CAN_EDIT, prefixed_test_dag_id)
            is not None
        )

    @mock.patch('airflow.www.security.AirflowSecurityManager._has_perm')
    @mock.patch('airflow.www.security.AirflowSecurityManager._has_role')
    def test_has_all_dag_access(self, mock_has_role, mock_has_perm):
        mock_has_role.return_value = True
        assert self.security_manager.has_all_dags_access()

        mock_has_role.return_value = False
        mock_has_perm.return_value = False
        assert not self.security_manager.has_all_dags_access()

        mock_has_perm.return_value = True
        assert self.security_manager.has_all_dags_access()

    def test_access_control_with_non_existent_role(self):
        with pytest.raises(AirflowException) as ctx:
            self.security_manager.sync_perm_for_dag(
                dag_id='access-control-test',
                access_control={
                    'this-role-does-not-exist': [permissions.ACTION_CAN_EDIT, permissions.ACTION_CAN_READ]
                },
            )
        assert "role does not exist" in str(ctx.value)

    def test_all_dag_access_doesnt_give_non_dag_access(self):
        username = 'dag_access_user'
        role_name = 'dag_access_role'
        with self.app.app_context():
            user = fab_utils.create_user(
                self.app,
                username,
                role_name,
                permissions=[
                    (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                    (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                ],
            )
            assert self.security_manager.has_access(
                permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG, user
            )
            assert not self.security_manager.has_access(
                permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE, user
            )

    def test_access_control_with_invalid_permission(self):
        invalid_permissions = [
            'can_varimport',  # a real permission, but not a member of DAG_PERMS
            'can_eat_pudding',  # clearly not a real permission
        ]
        username = "LaUser"
        user = fab_utils.create_user(
            self.app,
            username=username,
            role_name='team-a',
        )
        for permission in invalid_permissions:
            self.expect_user_is_in_role(user, rolename='team-a')
            with pytest.raises(AirflowException) as ctx:
                self.security_manager.sync_perm_for_dag(
                    'access_control_test', access_control={'team-a': {permission}}
                )
            assert "invalid permissions" in str(ctx.value)

    def test_access_control_is_set_on_init(self):
        username = 'access_control_is_set_on_init'
        role_name = 'team-a'
        with self.app.app_context():
            user = fab_utils.create_user(
                self.app,
                username,
                role_name,
                permissions=[],
            )
            self.expect_user_is_in_role(user, rolename='team-a')
            self.security_manager.sync_perm_for_dag(
                'access_control_test',
                access_control={'team-a': [permissions.ACTION_CAN_EDIT, permissions.ACTION_CAN_READ]},
            )
            self.assert_user_has_dag_perms(
                perms=[permissions.ACTION_CAN_EDIT, permissions.ACTION_CAN_READ],
                dag_id='access_control_test',
                user=user,
            )

            self.expect_user_is_in_role(user, rolename='NOT-team-a')
            self.assert_user_does_not_have_dag_perms(
                perms=[permissions.ACTION_CAN_EDIT, permissions.ACTION_CAN_READ],
                dag_id='access_control_test',
                user=user,
            )

    def test_access_control_stale_perms_are_revoked(self):
        username = 'access_control_stale_perms_are_revoked'
        role_name = 'team-a'
        with self.app.app_context():
            user = fab_utils.create_user(
                self.app,
                username,
                role_name,
                permissions=[],
            )
            self.expect_user_is_in_role(user, rolename='team-a')
            self.security_manager.sync_perm_for_dag(
                'access_control_test', access_control={'team-a': READ_WRITE}
            )
            self.assert_user_has_dag_perms(perms=READ_WRITE, dag_id='access_control_test', user=user)

            self.security_manager.sync_perm_for_dag(
                'access_control_test', access_control={'team-a': READ_ONLY}
            )
            self.assert_user_has_dag_perms(
                perms=[permissions.ACTION_CAN_READ], dag_id='access_control_test', user=user
            )
            self.assert_user_does_not_have_dag_perms(
                perms=[permissions.ACTION_CAN_EDIT], dag_id='access_control_test', user=user
            )

    def test_no_additional_dag_permission_views_created(self):
        ab_perm_view_role = sqla_models.assoc_permissionview_role

        self.security_manager.sync_roles()
        num_pv_before = self.db.session().query(ab_perm_view_role).count()
        self.security_manager.sync_roles()
        num_pv_after = self.db.session().query(ab_perm_view_role).count()
        assert num_pv_before == num_pv_after

    def test_override_role_vm(self):
        test_security_manager = MockSecurityManager(appbuilder=self.appbuilder)
        assert len(test_security_manager.VIEWER_VMS) == 1
        assert test_security_manager.VIEWER_VMS == {'Airflow'}

    def test_correct_roles_have_perms_to_read_config(self):
        roles_to_check = self.security_manager.get_all_roles()
        assert len(roles_to_check) >= 5
        for role in roles_to_check:
            if role.name in ["Admin", "Op"]:
                assert self.security_manager.exist_permission_on_roles(
                    permissions.RESOURCE_CONFIG, permissions.ACTION_CAN_READ, [role.id]
                )
            else:
                assert not self.security_manager.exist_permission_on_roles(
                    permissions.RESOURCE_CONFIG, permissions.ACTION_CAN_READ, [role.id]
                ), (
                    f"{role.name} should not have {permissions.ACTION_CAN_READ} "
                    f"on {permissions.RESOURCE_CONFIG}"
                )
