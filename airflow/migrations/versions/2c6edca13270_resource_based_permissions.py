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

"""Resource based permissions.

Revision ID: 2c6edca13270
Revises: 849da589634d
Create Date: 2020-10-21 00:18:52.529438

"""
import logging

from airflow.security import permissions
from airflow.www.app import create_app

# revision identifiers, used by Alembic.
revision = '2c6edca13270'
down_revision = '849da589634d'
branch_labels = None
depends_on = None


mapping = {
    ("Airflow", "can_blocked"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
    ],
    ("Airflow", "can_clear"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_TASK_INSTANCE),
    ],
    ("Airflow", "can_code"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_CODE),
    ],
    ("Airflow", "can_dag_details"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
    ],
    ("Airflow", "can_dag_stats"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
    ],
    ("Airflow", "can_dagrun_clear"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_DAG_RUN),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
    ],
    ("Airflow", "can_dagrun_failed"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG_RUN),
    ],
    ("Airflow", "can_dagrun_success"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG_RUN),
    ],
    ("Airflow", permissions.ACTION_CAN_DELETE): [
        (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_DAG),
    ],
    ("Airflow", "can_duration"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
    ],
    ("Airflow", "can_extra_links"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
    ],
    ("Airflow", "can_failed"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_TASK_INSTANCE),
    ],
    ("Airflow", "can_gantt"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
    ],
    ("Airflow", "can_get_logs_with_metadata"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_LOG),
    ],
    ("Airflow", "can_graph"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_LOG),
    ],
    ("Airflow", "can_index"): [(permissions.ACTION_CAN_READ, "Website")],
    ("Airflow", "can_landing_times"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
    ],
    ("Airflow", "can_log"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_LOG),
    ],
    ("Airflow", "can_paused"): [
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
    ],
    ("Airflow", "can_redirect_to_external_log"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_LOG),
    ],
    ("Airflow", "can_refresh"): [
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
    ],
    ("Airflow", "can_refresh_all"): [
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
    ],
    ("Airflow", "can_rendered"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
    ],
    ("Airflow", "can_run"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_TASK_INSTANCE),
    ],
    ("Airflow", "can_success"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_TASK_INSTANCE),
    ],
    ("Airflow", "can_task"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
    ],
    ("Airflow", "can_task_instances"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
    ],
    ("Airflow", "can_task_stats"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
    ],
    ("Airflow", "can_last_dagruns"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
    ],
    ("Airflow", "can_tree"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_LOG),
    ],
    ("Airflow", "can_tries"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
    ],
    ("Airflow", "can_trigger"): [
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_DAG_RUN),
    ],
    ("Airflow", "can_xcom"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_XCOM),
    ],
    ("ConfigurationView", "can_conf"): [(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONFIG)],
    ("Config", "can_read"): [(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONFIG)],
    ("DagModelView", "can_list"): [(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG)],
    ("DagModelView", "can_read"): [(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG)],
    ("DagModelView", "can_show"): [(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG)],
    ("DagModelView", "show"): [(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG)],
    ("Dags", "can_read"): [(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG)],
    ("Dags", "can_edit"): [(permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG)],
    ("DagRunModelView", "clear"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_TASK_INSTANCE),
    ],
    ("DagRunModelView", "can_add"): [(permissions.ACTION_CAN_CREATE, permissions.RESOURCE_DAG_RUN)],
    ("DagRunModelView", "can_list"): [(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN)],
    ("DagRunModelView", "muldelete"): [(permissions.ACTION_CAN_DELETE, permissions.RESOURCE_DAG_RUN)],
    ("DagRunModelView", "set_running"): [(permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG_RUN)],
    ("DagRunModelView", "set_failed"): [(permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG_RUN)],
    ("DagRunModelView", "set_success"): [(permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG_RUN)],
    ("DagRun", "can_create"): [(permissions.ACTION_CAN_CREATE, permissions.RESOURCE_DAG_RUN)],
    ("DagRun", "can_read"): [(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN)],
    ("DagRun", "can_delete"): [(permissions.ACTION_CAN_DELETE, permissions.RESOURCE_DAG_RUN)],
    ("JobModelView", "can_list"): [(permissions.ACTION_CAN_READ, permissions.RESOURCE_JOB)],
    ("LogModelView", "can_list"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_AUDIT_LOG),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_AUDIT_LOG),
    ],
    ("Logs", permissions.ACTION_CAN_ACCESS_MENU): [
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_AUDIT_LOG)
    ],
    ("Log", "can_read"): [(permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_LOG)],
    ("SlaMissModelView", "can_list"): [(permissions.ACTION_CAN_READ, permissions.RESOURCE_SLA_MISS)],
    ("TaskInstanceModelView", "can_list"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE)
    ],
    ("TaskInstanceModelView", "clear"): [(permissions.ACTION_CAN_EDIT, permissions.RESOURCE_TASK_INSTANCE)],
    ("TaskInstanceModelView", "set_failed"): [
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_TASK_INSTANCE)
    ],
    ("TaskInstanceModelView", "set_retry"): [
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_TASK_INSTANCE)
    ],
    ("TaskInstanceModelView", "set_running"): [
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_TASK_INSTANCE)
    ],
    ("TaskInstanceModelView", "set_success"): [
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_TASK_INSTANCE)
    ],
    ("TaskRescheduleModelView", "can_list"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_RESCHEDULE)
    ],
    ("TaskInstance", "can_read"): [(permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE)],
    ("Tasks", permissions.ACTION_CAN_CREATE): [
        (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_TASK_INSTANCE)
    ],
    ("Tasks", permissions.ACTION_CAN_READ): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE)
    ],
    ("Tasks", permissions.ACTION_CAN_EDIT): [
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_TASK_INSTANCE)
    ],
    ("Tasks", permissions.ACTION_CAN_DELETE): [
        (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_TASK_INSTANCE)
    ],
    ("ConnectionModelView", "can_add"): [(permissions.ACTION_CAN_CREATE, permissions.RESOURCE_CONNECTION)],
    ("ConnectionModelView", "can_list"): [(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONNECTION)],
    ("ConnectionModelView", permissions.ACTION_CAN_EDIT): [
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_CONNECTION)
    ],
    ("ConnectionModelView", permissions.ACTION_CAN_DELETE): [
        (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_CONNECTION)
    ],
    ("ConnectionModelView", "muldelete"): [(permissions.ACTION_CAN_DELETE, permissions.RESOURCE_CONNECTION)],
    ("Connection", "can_create"): [(permissions.ACTION_CAN_CREATE, permissions.RESOURCE_CONNECTION)],
    ("Connection", "can_read"): [(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONNECTION)],
    ("Connection", "can_edit"): [(permissions.ACTION_CAN_EDIT, permissions.RESOURCE_CONNECTION)],
    ("Connection", "can_delete"): [(permissions.ACTION_CAN_DELETE, permissions.RESOURCE_CONNECTION)],
    ("DagCode", "can_read"): [(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN)],
    ("PluginView", "can_list"): [(permissions.ACTION_CAN_READ, permissions.RESOURCE_PLUGIN)],
    ("PoolModelView", "can_add"): [(permissions.ACTION_CAN_CREATE, permissions.RESOURCE_POOL)],
    ("PoolModelView", "can_list"): [(permissions.ACTION_CAN_READ, permissions.RESOURCE_POOL)],
    ("PoolModelView", permissions.ACTION_CAN_EDIT): [
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_POOL)
    ],
    ("PoolModelView", permissions.ACTION_CAN_DELETE): [
        (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_POOL)
    ],
    ("PoolModelView", "muldelete"): [(permissions.ACTION_CAN_DELETE, permissions.RESOURCE_POOL)],
    ("Pool", "can_create"): [(permissions.ACTION_CAN_CREATE, permissions.RESOURCE_POOL)],
    ("Pool", "can_read"): [(permissions.ACTION_CAN_READ, permissions.RESOURCE_POOL)],
    ("Pool", "can_edit"): [(permissions.ACTION_CAN_EDIT, permissions.RESOURCE_POOL)],
    ("Pool", "can_delete"): [(permissions.ACTION_CAN_DELETE, permissions.RESOURCE_POOL)],
    ("VariableModelView", "can_add"): [(permissions.ACTION_CAN_CREATE, permissions.RESOURCE_VARIABLE)],
    ("VariableModelView", "can_list"): [(permissions.ACTION_CAN_READ, permissions.RESOURCE_VARIABLE)],
    ("VariableModelView", permissions.ACTION_CAN_EDIT): [
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_VARIABLE)
    ],
    ("VariableModelView", permissions.ACTION_CAN_DELETE): [
        (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_VARIABLE)
    ],
    ("VariableModelView", "can_varimport"): [(permissions.ACTION_CAN_CREATE, permissions.RESOURCE_VARIABLE)],
    ("VariableModelView", "muldelete"): [(permissions.ACTION_CAN_DELETE, permissions.RESOURCE_VARIABLE)],
    ("VariableModelView", "varexport"): [(permissions.ACTION_CAN_READ, permissions.RESOURCE_VARIABLE)],
    ("Variable", "can_create"): [(permissions.ACTION_CAN_CREATE, permissions.RESOURCE_VARIABLE)],
    ("Variable", "can_read"): [(permissions.ACTION_CAN_READ, permissions.RESOURCE_VARIABLE)],
    ("Variable", "can_edit"): [(permissions.ACTION_CAN_EDIT, permissions.RESOURCE_VARIABLE)],
    ("Variable", "can_delete"): [(permissions.ACTION_CAN_DELETE, permissions.RESOURCE_VARIABLE)],
    ("XComModelView", "can_list"): [(permissions.ACTION_CAN_READ, permissions.RESOURCE_XCOM)],
    ("XComModelView", permissions.ACTION_CAN_DELETE): [
        (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_XCOM)
    ],
    ("XComModelView", "muldelete"): [(permissions.ACTION_CAN_DELETE, permissions.RESOURCE_XCOM)],
    ("XCom", "can_read"): [(permissions.ACTION_CAN_READ, permissions.RESOURCE_XCOM)],
}


def remap_permissions():
    """Apply Map Airflow permissions."""
    appbuilder = create_app(config={'FAB_UPDATE_PERMS': False}).appbuilder
    for old, new in mapping.items():
        (old_resource_name, old_action_name) = old
        old_permission = appbuilder.sm.get_permission(old_action_name, old_resource_name)
        if not old_permission:
            continue
        for new_action_name, new_resource_name in new:
            new_permission = appbuilder.sm.create_permission(new_action_name, new_resource_name)
            for role in appbuilder.sm.get_all_roles():
                if appbuilder.sm.exist_permission_on_roles(old_resource_name, old_action_name, [role.id]):
                    appbuilder.sm.add_permission_to_role(role, new_permission)
                    appbuilder.sm.remove_permission_from_role(role, old_permission)
        appbuilder.sm.delete_permission(old_action_name, old_resource_name)

        if not appbuilder.sm.get_action(old_action_name):
            continue
        resources = appbuilder.sm.get_all_resources()
        if not any(appbuilder.sm.get_permission(old_action_name, resource.name) for resource in resources):
            appbuilder.sm.delete_action(old_action_name)


def upgrade():
    """Apply Resource based permissions."""
    log = logging.getLogger()
    handlers = log.handlers[:]
    remap_permissions()
    log.handlers = handlers


def downgrade():
    """Unapply Resource based permissions."""
    pass
