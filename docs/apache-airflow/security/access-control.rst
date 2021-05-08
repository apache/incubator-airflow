 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

Access Control
==============

Access Control of Airflow's Webserver UI is handled by Flask AppBuilder (FAB).
Please read its related `security document <http://flask-appbuilder.readthedocs.io/en/latest/security.html>`_
regarding its security model.

.. contents::
  :depth: 1
  :local:

.. spelling::
    clearTaskInstances
    dagRuns
    dagSources
    eventLogs
    importErrors
    taskInstances
    xcomEntries

Default Roles
'''''''''''''

By default, Airflow 2.0+ uses role-based access control (RBAC) to manage all permissions across your environment. Each role has permissions which provide varying levels of access to Airflow's key resources (DAGs, Connections, etc).

An environment's roles can be found under the **Security** tab:

.. image:: /img/list-roles.png

Airflow ships with a default set of roles:

Admin
^^^^^
``Admin`` users have all possible permissions, including granting or revoking permissions from
other users.

Only ``Admin`` users can configure/alter the permissions for other roles. While they can reconfigure Airflow's other default rules, it's not recommended.
The best practice is to instead create a new role with the desired permissions.

Public
^^^^^^
``Public`` users (anonymous) don't have any permissions.

Viewer
^^^^^^
``Viewer`` users have limited viewer permissions on limited web views. The following table lists all permissions granted to ``Viewers``:

.. exampleinclude:: /../../airflow/www/security.py
    :language: python
    :start-after: [START security_viewer_perms]
    :end-before: [END security_viewer_perms]


User
^^^^
``User`` users have all of the ``Viewer`` permissions listed above, plus additional user permissions for modifying DAGs, task instances, and DAG runs:

.. exampleinclude:: /../../airflow/www/security.py
    :language: python
    :start-after: [START security_user_perms]
    :end-before: [END security_user_perms]

Op
^^^
``Op`` users have all of the permissions that ``Viewers`` and ``Users`` have, plus additional permissions for modifying resources like connections and pools:

.. exampleinclude:: /../../airflow/www/security.py
    :language: python
    :start-after: [START security_op_perms]
    :end-before: [END security_op_perms]


Custom Roles
''''''''''''

Creating custom roles is the recommended way to customize your environment access. To do so:

1. In the **List Roles** menu, click the blue button to create a new role. Alternatively, select an existing role and click **Actions** > **Copy Role** to create a role based on your selection.

 .. image:: /img/list-roles.png

2. Specify a name and add permissions for the role. Note that the names for permissions will vary slightly between the UI and the source code.

 .. image:: /img/add-permissions.png

3. Click Save.

You can also create roles via the CLI using the following command:

.. code-block:: bash

  airflow roles create <Role1> <Role2>

You can then assign the given role to a new user using the ``airflow users add-role`` CLI command. Note that adding and removing permissions for a role can only be completed in the UI.

Resource-Based permissions
^^^^^^^^^^^^^^^^^^^^^^^^^^

Starting with version 2.0, permissions are based on individual resources and a small set of actions with those
resources. Resources match standard Airflow concepts, such as ``Dag``, ``DagRun``, ``Task``, and
``Connection``. Actions include ``can_create``, ``can_read``, ``can_edit``, and ``can_delete``.

Each endpoint in Airflow requires one or more permissions to be accessed. To access an endpoint, a role needs all permissions required by that endpoint.

DAG-Based Permissions
^^^^^^^^^^^^^^^^^^^^^

``Admin`` users can create roles with permission to access only specific DAGs. This is called DAG-level access.

DAG-level permissions differ from resource-level permissions in a few key ways:
- Resource-based permissions apply to all DAGs, while DAG-based permissions apply only to specific DAGs.
- DAG-based permissions control only``can_read`` and ``can_edit`` actions (``can_dag_read`` and ``can_dag_edit`` were deprecated in 2.0.0).
- You have to specify an existing DAG in the name of a DAG-based permission. The specific naming convention is ``DAG:<dag-name>.can_read`` or ``DAG:<dag-name>.can_edit``.

For example, the following images show how you would create a role which can only write to ``example_python_operator``. Once you create the role, the permission appears in the **List Roles** menu.

.. image:: /img/add-role.png
.. image:: /img/new-role.png

If an endpoint for a specific DAG requires a permission based on the DAG resource (for example, ``DAGs.can_create``), a role will have access if it has either the resource-based permission or a DAG-based permission.

For example, if an endpoint for the ``example_python_operator`` DAG requires the resource-based ``DAGS.can_read`` permission, a role with the DAG-based permission ``DAG:example_python_operator.can_read`` can still access that endpoint even without ``DAGS.can_read``.

Reference: Action and Endpoint Permissions
''''''''''''''''''''''''''''''''''''''''''

The following tables list all of the necessary permissions required for access to various Airflow endpoints and actions. Note that the ``Minimum Role`` table only applies if you haven't edited your default roles.

================================================================================== ====== ================================================================= ============
Stable API Permissions
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Endpoint                                                                           Method Permissions                                                       Minimum Role
================================================================================== ====== ================================================================= ============
/config                                                                            GET    Configurations.can_read                                           Op
/connections                                                                       GET    Connections.can_read                                              Op
/connections                                                                       POST   Connections.can_create                                            Op
/connections/{connection_id}                                                       DELETE Connections.can_delete                                            Op
/connections/{connection_id}                                                       PATCH  Connections.can_edit                                              Op
/connections/{connection_id}                                                       GET    Connections.can_read                                              Op
/dagSources/{file_token}                                                           GET    DAG Code.can_read                                                 Viewer
/dags                                                                              GET    DAGs.can_read                                                     Viewer
/dags/{dag_id}                                                                     GET    DAGs.can_read                                                     Viewer
/dags/{dag_id}                                                                     PATCH  DAGs.can_edit                                                     User
/dags/{dag_id}/clearTaskInstances                                                  POST   DAGs.can_read, DAG Runs.can_read, Task Instances.can_edit         User
/dags/{dag_id}/details                                                             GET    DAGs.can_read                                                     Viewer
/dags/{dag_id}/tasks                                                               GET    DAGs.can_read, Task Instances.can_read                            Viewer
/dags/{dag_id}/tasks/{task_id}                                                     GET    DAGs.can_read, Task Instances.can_read                            Viewer
/dags/{dag_id}/dagRuns                                                             GET    DAGs.can_read, DAG Runs.can_read                                  Viewer
/dags/{dag_id}/dagRuns                                                             POST   DAGs.can_edit, DAG Runs.can_create                                User
/dags/{dag_id}/dagRuns/{dag_run_id}                                                DELETE DAGs.can_read, DAG Runs.can_delete                                User
/dags/{dag_id}/dagRuns/{dag_run_id}                                                GET    DAGs.can_read, DAG Runs.can_read                                  Viewer
/dags/~/dagRuns/list                                                               POST   DAGs.can_read, DAG Runs.can_read                                  Viewer
/eventLogs                                                                         GET    Audit Logs.can_read                                               Viewer
/eventLogs/{event_log_id}                                                          GET    Audit Logs.can_read                                               Viewer
/importErrors                                                                      GET    ImportError.can_read                                              Viewer
/importErrors/{import_error_id}                                                    GET    ImportError.can_read                                              Viewer
/health                                                                            GET    None                                                              Public
/version                                                                           GET    None                                                              Public
/pools                                                                             GET    Pool.can_read                                                     Op
/pools                                                                             POST   Pool.can_create                                                   Op
/pools/{pool_name}                                                                 DELETE Pool.can_delete                                                   Op
/pools/{pool_name}                                                                 GET    Pool.can_read                                                     Op
/pools/{pool_name}                                                                 PATCH  Pool.can_edit                                                     Op
/providers                                                                         GET    Provider.can_read                                                 Op
/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances                                  GET    DAGs.can_read, DAG Runs.can_read, Task Instances.can_read         Viewer
/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}                        GET    DAGs.can_read, DAG Runs.can_read, Task Instances.can_read         Viewer
/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/links                  GET    DAGs.can_read, DAG Runs.can_read, Task Instances.can_read         Viewer
/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{task_try_number} GET    DAGs.can_read, DAG Runs.can_read, Task Instances.can_read         Viewer
/dags/~/dagRuns/~/taskInstances/list                                               POST   DAGs.can_read, DAG Runs.can_read, Task Instances.can_read         Viewer
/variables                                                                         GET    Variables.can_read                                                Op
/variables                                                                         POST   Variables.can_create                                              Op
/variables/{variable_key}                                                          DELETE Variables.can_delete                                              Op
/variables/{variable_key}                                                          GET    Variables.can_read                                                Op
/variables/{variable_key}                                                          PATCH  Variables.can_edit                                                Op
/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries            GET    DAGs.can_read, DAG Runs.can_read,                                 Viewer
                                                                                          Task Instances.can_read, XComs.can_read
/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries/{xcom_key} GET    DAGs.can_read, DAG Runs.can_read,                                 Viewer
                                                                                          Task Instances.can_read, XComs.can_read
/users                                                                             GET    Users.can_read                                                    Admin
/users/{username}                                                                  GET    Users.can_read                                                    Admin
================================================================================== ====== ================================================================= ============


====================================== ======================================================================= ============
Website Permissions
-------------------------------------- ------------------------------------------------------------------------------------
Action                                 Permissions                                                             Minimum Role
====================================== ======================================================================= ============
Access homepage                        Website.can_read                                                        Viewer
Get DAG stats                          Dags.can_read, DAG Runs.can_read                                        Viewer
Get Task stats                         Dags.can_read, DAG Runs.can_read, Task Instances.can_read               Viewer
Get last DAG runs                      Dags.can_read, DAG Runs.can_read                                        Viewer
Get DAG code                           Dags.can_read, DAG Code.can_read                                        Viewer
Get DAG details                        Dags.can_read, DAG Runs.can_read                                        Viewer
Get rendered DAG                       DAGs.can_read, Task Instances.can_read                                  Viewer
Get Logs with metadata                 DAGs.can_read, Task Instances.can_read, Task Logs.can_read              Viewer
Get Log                                DAGs.can_read, Task Instances.can_read, Task Logs.can_read              Viewer
Redirect to external Log               DAGs.can_read, Task Instances.can_read, Task Logs.can_read              Viewer
Get Task                               DAGs.can_read, Task Instances.can_read                                  Viewer
Get XCom                               DAGs.can_read, Task Instances.can_read, XComs.can_read                  Viewer
Triggers Task Instance                 DAGs.can_read, Task Instances.can_create                                User
Delete DAG                             DAGs.can_delete                                                         User
Trigger DAG run                        Dags.can_edit, DAG Runs.can_create                                      User
Clear DAG                              DAGs.can_read, Task Instances.can_delete                                User
Clear DAG Run                          DAGs.can_read, Task Instances.can_delete                                User
Mark DAG as blocked                    Dags.can_read, DAG Runs.can_read                                        User
Mark DAG Run as failed                 Dags.can_read, DAG Runs.can_edit                                        User
Mark DAG Run as success                Dags.can_read, DAG Runs.can_edit                                        User
Mark Task as failed                    DAGs.can_read, Task Instances.can_edit                                  User
Mark Task as success                   DAGs.can_read, Task Instances.can_edit                                  User
Get DAG as tree                        DAGs.can_read, Task Instances.can_read,                                 Viewer
                                       Task Logs.can_read
Get DAG as graph                       DAGs.can_read, Task Instances.can_read,                                 Viewer
                                       Task Logs.can_read
Get DAG as duration graph              DAGs.can_read, Task Instances.can_read                                  Viewer
Show all tries                         DAGs.can_read, Task Instances.can_read                                  Viewer
Show landing times                     DAGs.can_read, Task Instances.can_read                                  Viewer
Toggle DAG paused status               DAGs.can_edit                                                           User
Refresh DAG                            DAGs.can_edit                                                           User
Refresh all DAGs                       DAGs.can_edit                                                           User
Show Gantt Chart                       DAGs.can_read, Task Instances.can_read                                  Viewer
Get external links                     DAGs.can_read, Task Instances.can_read                                  Viewer
Show Task Instances                    DAGs.can_read, Task Instances.can_read                                  Viewer
Show Configs                           Configurations.can_read                                                 Viewer
Delete multiple records                DAGs.can_edit                                                           User
Set Task Instance as running           DAGs.can_edit                                                           User
Set Task Instance as failed            DAGs.can_edit                                                           User
Set Task Instance as success           DAGs.can_edit                                                           User
Set Task Instance as up_for_retry      DAGs.can_edit                                                           User
Autocomplete                           DAGs.can_read                                                           Viewer
List Logs                              Audit Logs.can_read                                                     Viewer
List Jobs                              Jobs.can_read                                                           Viewer
List SLA Misses                        SLA Misses.can_read                                                     Viewer
List Plugins                           Plugins.can_read                                                        Viewer
List Task Reschedules                  Task Reschedules.can_read                                               Admin
====================================== ======================================================================= ============
