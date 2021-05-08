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



.. _howto/connection:hiveserver2:

Hive Server2 Connection
=========================

The Hive Server2 connection type enables the Hive Server2 Integrations.

Authenticating to Hive Server2
------------------------------

Connect to Hive Server2 using `PyHive
<https://pypi.org/project/PyHive/>`_.
Choose between authenticating via LDAP, kerberos, or custom.

Default Connection IDs
----------------------

All hooks and operators related to Hive Server2 use ``hiveserver2_default`` by default.

Configuring the Connection
--------------------------

Login (optional)
    Specify your Hive Server2 username.

Password (optional)
    Specify your Hive password for use with LDAP and custom authentication.

Host (optional)
    Specify the host node for Hive Server2.

Port (optional)
    Specify your Hive Server2 port number.

Schema (optional)
    Specify the name for the database you would like to connect to with Hive Server2.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in Hive Server2 connection.
    The following parameters are all optional:

    * ``auth_mechanism``
      Specify the authentication method for PyHive choose between ``PLAIN``, ``LDAP``, ``KERBEROS`` or, ``Custom`` the default is ``PLAIN``.
    * ``kerberos_service_name``
      If authenticating with kerberos specify the kerberos service name the default is ``hive``.
    * ``run_set_variable_statements``
      Specify the if you want to run set variable statements the default is ```True``.


When specifying the connection in environment variable you should specify
it using URI syntax.

Note that all components of the URI should be URL-encoded.

For example:

.. code-block:: bash

   export AIRFLOW_CONN_HIVESERVER2_DEFAULT='hiveserver2://username:password@hiveserver2-node:80/database?auth_mechanism=LDAP'
