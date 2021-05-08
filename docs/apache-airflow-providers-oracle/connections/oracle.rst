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



.. _howto/connection:oracle:

Oracle Connection
=================
The Oracle connection type provides connection to a Oracle database.

Configuring the Connection
--------------------------

Host (optional)
    The host to connect to.

Schema (optional)
    Specify the schema name to be used in the database.

Login (optional)
    Specify the user name to connect.

Password (optional)
    Specify the password to connect.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in Oracle
    connection. The following parameters are supported:

    * ``encoding`` - The encoding to use for regular database strings. If not specified,
      the environment variable ``NLS_LANG`` is used. If the environment variable ``NLS_LANG``
      is not set, ``ASCII`` is used.
    * ``nencoding`` - The encoding to use for national character set database strings.
      If not specified, the environment variable ``NLS_NCHAR`` is used. If the environment
      variable ``NLS_NCHAR`` is not used, the environment variable ``NLS_LANG`` is used instead,
      and if the environment variable ``NLS_LANG`` is not set, ``ASCII`` is used.
    * ``threaded`` - Whether or not Oracle should wrap accesses to connections with a mutex.
      Default value is False.
    * ``events`` - Whether or not to initialize Oracle in events mode.
    * ``mode`` - one of ``sysdba``, ``sysasm``, ``sysoper``, ``sysbkp``, ``sysdgd``, ``syskmt`` or ``sysrac``
      which are defined at the module level, Default mode is connecting.
    * ``purity`` - one of ``new``, ``self``, ``default``. Specify the session acquired from the pool.
      configuration parameter.
    * ``dsn``. Specify a Data Source Name (and ignore Host).
    * ``sid`` or ``service_name``. Use to form DSN instead of Schema.

    Connect using `dsn`, Host and `sid`, Host and `service_name`, or only Host `(OracleHook.getconn Documentation) <https://airflow.apache.org/docs/apache-airflow-providers-oracle/stable/_modules/airflow/providers/oracle/hooks/oracle.html#OracleHook.get_conn>`_.

    For example:

    .. code-block:: python

        Host = "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=dbhost.example.com)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=orclpdb1)))"

    or

    .. code-block:: python

        Host = "dbhost.example.com"
        Schema = "orclpdb1"

    or

    .. code-block:: python

        Host = "dbhost.example.com"
        Schema = "orcl"


    More details on all Oracle connect parameters supported can be found in `cx_Oracle documentation
    <https://cx-oracle.readthedocs.io/en/latest/api_manual/module.html#cx_Oracle.connect>`_.

    Information on creating an Oracle Connection through the web user interface can be found in Airflow's :doc:`Managing Connections Documentation <apache-airflow:howto/connection>`.


    Example "extras" field:

    .. code-block:: json

       {
          "encoding": "UTF-8",
          "nencoding": "UTF-8",
          "threaded": false,
          "events": false,
          "mode": "sysdba",
          "purity": "new"
       }

    When specifying the connection as URI (in :envvar:`AIRFLOW_CONN_{CONN_ID}` variable) you should specify it
    following the standard syntax of DB connections, where extras are passed as parameters
    of the URI (note that all components of the URI should be URL-encoded).

    For example:

    .. code-block:: bash

        export AIRFLOW_CONN_ORACLE_DEFAULT='oracle://oracle_user:XXXXXXXXXXXX@1.1.1.1:1521?encoding=UTF-8&nencoding=UTF-8&threaded=False&events=False&mode=sysdba&purity=new'
