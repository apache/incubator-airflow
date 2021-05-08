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

.. _howto/connection/odbc:

ODBC Connection
===============

The ``odbc`` connection type provides connection to ODBC data sources including MS SQL Server.

Enable with ``pip install apache-airflow[odbc]``.


System prerequisites
--------------------

This connection type uses `pyodbc <https://github.com/mkleehammer/pyodbc>`_, which has some system
dependencies, as documented on the `pyodbc wiki <https://github.com/mkleehammer/pyodbc/wiki/Install>`_.

You must also install a driver:

* `MS SQL ODBC drivers <https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver15>`_.

* `Exasol ODBC drivers <https://docs.exasol.com/connect_exasol/drivers/odbc/odbc_linux.htm>`_.


Configuring the Connection
--------------------------

To use the hook :py:class:`~airflow.providers.odbc.hooks.odbc.OdbcHook` you must specify the
driver you want to use either in ``Connection.extra`` or as a parameter at hook initialization.

Host (required)
    The host to connect to.

Schema (optional)
    Specify the schema name to be used in the database.

Login (required)
    Specify the user name to connect.

Password (required)
    Specify the password to connect.

Extra (optional)
    Any key / value parameters supplied here will be added to the ODBC connection string.

    Additionally there a few special optional keywords that are handled separately.

    - ``connect_kwargs``
        * key-value pairs under ``connect_kwargs`` will be passed onto ``pyodbc.connect`` as kwargs
    - ``sqlalchemy_scheme``
        * This is only used when ``get_uri`` is invoked in
          :py:meth:`~airflow.hooks.dbapi.DbApiHook.get_sqlalchemy_engine`.  By default, the hook uses
          scheme ``mssql+pyodbc``.  You may pass a string value here to override.

    .. note::
        You are responsible for installing an ODBC driver on your system.

        The following examples demonstrate usage of the `Microsoft ODBC driver <https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver15>`_.

    For example, consider the following value for ``extra``:

    .. code-block:: json

        {
          "Driver": "ODBC Driver 17 for SQL Server",
          "ApplicationIntent": "ReadOnly",
          "TrustedConnection": "Yes"
        }

    This would produce a connection string containing these params:

    .. code-block::

        DRIVER={ODBC Driver 17 for SQL Server};ApplicationIntent=ReadOnly;TrustedConnection=Yes;

    See `DSN and Connection String Keywords and Attributes <https://docs.microsoft.com/en-us/sql/connect/odbc/dsn-connection-string-attribute?view=sql-server-ver15>`_
    for more info.

    Example connection URI for use with environment variables etc:

    .. code-block:: bash

        export AIRFLOW_CONN_MSSQL_DEFAULT='mssql-odbc://my_user:XXXXXXXXXXXX@1.1.1.1:1433/my_database?Driver=ODBC+Driver+17+for+SQL+Server&ApplicationIntent=ReadOnly&TrustedConnection=Yes'

    If you want to pass keyword arguments to ``pyodbc.connect``, you may supply a dictionary
    under ``connect_kwargs``.

    For example with ``extra`` as below, ``pyodbc.connect`` will be called with ``autocommit=False`` and
    ``ansi=True``.

    .. code-block:: json

        {
          "Driver": "ODBC Driver 17 for SQL Server",
          "ApplicationIntent": "ReadOnly",
          "TrustedConnection": "Yes",
          "connect_kwargs": {
            "autocommit": false,
            "ansi": true
          }
        }

    See `pyodbc documentation <https://github.com/mkleehammer/pyodbc/wiki/Module>`_ for more details on what
    kwargs you can pass to ``connect``
