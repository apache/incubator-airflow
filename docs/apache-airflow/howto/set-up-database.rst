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



Set up a Database Backend
=========================

Airflow was built to interact with its metadata using `SqlAlchemy <https://docs.sqlalchemy.org/en/13/>`__.

The document below describes the database engine configurations, the necessary changes to their configuration to be used with Airflow, as well as changes to the Airflow configurations to connect to these databases.

Choosing database backend
-------------------------

If you want to take a real test drive of Airflow, you should consider setting up a database backend to **MySQL** and **PostgresSQL**.
By default, Airflow uses **SQLite**, which is intended for development purposes only.

Airflow supports the following database engine versions, so make sure which version you have. Old versions may not support all SQL statements.

  * PostgreSQL:  9.6, 10, 11, 12, 13
  * MySQL: 5.7, 8
  * SQLite: 3.15.0+

If you plan on running more than one scheduler, you have to meet additional requirements.
For details, see :ref:`Scheduler HA Database Requirements <scheduler:ha:db_requirements>`.

Database URI
------------

Airflow uses SQLAlchemy to connect to the database, which requires you to configure the Database URL.
You can do this in option ``sql_alchemy_conn`` in section ``[core]``. It is also common to configure
this option with ``AIRFLOW__CORE__SQL_ALCHEMY_CONN`` environment variable.

.. note::
    For more information on setting the configuration, see :doc:`/howto/set-config`.

If you want to check the current value, you can use ``airflow config get-value core sql_alchemy_conn`` command as in
the example below.

.. code-block:: bash

    $ airflow config get-value core sql_alchemy_conn
    sqlite:////tmp/airflow/airflow.db

The exact format description is described in the SQLAlchemy documentation, see `Database Urls <https://docs.sqlalchemy.org/en/14/core/engines.html>`__. We will also show you some examples below.

Setting up a SQLite Database
----------------------------

SQLite database can be used to run Airflow for development purpose as it does not require any database server
(the database is stored in a local file). There are a few limitations of using the SQLite database (for example
it only works with Sequential Executor) and it should NEVER be used for production.

There is a minimum version of sqlite3 required to run Airflow 2.0+ - minimum version is 3.15.0. Some of the
older systems have an earlier version of sqlite installed by default and for those system you need to manually
upgrade SQLite to use version newer than 3.15.0. Note, that this is not a ``python library`` version, it's the
SQLite system-level application that needs to be upgraded. There are different ways how SQLIte might be
installed, you can find some information about that at the `official website of SQLite
<https://www.sqlite.org/index.html>`_ and in the documentation specific to distribution of your Operating
System.

**Troubleshooting**

Sometimes even if you upgrade SQLite to higher version and your local python reports higher version,
the python interpreter used by Airflow might still use the older version available in the
``LD_LIBRARY_PATH`` set for the python interpreter that is used to start Airflow.

You can make sure which version is used by the interpreter by running this check:

.. code-block:: bash

    root@b8a8e73caa2c:/opt/airflow# python
    Python 3.6.12 (default, Nov 25 2020, 03:59:00)
    [GCC 8.3.0] on linux
    Type "help", "copyright", "credits" or "license" for more information.
    >>> import sqlite3
    >>> sqlite3.sqlite_version
    '3.27.2'
    >>>

But be aware that setting environment variables for your Airflow deployment might change which SQLite
library is found first, so you might want to make sure that the "high-enough" version of SQLite is the only
version installed in your system.

An example URI for the sqlite database:

.. code-block:: text

    sqlite:////home/airflow/airflow.db

**Upgrading SQLite on AmazonLinux AMI or Container Image**

AmazonLinux SQLite can only be upgraded to v3.7 using the source repos. Airflow requires v3.15 or higher. Use the
following instructions to setup the base image (or AMI) with latest SQLite3

Pre-requisite: You will need ``wget``, ``tar``, ``gzip``,`` gcc``, ``make``, and ``expect`` to get the upgrade process working.

.. code-block:: bash

  yum -y install wget tar gzip gcc make expect

Download source from https://sqlite.org/, make and install locally.

.. code-block:: bash

    wget https://www.sqlite.org/src/tarball/sqlite.tar.gz
    tar xzf sqlite.tar.gz
    cd sqlite/
    export CFLAGS="-DSQLITE_ENABLE_FTS3 \
        -DSQLITE_ENABLE_FTS3_PARENTHESIS \
        -DSQLITE_ENABLE_FTS4 \
        -DSQLITE_ENABLE_FTS5 \
        -DSQLITE_ENABLE_JSON1 \
        -DSQLITE_ENABLE_LOAD_EXTENSION \
        -DSQLITE_ENABLE_RTREE \
        -DSQLITE_ENABLE_STAT4 \
        -DSQLITE_ENABLE_UPDATE_DELETE_LIMIT \
        -DSQLITE_SOUNDEX \
        -DSQLITE_TEMP_STORE=3 \
        -DSQLITE_USE_URI \
        -O2 \
        -fPIC"
    export PREFIX="/usr/local"
    LIBS="-lm" ./configure --disable-tcl --enable-shared --enable-tempstore=always --prefix="$PREFIX"
    make
    make install

Post install add ``/usr/local/lib`` to library path

.. code-block:: bash

  export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH

Setting up a MySQL Database
---------------------------

You need to create a database and a database user that Airflow will use to access this database.
In the example below, a database ``airflow_db`` and user  with username ``airflow_user`` with password ``airflow_pass`` will be created

.. code-block:: sql

   CREATE DATABASE airflow_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
   CREATE USER 'airflow_user' IDENTIFIED BY 'airflow_pass';
   GRANT ALL PRIVILEGES ON airflow_db.* TO 'airflow_user';


.. note::

   The database must use a UTF-8 character set

We rely on more strict ANSI SQL settings for MySQL in order to have sane defaults.
Make sure to have specified ``explicit_defaults_for_timestamp=1`` option under ``[mysqld]`` section
in your ``my.cnf`` file. You can also activate these options with the ``--explicit-defaults-for-timestamp`` switch passed to ``mysqld`` executable

We recommend using the ``mysqlclient`` driver and specifying it in your SqlAlchemy connection string.

.. code-block:: text

    mysql+mysqldb://<user>:<password>@<host>[:<port>]/<dbname>

But we also support the ``mysql-connector-python`` driver, which lets you connect through SSL
without any cert options provided.

.. code-block:: text

   mysql+mysqlconnector://<user>:<password>@<host>[:<port>]/<dbname>

However if you want to use other drivers visit the `MySQL Dialect <https://docs.sqlalchemy.org/en/13/dialects/mysql.html>`__  in SQLAlchemy documentation for more information regarding download
and setup of the SqlAlchemy connection.

In addition, you also should pay particular attention to MySQL's encoding. Although the ``utf8mb4`` character set is more and more popular for MySQL (actually, ``utf8mb4`` becomes default character set in MySQL8.0), using the ``utf8mb4`` encoding requires additional setting in Airflow 2+ (See more details in `#7570 <https://github.com/apache/airflow/pull/7570>`__.). If you use ``utf8mb4`` as character set, you should also set ``sql_engine_collation_for_ids=utf8mb3_general_ci``.

Setting up a PostgreSQL Database
--------------------------------

You need to create a database and a database user that Airflow will use to access this database.
In the example below, a database ``airflow_db`` and user  with username ``airflow_user`` with password ``airflow_pass`` will be created

.. code-block:: sql

   CREATE DATABASE airflow_db;
   CREATE USER airflow_user WITH PASSWORD 'airflow_pass';
   GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;

.. note::

   The database must use a UTF-8 character set

You may need to update your Postgres ``pg_hba.conf`` to add the
``airflow`` user to the database access control list; and to reload
the database configuration to load your change. See
`The pg_hba.conf File <https://www.postgresql.org/docs/current/auth-pg-hba-conf.html>`__
in the Postgres documentation to learn more.

We recommend using the ``psycopg2`` driver and specifying it in your SqlAlchemy connection string.

.. code-block:: text

   postgresql+psycopg2://<user>:<password>@<host>/<db>

Also note that since SqlAlchemy does not expose a way to target a specific schema in the database URI, you may
want to set a default schema for your role with a SQL statement similar to ``ALTER ROLE username SET search_path = airflow, foobar;``

For more information regarding setup of the PostgresSQL connection, see `PostgreSQL dialect <https://docs.sqlalchemy.org/en/13/dialects/postgresql.html>`__ in SQLAlchemy documentation.

.. spelling::

     hba

Other configuration options
---------------------------

There are more configuration options for configuring SQLAlchemy behavior. For details, see :ref:`reference documentation <config:core>` for ``sqlalchemy_*`` option in ``[core]`` section.

Initialize the database
-----------------------

After configuring the database and connecting to it in Airflow configuration, you should create the database schema.

.. code-block:: bash

    airflow db init

What's next?
------------

By default, Airflow uses ``SequentialExecutor``, which does not provide parallelism. You should consider
configuring a different :doc:`executor </executor/index>` for better performance.
