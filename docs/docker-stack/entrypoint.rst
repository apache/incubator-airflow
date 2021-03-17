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

Entrypoint
==========

If you are using the default entrypoint of the production image,
there are a few actions that are automatically performed when the container starts.
In some cases, you can pass environment variables to the image to trigger some of that behaviour.

The variables that control the "execution" behaviour start with ``_AIRFLOW`` to distinguish them
from the variables used to build the image starting with ``AIRFLOW``.

Creating system user
--------------------

Airflow image is Open-Shift compatible, which means that you can start it with random user ID and group id 0.
Airflow will automatically create such a user and make it's home directory point to ``/home/airflow``.
You can read more about it in the "Support arbitrary user ids" chapter in the
`Openshift best practices <https://docs.openshift.com/container-platform/4.1/openshift_images/create-images.html#images-create-guide-openshift_create-images>`_.

Waits for Airflow DB connection
-------------------------------

In case Postgres or MySQL DB is used, the entrypoint will wait until the airflow DB connection becomes
available. This happens always when you use the default entrypoint.

The script detects backend type depending on the URL schema and assigns default port numbers if not specified
in the URL. Then it loops until the connection to the host/port specified can be established
It tries :envvar:`CONNECTION_CHECK_MAX_COUNT` times and sleeps :envvar:`CONNECTION_CHECK_SLEEP_TIME` between checks
To disable check, set ``CONNECTION_CHECK_MAX_COUNT=0``.

Supported schemes:

* ``postgres://`` - default port 5432
* ``mysql://``    - default port 3306
* ``sqlite://``

In case of SQLite backend, there is no connection to establish and waiting is skipped.

For older than Airflow 1.10.14, waiting for connection involves checking if a matching port is open.
The host information is derived from the variables :envvar:`AIRFLOW__CORE__SQL_ALCHEMY_CONN` and
:envvar:`AIRFLOW__CORE__SQL_ALCHEMY_CONN_CMD`. If :envvar:`AIRFLOW__CORE__SQL_ALCHEMY_CONN_CMD` variable
is passed to the container, it is evaluated as a command to execute and result of this evaluation is used
as :envvar:`AIRFLOW__CORE__SQL_ALCHEMY_CONN`. The :envvar:`AIRFLOW__CORE__SQL_ALCHEMY_CONN_CMD` variable
takes precedence over the :envvar:`AIRFLOW__CORE__SQL_ALCHEMY_CONN` variable.

For newer versions, the ``airflow db check`` command is used, which means that a ``select 1 as is_alive;`` query
is executed. This also means that you can keep your password in secret backend.

Upgrading Airflow DB
--------------------

If you set :envvar:`_AIRFLOW_DB_UPGRADE` variable to a non-empty value, the entrypoint will run
the ``airflow db upgrade`` command right after verifying the connection. You can also use this
when you are running airflow with internal SQLite database (default) to upgrade the db and create
admin users at entrypoint, so that you can start the webserver immediately. Note - using SQLite is
intended only for testing purpose, never use SQLite in production as it has severe limitations when it
comes to concurrency.

Creating admin user
-------------------

The entrypoint can also create webserver user automatically when you enter it. you need to set
:envvar:`_AIRFLOW_WWW_USER_CREATE` to a non-empty value in order to do that. This is not intended for
production, it is only useful if you would like to run a quick test with the production image.
You need to pass at least password to create such user via ``_AIRFLOW_WWW_USER_PASSWORD`` or
:envvar:`_AIRFLOW_WWW_USER_PASSWORD_CMD` similarly like for other ``*_CMD`` variables, the content of
the ``*_CMD`` will be evaluated as shell command and it's output will be set as password.

User creation will fail if none of the ``PASSWORD`` variables are set - there is no default for
password for security reasons.

+-----------+--------------------------+----------------------------------------------------------------------+
| Parameter | Default                  | Environment variable                                                 |
+===========+==========================+======================================================================+
| username  | admin                    | ``_AIRFLOW_WWW_USER_USERNAME``                                       |
+-----------+--------------------------+----------------------------------------------------------------------+
| password  |                          | ``_AIRFLOW_WWW_USER_PASSWORD_CMD`` or ``_AIRFLOW_WWW_USER_PASSWORD`` |
+-----------+--------------------------+----------------------------------------------------------------------+
| firstname | Airflow                  | ``_AIRFLOW_WWW_USER_FIRSTNAME``                                      |
+-----------+--------------------------+----------------------------------------------------------------------+
| lastname  | Admin                    | ``_AIRFLOW_WWW_USER_LASTNAME``                                       |
+-----------+--------------------------+----------------------------------------------------------------------+
| email     | airflowadmin@example.com | ``_AIRFLOW_WWW_USER_EMAIL``                                          |
+-----------+--------------------------+----------------------------------------------------------------------+
| role      | Admin                    | ``_AIRFLOW_WWW_USER_ROLE``                                           |
+-----------+--------------------------+----------------------------------------------------------------------+

In case the password is specified, the user will be attempted to be created, but the entrypoint will
not fail if the attempt fails (this accounts for the case that the user is already created).

You can, for example start the webserver in the production image with initializing the internal SQLite
database and creating an ``admin/admin`` Admin user with the following command:

.. code-block:: bash

  docker run -it -p 8080:8080 \
    --env "_AIRFLOW_DB_UPGRADE=true" \
    --env "_AIRFLOW_WWW_USER_CREATE=true" \
    --env "_AIRFLOW_WWW_USER_PASSWORD=admin" \
      apache/airflow:2.0.1-python3.8 webserver


.. code-block:: bash

  docker run -it -p 8080:8080 \
    --env "_AIRFLOW_DB_UPGRADE=true" \
    --env "_AIRFLOW_WWW_USER_CREATE=true" \
    --env "_AIRFLOW_WWW_USER_PASSWORD_CMD=echo admin" \
      apache/airflow:2.0.1-python3.8 webserver

The commands above perform initialization of the SQLite database, create admin user with admin password
and Admin role. They also forward local port ``8080`` to the webserver port and finally start the webserver.

Waits for celery broker connection
----------------------------------

In case Postgres or MySQL DB is used, and one of the ``scheduler``, ``celery``, ``worker``, or ``flower``
commands are used the entrypoint will wait until the celery broker DB connection is available.

The script detects backend type depending on the URL schema and assigns default port numbers if not specified
in the URL. Then it loops until connection to the host/port specified can be established
It tries :envvar:`CONNECTION_CHECK_MAX_COUNT` times and sleeps :envvar:`CONNECTION_CHECK_SLEEP_TIME` between checks.
To disable check, set ``CONNECTION_CHECK_MAX_COUNT=0``.

Supported schemes:

* ``amqp(s)://``  (rabbitmq) - default port 5672
* ``redis://``               - default port 6379
* ``postgres://``            - default port 5432
* ``mysql://``               - default port 3306

Waiting for connection involves checking if a matching port is open.
The host information is derived from the variables :envvar:`AIRFLOW__CELERY__BROKER_URL` and
:envvar:`AIRFLOW__CELERY__BROKER_URL_CMD`. If :envvar:`AIRFLOW__CELERY__BROKER_URL_CMD` variable
is passed to the container, it is evaluated as a command to execute and result of this evaluation is used
as :envvar:`AIRFLOW__CELERY__BROKER_URL`. The :envvar:`AIRFLOW__CELERY__BROKER_URL_CMD` variable
takes precedence over the :envvar:`AIRFLOW__CELERY__BROKER_URL` variable.

.. _entrypoint:commands:

Executing commands
------------------

If first argument equals to "bash" - you are dropped to a bash shell or you can executes bash command
if you specify extra arguments. For example:

.. code-block:: bash

  docker run -it apache/airflow:2.0.1-python3.6 bash -c "ls -la"
  total 16
  drwxr-xr-x 4 airflow root 4096 Jun  5 18:12 .
  drwxr-xr-x 1 root    root 4096 Jun  5 18:12 ..
  drwxr-xr-x 2 airflow root 4096 Jun  5 18:12 dags
  drwxr-xr-x 2 airflow root 4096 Jun  5 18:12 logs

If first argument is equal to ``python`` - you are dropped in python shell or python commands are executed if
you pass extra parameters. For example:

.. code-block:: bash

  > docker run -it apache/airflow:2.0.1-python3.6 python -c "print('test')"
  test

If first argument equals to "airflow" - the rest of the arguments is treated as an airflow command
to execute. Example:

.. code-block:: bash

   docker run -it apache/airflow:2.0.1-python3.6 airflow webserver

If there are any other arguments - they are simply passed to the "airflow" command

.. code-block:: bash

  > docker run -it apache/airflow:2.0.1-python3.6 version
  2.1.0
