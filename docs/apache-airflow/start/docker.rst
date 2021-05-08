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

Running Airflow in Docker
#########################

This quick-start guide will allow you to quickly start Airflow with :doc:`CeleryExecutor </executor/celery>` in Docker. This is the fastest way to start Airflow.

Before you begin
================

Follow these steps to install the necessary tools.

1. Install `Docker Community Edition (CE) <https://docs.docker.com/engine/installation/>`__ on your workstation. Depending on the OS, you may need to configure your Docker instance to use 4.00 GB of memory for all containers to run properly. Please refer to the Resources section if using `Docker for Windows <https://docs.docker.com/docker-for-windows/#resources>`__ or `Docker for Mac <https://docs.docker.com/docker-for-mac/#resources>`__ for more information.
2. Install `Docker Compose <https://docs.docker.com/compose/install/>`__ v1.27.0 and newer on your workstation.

Older versions of ``docker-compose`` do not support all features required by ``docker-compose.yaml`` file, so double check that it meets the minimum version requirements.

``docker-compose.yaml``
=======================

To deploy Airflow on Docker Compose, you should fetch `docker-compose.yaml <../docker-compose.yaml>`__.

.. jinja:: quick_start_ctx

    .. code-block:: bash

        curl -LfO '{{ doc_root_url }}docker-compose.yaml'

This file contains several service definitions:

- ``airflow-scheduler`` - The :doc:`scheduler </scheduler>` monitors all tasks and DAGs, then triggers the
  task instances once their dependencies are complete.
- ``airflow-webserver`` - The webserver available at ``http://localhost:8080``.
- ``airflow-worker`` - The worker that executes the tasks given by the scheduler.
- ``airflow-init`` - The initialization service.
- ``flower`` - `The flower app <https://flower.readthedocs.io/en/latest/>`__ for monitoring the environment. It is available at ``http://localhost:5555``.
- ``postgres`` - The database.
- ``redis`` - `The redis <https://redis.io/>`__ - broker that forwards messages from scheduler to worker.

All these services allow you to run Airflow with :doc:`CeleryExecutor </executor/celery>`. For more information, see :ref:`architecture`.

Some directories in the container are mounted, which means that their contents are synchronized between your computer and the container.

- ``./dags`` - you can put your DAG files here.
- ``./logs`` - contains logs from task execution and scheduler.
- ``./plugins`` - you can put your :doc:`custom plugins </plugins>` here.

This file uses the latest Airflow image (`apache/airflow <https://hub.docker.com/r/apache/airflow>`__). If you need install a new Python library or system library, you can :doc:`customize and extend it <docker-stack:index>`.

.. _initializing_docker_compose_environment:


Initializing Environment
========================

Before starting Airflow for the first time, You need to prepare your environment, i.e. create the necessary files, directories and initialize the database.

On **Linux**, the mounted volumes in container use the native Linux filesystem user/group permissions, so you have to make sure the container and host computer have matching file permissions.

.. code-block:: bash

    mkdir ./dags ./logs ./plugins
    echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env

See:ref:`Docker Compose environment variables <docker-compose-env-variables>`

On **all operating systems**, you need to run database migrations and create the first user account. To do it, run.

.. code-block:: bash

    docker-compose up airflow-init

After initialization is complete, you should see a message like below.

.. parsed-literal::

    airflow-init_1       | Upgrades done
    airflow-init_1       | Admin user airflow created
    airflow-init_1       | |version|
    start_airflow-init_1 exited with code 0

The account created has the login ``airflow`` and the password ``airflow``.

Running Airflow
===============

Now you can start all services:

.. code-block:: bash

    docker-compose up

In the second terminal you can check the condition of the containers and make sure that no containers are in unhealthy condition:

.. code-block:: text
    :substitutions:

    $ docker ps
    CONTAINER ID   IMAGE            |version-spacepad| COMMAND                  CREATED          STATUS                    PORTS                              NAMES
    247ebe6cf87a   apache/airflow:|version|   "/usr/bin/dumb-init …"   3 minutes ago    Up 3 minutes (healthy)    8080/tcp                           compose_airflow-worker_1
    ed9b09fc84b1   apache/airflow:|version|   "/usr/bin/dumb-init …"   3 minutes ago    Up 3 minutes (healthy)    8080/tcp                           compose_airflow-scheduler_1
    65ac1da2c219   apache/airflow:|version|   "/usr/bin/dumb-init …"   3 minutes ago    Up 3 minutes (healthy)    0.0.0.0:5555->5555/tcp, 8080/tcp   compose_flower_1
    7cb1fb603a98   apache/airflow:|version|   "/usr/bin/dumb-init …"   3 minutes ago    Up 3 minutes (healthy)    0.0.0.0:8080->8080/tcp             compose_airflow-webserver_1
    74f3bbe506eb   postgres:13      |version-spacepad| "docker-entrypoint.s…"   18 minutes ago   Up 17 minutes (healthy)   5432/tcp                           compose_postgres_1
    0bd6576d23cb   redis:latest     |version-spacepad| "docker-entrypoint.s…"   10 hours ago     Up 17 minutes (healthy)   0.0.0.0:6379->6379/tcp             compose_redis_1

Accessing the environment
=========================

After starting Airflow, you can interact with it in 3 ways;

* by running :doc:`CLI commands </usage-cli>`.
* via a browser using :doc:`the web interface </ui>`.
* using :doc:`the REST API </stable-rest-api-ref>`.

Running the CLI commands
------------------------

You can also run :doc:`CLI commands </usage-cli>`, but you have to do it in one of the defined ``airflow-*`` services. For example, to run ``airflow info``, run the following command:

.. code-block:: bash

    docker-compose run airflow-worker airflow info

If you have Linux or Mac OS, you can make your work easier and download a optional wrapper scripts that will allow you to run commands with a simpler command.

.. jinja:: quick_start_ctx

    .. code-block:: bash

        curl -LfO '{{ doc_root_url }}airflow.sh'
        chmod +x airflow.sh

Now you can run commands easier.

.. code-block:: bash

    ./airflow.sh info

You can also use ``bash`` as parameter to enter interactive bash shell in the container or ``python`` to enter
python container.

.. code-block:: bash

    ./airflow.sh bash

.. code-block:: bash

    ./airflow.sh python

Accessing the web interface
---------------------------

Once the cluster has started up, you can log in to the web interface and try to run some tasks.

The webserver available at: ``http://localhost:8080``.
The default account has the login ``airflow`` and the password ``airflow``.

Sending requests to the REST API
--------------------------------

`Basic username password authentication <https://tools.ietf.org/html/rfc7617
https://en.wikipedia.org/wiki/Basic_access_authentication>`_ is currently
supported for the REST API, which means you can use common tools to send requests to the API.

The webserver available at: ``http://localhost:8080``.
The default account has the login ``airflow`` and the password ``airflow``.

Here is a sample ``curl`` command, which sends a request to retrieve a pool list:

.. code-block:: bash

    ENDPOINT_URL="http://localhost:8080/"
    curl -X GET  \
        --user "airflow:airflow" \
        "${ENDPOINT_URL}/api/v1/pools"

Cleaning up
===========

To stop and delete containers, delete volumes with database data and download images, run:

.. code-block:: bash

    docker-compose down --volumes --rmi all

FAQ: Frequently asked questions
===============================

``ModuleNotFoundError: No module named 'XYZ'``
----------------------------------------------

The Docker Compose file uses the latest Airflow image (`apache/airflow <https://hub.docker.com/r/apache/airflow>`__). If you need install a new Python library or system library, you can :doc:`customize and extend it <docker-stack:index>`.

What's Next?
============

From this point, you can head to the :doc:`/tutorial` section for further examples or the :doc:`/howto/index` section if you're ready to get your hands dirty.

.. _docker-compose-env-variables:

Environment variables supported by Docker Compose
=================================================

Do not confuse the variable names here with the build arguments set when image is built. The
``AIRFLOW_UID`` and ``AIRFLOW_GID`` build args default to ``50000`` when the image is built, so they are
"baked" into the image. On the other hand, the environment variables below can be set when the container
is running, using - for example - result of ``id -u`` command, which allows to use the dynamic host
runtime user id which is unknown at the time of building the image.

+--------------------------------+-----------------------------------------------------+--------------------------+
|   Variable                     | Description                                         | Default                  |
+================================+=====================================================+==========================+
| ``AIRFLOW_IMAGE_NAME``         | Airflow Image to use.                               | apache/airflow:|version| |
+--------------------------------+-----------------------------------------------------+--------------------------+
| ``AIRFLOW_UID``                | UID of the user to run Airflow containers as.       | ``50000``                |
|                                | Override if you want to use use non-default Airflow |                          |
|                                | UID (for example when you map folders from host,    |                          |
|                                | it should be set to result of ``id -u`` call. If    |                          |
|                                | you change it from default 50000, you must set      |                          |
|                                | ``AIRFLOW_GID`` to ``0``. When it is changed,       |                          |
|                                | a 2nd user with the UID specified is dynamically    |                          |
|                                | created with ``default`` name inside the container  |                          |
|                                | and home of the use is set to ``/airflow/home/``    |                          |
|                                | in order to share Python libraries installed there. |                          |
|                                | This is in order to achieve the  OpenShift          |                          |
|                                | compatibility. See more in the                      |                          |
|                                | :ref:`Arbitrary Docker User <arbitrary-docker-user>`|                          |
+--------------------------------+-----------------------------------------------------+--------------------------+
| ``AIRFLOW_GID``                | Group ID in Airflow containers. It overrides the    | ``50000``                |
|                                | GID of the user. It is ``50000`` by default but if  |                          |
|                                | you want to use different UID than default it must  |                          |
|                                | be set to ``0``.                                    |                          |
+--------------------------------+-----------------------------------------------------+--------------------------+
| ``_AIRFLOW_WWW_USER_USERNAME`` | Username for the administrator UI account.          |                          |
|                                | If this value is specified, admin UI user gets      |                          |
|                                | created automatically. This is only useful when     |                          |
|                                | you want to run Airflow for a test-drive and        |                          |
|                                | want to start a container with embedded development |                          |
|                                | database.                                           |                          |
+--------------------------------+-----------------------------------------------------+--------------------------+
| ``_AIRFLOW_WWW_USER_PASSWORD`` | Password for the administrator UI account.          |                          |
|                                | Only used when ``_AIRFLOW_WWW_USER_USERNAME`` set.  |                          |
+--------------------------------+-----------------------------------------------------+--------------------------+
