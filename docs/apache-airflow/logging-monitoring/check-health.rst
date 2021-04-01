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



Checking Airflow Health Status
==============================

Airflow has two methods to check the health of components - HTTP checks and CLI checks. Their choice depends on the role of the component as well as what tools it uses to monitor the deployment.

For example, when running on Kubernetes, use `a Liveness probes <https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/>`__ (``livenessProbe`` property) with :ref:`CLI checks <check-health/cli-checks-for-scheduler>` on the scheduler deployment to restart it when it fail. For the webserver, you can configure the readiness probe (``readinessProbe`` property) using :ref:`check-health/http-endpoint`.

For an example for a Docker Compose environment, see the ``docker-compose.yaml`` file available in the :doc:`/start/docker`.

.. _check-health/http-endpoint:

Health Check Endpoint
---------------------

To check the health status of your Airflow instance, you can simply access the endpoint
``/health``. It will return a JSON object in which a high-level glance is provided.

.. code-block:: JSON

  {
    "metadatabase":{
      "status":"healthy"
    },
    "scheduler":{
      "status":"healthy",
      "latest_scheduler_heartbeat":"2018-12-26 17:15:11+00:00"
    }
  }

* The ``status`` of each component can be either "healthy" or "unhealthy"

  * The status of ``metadatabase`` depends on whether a valid connection can be initiated with the database

  * The status of ``scheduler`` depends on when the latest scheduler heartbeat was received

    * If the last heartbeat was received more than 30 seconds (default value) earlier than the current time, the scheduler is
      considered unhealthy
    * This threshold value can be specified using the option ``scheduler_health_check_threshold`` within the
      ``[scheduler]`` section in ``airflow.cfg``
    * If you run more than one scheduler, only the state of one scheduler will be reported, i.e. only one working scheduler is enough
      for the scheduler state to be considered healthy

Please keep in mind that the HTTP response code of ``/health`` endpoint **should not** be used to determine the health
status of the application. The return code is only indicative of the state of the rest call (200 for success).

.. note::

  For this check to work, at least one working web server is required. Suppose you use this check for scheduler
  monitoring, then in case of failure of the web server, you will lose the ability to monitor scheduler, which means
  that it can be restarted even if it is in good condition. For greater confidence, consider using :ref:`CLI Check for Scheduler <check-health/cli-checks-for-scheduler>`.

.. _check-health/cli-checks-for-scheduler:

CLI Check for Scheduler
-----------------------

Scheduler creates an entry in the table :class:`airflow.jobs.base_job.BaseJob` with information about the host and
timestamp (heartbeat) at startup, and then updates it regularly. You can use this to check if the scheduler is
working correctly. To do this, you can use the ``airflow jobs checks`` command. On failure, the command will exit
with a non-zero error code.

To check if the local scheduler is still working properly, run:

.. code-block:: bash

    airflow jobs check --job-type SchedulerJob --hostname "$(hostname)"

To check if any scheduler is running when you are using high availability, run:

.. code-block:: bash

    airflow jobs check --job-type SchedulerJob --allow-multiple --limit 100

CLI Check for Database
----------------------

To verify that the database is working correctly, you can use the ``airflow db check`` command. On failure, the command will exit
with a non-zero error code.

HTTP monitoring for Celery Cluster
----------------------------------

You can use Flower to monitor the health of the Celery cluster. It also provides an HTTP API that you can use to build a health check for your environment.

For details about installation, see: :ref:`executor:CeleryExecutor`. For details about usage, see: `The Flower project documentation <https://flower.readthedocs.io/en/stable/>`__.

CLI Check for Celery Workers
----------------------------

To verify that the database is working correctly, you can use the ``celery inspect ping`` command. On failure, the command will exit
with a non-zero error code.

To check if the worker running on the local host is working correctly, run:

.. code-block:: bash

    celery --app airflow.executors.celery_executor.app inspect ping -d celery@${HOSTNAME}

To check if the all workers in the cluster running is working correctly, run:

.. code-block:: bash

    celery --app airflow.executors.celery_executor.app inspect ping

For more information, see: `Management Command-line Utilities (inspect/control) <https://docs.celeryproject.org/en/stable/userguide/monitoring.html#monitoring-control>`__ and `Workers Guide <https://docs.celeryproject.org/en/stable/userguide/workers.html>`__ in the Celery documentation.
