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

Production Deployment
^^^^^^^^^^^^^^^^^^^^^

It is time to deploy your DAG in production. To do this, first, you need to make sure that the Airflow
is itself production-ready. Let's see what precautions you need to take.

Database backend
================

Airflow comes with an ``SQLite`` backend by default. This allows the user to run Airflow without any external
database. However, such a setup is meant to be used for testing purposes only; running the default setup
in production can lead to data loss in multiple scenarios. If you want to run production-grade Airflow,
make sure you :doc:`configure the backend <howto/set-up-database>` to be an external database
such as PostgreSQL or MySQL.

You can change the backend using the following config

.. code-block:: ini

    [core]
    sql_alchemy_conn = my_conn_string

Once you have changed the backend, airflow needs to create all the tables required for operation.
Create an empty DB and give airflow's user the permission to ``CREATE/ALTER`` it.
Once that is done, you can run -

.. code-block:: bash

    airflow db upgrade

``upgrade`` keeps track of migrations already applied, so it's safe to run as often as you need.

.. note::

    Do not use ``airflow db init`` as it can create a lot of default connections, charts, etc. which are not
    required in production DB.


Multi-Node Cluster
==================

Airflow uses :class:`~airflow.executors.sequential_executor.SequentialExecutor` by default. However, by its
nature, the user is limited to executing at most one task at a time. ``Sequential Executor`` also pauses
the scheduler when it runs a task, hence it is not recommended in a production setup. You should use the
:class:`~airflow.executors.local_executor.LocalExecutor` for a single machine.
For a multi-node setup, you should use the :doc:`Kubernetes executor <../executor/kubernetes>` or
the :doc:`Celery executor <../executor/celery>`.


Once you have configured the executor, it is necessary to make sure that every node in the cluster contains
the same configuration and dags. Airflow sends simple instructions such as "execute task X of dag Y", but
does not send any dag files or configuration. You can use a simple cronjob or any other mechanism to sync
DAGs and configs across your nodes, e.g., checkout DAGs from git repo every 5 minutes on all nodes.


Logging
=======

If you are using disposable nodes in your cluster, configure the log storage to be a distributed file system
(DFS) such as ``S3`` and ``GCS``, or external services such as Stackdriver Logging, Elasticsearch or
Amazon CloudWatch. This way, the logs are available even after the node goes down or gets replaced.
See :doc:`logging-monitoring/logging-tasks` for configurations.

.. note::

    The logs only appear in your DFS after the task has finished. You can view the logs while the task is
    running in UI itself.


Configuration
=============

Airflow comes bundled with a default ``airflow.cfg`` configuration file.
You should use environment variables for configurations that change across deployments
e.g. metadata DB, password, etc. You can accomplish this using the format :envvar:`AIRFLOW__{SECTION}__{KEY}`

.. code-block:: bash

 AIRFLOW__CORE__SQL_ALCHEMY_CONN=my_conn_id
 AIRFLOW__WEBSERVER__BASE_URL=http://host:port

Some configurations such as the Airflow Backend connection URI can be derived from bash commands as well:

.. code-block:: ini

 sql_alchemy_conn_cmd = bash_command_to_run


Scheduler Uptime
================

Airflow users occasionally report instances of the scheduler hanging without a trace, for example in these issues:

* `Scheduler gets stuck without a trace <https://github.com/apache/airflow/issues/7935>`_
* `Scheduler stopping frequently <https://github.com/apache/airflow/issues/13243>`_

To mitigate these issues, make sure you have a :doc:`health check </logging-monitoring/check-health>` set up that will detect when your scheduler has not heartbeat in a while.

.. _docker_image:

Production Container Images
===========================

We provide :doc:`a Docker Image (OCI) for Apache Airflow <docker-stack:index>` for use in a containerized environment. Consider using it to guarantee that software will always run the same no matter where it’s deployed.

.. _production-deployment:kerberos:

Kerberos-authenticated workers
==============================

Apache Airflow has a built-in mechanism for authenticating the operation with a KDC (Key Distribution Center).
Airflow has a separate command ``airflow kerberos`` that acts as token refresher. It uses the pre-configured
Kerberos Keytab to authenticate in the KDC to obtain a valid token, and then refreshing valid token
at regular intervals within the current token expiry window.

Each request for refresh uses a configured principal, and only keytab valid for the principal specified
is capable of retrieving the authentication token.

The best practice to implement proper security mechanism in this case is to make sure that worker
workloads have no access to the Keytab but only have access to the periodically refreshed, temporary
authentication tokens. This can be achieved in docker environment by running the ``airflow kerberos``
command and the worker command in separate containers - where only the ``airflow kerberos`` token has
access to the Keytab file (preferably configured as secret resource). Those two containers should share
a volume where the temporary token should be written by the ``airflow kerberos`` and read by the workers.

In the Kubernetes environment, this can be realized by the concept of side-car, where both Kerberos
token refresher and worker are part of the same Pod. Only the Kerberos side-car has access to
Keytab secret and both containers in the same Pod share the volume, where temporary token is written by
the side-care container and read by the worker container.

This concept is implemented in the development version of the Helm Chart that is part of Airflow source code.


.. spelling::

   pypirc
   dockerignore


Secured Server and Service Access on Google Cloud
=================================================

This section describes techniques and solutions for securely accessing servers and services when your Airflow
environment is deployed on Google Cloud, or you connect to Google services, or you are connecting
to the Google API.

IAM and Service Accounts
------------------------

You should not rely on internal network segmentation or firewalling as our primary security mechanisms.
To protect your organization's data, every request you make should contain sender identity. In the case of
Google Cloud, the identity is provided by
`the IAM and Service account <https://cloud.google.com/iam/docs/service-accounts>`__. Each Compute Engine
instance has an associated service account identity. It provides cryptographic credentials that your workload
can use to prove its identity when making calls to Google APIs or third-party services. Each instance has
access only to short-lived credentials. If you use Google-managed service account keys, then the private
key is always held in escrow and is never directly accessible.

If you are using Kubernetes Engine, you can use
`Workload Identity <https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity>`__ to assign
an identity to individual pods.

For more information about service accounts in the Airflow, see :ref:`howto/connection:gcp`

Impersonate Service Accounts
----------------------------

If you need access to other service accounts, you can
:ref:`impersonate other service accounts <howto/connection:gcp:impersonation>` to exchange the token with
the default identity to another service account. Thus, the account keys are still managed by Google
and cannot be read by your workload.

It is not recommended to generate service account keys and store them in the metadata database or the
secrets backend. Even with the use of the backend secret, the service account key is available for
your workload.

Access to Compute Engine Instance
---------------------------------

If you want to establish an SSH connection to the Compute Engine instance, you must have the network address
of this instance and credentials to access it. To simplify this task, you can use
:class:`~airflow.providers.google.cloud.hooks.compute.ComputeEngineHook`
instead of :class:`~airflow.providers.ssh.hooks.ssh.SSHHook`

The :class:`~airflow.providers.google.cloud.hooks.compute.ComputeEngineHook` support authorization with
Google OS Login service. It is an extremely robust way to manage Linux access properly as it stores
short-lived ssh keys in the metadata service, offers PAM modules for access and sudo privilege checking
and offers the ``nsswitch`` user lookup into the metadata service as well.

It also solves the discovery problem that arises as your infrastructure grows. You can use the
instance name instead of the network address.

Access to Amazon Web Service
----------------------------

Thanks to the
`Web Identity Federation <https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_providers_oidc.html>`__,
you can exchange the Google Cloud Platform identity to the Amazon Web Service identity,
which effectively means access to Amazon Web Service platform.
For more information, see: :ref:`howto/connection:aws:gcp-federation`

.. spelling::

    nsswitch
    cryptographic
    firewalling
    ComputeEngineHook
