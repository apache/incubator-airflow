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

.. image:: /img/helm-logo.svg
    :width: 100

Helm Chart for Apache Airflow
=============================

.. toctree::
    :hidden:

    Home <self>
    quick-start
    airflow-configuration
    manage-dags-files
    keda
    external-redis
    using-additional-containers

.. toctree::
    :hidden:
    :caption: References

    Parameters <parameters-ref>


This chart will bootstrap an `Airflow <https://airflow.apache.org>`__
deployment on a `Kubernetes <http://kubernetes.io>`__ cluster using the
`Helm <https://helm.sh>`__ package manager.

Requirements
------------

-  Kubernetes 1.14+ cluster
-  Helm 2.11+ or Helm 3.0+
-  PV provisioner support in the underlying infrastructure (optionally)

Features
--------

* Supported executors: ``LocalExecutor``, ``CeleryExecutor``, ``CeleryKubernetesExecutor``, ``KubernetesExecutor``.
* Supported Airflow version: ``1.10+``, ``2.0+``
* Supported database backend: ``PostgresSQL``, ``MySQL``
* Autoscaling for ``CeleryExecutor`` provided by KEDA
* PostgresSQL and PgBouncer with a battle-tested configuration
* Monitoring:
   * StatsD/Prometheus metrics for Airflow
   * Prometheus metrics for PgBouncer
   * Flower
* Automatic database migration after a new deployment
* Administrator account creation during deployment
* Kerberos secure configuration
* One-command deployment for any type of executor. You don't need to provide other services e.g. Redis/Database to test the Airflow.

Installing the Chart
--------------------

To install this repository from source (using helm 3)

.. code-block:: bash

    kubectl create namespace airflow
    helm dep update
    helm install airflow . --namespace airflow

The command deploys Airflow on the Kubernetes cluster in the default configuration. The :doc:`parameters-ref`
section lists the parameters that can be configured during installation.

> **Tip**: List all releases using ``helm list``.

Upgrading the Chart
-------------------

To upgrade the chart with the release name ``airflow``:

.. code-block:: bash

    helm upgrade airflow . --namespace airflow

Uninstalling the Chart
----------------------

To uninstall/delete the ``airflow`` deployment:

.. code-block:: bash

    helm delete airflow --namespace airflow

The command removes all the Kubernetes components associated with the chart and deletes the release.
