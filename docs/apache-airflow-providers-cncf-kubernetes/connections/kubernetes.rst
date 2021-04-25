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

.. _howto/connection:kubernetes:

Kubernetes cluster Connection
=============================

The Kubernetes cluster Connection type enables connection to a Kubernetes cluster by :class:`~airflow.providers.cncf.kubernetes.operators.spark_kubernetes.SparkKubernetesOperator` tasks. They are not used by ``KubernetesPodOperator`` tasks.


Authenticating to Kubernetes cluster
------------------------------------

There are three ways to connect to Kubernetes using Airflow.

1. Use kube_config that reside in the default location on the machine(~/.kube/config) - just leave all fields empty
2. Use in_cluster config, if Airflow runs inside Kubernetes cluster take the configuration from the cluster - mark:
    In cluster configuration
3. Use kube_config from different location - insert the path into ``Kube config path``
4. Use kube_config in JSON format from connection configuration - paste  kube_config into
    ``Kube config (JSON format)``


Default Connection IDs
----------------------

The default connection ID is ``kubernetes_default`` .

Configuring the Connection
--------------------------


In cluster configuration
  Use in cluster configuration.

Kube config path
  Use custom path to kube config.

Kube config (JSON format)
  `Kube config <https://kubernetes.io/docs/tasks/access-application-cluster/configure-access-multiple-clusters/>`_
  that used to connect to Kubernetes client.

Namespace
  Default kubernetes namespace for the connection.
