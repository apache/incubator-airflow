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



Google Kubernetes Engine Operators
==================================

`Google Kubernetes Engine (GKE) <https://cloud.google.com/kubernetes-engine/>`__ provides a managed environment for
deploying, managing, and scaling your containerized applications using Google infrastructure. The GKE environment
consists of multiple machines (specifically, Compute Engine instances) grouped together to form a cluster.

.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include::/operators/_partials/prerequisite_tasks.rst

Manage GKE cluster
^^^^^^^^^^^^^^^^^^

A cluster is the foundation of GKE - all workloads run on top of the cluster. It is made up on a cluster master
and worker nodes. The lifecycle of the master is managed by GKE when creating or deleting a cluster.
The worker nodes are represented as Compute Engine VM instances that GKE creates on your behalf when creating a cluster.

.. _howto/operator:GKECreateClusterOperator:

Create GKE cluster
""""""""""""""""""

Here is an example of a cluster definition:

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_kubernetes_engine.py
    :language: python
    :start-after: [START howto_operator_gcp_gke_create_cluster_definition]
    :end-before: [END howto_operator_gcp_gke_create_cluster_definition]

A dict object like this, or a
:class:`~google.cloud.container_v1.types.Cluster`
definition, is required when creating a cluster with
:class:`~airflow.providers.google.cloud.operators.kubernetes_engine.GKECreateClusterOperator`.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_kubernetes_engine.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gke_create_cluster]
    :end-before: [END howto_operator_gke_create_cluster]

.. _howto/operator:GKEDeleteClusterOperator:

Delete GKE cluster
""""""""""""""""""

To delete a cluster, use
:class:`~airflow.providers.google.cloud.operators.kubernetes_engine.GKEDeleteClusterOperator`.
This would also delete all the nodes allocated to the cluster.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_kubernetes_engine.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gke_delete_cluster]
    :end-before: [END howto_operator_gke_delete_cluster]

Manage workloads on a GKE cluster
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

GKE works with containerized applications, such as those created on Docker, and deploys them to run on the cluster.
These are called workloads, and when deployed on the cluster they leverage the CPU and memory resources of the cluster
to run effectively.

.. _howto/operator:GKEStartPodOperator:

Run a Pod on a GKE cluster
""""""""""""""""""""""""""

There are two operators available in order to run a pod on a GKE cluster:

* :class:`~airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator`
* :class:`~airflow.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperator`

``GKEStartPodOperator`` extends ``KubernetesPodOperator`` to provide authorization using Google Cloud credentials.
There is no need to manage the ``kube_config`` file, as it will be generated automatically.
All Kubernetes parameters (except ``config_file``) are also valid for the ``GKEStartPodOperator``.
For more information on ``KubernetesPodOperator``, please look at: :ref:`howto/operator:KubernetesPodOperator` guide.

Using with Private cluster
'''''''''''''''''''''''''''

All clusters have a canonical endpoint. The endpoint is the IP address of the Kubernetes API server that
Airflow use to communicate with your cluster master. The endpoint is displayed in Cloud Console under the **Endpoints** field of the cluster's Details tab, and in the
output of ``gcloud container clusters describe`` in the endpoint field.

Private clusters have two unique endpoint values: ``privateEndpoint``, which is an internal IP address, and
``publicEndpoint``, which is an external one. Running ``GKEStartPodOperator`` against a private cluster
sets the external IP address as the endpoint by default. If you prefer to use the internal IP as the
endpoint, you need to set ``use_private`` parameter to ``True``.

Use of XCom
'''''''''''

We can enable the usage of :ref:`XCom <concepts:xcom>` on the operator. This works by launching a sidecar container
with the pod specified. The sidecar is automatically mounted when the XCom usage is specified and it's mount point
is the path ``/airflow/xcom``. To provide values to the XCom, ensure your Pod writes it into a file called
``return.json`` in the sidecar. The contents of this can then be used downstream in your DAG.
Here is an example of it being used:

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_kubernetes_engine.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gke_start_pod_xcom]
    :end-before: [END howto_operator_gke_start_pod_xcom]

And then use it in other operators:

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_kubernetes_engine.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gke_xcom_result]
    :end-before: [END howto_operator_gke_xcom_result]

Reference
^^^^^^^^^

For further information, look at:

* `GKE API Documentation <https://cloud.google.com/kubernetes-engine/docs/reference/rest>`__
* `Product Documentation <https://cloud.google.com/kubernetes-engine/docs/>`__
* `Kubernetes Documentation <https://kubernetes.io/docs/home/>`__
* `Configuring GKE cluster access for kubectl <https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl>`__
