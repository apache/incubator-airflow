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



.. _howto/operator:KubernetesPodOperator:

KubernetesPodOperator - The Comprehensive Guide
===============================================

The :class:`~airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator` allows
you to create and run Pods on a Kubernetes cluster. The task wrapped in the KubernetesPodOperator is then executed in
these pods.

.. contents::
  :depth: 1
  :local:

.. note::
  If you use `Google Kubernetes Engine <https://cloud.google.com/kubernetes-engine/>`__, consider
  using the
  :ref:`GKEStartPodOperator <howto/operator:GKEStartPodOperator>` operator as it
  simplifies the Kubernetes authorization process.

How does this operator work?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator` uses the
Kubernetes API to launch a pod in a Kubernetes cluster. By supplying an
image URL and a command with optional arguments, the operator uses the Kube Python Client to generate a Kubernetes API
request that dynamically launches those individual pods.
Essentially, KubernetesPodOperator packages all the supplied parameters into a request object which is then shipped off
to Kubernetes API Server so that the pod to execute your task is created. Whenever a task is triggered, a new worker pod
is spun up to execute that task. And once the task is completed, by default the worker pod is deleted
and the resources reclaimed.
Users can specify a kubeconfig file using the ``config_file`` parameter, otherwise the operator will default
to ``~/.kube/config``.

How does the KubernetesPodOperator differ from the KubernetesExecutor
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note::
  The :doc:`Kubernetes executor <apache-airflow:executor/kubernetes>` is **not** required to use this operator.


What problems does KubernetesPodOperator solve?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* The :class:`~airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator` enables task-level
  resource configuration and is optimal for custom Python dependencies that are not available through the
  public PyPI repository.

* It allows users to supply a template YAML file using the ``pod_template_file`` parameter.

* It allows isolation of deployments, configuration reuse, delegation and better management of secrets.

* Ultimately, it allows Airflow to act a job orchestrator - no matter the language those jobs are written in.


Difference between ``KubernetesPodOperator`` and Kubernetes object spec
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The :class:`~airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator` can be considered
a substitute for a Kubernetes object spec definition that is able
to be run in the Airflow scheduler in the DAG context. If using the operator, there is no need to create the
equivalent YAML/JSON object spec for the Pod you would like to run.
The YAML file can still be provided with the ``pod_template_file`` or even the Pod Spec constructed in Python via
the ``full_pod_spec`` parameter which requires a Kubernetes ``V1Pod``.


How to use cluster ConfigMaps, Secrets, and Volumes with Pod?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To add ConfigMaps, Volumes, and other Kubernetes native objects, we recommend that you import the Kubernetes model API
like this:

.. code-block:: python

  from kubernetes.client import models as k8s

With this API object, you can have access to all Kubernetes API objects in the form of python classes.
Using this method will ensure correctness
and type safety. While we have removed almost all Kubernetes convenience classes, we have kept the
:class:`~airflow.kubernetes.secret.Secret` class to simplify the process of generating secret volumes/env variables.

.. exampleinclude:: /../../airflow/providers/cncf/kubernetes/example_dags/example_kubernetes.py
    :language: python
    :start-after: [START howto_operator_k8s_cluster_resources]
    :end-before: [END howto_operator_k8s_cluster_resources]


How to use KubernetesPodOperator with YAML file/JSON spec?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * WIP

How to define env for KubernetesPodOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * WIP

How to define limit/requests resources?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * WIP

How to use private images (container registry)?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
By default, the :class:`~airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator` will
look for images hosted publicly on Dockerhub.
To pull images from a private registry (such as ECR, GCR, Quay, or others), you must create a
Kubernetes Secret that represents the credentials for accessing images from the private registry that is ultimately
specified in the ``image_pull_secrets`` parameter.

Create the Secret using ``kubectl``:

.. code-block:: none

    kubectl create secret docker-registry testquay \
        --docker-server=quay.io \
        --docker-username=<Profile name> \
        --docker-password=<password>

Then use it in your pod like so:

.. exampleinclude:: /../../airflow/providers/cncf/kubernetes/example_dags/example_kubernetes.py
    :language: python
    :start-after: [START howto_operator_k8s_private_image]
    :end-before: [END howto_operator_k8s_private_image]

How does XCom work?
^^^^^^^^^^^^^^^^^^^
The :class:`~airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator` handles
XCom values differently than other operators. In order to pass a XCom value
from your Pod you must specify the ``do_xcom_push`` as ``True``. This will create a sidecar container that runs
alongside the Pod. The Pod must write the XCom value into this location at the ``/airflow/xcom/return.json`` path.

See the following example on how this occurs:

.. exampleinclude:: /../../airflow/providers/cncf/kubernetes/example_dags/example_kubernetes.py
    :language: python
    :start-after: [START howto_operator_k8s_write_xcom]
    :end-before: [END howto_operator_k8s_write_xcom]


How to deal with common KubernetesPodOperator issues or gotchas
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * WIP

Best practices for using KubernetesPodOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * WIP

Using KubernetesPodOperator with GKEStartPodOperator (GKE)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * WIP

Reference
^^^^^^^^^
For further information, look at:

* `Kubernetes Documentation <https://kubernetes.io/docs/home/>`__
* `Pull and Image from a Private Registry <https://kubernetes.io/docs/tasks/configure-pod-container/pull-image-private-registry/>`__
