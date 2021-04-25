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

Quick start with kind
=====================

This article will show you how to install Airflow using Helm Chart on `Kind <https://kind.sigs.k8s.io/>`__

Install kind, and create a cluster
----------------------------------

We recommend testing with Kubernetes 1.15, as this image doesn’t support
Kubernetes 1.16+ for CeleryExecutor presently.

.. code-block:: bash

   kind create cluster \
     --image kindest/node:v1.15.7@sha256:e2df133f80ef633c53c0200114fce2ed5e1f6947477dbc83261a6a921169488d

Confirm it’s up:

.. code-block:: bash

   kubectl cluster-info --context kind-kind

Create namespace and Install the chart
--------------------------------------

.. code-block:: bash

   kubectl create namespace airflow
   helm install airflow --n airflow .

It may take a few minutes. Confirm the pods are up:

.. code-block:: bash

   kubectl get pods --all-namespaces
   helm list -n airflow

Run ``kubectl port-forward svc/airflow-webserver 8080:8080 -n airflow``
to port-forward the Airflow UI to http://localhost:8080/ to confirm
Airflow is working.

Build a Docker image from your DAGs
-----------------------------------

1. Create a project

    .. code-block:: bash

        mkdir my-airflow-project && cd my-airflow-project
        mkdir dags  # put dags here
        cat <<EOM > Dockerfile
        FROM apache/airflow
        COPY . .
        EOM


2. Then build the image:

    .. code-block:: bash

        docker build -t my-dags:0.0.1 .


3. Load the image into kind:

    .. code-block:: bash

      kind load docker-image my-dags:0.0.1

4. Upgrade Helm deployment:

    .. code-block:: bash

      # from airflow chart directory
      helm upgrade airflow -n airflow \
          --set images.airflow.repository=my-dags \
          --set images.airflow.tag=0.0.1 \
          .
