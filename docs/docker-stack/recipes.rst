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

Recipes
=======

Users sometimes share interesting ways of using the Docker images. We encourage users to contribute these
recipes to the documentation in case they prove useful to other members of the community by
submitting a pull request. The sections below capture this knowledge.

Google Cloud SDK installation
-----------------------------

Some operators, such as :class:`~airflow.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperator`,
:class:`~airflow.providers.google.cloud.operators.dataflow.DataflowStartSqlJobOperator`, require
the installation of `Google Cloud SDK <https://cloud.google.com/sdk>`__ (includes ``gcloud``).
You can also run these commands with BashOperator.

Create a new Dockerfile like the one shown below.

.. exampleinclude:: /docker-images-recipes/gcloud.Dockerfile
    :language: dockerfile

Then build a new image.

.. code-block:: bash

  docker build . \
    --build-arg BASE_AIRFLOW_IMAGE="apache/airflow:2.0.1" \
    -t my-airflow-image


Apache Hadoop Stack installation
--------------------------------

Airflow is often used to run tasks on Hadoop cluster. It required Java Runtime Environment (JRE) to run.
Below are the steps to take tools that are frequently used in Hadoop-world:

- Java Runtime Environment (JRE)
- Apache Hadoop
- Apache Hive
- `Cloud Storage connector for Apache Hadoop <https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage>`__


Create a new Dockerfile like the one shown below.

.. exampleinclude:: /docker-images-recipes/hadoop.Dockerfile
    :language: dockerfile

Then build a new image.

.. code-block:: bash

  docker build . \
    --build-arg BASE_AIRFLOW_IMAGE="apache/airflow:2.0.1" \
    -t my-airflow-image
