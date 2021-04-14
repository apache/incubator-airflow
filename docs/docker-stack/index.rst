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

.. image:: /img/docker-logo.png
    :width: 100

Docker Image for Apache Airflow
===============================

.. toctree::
    :hidden:

    Home <self>
    build
    entrypoint
    recipes

.. toctree::
    :hidden:
    :caption: References

    build-arg-ref

For the ease of deployment in production, the community releases a production-ready reference container
image.

The docker image provided (as convenience binary package) in the
`apache/airflow DockerHub <https://hub.docker.com/r/apache/airflow>`_ is a bare image
that has a few external dependencies and extras installed..

The Apache Airflow image provided as convenience package is optimized for size, so
it provides just a bare minimal set of the extras and dependencies installed and in most cases
you want to either extend or customize the image. You can see all possible extras in
:doc:`extra-packages-ref`. The set of extras used in Airflow Production image are available in the
`Dockerfile <https://github.com/apache/airflow/blob/2c6c7fdb2308de98e142618836bdf414df9768c8/Dockerfile#L39>`_.

The production images are build in DockerHub from released version and release candidates. There
are also images published from branches but they are used mainly for development and testing purpose.
See `Airflow Git Branching <https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst#airflow-git-branches>`_
for details.
