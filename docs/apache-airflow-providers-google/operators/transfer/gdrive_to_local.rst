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


Downloads data from Google Drive Storage to Local Filesystem
============================================================
The `Google Drive <https://www.google.com/drive/>`__ is
used to store daily use data, including documents and photos. Google Drive has built-in mechanisms to facilitate group work e.g.
document editor, file sharing mechanisms.

.. contents::
  :depth: 1
  :local:


Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include::/operators/_partials/prerequisite_tasks.rst

.. _howto/operator:GoogleDriveToLocalOperator:

GCSToLocalFilesystemOperator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:class:`~airflow.providers.google.cloud.transfers.gdrive_to_local.GoogleDriveToLocalOperator` allows you to download
data from Google Drive to local filesystem.


Below is an example of using this operator to download file from Google Drive to Local Filesystem.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_gdrive_to_local.py
    :language: python
    :dedent: 4
    :start-after: [START download_from_gdrive_to_local]
    :end-before: [END download_from_gdrive_to_local]


Reference
---------

For further information, look at:

* `Google Drive API Documentation <https://developers.google.com/drive/api/v3/about-sdk>`__
