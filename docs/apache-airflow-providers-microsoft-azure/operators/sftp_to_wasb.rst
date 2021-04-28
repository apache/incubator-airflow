
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



Azure Blob Storage Transfer Operator
====================================
The Blob service stores text and binary data as objects in the cloud.
The Blob service offers the following three resources: the storage account, containers, and blobs.
Within your storage account, containers provide a way to organize sets of blobs.
For more information about the service visit `Azure Blob Storage API documentation <https://docs.microsoft.com/en-us/rest/api/storageservices/blob-service-rest-api>`_.

Before you begin
^^^^^^^^^^^^^^^^
Before using Blob Storage within Airflow you need to authenticate your account with Token, Login and Password.
Please follow Azure
`instructions <https://docs.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage?tabs=azure-portal>`_
to do it.

TOKEN should be added to the Connection in Airflow in JSON format, Login and Password as plain text.
You can check `how to do such connection <https://airflow.apache.org/docs/stable/howto/connection/index.html#editing-a-connection-with-the-ui>`_.

See following example.
Set values for these fields:

.. code-block::

  SFTP Conn Id: sftp_default
  WASB Conn Id: wasb_default
  Login: Storage Account Name
  Password: KEY1
  Extra: {"sas_token": "TOKEN"}

.. contents::
  :depth: 1
  :local:

.. _howto/operator:SFTPToWasbOperator:

Transfer Data from SFTP Source Path to Blob Storage
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Operator transfers data from SFTP Source Path to specified container in Azure Blob Storage

To get information about jobs within a Azure Blob Storage use:
:class:`~airflow.providers.microsoft.azure.transfers.sftp_to_wasb.SFTPToWasbOperator`
Example usage:

.. exampleinclude:: /../../airflow/providers/microsoft/azure/example_dags/example_sftp_to_wasb.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_sftp_to_wasb]
    :end-before: [END how_to_sftp_to_wasb]
