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

Google Cloud Data Loss Prevention Operator
==========================================
`Google Cloud DLP <https://cloud.google.com/dlp>`__, provides tools to classify, mask, tokenize, and transform sensitive
elements to help you better manage the data that you collect, store, or use for business or analytics.

.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include::/operators/_partials/prerequisite_tasks.rst

Info-Types
^^^^^^^^^^
Google Cloud DLP uses info-types to define what scans for.

.. _howto/operator:CloudDLPCreateStoredInfoTypeOperator:

Create Stored Info-Type
"""""""""""""""""""""""

To create a custom info-type you can use
:class:`~airflow.providers.google.cloud.operators.cloud.dlp.CloudDLPCreateStoredInfoTypeOperator`.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_dlp.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dlp_create_info_type]
    :end-before: [END howto_operator_dlp_create_info_type]



.. _howto/operator:CloudDLPGetStoredInfoTypeOperator:
.. _howto/operator:CloudDLPListInfoTypesOperator:
.. _howto/operator:CloudDLPListStoredInfoTypesOperator:

Retrieve Stored Info-Type
"""""""""""""""""""""""""

To retrieve the lists of sensitive info-types supported by DLP-API for reference, you can use
:class:`~airflow.providers.google.cloud.operators.cloud.dlp.CloudDLPListInfoTypesOperator`.

Similarly to retrieve the list custom info-types, you can use
:class:`~airflow.providers.google.cloud.operators.cloud.dlp.CloudDLPListStoredInfoTypesOperator`.

To retrieve a single info-type
:class:`~airflow.providers.google.cloud.operators.cloud.dlp.CloudDLPGetStoredInfoTypeOperator`


.. _howto/operator:CloudDLPUpdateStoredInfoTypeOperator:

Update Stored Info-Type
"""""""""""""""""""""""

To update a info-type you can use
:class:`~airflow.providers.google.cloud.operators.cloud.dlp.CloudDLPUpdateStoredInfoTypeOperator`.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_dlp.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dlp_update_info_type]
    :end-before: [END howto_operator_dlp_update_info_type]


.. _howto/operator:CloudDLPDeleteStoredInfoTypeOperator:

Deleting Stored Info-Type
"""""""""""""""""""""""""

To delete a info-type you can use
:class:`~airflow.providers.google.cloud.operators.cloud.dlp.CloudDLPDeleteStoredInfoTypeOperator`.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_dlp.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dlp_delete_info_type]
    :end-before: [END howto_operator_dlp_delete_info_type]


Templates
^^^^^^^^^

Templates can be used to create and persist
configuration information to use with the Cloud Data Loss Prevention.
There are two types of DLP templates supported by Airflow:

* Inspection Template
* De-Identification Template

Here we will be using identification template for our example

.. _howto/operator:CloudDLPCreateInspectTemplateOperator:

Creating Template
"""""""""""""""""

To create a inspection template you can use
:class:`~airflow.providers.google.cloud.operators.cloud.dlp.CloudDLPCreateInspectTemplateOperator`.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_dlp.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dlp_create_inspect_template]
    :end-before: [END howto_operator_dlp_create_inspect_template]



.. _howto/operator:CloudDLPGetInspectTemplateOperator:
.. _howto/operator:CloudDLPListInspectTemplatesOperator:

Retrieving Template
"""""""""""""""""""

If you already have an existing inspect template you can retrieve it by use
:class:`~airflow.providers.google.cloud.operators.cloud.dlp.CloudDLPGetInspectTemplateOperator`
List of existing inspect templates can be retrieved by
:class:`~airflow.providers.google.cloud.operators.cloud.dlp.CloudDLPListInspectTemplatesOperator`

.. _howto/operator:CloudDLPInspectContentOperator:

Using Template
""""""""""""""

To find potentially sensitive info using the inspection template we just created, we can use
:class:`~airflow.providers.google.cloud.operators.cloud.dlp.CloudDLPInspectContentOperator`

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_dlp.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dlp_use_inspect_template]
    :end-before: [END howto_operator_dlp_use_inspect_template]

.. _howto/operator:CloudDLPUpdateInspectTemplateOperator:

Updating Template
"""""""""""""""""

To update the template you can use
:class:`~airflow.providers.google.cloud.operators.cloud.CloudDLPUpdateInspectTemplateOperator`.

.. _howto/operator:CloudDLPDeleteInspectTemplateOperator:

Deleting Template
"""""""""""""""""

To delete the template you can use
:class:`~airflow.providers.google.cloud.operators.cloud.dlp.CloudDLPDeleteInspectTemplateOperator`.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_dlp.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dlp_delete_inspect_template]
    :end-before: [END howto_operator_dlp_delete_inspect_template]

.. _howto/operator:CloudDLPCreateDeidentifyTemplateOperator:
.. _howto/operator:CloudDLPDeleteDeidentifyTemplateOperator:
.. _howto/operator:CloudDLPUpdateDeidentifyTemplateOperator:
.. _howto/operator:CloudDLPGetDeidentifyTemplateOperator:
.. _howto/operator:CloudDLPListDeidentifyTemplatesOperator:

De-Identification Template
""""""""""""""""""""""""""

Like Inspect templates, De-Identification templates also have CRUD operators

* :class:`~airflow.providers.google.cloud.operators.cloud.dlp.CloudDLPCreateDeidentifyTemplateOperator`
* :class:`~airflow.providers.google.cloud.operators.cloud.dlp.CloudDLPDeleteDeidentifyTemplateOperator`
* :class:`~airflow.providers.google.cloud.operators.cloud.dlp.CloudDLPUpdateDeidentifyTemplateOperator`
* :class:`~airflow.providers.google.cloud.operators.cloud.dlp.CloudDLPGetDeidentifyTemplateOperator`
* :class:`~airflow.providers.google.cloud.operators.cloud.dlp.CloudDLPListDeidentifyTemplatesOperator`


Jobs & Job Triggers
^^^^^^^^^^^^^^^^^^^

Cloud Data Loss Protection uses a job to run actions to scan content for sensitive data or
calculate the risk of re-identification. You can schedule these jobs using job triggers.

.. _howto/operator:CloudDLPCreateDLPJobOperator:

Creating Job
""""""""""""

To create a job you can use
:class:`~airflow.providers.google.cloud.operators.cloud.dlp.CloudDLPCreateDLPJobOperator`.


.. _howto/operator:CloudDLPListDLPJobsOperator:
.. _howto/operator:CloudDLPGetDLPJobOperator:

Retrieving Job
""""""""""""""

To retrieve the list of jobs you can use
:class:`~airflow.providers.google.cloud.operators.cloud.dlp.CloudDLPListDLPJobsOperator`.
To retrieve a single job
:class:`~airflow.providers.google.cloud.operators.cloud.dlp.CloudDLPGetDLPJobOperator`.

.. _howto/operator:CloudDLPDeleteDLPJobOperator:

Deleting Job
""""""""""""

To delete a job you can use
:class:`~airflow.providers.google.cloud.operators.cloud.dlp.CloudDLPDeleteDLPJobOperator`.

.. _howto/operator:CloudDLPCancelDLPJobOperator:

Canceling a Job
""""""""""""""""

To start asynchronous cancellation of a long-running DLP job you can use
:class:`~airflow.providers.google.cloud.operators.cloud.dlp.CloudDLPCancelDLPJobOperator`.


.. _howto/operator:CloudDLPCreateJobTriggerOperator:

Creating Job Trigger
""""""""""""""""""""

To create a job trigger you can use
:class:`~airflow.providers.google.cloud.operators.cloud.dlp.CloudDLPCreateJobTriggerOperator`.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_dlp.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dlp_create_job_trigger]
    :end-before: [END howto_operator_dlp_create_job_trigger]

.. _howto/operator:CloudDLPListJobTriggersOperator:
.. _howto/operator:CloudDLPGetDLPJobTriggerOperator:

Retrieving Job Trigger
""""""""""""""""""""""

To retrieve list of job triggers you can use
:class:`~airflow.providers.google.cloud.operators.cloud.dlp.CloudDLPListJobTriggersOperator`.
To retrieve a single job trigger you can use
:class:`~airflow.providers.google.cloud.operators.cloud.dlp.CloudDLPGetDLPJobTriggerOperator`.

.. _howto/operator:CloudDLPUpdateJobTriggerOperator:

Updating Job Trigger
""""""""""""""""""""

To update a job trigger you can use
:class:`~airflow.providers.google.cloud.operators.cloud.dlp.CloudDLPUpdateJobTriggerOperator`.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_dlp.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dlp_update_job_trigger]
    :end-before: [END howto_operator_dlp_update_job_trigger]

.. _howto/operator:CloudDLPDUpdateJobTriggerOperator:

Deleting Job Trigger
"""""""""""""""""""""

To delete a job trigger you can use
:class:`~airflow.providers.google.cloud.operators.cloud.dlp.CloudDLPDeleteJobTriggerOperator`.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_dlp.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dlp_delete_job_trigger]
    :end-before: [END howto_operator_dlp_delete_job_trigger]

.. _howto/operator:CloudDLPDeleteJobTriggerOperator:

Content Method
^^^^^^^^^^^^^^

Unlike storage methods (Jobs) content method are synchronous, stateless methods.

.. _howto/operator:CloudDLPDeidentifyContentOperator:

De-identify Content
"""""""""""""""""""
De-identification is the process of removing identifying information from data.
Configuration information defines how you want the sensitive data de-identified.

This config can either be saved and persisted in de-identification templates or defined in a :class:`~google.cloud.dlp_v2.types.DeidentifyConfig` object:

.. literalinclude:: /../../airflow/providers/google/cloud/example_dags/example_dlp.py
    :language: python
    :start-after: [START dlp_deidentify_config_example]
    :end-before: [END dlp_deidentify_config_example]

To de-identify potentially sensitive information from a content item, you can use
:class:`~airflow.providers.google.cloud.operators.cloud.dlp.CloudDLPDeidentifyContentOperator`.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_dlp.py
    :language: python
    :dedent: 4
    :start-after: [START _howto_operator_dlp_deidentify_content]
    :end-before: [END _howto_operator_dlp_deidentify_content]

.. _howto/operator:CloudDLPReidentifyContentOperator:

Re-identify Content
"""""""""""""""""""

To re-identify the content that has been de-identified you can use
:class:`~airflow.providers.google.cloud.operators.cloud.dlp.CloudDLPReidentifyContentOperator`.

.. _howto/operator:CloudDLPReIdentifyOperator:

Redact Image
""""""""""""

To redact potentially sensitive information from the content image you can use
:class:`~airflow.providers.google.cloud.operators.cloud.dlp.CloudDLPRedactImageOperator`.

.. _howto/operator:CloudDLPRedactImageOperator:

Reference
^^^^^^^^^

For further information, look at:

* `Client Library Documentation <https://googleapis.dev/python/dlp/latest/index.html>`__
* `Product Documentation <https://cloud.google.com/dlp/docs>`__
