#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Example Airflow DAG that execute the following tasks using
Cloud DLP service in the Google Cloud:
1) Creating a content inspect template;
2) Using the created template to inspect content;
3) Deleting the template from Google Cloud .
"""

import os

from google.cloud.dlp_v2.types import ContentItem, InspectConfig, InspectTemplate

from airflow import models
from airflow.providers.google.cloud.operators.dlp import (
    CloudDLPCreateInspectTemplateOperator,
    CloudDLPCreateJobTriggerOperator,
    CloudDLPCreateStoredInfoTypeOperator,
    CloudDLPDeidentifyContentOperator,
    CloudDLPDeleteInspectTemplateOperator,
    CloudDLPDeleteJobTriggerOperator,
    CloudDLPDeleteStoredInfoTypeOperator,
    CloudDLPInspectContentOperator,
    CloudDLPUpdateJobTriggerOperator,
    CloudDLPUpdateStoredInfoTypeOperator,
)
from airflow.utils.dates import days_ago

GCP_PROJECT = os.environ.get("GCP_PROJECT_ID", "example-project")
TEMPLATE_ID = "dlp-inspect-838746"
ITEM = ContentItem(
    table={
        "headers": [{"name": "column1"}],
        "rows": [{"values": [{"string_value": "My phone number is (206) 555-0123"}]}],
    }
)
INSPECT_CONFIG = InspectConfig(info_types=[{"name": "PHONE_NUMBER"}, {"name": "US_TOLLFREE_PHONE_NUMBER"}])
INSPECT_TEMPLATE = InspectTemplate(inspect_config=INSPECT_CONFIG)
OUTPUT_BUCKET = os.environ.get("DLP_OUTPUT_BUCKET", "gs://test-dlp-airflow")
OUTPUT_FILENAME = "test.txt"

OBJECT_GCS_URI = os.path.join(OUTPUT_BUCKET, "tmp")
OBJECT_GCS_OUTPUT_URI = os.path.join(OUTPUT_BUCKET, "tmp", OUTPUT_FILENAME)

with models.DAG(
    "example_gcp_dlp",
    schedule_interval=None,  # Override to match your needs
    start_date=days_ago(1),
    tags=['example'],
) as dag1:
    # [START howto_operator_dlp_create_inspect_template]
    create_template = CloudDLPCreateInspectTemplateOperator(
        project_id=GCP_PROJECT,
        inspect_template=INSPECT_TEMPLATE,
        template_id=TEMPLATE_ID,
        task_id="create_template",
        do_xcom_push=True,
    )
    # [END howto_operator_dlp_create_inspect_template]

    # [START howto_operator_dlp_use_inspect_template]
    inspect_content = CloudDLPInspectContentOperator(
        task_id="inspect_content",
        project_id=GCP_PROJECT,
        item=ITEM,
        inspect_template_name="{{ task_instance.xcom_pull('create_template', key='return_value')['name'] }}",
    )
    # [END howto_operator_dlp_use_inspect_template]

    # [START howto_operator_dlp_delete_inspect_template]
    delete_template = CloudDLPDeleteInspectTemplateOperator(
        task_id="delete_template",
        template_id=TEMPLATE_ID,
        project_id=GCP_PROJECT,
    )
    # [END howto_operator_dlp_delete_inspect_template]

    create_template >> inspect_content >> delete_template

CUSTOM_INFO_TYPE_ID = "custom_info_type"
CUSTOM_INFO_TYPES = {
    "large_custom_dictionary": {
        "output_path": {"path": OBJECT_GCS_OUTPUT_URI},
        "cloud_storage_file_set": {"url": OBJECT_GCS_URI + "/"},
    }
}
UPDATE_CUSTOM_INFO_TYPE = {
    "large_custom_dictionary": {
        "output_path": {"path": OBJECT_GCS_OUTPUT_URI},
        "cloud_storage_file_set": {"url": OBJECT_GCS_URI + "/"},
    }
}

with models.DAG(
    "example_gcp_dlp_info_types",
    schedule_interval=None,
    start_date=days_ago(1),
    tags=["example", "dlp", "info-types"],
) as dag2:
    # [START howto_operator_dlp_create_info_type]
    create_info_type = CloudDLPCreateStoredInfoTypeOperator(
        project_id=GCP_PROJECT,
        config=CUSTOM_INFO_TYPES,
        stored_info_type_id=CUSTOM_INFO_TYPE_ID,
        task_id="create_info_type",
    )
    # [END howto_operator_dlp_create_info_type]
    # [START howto_operator_dlp_update_info_type]
    update_info_type = CloudDLPUpdateStoredInfoTypeOperator(
        project_id=GCP_PROJECT,
        stored_info_type_id=CUSTOM_INFO_TYPE_ID,
        config=UPDATE_CUSTOM_INFO_TYPE,
        task_id="update_info_type",
    )
    # [END howto_operator_dlp_update_info_type]
    # [START howto_operator_dlp_delete_info_type]
    delete_info_type = CloudDLPDeleteStoredInfoTypeOperator(
        project_id=GCP_PROJECT,
        stored_info_type_id=CUSTOM_INFO_TYPE_ID,
        task_id="delete_info_type",
    )
    # [END howto_operator_dlp_delete_info_type]
    create_info_type >> update_info_type >> delete_info_type

JOB_TRIGGER = {
    "inspect_job": {
        "storage_config": {
            "datastore_options": {"partition_id": {"project_id": GCP_PROJECT}, "kind": {"name": "test"}}
        }
    },
    "triggers": [{"schedule": {"recurrence_period_duration": {"seconds": 60 * 60 * 24}}}],
    "status": "HEALTHY",
}

TRIGGER_ID = "example_trigger"

with models.DAG(
    "example_gcp_dlp_job", schedule_interval=None, start_date=days_ago(1), tags=["example", "dlp_job"]
) as dag3:  # [START howto_operator_dlp_create_job_trigger]
    create_trigger = CloudDLPCreateJobTriggerOperator(
        project_id=GCP_PROJECT,
        job_trigger=JOB_TRIGGER,
        trigger_id=TRIGGER_ID,
        task_id="create_trigger",
    )
    # [END howto_operator_dlp_create_job_trigger]

    JOB_TRIGGER["triggers"] = [{"schedule": {"recurrence_period_duration": {"seconds": 2 * 60 * 60 * 24}}}]

    # [START howto_operator_dlp_update_job_trigger]
    update_trigger = CloudDLPUpdateJobTriggerOperator(
        project_id=GCP_PROJECT,
        job_trigger_id=TRIGGER_ID,
        job_trigger=JOB_TRIGGER,
        task_id="update_info_type",
    )
    # [END howto_operator_dlp_update_job_trigger]
    # [START howto_operator_dlp_delete_job_trigger]
    delete_trigger = CloudDLPDeleteJobTriggerOperator(
        project_id=GCP_PROJECT, job_trigger_id=TRIGGER_ID, task_id="delete_info_type"
    )
    # [END howto_operator_dlp_delete_job_trigger]
    create_trigger >> update_trigger >> delete_trigger

# [START dlp_deidentify_config_example]
DEIDENTIFY_CONFIG = {
    "info_type_transformations": {
        "transformations": [
            {
                "primitive_transformation": {
                    "replace_config": {"new_value": {"string_value": "[deidentified_number]"}}
                }
            }
        ]
    }
}
# [END dlp_deidentify_config_example]

with models.DAG(
    "example_gcp_dlp_deidentify_content",
    schedule_interval=None,
    start_date=days_ago(1),
    tags=["example", "dlp", "deidentify"],
) as dag4:
    # [START _howto_operator_dlp_deidentify_content]
    deidentify_content = CloudDLPDeidentifyContentOperator(
        project_id=GCP_PROJECT,
        item=ITEM,
        deidentify_config=DEIDENTIFY_CONFIG,
        inspect_config=INSPECT_CONFIG,
        task_id="deidentify_content",
    )
    # [END _howto_operator_dlp_deidentify_content]
