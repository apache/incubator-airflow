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
"""This module contains Facebook Ad Reporting to GCS operators."""
import csv
import tempfile
from typing import Any, Dict, List, Optional, Sequence, Union

from airflow.models import BaseOperator
from airflow.providers.facebook.ads.hooks.ads import FacebookAdsReportingHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.decorators import apply_defaults


class FacebookAdsReportToGcsOperator(BaseOperator):
    """
    Fetches the results from the Facebook Ads API as desired in the params
    Converts and saves the data as a temporary JSON file
    Uploads the JSON to Google Cloud Storage

    .. seealso::
        For more information on the Facebook Ads API, take a look at the API docs:
        https://developers.facebook.com/docs/marketing-apis/

    .. seealso::
        For more information on the Facebook Ads Python SDK, take a look at the docs:
        https://github.com/facebook/facebook-python-business-sdk

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:FacebookAdsReportToGcsOperator`

    :param bucket_name: The GCS bucket to upload to
    :type bucket_name: str
    :param object_name: GCS path to save the object. Must be the full file path (ex. `path/to/file.txt`)
    :type object_name: str
    :param gcp_conn_id: Airflow Google Cloud connection ID
    :type gcp_conn_id: str
    :param facebook_conn_id: Airflow Facebook Ads connection ID
    :type facebook_conn_id: str
    :param api_version: The version of Facebook API. Default to v6.0
    :type api_version: str
    :param fields: List of fields that is obtained from Facebook. Found in AdsInsights.Field class.
        https://developers.facebook.com/docs/marketing-api/insights/parameters/v6.0
    :type fields: List[str]
    :param params: Parameters that determine the query for Facebook
        https://developers.facebook.com/docs/marketing-api/insights/parameters/v6.0
    :type params: Dict[str, Any]
    :param gzip: Option to compress local file or file data for upload
    :type gzip: bool
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = (
        "facebook_conn_id",
        "bucket_name",
        "object_name",
        "impersonation_chain",
    )

    @apply_defaults
    def __init__(
        self,
        *,
        bucket_name: str,
        object_name: str,
        fields: List[str],
        params: Dict[str, Any],
        gzip: bool = False,
        api_version: str = "v6.0",
        gcp_conn_id: str = "google_cloud_default",
        facebook_conn_id: str = "facebook_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.gcp_conn_id = gcp_conn_id
        self.facebook_conn_id = facebook_conn_id
        self.api_version = api_version
        self.fields = fields
        self.params = params
        self.gzip = gzip
        self.impersonation_chain = impersonation_chain

    def execute(self, context: dict):
        service = FacebookAdsReportingHook(
            facebook_conn_id=self.facebook_conn_id, api_version=self.api_version
        )
        rows = service.bulk_facebook_report(params=self.params, fields=self.fields)

        converted_rows = [dict(row) for row in rows]
        self.log.info("Facebook Returned %s data points", len(converted_rows))

        if converted_rows:
            headers = converted_rows[0].keys()
            with tempfile.NamedTemporaryFile("w", suffix=".csv") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=headers)
                writer.writeheader()
                writer.writerows(converted_rows)
                csvfile.flush()
                hook = GCSHook(
                    gcp_conn_id=self.gcp_conn_id,
                    impersonation_chain=self.impersonation_chain,
                )
                hook.upload(
                    bucket_name=self.bucket_name,
                    object_name=self.object_name,
                    filename=csvfile.name,
                    gzip=self.gzip,
                )
                self.log.info("%s uploaded to GCS", csvfile.name)
