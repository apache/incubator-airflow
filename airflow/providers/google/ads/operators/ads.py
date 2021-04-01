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
"""This module contains Google Ad to GCS operators."""
import csv
from tempfile import NamedTemporaryFile
from typing import Optional, Sequence, Union

from airflow.models import BaseOperator
from airflow.providers.google.ads.hooks.ads import GoogleAdsHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.decorators import apply_defaults


class GoogleAdsListAccountsOperator(BaseOperator):
    """
    Saves list of customers on GCS in form of a csv file.

    The resulting list of customers is based on your OAuth credentials. The request returns a list
    of all accounts that you are able to act upon directly given your current credentials. This will
    not necessarily include all accounts within the account hierarchy; rather, it will only include
    accounts where your authenticated user has been added with admin or other rights in the account.

    .. seealso::
        https://developers.google.com/google-ads/api/reference/rpc


    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleAdsListAccountsOperator`

    :param bucket: The GCS bucket to upload to
    :type bucket: str
    :param object_name: GCS path to save the csv file. Must be the full file path (ex. `path/to/file.csv`)
    :type object_name: str
    :param gcp_conn_id: Airflow Google Cloud connection ID
    :type gcp_conn_id: str
    :param google_ads_conn_id: Airflow Google Ads connection ID
    :type google_ads_conn_id: str
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
        "bucket",
        "object_name",
        "impersonation_chain",
    )

    @apply_defaults
    def __init__(
        self,
        *,
        bucket: str,
        object_name: str,
        gcp_conn_id: str = "google_cloud_default",
        google_ads_conn_id: str = "google_ads_default",
        gzip: bool = False,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.bucket = bucket
        self.object_name = object_name
        self.gcp_conn_id = gcp_conn_id
        self.google_ads_conn_id = google_ads_conn_id
        self.gzip = gzip
        self.impersonation_chain = impersonation_chain

    def execute(self, context: dict) -> str:
        uri = f"gs://{self.bucket}/{self.object_name}"

        ads_hook = GoogleAdsHook(gcp_conn_id=self.gcp_conn_id, google_ads_conn_id=self.google_ads_conn_id)

        gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        with NamedTemporaryFile("w+") as temp_file:
            # Download accounts
            accounts = ads_hook.list_accessible_customers()
            writer = csv.writer(temp_file)
            writer.writerows(accounts)
            temp_file.flush()

            # Upload to GCS
            gcs_hook.upload(
                bucket_name=self.bucket, object_name=self.object_name, gzip=self.gzip, filename=temp_file.name
            )
            self.log.info("Uploaded %s to %s", len(accounts), uri)

        return uri
