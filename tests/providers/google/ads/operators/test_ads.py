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
from unittest import mock

from airflow.providers.google.ads.operators.ads import GoogleAdsListAccountsOperator

CLIENT_IDS = ["1111111111", "2222222222"]
BUCKET = "gs://test-google-ads-bucket"
GCS_OBJ_PATH = "folder_name/google-ads-api-results.csv"
QUERY = """
    SELECT
        segments.date,
        customer.id,
    FROM
        ad_group_ad
    WHERE
        segments.date >= '2020-02-01'
        AND segments.date <= '2020-02-29'
    """

FIELDS_TO_EXTRACT = ["segments.date.value", "customer.id.value"]
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]

gcp_conn_id = "gcp_conn_id"
google_ads_conn_id = "google_ads_conn_id"
api_version = "v5"


class TestGoogleAdsListAccountsOperator:
    @mock.patch("airflow.providers.google.ads.operators.ads.GoogleAdsHook")
    @mock.patch("airflow.providers.google.ads.operators.ads.GCSHook")
    @mock.patch("airflow.providers.google.ads.operators.ads.NamedTemporaryFile")
    @mock.patch("airflow.providers.google.ads.operators.ads.csv.writer")
    def test_execute(self, mocks_csv_writer, mock_tempfile, mock_gcs_hook, mock_ads_hook):
        filename = "test.csv"
        file_object = mock_tempfile.return_value.__enter__.return_value
        file_object.name = filename
        accounts = ["a", "b", "c"]
        mock_ads_hook.return_value.list_accessible_customers.return_value = accounts

        op = GoogleAdsListAccountsOperator(
            gcp_conn_id=gcp_conn_id,
            google_ads_conn_id=google_ads_conn_id,
            object_name=GCS_OBJ_PATH,
            bucket=BUCKET,
            task_id="run_operator",
            impersonation_chain=IMPERSONATION_CHAIN,
            api_version=api_version,
        )
        op.execute({})

        mock_ads_hook.assert_called_once_with(
            gcp_conn_id=gcp_conn_id,
            google_ads_conn_id=google_ads_conn_id,
            api_version=api_version,
        )
        mock_gcs_hook.assert_called_once_with(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_ads_hook.return_value.list_accessible_customers.assert_called_once_with()
        mocks_csv_writer.assert_called_once_with(file_object)
        mocks_csv_writer.return_value.writerows.assert_called_once_with(accounts)
        file_object.flush.assert_called_once_with()

        mock_gcs_hook.return_value.upload.assert_called_once_with(
            bucket_name=BUCKET, object_name=GCS_OBJ_PATH, filename=filename, gzip=False
        )
