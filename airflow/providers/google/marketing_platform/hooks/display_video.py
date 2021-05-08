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
"""This module contains Google DisplayVideo hook."""

from typing import Any, Dict, List, Optional, Sequence, Union

from googleapiclient.discovery import Resource, build

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class GoogleDisplayVideo360Hook(GoogleBaseHook):
    """Hook for Google Display & Video 360."""

    _conn = None  # type: Optional[Any]

    def __init__(
        self,
        api_version: str = "v1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )
        self.api_version = api_version

    def get_conn(self) -> Resource:
        """Retrieves connection to DisplayVideo."""
        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build(
                "doubleclickbidmanager",
                self.api_version,
                http=http_authorized,
                cache_discovery=False,
            )
        return self._conn

    def get_conn_to_display_video(self) -> Resource:
        """Retrieves connection to DisplayVideo."""
        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build(
                "displayvideo",
                self.api_version,
                http=http_authorized,
                cache_discovery=False,
            )
        return self._conn

    @staticmethod
    def erf_uri(partner_id, entity_type) -> List[str]:
        """
        Return URI for all Entity Read Files in bucket.

        For example, if you were generating a file name to retrieve the entity read file
        for partner 123 accessing the line_item table from April 2, 2013, your filename
        would look something like this:
        gdbm-123/entity/20130402.0.LineItem.json

        More information:
        https://developers.google.com/bid-manager/guides/entity-read/overview

        :param partner_id The numeric ID of your Partner.
        :type partner_id: int
        :param entity_type: The type of file Partner, Advertiser, InsertionOrder,
        LineItem, Creative, Pixel, InventorySource, UserList, UniversalChannel, and summary.
        :type entity_type: str
        """
        return [f"gdbm-{partner_id}/entity/{{{{ ds_nodash }}}}.*.{entity_type}.json"]

    def create_query(self, query: Dict[str, Any]) -> dict:
        """
        Creates a query.

        :param query: Query object to be passed to request body.
        :type query: Dict[str, Any]
        """
        response = (
            self.get_conn()  # pylint: disable=no-member
            .queries()
            .createquery(body=query)
            .execute(num_retries=self.num_retries)
        )
        return response

    def delete_query(self, query_id: str) -> None:
        """
        Deletes a stored query as well as the associated stored reports.

        :param query_id: Query ID to delete.
        :type query_id: str
        """
        (
            self.get_conn()  # pylint: disable=no-member
            .queries()
            .deletequery(queryId=query_id)
            .execute(num_retries=self.num_retries)
        )

    def get_query(self, query_id: str) -> dict:
        """
        Retrieves a stored query.

        :param query_id: Query ID to retrieve.
        :type query_id: str
        """
        response = (
            self.get_conn()  # pylint: disable=no-member
            .queries()
            .getquery(queryId=query_id)
            .execute(num_retries=self.num_retries)
        )
        return response

    def list_queries(
        self,
    ) -> List[Dict]:
        """Retrieves stored queries."""
        response = (
            self.get_conn()  # pylint: disable=no-member
            .queries()
            .listqueries()
            .execute(num_retries=self.num_retries)
        )
        return response.get('queries', [])

    def run_query(self, query_id: str, params: Dict[str, Any]) -> None:
        """
        Runs a stored query to generate a report.

        :param query_id: Query ID to run.
        :type query_id: str
        :param params: Parameters for the report.
        :type params: Dict[str, Any]
        """
        (
            self.get_conn()  # pylint: disable=no-member
            .queries()
            .runquery(queryId=query_id, body=params)
            .execute(num_retries=self.num_retries)
        )

    def upload_line_items(self, line_items: Any) -> List[Dict[str, Any]]:
        """
        Uploads line items in CSV format.

        :param line_items: downloaded data from GCS and passed to the body request
        :type line_items: Any
        :return: response body.
        :rtype: List[Dict[str, Any]]
        """
        request_body = {
            "lineItems": line_items,
            "dryRun": False,
            "format": "CSV",
        }

        response = (
            self.get_conn()  # pylint: disable=no-member
            .lineitems()
            .uploadlineitems(body=request_body)
            .execute(num_retries=self.num_retries)
        )
        return response

    def download_line_items(self, request_body: Dict[str, Any]) -> List[Any]:
        """
        Retrieves line items in CSV format.

        :param request_body: dictionary with parameters that should be passed into.
            More information about it can be found here:
            https://developers.google.com/bid-manager/v1.1/lineitems/downloadlineitems
        :type request_body: Dict[str, Any]
        """
        response = (
            self.get_conn()  # pylint: disable=no-member
            .lineitems()
            .downloadlineitems(body=request_body)
            .execute(num_retries=self.num_retries)
        )
        return response["lineItems"]

    def create_sdf_download_operation(self, body_request: Dict[str, Any]) -> Dict[str, Any]:
        """
        Creates an SDF Download Task and Returns an Operation.

        :param body_request: Body request.
        :type body_request: Dict[str, Any]

        More information about body request n be found here:
        https://developers.google.com/display-video/api/reference/rest/v1/sdfdownloadtasks/create
        """
        result = (
            self.get_conn_to_display_video()  # pylint: disable=no-member
            .sdfdownloadtasks()
            .create(body=body_request)
            .execute(num_retries=self.num_retries)
        )
        return result

    def get_sdf_download_operation(self, operation_name: str):
        """
        Gets the latest state of an asynchronous SDF download task operation.

        :param operation_name: The name of the operation resource.
        :type operation_name: str
        """
        result = (
            self.get_conn_to_display_video()  # pylint: disable=no-member
            .sdfdownloadtasks()
            .operations()
            .get(name=operation_name)
            .execute(num_retries=self.num_retries)
        )
        return result

    def download_media(self, resource_name: str):
        """
        Downloads media.

        :param resource_name: of the media that is being downloaded.
        :type resource_name: str
        """
        request = (
            self.get_conn_to_display_video()  # pylint: disable=no-member
            .media()
            .download_media(resourceName=resource_name)
        )
        return request
