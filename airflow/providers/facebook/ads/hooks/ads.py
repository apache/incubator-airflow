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
"""This module contains Facebook Ads Reporting hooks"""
import time
from enum import Enum
from typing import Any, Dict, List

try:
    from functools import cached_property
except ImportError:
    from cached_property import cached_property
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.api import FacebookAdsApi

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class JobStatus(Enum):
    """Available options for facebook async task status"""

    COMPLETED = 'Job Completed'
    STARTED = 'Job Started'
    RUNNING = 'Job Running'
    FAILED = 'Job Failed'
    SKIPPED = 'Job Skipped'


class FacebookAdsReportingHook(BaseHook):
    """
    Hook for the Facebook Ads API

    .. seealso::
        For more information on the Facebook Ads API, take a look at the API docs:
        https://developers.facebook.com/docs/marketing-apis/

    :param facebook_conn_id: Airflow Facebook Ads connection ID
    :type facebook_conn_id: str
    :param api_version: The version of Facebook API. Default to v6.0
    :type api_version: str

    """

    conn_name_attr = 'facebook_conn_id'
    default_conn_name = 'facebook_default'
    conn_type = 'facebook_social'
    hook_name = 'Facebook Ads'

    def __init__(
        self,
        facebook_conn_id: str = default_conn_name,
        api_version: str = "v6.0",
    ) -> None:
        super().__init__()
        self.facebook_conn_id = facebook_conn_id
        self.api_version = api_version
        self.client_required_fields = ["app_id", "app_secret", "access_token", "account_id"]

    def _get_service(self) -> FacebookAdsApi:
        """Returns Facebook Ads Client using a service account"""
        config = self.facebook_ads_config
        return FacebookAdsApi.init(
            app_id=config["app_id"],
            app_secret=config["app_secret"],
            access_token=config["access_token"],
            account_id=config["account_id"],
            api_version=self.api_version,
        )

    @cached_property
    def facebook_ads_config(self) -> Dict:
        """
        Gets Facebook ads connection from meta db and sets
        facebook_ads_config attribute with returned config file
        """
        self.log.info("Fetching fb connection: %s", self.facebook_conn_id)
        conn = self.get_connection(self.facebook_conn_id)
        config = conn.extra_dejson
        missing_keys = self.client_required_fields - config.keys()
        if missing_keys:
            message = f"{missing_keys} fields are missing"
            raise AirflowException(message)
        return config

    def bulk_facebook_report(
        self,
        params: Dict[str, Any],
        fields: List[str],
        sleep_time: int = 5,
    ) -> List[AdsInsights]:
        """
        Pulls data from the Facebook Ads API

        :param fields: List of fields that is obtained from Facebook. Found in AdsInsights.Field class.
            https://developers.facebook.com/docs/marketing-api/insights/parameters/v6.0
        :type fields: List[str]
        :param params: Parameters that determine the query for Facebook
            https://developers.facebook.com/docs/marketing-api/insights/parameters/v6.0
        :type fields: Dict[str, Any]
        :param sleep_time: Time to sleep when async call is happening
        :type sleep_time: int

        :return: Facebook Ads API response, converted to Facebook Ads Row objects
        :rtype: List[AdsInsights]
        """
        api = self._get_service()
        ad_account = AdAccount(api.get_default_account_id(), api=api)
        _async = ad_account.get_insights(params=params, fields=fields, is_async=True)
        while True:
            request = _async.api_get()
            async_status = request[AdReportRun.Field.async_status]
            percent = request[AdReportRun.Field.async_percent_completion]
            self.log.info("%s %s completed, async_status: %s", percent, "%", async_status)
            if async_status == JobStatus.COMPLETED.value:
                self.log.info("Job run completed")
                break
            if async_status in [JobStatus.SKIPPED.value, JobStatus.FAILED.value]:
                message = f"{async_status}. Please retry."
                raise AirflowException(message)
            time.sleep(sleep_time)
        report_run_id = _async.api_get()["report_run_id"]
        report_object = AdReportRun(report_run_id, api=api)
        insights = report_object.get_insights()
        self.log.info("Extracting data from returned Facebook Ads Iterators")
        return list(insights)
