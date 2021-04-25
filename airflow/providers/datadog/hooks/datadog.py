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

import time
from typing import Any, Dict, List, Optional, Union

from datadog import api, initialize

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin


class DatadogHook(BaseHook, LoggingMixin):
    """
    Uses datadog API to send metrics of practically anything measurable,
    so it's possible to track # of db records inserted/deleted, records read
    from file and many other useful metrics.

    Depends on the datadog API, which has to be deployed on the same server where
    Airflow runs.

    :param datadog_conn_id: The connection to datadog, containing metadata for api keys.
    :type datadog_conn_id: str
    """

    def __init__(self, datadog_conn_id: str = 'datadog_default') -> None:
        super().__init__()
        conn = self.get_connection(datadog_conn_id)
        self.api_key = conn.extra_dejson.get('api_key', None)
        self.app_key = conn.extra_dejson.get('app_key', None)
        self.source_type_name = conn.extra_dejson.get('source_type_name', None)

        # If the host is populated, it will use that hostname instead.
        # for all metric submissions.
        self.host = conn.host

        if self.api_key is None:
            raise AirflowException("api_key must be specified in the Datadog connection details")

        self.log.info("Setting up api keys for Datadog")
        initialize(api_key=self.api_key, app_key=self.app_key)

    def validate_response(self, response: Dict[str, Any]) -> None:
        """Validate Datadog response"""
        if response['status'] != 'ok':
            self.log.error("Datadog returned: %s", response)
            raise AirflowException("Error status received from Datadog")

    def send_metric(
        self,
        metric_name: str,
        datapoint: Union[float, int],
        tags: Optional[List[str]] = None,
        type_: Optional[str] = None,
        interval: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Sends a single datapoint metric to DataDog

        :param metric_name: The name of the metric
        :type metric_name: str
        :param datapoint: A single integer or float related to the metric
        :type datapoint: int or float
        :param tags: A list of tags associated with the metric
        :type tags: list
        :param type_: Type of your metric: gauge, rate, or count
        :type type_: str
        :param interval: If the type of the metric is rate or count, define the corresponding interval
        :type interval: int
        """
        response = api.Metric.send(
            metric=metric_name, points=datapoint, host=self.host, tags=tags, type=type_, interval=interval
        )

        self.validate_response(response)
        return response

    def query_metric(self, query: str, from_seconds_ago: int, to_seconds_ago: int) -> Dict[str, Any]:
        """
        Queries datadog for a specific metric, potentially with some
        function applied to it and returns the results.

        :param query: The datadog query to execute (see datadog docs)
        :type query: str
        :param from_seconds_ago: How many seconds ago to start querying for.
        :type from_seconds_ago: int
        :param to_seconds_ago: Up to how many seconds ago to query for.
        :type to_seconds_ago: int
        """
        now = int(time.time())

        response = api.Metric.query(start=now - from_seconds_ago, end=now - to_seconds_ago, query=query)

        self.validate_response(response)
        return response

    # pylint: disable=too-many-arguments
    def post_event(
        self,
        title: str,
        text: str,
        aggregation_key: Optional[str] = None,
        alert_type: Optional[str] = None,
        date_happened: Optional[int] = None,
        handle: Optional[str] = None,
        priority: Optional[str] = None,
        related_event_id: Optional[int] = None,
        tags: Optional[List[str]] = None,
        device_name: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Posts an event to datadog (processing finished, potentially alerts, other issues)
        Think about this as a means to maintain persistence of alerts, rather than
        alerting itself.

        :param title: The title of the event
        :type title: str
        :param text: The body of the event (more information)
        :type text: str
        :param aggregation_key: Key that can be used to aggregate this event in a stream
        :type aggregation_key: str
        :param alert_type: The alert type for the event, one of
            ["error", "warning", "info", "success"]
        :type alert_type: str
        :param date_happened: POSIX timestamp of the event; defaults to now
        :type date_happened: int
        :handle: User to post the event as; defaults to owner of the application key used
            to submit.
        :param handle: str
        :param priority: Priority to post the event as. ("normal" or "low", defaults to "normal")
        :type priority: str
        :param related_event_id: Post event as a child of the given event
        :type related_event_id: id
        :param tags: List of tags to apply to the event
        :type tags: list[str]
        :param device_name: device_name to post the event with
        :type device_name: list
        """
        response = api.Event.create(
            title=title,
            text=text,
            aggregation_key=aggregation_key,
            alert_type=alert_type,
            date_happened=date_happened,
            handle=handle,
            priority=priority,
            related_event_id=related_event_id,
            tags=tags,
            host=self.host,
            device_name=device_name,
            source_type_name=self.source_type_name,
        )

        self.validate_response(response)
        return response
