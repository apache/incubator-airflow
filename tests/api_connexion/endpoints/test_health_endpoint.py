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
from datetime import timedelta
from unittest import mock

import pytest

from airflow.jobs.base_job import BaseJob
from airflow.utils import timezone
from airflow.utils.session import create_session, provide_session
from airflow.utils.state import State

HEALTHY = "healthy"
UNHEALTHY = "unhealthy"


class TestHealthTestBase:
    @pytest.fixture(autouse=True)
    def setup_attrs(self, minimal_app_for_api) -> None:
        self.app = minimal_app_for_api
        self.client = self.app.test_client()  # type:ignore
        with create_session() as session:
            session.query(BaseJob).delete()

    def teardown_method(self):
        with create_session() as session:
            session.query(BaseJob).delete()


class TestGetHealth(TestHealthTestBase):
    @provide_session
    def test_healthy_scheduler_status(self, session):
        last_scheduler_heartbeat_for_testing_1 = timezone.utcnow()
        session.add(
            BaseJob(
                job_type="SchedulerJob",
                state=State.RUNNING,
                latest_heartbeat=last_scheduler_heartbeat_for_testing_1,
            )
        )
        session.commit()
        resp_json = self.client.get("/api/v1/health").json
        assert "healthy" == resp_json["metadatabase"]["status"]
        assert "healthy" == resp_json["scheduler"]["status"]
        assert (
            last_scheduler_heartbeat_for_testing_1.isoformat()
            == resp_json["scheduler"]["latest_scheduler_heartbeat"]
        )

    @provide_session
    def test_unhealthy_scheduler_is_slow(self, session):
        last_scheduler_heartbeat_for_testing_2 = timezone.utcnow() - timedelta(minutes=1)
        session.add(
            BaseJob(
                job_type="SchedulerJob",
                state=State.RUNNING,
                latest_heartbeat=last_scheduler_heartbeat_for_testing_2,
            )
        )
        session.commit()
        resp_json = self.client.get("/api/v1/health").json
        assert "healthy" == resp_json["metadatabase"]["status"]
        assert "unhealthy" == resp_json["scheduler"]["status"]
        assert (
            last_scheduler_heartbeat_for_testing_2.isoformat()
            == resp_json["scheduler"]["latest_scheduler_heartbeat"]
        )

    def test_unhealthy_scheduler_no_job(self):
        resp_json = self.client.get("/api/v1/health").json
        assert "healthy" == resp_json["metadatabase"]["status"]
        assert "unhealthy" == resp_json["scheduler"]["status"]
        assert resp_json["scheduler"]["latest_scheduler_heartbeat"] is None

    @mock.patch("airflow.api_connexion.endpoints.health_endpoint.SchedulerJob.most_recent_job")
    def test_unhealthy_metadatabase_status(self, mock_scheduler_most_recent_job):
        mock_scheduler_most_recent_job.side_effect = Exception
        resp_json = self.client.get("/api/v1/health").json
        assert "unhealthy" == resp_json["metadatabase"]["status"]
        assert resp_json["scheduler"]["latest_scheduler_heartbeat"] is None
