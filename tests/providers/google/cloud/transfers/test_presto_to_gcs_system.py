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
import os
from contextlib import closing, suppress

import pytest

from airflow.models import Connection
from airflow.providers.presto.hooks.presto import PrestoHook
from airflow.utils.session import create_session
from tests.providers.google.cloud.utils.gcp_authenticator import GCP_BIGQUERY_KEY, GCP_GCS_KEY
from tests.test_utils.gcp_system_helpers import CLOUD_DAG_FOLDER, GoogleSystemTest, provide_gcp_context

GCS_BUCKET = os.environ.get("GCP_PRESTO_TO_GCS_BUCKET_NAME", "test-presto-to-gcs-bucket")
DATASET_NAME = os.environ.get("GCP_PRESTO_TO_GCS_DATASET_NAME", "test_presto_to_gcs_dataset")

CREATE_QUERY = """
CREATE TABLE memory.default.test_multiple_types (
  -- Boolean
  z_boolean BOOLEAN,
  -- Integers
  z_tinyint TINYINT,
  z_smallint SMALLINT,
  z_integer INTEGER,
  z_bigint BIGINT,
  -- Floating-Point
  z_real REAL,
  z_double DOUBLE,
  -- Fixed-Point
  z_decimal DECIMAL(10,2),
  -- String
  z_varchar VARCHAR(20),
  z_char CHAR(20),
  z_varbinary VARBINARY,
  z_json JSON,
  -- Date and Time
  z_date DATE,
  z_time TIME,
  z_time_with_time_zone TIME WITH TIME ZONE,
  z_timestamp TIMESTAMP,
  z_timestamp_with_time_zone TIMESTAMP WITH TIME ZONE,
  -- Network Address
  z_ipaddress_v4 IPADDRESS,
  z_ipaddress_v6 IPADDRESS,
  -- UUID
  z_uuid UUID
)
"""

LOAD_QUERY = """
INSERT INTO memory.default.test_multiple_types VALUES(
  -- Boolean
  true,                                                    -- z_boolean BOOLEAN,
  -- Integers
  CAST(POW(2, 7 ) - 42 AS TINYINT),                        -- z_tinyint TINYINT,
  CAST(POW(2, 15) - 42 AS SMALLINT),                       -- z_smallint SMALLINT,
  CAST(POW(2, 31) - 42 AS INTEGER),                        -- z_integer INTEGER,
  CAST(POW(2, 32) - 42 AS BIGINT) * 2,                     -- z_bigint BIGINT,
  -- Floating-Point
  REAL '42',                                               -- z_real REAL,
  DOUBLE '1.03e42',                                        -- z_double DOUBLE,
  -- Floating-Point
  DECIMAL '1.1',                                           -- z_decimal DECIMAL(10, 2),
  -- String
  U&'Hello winter \2603 !',                                -- z_vaarchar VARCHAR(20),
  'cat',                                                   -- z_char CHAR(20),
  X'65683F',                                               -- z_varbinary VARBINARY,
  CAST('["A", 1, true]' AS JSON),                          -- z_json JSON,
  -- Date and Time
  DATE '2001-08-22',                                       -- z_date DATE,
  TIME '01:02:03.456',                                     -- z_time TIME,
  TIME '01:02:03.456 America/Los_Angeles',                 -- z_time_with_time_zone TIME WITH TIME ZONE,
  TIMESTAMP '2001-08-22 03:04:05.321',                     -- z_timestamp TIMESTAMP,
  TIMESTAMP '2001-08-22 03:04:05.321 America/Los_Angeles', -- z_timestamp_with_time_zone TIMESTAMP WITH TIME
                                                           -- ZONE,
  -- Network Address
  IPADDRESS '10.0.0.1',                                    -- z_ipaddress_v4 IPADDRESS,
  IPADDRESS '2001:db8::1',                                 -- z_ipaddress_v6 IPADDRESS,
  -- UUID
  UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59'              -- z_uuid UUID
)
"""
DELETE_QUERY = "DROP TABLE memory.default.test_multiple_types"


@pytest.mark.integration("presto")
class PrestoToGCSSystemTest(GoogleSystemTest):
    @staticmethod
    def init_connection():
        with create_session() as session:
            session.query(Connection).filter(Connection.conn_id == "presto_default").delete()
            session.merge(
                Connection(
                    conn_id="presto_default", conn_type="conn_type", host="presto", port=8080, login="airflow"
                )
            )

    @staticmethod
    def init_db():
        hook = PrestoHook()
        with hook.get_conn() as conn:
            with closing(conn.cursor()) as cur:
                cur.execute(CREATE_QUERY)
                # Presto does not execute queries until the result is fetched. :-(
                cur.fetchone()
                cur.execute(LOAD_QUERY)
                cur.fetchone()

    @staticmethod
    def drop_db():
        hook = PrestoHook()
        with hook.get_conn() as conn:
            with closing(conn.cursor()) as cur:
                cur.execute(DELETE_QUERY)
                # Presto does not execute queries until the result is fetched. :-(
                cur.fetchone()

    @provide_gcp_context(GCP_GCS_KEY)
    def setUp(self):
        super().setUp()
        self.init_connection()
        self.create_gcs_bucket(GCS_BUCKET)
        with suppress(Exception):
            self.drop_db()
        self.init_db()
        self.execute_with_ctx(
            ["bq", "rm", "--recursive", "--force", f"{self._project_id()}:{DATASET_NAME}"],
            key=GCP_BIGQUERY_KEY,
        )

    @provide_gcp_context(GCP_BIGQUERY_KEY)
    def test_run_example_dag(self):
        self.run_dag("example_presto_to_gcs", CLOUD_DAG_FOLDER)

    @provide_gcp_context(GCP_GCS_KEY)
    def tearDown(self):
        self.delete_gcs_bucket(GCS_BUCKET)
        self.drop_db()
        self.execute_with_ctx(
            ["bq", "rm", "--recursive", "--force", f"{self._project_id()}:{DATASET_NAME}"],
            key=GCP_BIGQUERY_KEY,
        )
        super().tearDown()
