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
ARG STATSD_VERSION="missing_version"

FROM prom/statsd-exporter:${STATSD_VERSION}

ARG STATSD_VERSION
ARG AIRFLOW_STATSD_EXPORTER_VERSION
ARG COMMIT_SHA

LABEL org.apache.airflow.component="statsd-exporter"
LABEL org.apache.airflow.stasd.version="${STATSD_VERSION}"
LABEL org.apache.airflow.airflow-stasd-exporter.version="${AIRFLOW_STATSD_EXPORTER_VERSION}"
LABEL org.apache.airflow.commit-sha="${COMMIT_SHA}"
LABEL maintainer="Apache Airflow Community <dev@airflow.apache.org>"

COPY mappings.yml /etc/statsd-exporter/mappings.yml
