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
# shellcheck disable=SC1091
ARG ALPINE_VERSION="3.12"

FROM alpine:${ALPINE_VERSION}

ARG STRESS_VERSION
ARG AIRFLOW_STRESS_VERSION
ARG COMMIT_SHA

ARG STRESS_VERSION=1.0.4

# Those are build deps only but still we want the latest versions of those
# "Pin versions in apk add" https://github.com/hadolint/hadolint/wiki/DL3018
# It's ok to switch temporarily to the /tmp directory
# "Use WORKDIR to switch to a directory" https://github.com/hadolint/hadolint/wiki/DL3003
# hadolint ignore=DL3018,DL3003
RUN apk --no-cache add bash g++ make curl && \
    curl -o "/tmp/stress-${STRESS_VERSION}.tgz" \
        "https://src.fedoraproject.org/repo/pkgs/stress/stress-${STRESS_VERSION}.tar.gz/a607afa695a511765b40993a64c6e2f4/stress-${STRESS_VERSION}.tar.gz" && \
    cd "/tmp" && \
    tar xvf "stress-${STRESS_VERSION}.tgz" && \
    rm "/tmp/stress-${STRESS_VERSION}.tgz" && \
    cd "/tmp/stress-${STRESS_VERSION}" &&  \
    ./configure && make "-j$(getconf _NPROCESSORS_ONLN)" && \
    make install && \
    apk del g++ make curl &&  \
    rm -rf "/tmp/*" "/var/tmp/*" "/var/cache/distfiles/*"

LABEL org.apache.airflow.component="stress"
LABEL org.apache.airflow.stress.version="${STRESS_VERSION}"
LABEL org.apache.airflow.airflow-stress.version="${AIRFLOW_STRESS_VERSION}"
LABEL org.apache.airflow.commit-sha="${COMMIT_SHA}"
LABEL maintainer="Apache Airflow Community <dev@airflow.apache.org>"

CMD ["/usr/local/bin/stress"]
