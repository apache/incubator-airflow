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
ARG ALPINE_VERSION="3.12"
FROM alpine:${ALPINE_VERSION} AS builder
SHELL ["/bin/ash", "-e", "-x", "-c", "-o", "pipefail"]

ARG PGBOUNCER_VERSION
ARG AIRFLOW_PGBOUNCER_VERSION

ARG PGBOUNCER_SHA256="a0c13d10148f557e36ff7ed31793abb7a49e1f8b09aa2d4695d1c28fa101fee7"

# Those are build deps only but still we want the latest versions of those
# "Pin versions in apk add" https://github.com/hadolint/hadolint/wiki/DL3018
# hadolint ignore=DL3018
RUN apk --no-cache add make pkgconfig build-base libtool wget gcc g++ libevent-dev libressl-dev c-ares-dev ca-certificates
# We are not using Dash so we can safely ignore the "Dash warning"
# "In dash, something is not supported." https://github.com/koalaman/shellcheck/wiki/SC2169
# hadolint ignore=SC2169
RUN wget --progress=dot:giga "https://github.com/pgbouncer/pgbouncer/releases/download/pgbouncer_${PGBOUNCER_VERSION//\./_}/pgbouncer-${PGBOUNCER_VERSION}.tar.gz" \
    && echo "${PGBOUNCER_SHA256}  pgbouncer-${PGBOUNCER_VERSION}.tar.gz" | sha256sum -c - \
    && tar -xzvf pgbouncer-$PGBOUNCER_VERSION.tar.gz

WORKDIR /pgbouncer-$PGBOUNCER_VERSION
RUN ./configure --prefix=/usr --disable-debug && make && make install \
    && mkdir /etc/pgbouncer \
    && cp ./etc/pgbouncer.ini /etc/pgbouncer/ \
    && touch /etc/pgbouncer/userlist.txt \
    && sed -i -e "s|logfile = |#logfile = |"  \
           -e "s|pidfile = |#pidfile = |"  \
           -e "s|listen_addr = .*|listen_addr = 0.0.0.0|" \
           -e "s|auth_type = .*|auth_type = md5|" \
           /etc/pgbouncer/pgbouncer.ini

FROM alpine:${ALPINE_VERSION}

ARG PGBOUNCER_VERSION
ARG AIRFLOW_PGBOUNCER_VERSION
ARG COMMIT_SHA

LABEL org.apache.airflow.component="pgbouncer"
LABEL org.apache.airflow.pgbouncer.version="${PGBOUNCER_VERSION}"
LABEL org.apache.airflow.airflow-pgbouncer.version="${AIRFLOW_PGBOUNCER_VERSION}"
LABEL org.apache.airflow.commit-sha="${COMMIT_SHA}"
LABEL maintainer="Apache Airflow Community <dev@airflow.apache.org>"

# We want to make sure this one includes latest security fixes.
# "Pin versions in apk add" https://github.com/hadolint/hadolint/wiki/DL3018
# hadolint ignore=DL3018
RUN apk --no-cache add libevent libressl c-ares

COPY --from=builder /etc/pgbouncer /etc/pgbouncer
COPY --from=builder /usr/bin/pgbouncer /usr/bin/pgbouncer

# Healthcheck
HEALTHCHECK --interval=10s --timeout=3s CMD stat /tmp/.s.PGSQL.*

EXPOSE 6432

# pgbouncer can't run as root, so let's drop to 'nobody'
ENTRYPOINT ["/usr/bin/pgbouncer", "-u", "nobody", "/etc/pgbouncer/pgbouncer.ini" ]
