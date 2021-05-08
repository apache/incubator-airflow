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
FROM ibmjava:sfj-alpine

ARG AIRFLOW_APACHERAT_VERSION
ARG APACHERAT_VERSION
ARG APACHE_MIRROR="https://www-us.apache.org/dist"

SHELL ["/bin/sh", "-e", "-x", "-c"]

# We don't download the KEYS file, but instead pre-bake them, so that any
# change to approved keys needs a manual approval. Rat is released fairly
# infrequently so this shouldn't be too onerous
# Those are build deps only but still we want the latest versions of those
# "Pin versions in apk add" https://github.com/hadolint/hadolint/wiki/DL3018
# hadolint ignore=DL3018
RUN apk add -U --no-cache --virtual .build-deps \
        gnupg \
        unzip \
    && wget -qT 30 "${APACHE_MIRROR}/creadur/KEYS" \
    && gpg --import KEYS \
    && rm KEYS \
    && dist_name="apache-rat-${APACHERAT_VERSION}" \
    && rat_tgz="${dist_name}-bin.tar.gz" \
    && wget -qT 30 "${APACHE_MIRROR}/creadur/${dist_name}/${rat_tgz}.asc" \
    && wget -qT 30 "${APACHE_MIRROR}/creadur/${dist_name}/${rat_tgz}" \
    && gpg --verify "${rat_tgz}.asc" "${rat_tgz}" \
    && tar --extract --gzip --file "${rat_tgz}" --strip-components=1 "${dist_name}/${dist_name}.jar" "${dist_name}/LICENSE" \
    && rm -vrf "${rat_tgz}" "${rat_tgz}.asc" \
    && ln -s "${dist_name}.jar" "apache-rat.jar" \
    && apk del .build-deps \
    && rm -rf ~/.gnupg

LABEL org.apache.airflow.component="apache-rat"
LABEL org.apache.airflow.apache-rat.version="${APACHERAT_VERSION}"
LABEL org.apache.airflow.airflow-apache-rat.version="${AIRFLOW_APACHERAT_VERSION}"
LABEL org.apache.airflow.commit-sha="${COMMIT_SHA}"
LABEL maintainer="Apache Airflow Community <dev@airflow.apache.org>"

ENTRYPOINT ["java", "-jar", "apache-rat.jar" ]

CMD ["--help"]
