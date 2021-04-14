# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
ARG BASE_AIRFLOW_IMAGE

FROM ${BASE_AIRFLOW_IMAGE}

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

USER 0

# Install Java
RUN mkdir -pv /usr/share/man/man1 \
    && mkdir -pv /usr/share/man/man7 \
    && curl -fsSL https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public | apt-key add - \
    && echo 'deb https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/ buster main' > \
        /etc/apt/sources.list.d/adoptopenjdk.list \
    && apt-get update \
    && apt-get install --no-install-recommends -y \
      adoptopenjdk-8-hotspot-jre \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/adoptopenjdk-8-hotspot-jre-amd64

RUN mkdir -p /opt/spark/jars

# Install Apache Hadoop
ARG HADOOP_VERSION=2.10.1
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=/etc/hadoop
ENV MULTIHOMED_NETWORK=1
ENV USER=root

RUN HADOOP_URL="https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz" \
    && curl 'https://dist.apache.org/repos/dist/release/hadoop/common/KEYS' | gpg --import - \
    && curl -fSL "$HADOOP_URL" -o /tmp/hadoop.tar.gz \
    && curl -fSL "$HADOOP_URL.asc" -o /tmp/hadoop.tar.gz.asc \
    && gpg --verify /tmp/hadoop.tar.gz.asc \
    && tar -xvf /tmp/hadoop.tar.gz -C "${HADOOP_HOME}" --strip-components=1 \
    && rm /tmp/hadoop.tar.gz /tmp/hadoop.tar.gz.asc \
    && ln -s "${HADOOP_HOME}/etc/hadoop" /etc/hadoop \
    && mkdir "${HADOOP_HOME}/logs" \
    && mkdir /hadoop-data

ENV PATH="$HADOOP_HOME/bin/:$PATH"

# Install Apache Hive
ARG HIVE_VERSION=2.3.7
ENV HIVE_HOME=/opt/hive
ENV HIVE_CONF_DIR=/etc/hive

RUN HIVE_URL="https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz" \
    && curl -fSL 'https://downloads.apache.org/hive/KEYS' | gpg --import - \
    && curl -fSL "$HIVE_URL" -o /tmp/hive.tar.gz \
    && curl -fSL "$HIVE_URL.asc" -o /tmp/hive.tar.gz.asc \
    && gpg --verify /tmp/hive.tar.gz.asc \
    && mkdir -p "${HIVE_HOME}" \
    && tar -xf /tmp/hive.tar.gz -C "${HIVE_HOME}" --strip-components=1 \
    && rm /tmp/hive.tar.gz /tmp/hive.tar.gz.asc \
    && ln -s "${HIVE_HOME}/etc/hive" "${HIVE_CONF_DIR}" \
    && mkdir "${HIVE_HOME}/logs"

ENV PATH="$HIVE_HOME/bin/:$PATH"

# Install GCS connector for Apache Hadoop
# See: https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage
ARG GCS_VARIANT="hadoop2"
ARG GCS_VERSION="2.1.5"

RUN GCS_JAR_PATH="/opt/spark/jars/gcs-connector-${GCS_VARIANT}-${GCS_VERSION}.jar" \
    && GCS_JAR_URL="https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-${GCS_VARIANT}-${GCS_VERSION}.jar" \
    && curl "${GCS_JAR_URL}" -o "${GCS_JAR_PATH}"

ENV HADOOP_CLASSPATH="/opt/spark/jars/gcs-connector-${GCS_VARIANT}-${GCS_VERSION}.jar:$HADOOP_CLASSPATH"

USER ${AIRFLOW_UID}
