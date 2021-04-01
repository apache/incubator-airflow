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
# Dockerfile - kdc-server
#
# see docker-compose.yml

FROM centos:7

# build environment
WORKDIR /root/

# Dev stuff
# hadolint ignore=DL3033
RUN yum -y install curl wget && yum clean all

# python
RUN curl "https://bootstrap.pypa.io/get-pip.py" -o /tmp/get-pip.py && \
    python /tmp/get-pip.py && \
    rm /tmp/get-pip.py

# supervisord
RUN pip install --no-cache-dir supervisor==3.3.3 && \
    mkdir -p /var/log/supervisord/

# kerberos server
# hadolint ignore=DL3033
RUN yum -y install ntp krb5-server krb5-libs && yum clean all

# kerberos server configuration
ENV KRB5_CONFIG=/etc/krb5.conf
ENV KRB5_KDC_PROFILE=/var/kerberos/krb5kdc/kdc.conf
RUN mkdir -pv /var/kerberos/krb5kdc/
COPY kdc.conf /var/kerberos/krb5kdc/kdc.conf
COPY kadm5.acl /var/kerberos/krb5kdc/kadm5.acl
COPY krb5.conf /etc/krb5.conf
RUN mkdir -pv /var/log/kerberos/ && \
    touch /var/log/kerberos/kadmin.log && \
    touch /var/log/kerberos/krb5lib.log && \
    touch /var/log/kerberos/krb5.log && \
    kdb5_util -r EXAMPLE.COM -P krb5 create -s

# kerberos utils
COPY utils /opt/kerberos-utils/

# supervisord configuration
COPY supervisord.conf /etc/supervisord.conf

# entrypoint
COPY entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]

LABEL org.apache.airflow.component="krb5-kdc-server" \
    org.apache.airflow.krb5-kdc-server.core.version="krb5" \
    org.apache.airflow.airflow_bats.version="${AIRFLOW_KRB5KDCSERVER_VERSION}" \
    org.apache.airflow.commit_sha="${COMMIT_SHA}" \
    maintainer="Apache Airflow Community <dev@airflow.apache.org>"

# when container is starting
CMD ["/usr/bin/supervisord", "-n", "-c", "/etc/supervisord.conf"]
