#  Licensed to the Apache Software Foundation (ASF) under one   *
#  or more contributor license agreements.  See the NOTICE file *
#  distributed with this work for additional information        *
#  regarding copyright ownership.  The ASF licenses this file   *
#  to you under the Apache License, Version 2.0 (the            *
#  "License"); you may not use this file except in compliance   *
#  with the License.  You may obtain a copy of the License at   *
#                                                               *
#    http://www.apache.org/licenses/LICENSE-2.0                 *
#                                                               *
#  Unless required by applicable law or agreed to in writing,   *
#  software distributed under the License is distributed on an  *
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY       *
#  KIND, either express or implied.  See the License for the    *
#  specific language governing permissions and limitations      *
#  under the License.                                           *

FROM python:3.7.0-slim-stretch

ENV SLUGIFY_USES_TEXT_UNIDECODE=yes

COPY airflow.tar.gz /tmp/airflow.tar.gz
COPY airflow-test-env-init.sh /tmp/airflow-test-env-init.sh
COPY bootstrap.sh /bootstrap.sh

RUN apt-get update && \
    apt-get install --no-install-recommends -y build-essential && \
    pip install --upgrade pip && \
    pip install -U setuptools && \
    pip install kubernetes && \
    pip install cryptography && \
    pip install psycopg2-binary && \
    pip install /tmp/airflow.tar.gz && \
    chmod +x /bootstrap.sh && \
    apt-get remove --purge -y build-essential && \
    apt-get autoremove --purge -y && \
    rm -rf /var/lib/apt/lists/* \

ENTRYPOINT ["/bootstrap.sh"]
