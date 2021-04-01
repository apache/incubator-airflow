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

from airflow.utils.docs import get_docs_url


def init_appbuilder_links(app):
    """Add links to Docs menu in navbar"""
    appbuilder = app.appbuilder
    appbuilder.add_link(name="Documentation", label="Documentation", href=get_docs_url(), category="Docs")
    appbuilder.add_link(
        name="Documentation", label="Airflow Website", href='https://airflow.apache.org', category="Docs"
    )
    appbuilder.add_link(
        name="Documentation", label="GitHub Repo", href='https://github.com/apache/airflow', category="Docs"
    )
    appbuilder.add_link(
        name="Documentation",
        label="REST API Reference (Swagger UI)",
        href='/api/v1./api/v1_swagger_ui_index',
        category="Docs",
    )
    appbuilder.add_link(
        name="Documentation", label="REST API Reference (Redoc)", href="RedocView.redoc", category='Docs'
    )
