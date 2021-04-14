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
from airflow.api_connexion import security
from airflow.api_connexion.parameters import check_limit, format_parameters
from airflow.api_connexion.schemas.plugin_schema import PluginCollection, plugin_collection_schema
from airflow.plugins_manager import get_plugin_info
from airflow.security import permissions


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_PLUGIN)])
@format_parameters({'limit': check_limit})
def get_plugins(limit, offset=0):
    """Get plugins endpoint"""
    plugins_info = get_plugin_info()
    total_entries = len(plugins_info)
    plugins_info = plugins_info[offset:]
    plugins_info = plugins_info[:limit]
    return plugin_collection_schema.dump(PluginCollection(plugins=plugins_info, total_entries=total_entries))
