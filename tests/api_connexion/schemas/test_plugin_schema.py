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

import unittest

from airflow.api_connexion.schemas.plugin_schema import (
    PluginCollection,
    plugin_collection_schema,
    plugin_schema,
)
from airflow.plugins_manager import AirflowPlugin


class TestPluginBase(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_plugin = AirflowPlugin()
        self.mock_plugin.name = "test_plugin"

        self.mock_plugin_2 = AirflowPlugin()
        self.mock_plugin_2.name = "test_plugin_2"


class TestPluginSchema(TestPluginBase):
    def test_serialize(self):
        deserialized_plugin = plugin_schema.dump(self.mock_plugin)
        assert deserialized_plugin == {
            'appbuilder_menu_items': [],
            'appbuilder_views': [],
            'executors': [],
            'flask_blueprints': [],
            'global_operator_extra_links': [],
            'hooks': [],
            'macros': [],
            'operator_extra_links': [],
            'source': None,
            'name': 'test_plugin',
        }


class TestPluginCollectionSchema(TestPluginBase):
    def test_serialize(self):
        plugins = [self.mock_plugin, self.mock_plugin_2]

        deserialized = plugin_collection_schema.dump(PluginCollection(plugins=plugins, total_entries=2))
        assert deserialized == {
            'plugins': [
                {
                    'appbuilder_menu_items': [],
                    'appbuilder_views': [],
                    'executors': [],
                    'flask_blueprints': [],
                    'global_operator_extra_links': [],
                    'hooks': [],
                    'macros': [],
                    'operator_extra_links': [],
                    'source': None,
                    'name': 'test_plugin',
                },
                {
                    'appbuilder_menu_items': [],
                    'appbuilder_views': [],
                    'executors': [],
                    'flask_blueprints': [],
                    'global_operator_extra_links': [],
                    'hooks': [],
                    'macros': [],
                    'operator_extra_links': [],
                    'source': None,
                    'name': 'test_plugin_2',
                },
            ],
            'total_entries': 2,
        }
