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
import importlib
import logging
import sys
import unittest
from unittest import mock

from airflow.hooks.base import BaseHook
from airflow.plugins_manager import AirflowPlugin
from airflow.www import app as application
from tests.test_utils.mock_plugins import mock_plugin_manager

py39 = sys.version_info >= (3, 9)
importlib_metadata = 'importlib.metadata' if py39 else 'importlib_metadata'


class TestPluginsRBAC(unittest.TestCase):
    def setUp(self):
        self.app = application.create_app(testing=True)
        self.appbuilder = self.app.appbuilder  # pylint: disable=no-member

    def test_flaskappbuilder_views(self):
        from tests.plugins.test_plugin import v_appbuilder_package

        appbuilder_class_name = str(v_appbuilder_package['view'].__class__.__name__)
        plugin_views = [
            view for view in self.appbuilder.baseviews if view.blueprint.name == appbuilder_class_name
        ]

        assert len(plugin_views) == 1

        # view should have a menu item matching category of v_appbuilder_package
        links = [
            menu_item
            for menu_item in self.appbuilder.menu.menu
            if menu_item.name == v_appbuilder_package['category']
        ]

        assert len(links) == 1

        # menu link should also have a link matching the name of the package.
        link = links[0]
        assert link.name == v_appbuilder_package['category']
        assert link.childs[0].name == v_appbuilder_package['name']

    def test_flaskappbuilder_nomenu_views(self):
        from tests.plugins.test_plugin import v_nomenu_appbuilder_package

        class AirflowNoMenuViewsPlugin(AirflowPlugin):
            appbuilder_views = [v_nomenu_appbuilder_package]

        appbuilder_class_name = str(v_nomenu_appbuilder_package['view'].__class__.__name__)

        with mock_plugin_manager(plugins=[AirflowNoMenuViewsPlugin()]):
            appbuilder = application.create_app(testing=True).appbuilder  # pylint: disable=no-member

            plugin_views = [
                view for view in appbuilder.baseviews if view.blueprint.name == appbuilder_class_name
            ]

            assert len(plugin_views) == 1

    def test_flaskappbuilder_menu_links(self):
        from tests.plugins.test_plugin import appbuilder_mitem, appbuilder_mitem_toplevel

        # menu item (category) should exist matching appbuilder_mitem.category
        categories = [
            menu_item
            for menu_item in self.appbuilder.menu.menu
            if menu_item.name == appbuilder_mitem['category']
        ]
        assert len(categories) == 1

        # menu link should be a child in the category
        category = categories[0]
        assert category.name == appbuilder_mitem['category']
        assert category.childs[0].name == appbuilder_mitem['name']
        assert category.childs[0].href == appbuilder_mitem['href']

        # a top level link isn't nested in a category
        top_levels = [
            menu_item
            for menu_item in self.appbuilder.menu.menu
            if menu_item.name == appbuilder_mitem_toplevel['name']
        ]
        assert len(top_levels) == 1
        link = top_levels[0]
        assert link.href == appbuilder_mitem_toplevel['href']
        assert link.label == appbuilder_mitem_toplevel['label']

    def test_app_blueprints(self):
        from tests.plugins.test_plugin import bp

        # Blueprint should be present in the app
        assert 'test_plugin' in self.app.blueprints
        assert self.app.blueprints['test_plugin'].name == bp.name


class TestPluginsManager:
    def test_no_log_when_no_plugins(self, caplog):

        with mock_plugin_manager(plugins=[]):
            from airflow import plugins_manager

            plugins_manager.ensure_plugins_loaded()

        assert caplog.record_tuples == []

    def test_should_load_plugins_from_property(self, caplog):
        class AirflowTestPropertyPlugin(AirflowPlugin):
            name = "test_property_plugin"

            @property
            def hooks(self):
                class TestPropertyHook(BaseHook):
                    pass

                return [TestPropertyHook]

        with mock_plugin_manager(plugins=[AirflowTestPropertyPlugin()]):
            from airflow import plugins_manager

            caplog.set_level(logging.DEBUG, "airflow.plugins_manager")
            plugins_manager.ensure_plugins_loaded()

            assert 'AirflowTestPropertyPlugin' in str(plugins_manager.plugins)
            assert 'TestPropertyHook' in str(plugins_manager.registered_hooks)

        assert caplog.records[-1].levelname == 'DEBUG'
        assert caplog.records[-1].msg == 'Loading %d plugin(s) took %.2f seconds'

    def test_should_warning_about_incompatible_plugins(self, caplog):
        class AirflowAdminViewsPlugin(AirflowPlugin):
            name = "test_admin_views_plugin"

            admin_views = [mock.MagicMock()]

        class AirflowAdminMenuLinksPlugin(AirflowPlugin):
            name = "test_menu_links_plugin"

            menu_links = [mock.MagicMock()]

        with mock_plugin_manager(
            plugins=[AirflowAdminViewsPlugin(), AirflowAdminMenuLinksPlugin()]
        ), caplog.at_level(logging.WARNING, logger='airflow.plugins_manager'):
            from airflow import plugins_manager

            plugins_manager.initialize_web_ui_plugins()

        assert caplog.record_tuples == [
            (
                "airflow.plugins_manager",
                logging.WARNING,
                "Plugin 'test_admin_views_plugin' may not be compatible with the current Airflow version. "
                "Please contact the author of the plugin.",
            ),
            (
                "airflow.plugins_manager",
                logging.WARNING,
                "Plugin 'test_menu_links_plugin' may not be compatible with the current Airflow version. "
                "Please contact the author of the plugin.",
            ),
        ]

    def test_should_not_warning_about_fab_plugins(self, caplog):
        class AirflowAdminViewsPlugin(AirflowPlugin):
            name = "test_admin_views_plugin"

            appbuilder_views = [mock.MagicMock()]

        class AirflowAdminMenuLinksPlugin(AirflowPlugin):
            name = "test_menu_links_plugin"

            appbuilder_menu_items = [mock.MagicMock()]

        with mock_plugin_manager(
            plugins=[AirflowAdminViewsPlugin(), AirflowAdminMenuLinksPlugin()]
        ), caplog.at_level(logging.WARNING, logger='airflow.plugins_manager'):
            from airflow import plugins_manager

            plugins_manager.initialize_web_ui_plugins()

        assert caplog.record_tuples == []

    def test_should_not_warning_about_fab_and_flask_admin_plugins(self, caplog):
        class AirflowAdminViewsPlugin(AirflowPlugin):
            name = "test_admin_views_plugin"

            admin_views = [mock.MagicMock()]
            appbuilder_views = [mock.MagicMock()]

        class AirflowAdminMenuLinksPlugin(AirflowPlugin):
            name = "test_menu_links_plugin"

            menu_links = [mock.MagicMock()]
            appbuilder_menu_items = [mock.MagicMock()]

        with mock_plugin_manager(
            plugins=[AirflowAdminViewsPlugin(), AirflowAdminMenuLinksPlugin()]
        ), caplog.at_level(logging.WARNING, logger='airflow.plugins_manager'):
            from airflow import plugins_manager

            plugins_manager.initialize_web_ui_plugins()

        assert caplog.record_tuples == []

    def test_entrypoint_plugin_errors_dont_raise_exceptions(self, caplog):
        """
        Test that Airflow does not raise an error if there is any Exception because of a plugin.
        """
        from airflow.plugins_manager import import_errors, load_entrypoint_plugins

        mock_dist = mock.Mock()

        mock_entrypoint = mock.Mock()
        mock_entrypoint.name = 'test-entrypoint'
        mock_entrypoint.group = 'airflow.plugins'
        mock_entrypoint.module = 'test.plugins.test_plugins_manager'
        mock_entrypoint.load.side_effect = ImportError('my_fake_module not found')
        mock_dist.entry_points = [mock_entrypoint]

        with mock.patch(f'{importlib_metadata}.distributions', return_value=[mock_dist]), caplog.at_level(
            logging.ERROR, logger='airflow.plugins_manager'
        ):
            load_entrypoint_plugins()

            received_logs = caplog.text
            # Assert Traceback is shown too
            assert "Traceback (most recent call last):" in received_logs
            assert "my_fake_module not found" in received_logs
            assert "Failed to import plugin test-entrypoint" in received_logs
            assert ("test.plugins.test_plugins_manager", "my_fake_module not found") in import_errors.items()

    def test_registering_plugin_macros(self, request):
        """
        Tests whether macros that originate from plugins are being registered correctly.
        """
        from airflow import macros
        from airflow.plugins_manager import integrate_macros_plugins

        def cleanup_macros():
            """Reloads the airflow.macros module such that the symbol table is reset after the test."""
            # We're explicitly deleting the module from sys.modules and importing it again
            # using import_module() as opposed to using importlib.reload() because the latter
            # does not undo the changes to the airflow.macros module that are being caused by
            # invoking integrate_macros_plugins()
            del sys.modules['airflow.macros']
            importlib.import_module('airflow.macros')

        request.addfinalizer(cleanup_macros)

        def custom_macro():
            return 'foo'

        class MacroPlugin(AirflowPlugin):
            name = 'macro_plugin'
            macros = [custom_macro]

        with mock_plugin_manager(plugins=[MacroPlugin()]):
            # Ensure the macros for the plugin have been integrated.
            integrate_macros_plugins()
            # Test whether the modules have been created as expected.
            plugin_macros = importlib.import_module(f"airflow.macros.{MacroPlugin.name}")
            for macro in MacroPlugin.macros:
                # Verify that the macros added by the plugin are being set correctly
                # on the plugin's macro module.
                assert hasattr(plugin_macros, macro.__name__)
            # Verify that the symbol table in airflow.macros has been updated with an entry for
            # this plugin, this is necessary in order to allow the plugin's macros to be used when
            # rendering templates.
            assert hasattr(macros, MacroPlugin.name)


class TestPluginsDirectorySource(unittest.TestCase):
    def test_should_return_correct_path_name(self):
        from airflow import plugins_manager

        source = plugins_manager.PluginsDirectorySource(__file__)
        assert "test_plugins_manager.py" == source.path
        assert "$PLUGINS_FOLDER/test_plugins_manager.py" == str(source)
        assert "<em>$PLUGINS_FOLDER/</em>test_plugins_manager.py" == source.__html__()


class TestEntryPointSource:
    def test_should_return_correct_source_details(self):
        from airflow import plugins_manager

        mock_entrypoint = mock.Mock()
        mock_entrypoint.name = 'test-entrypoint-plugin'
        mock_entrypoint.module = 'module_name_plugin'

        mock_dist = mock.Mock()
        mock_dist.metadata = {'name': 'test-entrypoint-plugin'}
        mock_dist.version = '1.0.0'
        mock_dist.entry_points = [mock_entrypoint]

        with mock.patch(f'{importlib_metadata}.distributions', return_value=[mock_dist]):
            plugins_manager.load_entrypoint_plugins()

        source = plugins_manager.EntryPointSource(mock_entrypoint, mock_dist)
        assert str(mock_entrypoint) == source.entrypoint
        assert "test-entrypoint-plugin==1.0.0: " + str(mock_entrypoint) == str(source)
        assert "<em>test-entrypoint-plugin==1.0.0:</em> " + str(mock_entrypoint) == source.__html__()
