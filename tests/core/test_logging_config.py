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
import contextlib
import importlib
import logging
import os
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch

import pytest

from airflow.configuration import conf
from tests.test_utils.config import conf_vars

SETTINGS_FILE_VALID = """
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'airflow.task': {
            'format': '[%%(asctime)s] {{%%(filename)s:%%(lineno)d}} %%(levelname)s - %%(message)s'
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'airflow.task',
            'stream': 'ext://sys.stdout'
        },
        'task': {
            'class': 'logging.StreamHandler',
            'formatter': 'airflow.task',
            'stream': 'ext://sys.stdout'
        },
    },
    'loggers': {
        'airflow.task': {
            'handlers': ['task'],
            'level': 'INFO',
            'propagate': False,
        },
    }
}
"""

SETTINGS_FILE_INVALID = """
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'airflow.task': {
            'format': '[%%(asctime)s] {{%%(filename)s:%%(lineno)d}} %%(levelname)s - %%(message)s'
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'airflow.task',
            'stream': 'ext://sys.stdout'
        }
    },
    'loggers': {
        'airflow': {
            'handlers': ['file.handler'], # this handler does not exists
            'level': 'INFO',
            'propagate': False
        }
    }
}
"""

SETTINGS_FILE_EMPTY = """
# Other settings here
"""

SETTINGS_DEFAULT_NAME = 'custom_airflow_local_settings'


def reset_logging():
    """Reset Logging"""
    manager = logging.root.manager
    manager.disabled = logging.NOTSET
    airflow_loggers = [
        # pylint: disable=no-member
        logger
        for logger_name, logger in manager.loggerDict.items()
        if logger_name.startswith('airflow')
    ]
    for logger in airflow_loggers:  # pylint: disable=too-many-nested-blocks
        if isinstance(logger, logging.Logger):
            logger.setLevel(logging.NOTSET)
            logger.propagate = True
            logger.disabled = False
            logger.filters.clear()
            handlers = logger.handlers.copy()
            for handler in handlers:
                # Copied from `logging.shutdown`.
                try:
                    handler.acquire()
                    handler.flush()
                    handler.close()
                except (OSError, ValueError):
                    pass
                finally:
                    handler.release()
                logger.removeHandler(handler)


@contextlib.contextmanager
def settings_context(content, directory=None, name='LOGGING_CONFIG'):
    """
    Sets a settings file and puts it in the Python classpath

    :param content:
          The content of the settings file
    """
    initial_logging_config = os.environ.get("AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS", "")
    try:
        settings_root = tempfile.mkdtemp()
        filename = f"{SETTINGS_DEFAULT_NAME}.py"
        if directory:
            # Replace slashes by dots
            module = directory.replace('/', '.') + '.' + SETTINGS_DEFAULT_NAME + '.' + name

            # Create the directory structure
            dir_path = os.path.join(settings_root, directory)
            pathlib.Path(dir_path).mkdir(parents=True, exist_ok=True)

            # Add the __init__ for the directories
            # This is required for Python 2.7
            basedir = settings_root
            for part in directory.split('/'):
                open(os.path.join(basedir, '__init__.py'), 'w').close()
                basedir = os.path.join(basedir, part)
            open(os.path.join(basedir, '__init__.py'), 'w').close()

            settings_file = os.path.join(dir_path, filename)
        else:
            module = SETTINGS_DEFAULT_NAME + '.' + name
            settings_file = os.path.join(settings_root, filename)

        with open(settings_file, 'w') as handle:
            handle.writelines(content)
        sys.path.append(settings_root)

        # Using environment vars instead of conf_vars so value is accessible
        # to parent and child processes when using 'spawn' for multiprocessing.
        os.environ["AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS"] = module
        yield settings_file

    finally:
        os.environ["AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS"] = initial_logging_config
        sys.path.remove(settings_root)


class TestLoggingSettings(unittest.TestCase):
    # Make sure that the configure_logging is not cached
    def setUp(self):
        self.old_modules = dict(sys.modules)

    def tearDown(self):
        # Remove any new modules imported during the test run. This lets us
        # import the same source files for more than one test.
        from airflow.config_templates import airflow_local_settings
        from airflow.logging_config import configure_logging

        for mod in list(sys.modules):
            if mod not in self.old_modules:
                del sys.modules[mod]

        reset_logging()
        importlib.reload(airflow_local_settings)
        configure_logging()

    # When we try to load an invalid config file, we expect an error
    def test_loading_invalid_local_settings(self):
        from airflow.logging_config import configure_logging, log

        with settings_context(SETTINGS_FILE_INVALID):
            with patch.object(log, 'error') as mock_info:
                # Load config
                with pytest.raises(ValueError):
                    configure_logging()

                mock_info.assert_called_once_with(
                    'Unable to load the config, contains a configuration error.'
                )

    def test_loading_valid_complex_local_settings(self):
        # Test what happens when the config is somewhere in a subfolder
        module_structure = 'etc.airflow.config'
        dir_structure = module_structure.replace('.', '/')
        with settings_context(SETTINGS_FILE_VALID, dir_structure):
            from airflow.logging_config import configure_logging, log

            with patch.object(log, 'info') as mock_info:
                configure_logging()
                mock_info.assert_called_once_with(
                    'Successfully imported user-defined logging config from %s',
                    f'etc.airflow.config.{SETTINGS_DEFAULT_NAME}.LOGGING_CONFIG',
                )

    # When we try to load a valid config
    def test_loading_valid_local_settings(self):
        with settings_context(SETTINGS_FILE_VALID):
            from airflow.logging_config import configure_logging, log

            with patch.object(log, 'info') as mock_info:
                configure_logging()
                mock_info.assert_called_once_with(
                    'Successfully imported user-defined logging config from %s',
                    f'{SETTINGS_DEFAULT_NAME}.LOGGING_CONFIG',
                )

    # When we load an empty file, it should go to default
    def test_loading_no_local_settings(self):
        with settings_context(SETTINGS_FILE_EMPTY):
            from airflow.logging_config import configure_logging

            with pytest.raises(ImportError):
                configure_logging()

    # When the key is not available in the configuration
    def test_when_the_config_key_does_not_exists(self):
        from airflow import logging_config

        with conf_vars({('logging', 'logging_config_class'): None}):
            with patch.object(logging_config.log, 'debug') as mock_debug:
                logging_config.configure_logging()
                mock_debug.assert_any_call('Could not find key logging_config_class in config')

    # Just default
    def test_loading_local_settings_without_logging_config(self):
        from airflow.logging_config import configure_logging, log

        with patch.object(log, 'debug') as mock_info:
            configure_logging()
            mock_info.assert_called_once_with('Unable to load custom logging, using default config instead')

    def test_1_9_config(self):
        from airflow.logging_config import configure_logging

        with conf_vars({('logging', 'task_log_reader'): 'file.task'}):
            with pytest.warns(DeprecationWarning, match=r'file.task'):
                configure_logging()
            assert conf.get('logging', 'task_log_reader') == 'task'

    def test_loading_remote_logging_with_wasb_handler(self):
        """Test if logging can be configured successfully for Azure Blob Storage"""
        from airflow.config_templates import airflow_local_settings
        from airflow.logging_config import configure_logging
        from airflow.utils.log.wasb_task_handler import WasbTaskHandler

        with conf_vars(
            {
                ('logging', 'remote_logging'): 'True',
                ('logging', 'remote_log_conn_id'): 'some_wasb',
                ('logging', 'remote_base_log_folder'): 'wasb://some-folder',
            }
        ):
            importlib.reload(airflow_local_settings)
            configure_logging()

        logger = logging.getLogger('airflow.task')
        assert isinstance(logger.handlers[0], WasbTaskHandler)
