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

import functools
import json
import logging
import multiprocessing
import os
import pathlib
import re
import shlex
import subprocess
import sys
import warnings
from base64 import b64encode
from collections import OrderedDict

# Ignored Mypy on configparser because it thinks the configparser module has no _UNSET attribute
from configparser import _UNSET, ConfigParser, NoOptionError, NoSectionError  # type: ignore
from json.decoder import JSONDecodeError
from typing import Dict, List, Optional, Union

from airflow.exceptions import AirflowConfigException
from airflow.secrets import DEFAULT_SECRETS_SEARCH_PATH, BaseSecretsBackend
from airflow.utils.module_loading import import_string

log = logging.getLogger(__name__)

# show Airflow's deprecation warnings
if not sys.warnoptions:
    warnings.filterwarnings(action='default', category=DeprecationWarning, module='airflow')
    warnings.filterwarnings(action='default', category=PendingDeprecationWarning, module='airflow')


def expand_env_var(env_var):
    """
    Expands (potentially nested) env vars by repeatedly applying
    `expandvars` and `expanduser` until interpolation stops having
    any effect.
    """
    if not env_var:
        return env_var
    while True:
        interpolated = os.path.expanduser(os.path.expandvars(str(env_var)))
        if interpolated == env_var:
            return interpolated
        else:
            env_var = interpolated


def run_command(command):
    """Runs command and returns stdout"""
    process = subprocess.Popen(
        shlex.split(command), stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True
    )
    output, stderr = [stream.decode(sys.getdefaultencoding(), 'ignore') for stream in process.communicate()]

    if process.returncode != 0:
        raise AirflowConfigException(
            f"Cannot execute {command}. Error code is: {process.returncode}. "
            f"Output: {output}, Stderr: {stderr}"
        )

    return output


def _get_config_value_from_secret_backend(config_key):
    """Get Config option values from Secret Backend"""
    secrets_client = get_custom_secret_backend()
    if not secrets_client:
        return None
    return secrets_client.get_config(config_key)


def _default_config_file_path(file_name: str):
    templates_dir = os.path.join(os.path.dirname(__file__), 'config_templates')
    return os.path.join(templates_dir, file_name)


def default_config_yaml() -> dict:
    """
    Read Airflow configs from YAML file

    :return: Python dictionary containing configs & their info
    """
    import airflow.utils.yaml as yaml

    with open(_default_config_file_path('config.yml')) as config_file:
        return yaml.safe_load(config_file)


class AirflowConfigParser(ConfigParser):  # pylint: disable=too-many-ancestors
    """Custom Airflow Configparser supporting defaults and deprecated options"""

    # These configuration elements can be fetched as the stdout of commands
    # following the "{section}__{name}__cmd" pattern, the idea behind this
    # is to not store password on boxes in text files.
    # These configs can also be fetched from Secrets backend
    # following the "{section}__{name}__secret" pattern
    sensitive_config_values = {
        ('core', 'sql_alchemy_conn'),
        ('core', 'fernet_key'),
        ('celery', 'broker_url'),
        ('celery', 'flower_basic_auth'),
        ('celery', 'result_backend'),
        ('atlas', 'password'),
        ('smtp', 'smtp_password'),
        ('webserver', 'secret_key'),
    }

    # A mapping of (new section, new option) -> (old section, old option, since_version).
    # When reading new option, the old option will be checked to see if it exists. If it does a
    # DeprecationWarning will be issued and the old option will be used instead
    deprecated_options = {
        ('celery', 'worker_precheck'): ('core', 'worker_precheck', '2.0.0'),
        ('logging', 'base_log_folder'): ('core', 'base_log_folder', '2.0.0'),
        ('logging', 'remote_logging'): ('core', 'remote_logging', '2.0.0'),
        ('logging', 'remote_log_conn_id'): ('core', 'remote_log_conn_id', '2.0.0'),
        ('logging', 'remote_base_log_folder'): ('core', 'remote_base_log_folder', '2.0.0'),
        ('logging', 'encrypt_s3_logs'): ('core', 'encrypt_s3_logs', '2.0.0'),
        ('logging', 'logging_level'): ('core', 'logging_level', '2.0.0'),
        ('logging', 'fab_logging_level'): ('core', 'fab_logging_level', '2.0.0'),
        ('logging', 'logging_config_class'): ('core', 'logging_config_class', '2.0.0'),
        ('logging', 'colored_console_log'): ('core', 'colored_console_log', '2.0.0'),
        ('logging', 'colored_log_format'): ('core', 'colored_log_format', '2.0.0'),
        ('logging', 'colored_formatter_class'): ('core', 'colored_formatter_class', '2.0.0'),
        ('logging', 'log_format'): ('core', 'log_format', '2.0.0'),
        ('logging', 'simple_log_format'): ('core', 'simple_log_format', '2.0.0'),
        ('logging', 'task_log_prefix_template'): ('core', 'task_log_prefix_template', '2.0.0'),
        ('logging', 'log_filename_template'): ('core', 'log_filename_template', '2.0.0'),
        ('logging', 'log_processor_filename_template'): ('core', 'log_processor_filename_template', '2.0.0'),
        ('logging', 'dag_processor_manager_log_location'): (
            'core',
            'dag_processor_manager_log_location',
            '2.0.0',
        ),
        ('logging', 'task_log_reader'): ('core', 'task_log_reader', '2.0.0'),
        ('metrics', 'statsd_on'): ('scheduler', 'statsd_on', '2.0.0'),
        ('metrics', 'statsd_host'): ('scheduler', 'statsd_host', '2.0.0'),
        ('metrics', 'statsd_port'): ('scheduler', 'statsd_port', '2.0.0'),
        ('metrics', 'statsd_prefix'): ('scheduler', 'statsd_prefix', '2.0.0'),
        ('metrics', 'statsd_allow_list'): ('scheduler', 'statsd_allow_list', '2.0.0'),
        ('metrics', 'stat_name_handler'): ('scheduler', 'stat_name_handler', '2.0.0'),
        ('metrics', 'statsd_datadog_enabled'): ('scheduler', 'statsd_datadog_enabled', '2.0.0'),
        ('metrics', 'statsd_datadog_tags'): ('scheduler', 'statsd_datadog_tags', '2.0.0'),
        ('metrics', 'statsd_custom_client_path'): ('scheduler', 'statsd_custom_client_path', '2.0.0'),
        ('scheduler', 'parsing_processes'): ('scheduler', 'max_threads', '1.10.14'),
    }

    # A mapping of old default values that we want to change and warn the user
    # about. Mapping of section -> setting -> { old, replace, by_version }
    deprecated_values = {
        'core': {
            'hostname_callable': (re.compile(r':'), r'.', '2.1'),
        },
        'webserver': {
            'navbar_color': (re.compile(r'\A#007A87\Z', re.IGNORECASE), '#fff', '2.1'),
        },
        'email': {
            'email_backend': (
                re.compile(r'^airflow\.contrib\.utils\.sendgrid\.send_email$'),
                r'airflow.providers.sendgrid.utils.emailer.send_email',
                '2.1',
            ),
        },
    }

    # This method transforms option names on every read, get, or set operation.
    # This changes from the default behaviour of ConfigParser from lowercasing
    # to instead be case-preserving
    def optionxform(self, optionstr: str) -> str:
        return optionstr

    def __init__(self, default_config=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.airflow_defaults = ConfigParser(*args, **kwargs)
        if default_config is not None:
            self.airflow_defaults.read_string(default_config)

        self.is_validated = False

    def validate(self):

        self._validate_config_dependencies()

        for section, replacement in self.deprecated_values.items():
            for name, info in replacement.items():
                old, new, version = info
                current_value = self.get(section, name, fallback=None)
                if self._using_old_value(old, current_value):
                    new_value = re.sub(old, new, current_value)
                    self._update_env_var(section=section, name=name, new_value=new_value)
                    self._create_future_warning(
                        name=name,
                        section=section,
                        current_value=current_value,
                        new_value=new_value,
                        version=version,
                    )

        self.is_validated = True

    def _validate_config_dependencies(self):
        """
        Validate that config values aren't invalid given other config values
        or system-level limitations and requirements.
        """
        is_executor_without_sqlite_support = self.get("core", "executor") not in (
            'DebugExecutor',
            'SequentialExecutor',
        )
        is_sqlite = "sqlite" in self.get('core', 'sql_alchemy_conn')
        if is_sqlite and is_executor_without_sqlite_support:
            raise AirflowConfigException(f"error: cannot use sqlite with the {self.get('core', 'executor')}")
        if is_sqlite:
            import sqlite3
            from distutils.version import StrictVersion

            from airflow.utils.docs import get_docs_url

            # Some of the features in storing rendered fields require sqlite version >= 3.15.0
            min_sqlite_version = '3.15.0'
            if StrictVersion(sqlite3.sqlite_version) < StrictVersion(min_sqlite_version):
                raise AirflowConfigException(
                    f"error: sqlite C library version too old (< {min_sqlite_version}). "
                    f"See {get_docs_url('howto/set-up-database.rst#setting-up-a-sqlite-database')}"
                )

        if self.has_option('core', 'mp_start_method'):
            mp_start_method = self.get('core', 'mp_start_method')
            start_method_options = multiprocessing.get_all_start_methods()

            if mp_start_method not in start_method_options:
                raise AirflowConfigException(
                    "mp_start_method should not be "
                    + mp_start_method
                    + ". Possible values are "
                    + ", ".join(start_method_options)
                )

    def _using_old_value(self, old, current_value):  # noqa
        return old.search(current_value) is not None

    def _update_env_var(self, section, name, new_value):
        # Make sure the env var option is removed, otherwise it
        # would be read and used instead of the value we set
        env_var = self._env_var_name(section, name)
        os.environ.pop(env_var, None)
        self.set(section, name, new_value)

    @staticmethod
    def _create_future_warning(name, section, current_value, new_value, version):
        warnings.warn(
            'The {name} setting in [{section}] has the old default value '
            'of {current_value!r}. This value has been changed to {new_value!r} in the '
            'running config, but please update your config before Apache '
            'Airflow {version}.'.format(
                name=name, section=section, current_value=current_value, new_value=new_value, version=version
            ),
            FutureWarning,
        )

    ENV_VAR_PREFIX = 'AIRFLOW__'

    def _env_var_name(self, section: str, key: str) -> str:
        return f'{self.ENV_VAR_PREFIX}{section.upper()}__{key.upper()}'

    def _get_env_var_option(self, section, key):
        # must have format AIRFLOW__{SECTION}__{KEY} (note double underscore)
        env_var = self._env_var_name(section, key)
        if env_var in os.environ:
            return expand_env_var(os.environ[env_var])
        # alternatively AIRFLOW__{SECTION}__{KEY}_CMD (for a command)
        env_var_cmd = env_var + '_CMD'
        if env_var_cmd in os.environ:
            # if this is a valid command key...
            if (section, key) in self.sensitive_config_values:
                return run_command(os.environ[env_var_cmd])
        # alternatively AIRFLOW__{SECTION}__{KEY}_SECRET (to get from Secrets Backend)
        env_var_secret_path = env_var + '_SECRET'
        if env_var_secret_path in os.environ:
            # if this is a valid secret path...
            if (section, key) in self.sensitive_config_values:
                return _get_config_value_from_secret_backend(os.environ[env_var_secret_path])
        return None

    def _get_cmd_option(self, section, key):
        fallback_key = key + '_cmd'
        # if this is a valid command key...
        if (section, key) in self.sensitive_config_values:
            if super().has_option(section, fallback_key):
                command = super().get(section, fallback_key)
                return run_command(command)
        return None

    def _get_secret_option(self, section, key):
        """Get Config option values from Secret Backend"""
        fallback_key = key + '_secret'
        # if this is a valid secret key...
        if (section, key) in self.sensitive_config_values:
            if super().has_option(section, fallback_key):
                secrets_path = super().get(section, fallback_key)
                return _get_config_value_from_secret_backend(secrets_path)
        return None

    def get(self, section, key, **kwargs):
        section = str(section).lower()
        key = str(key).lower()

        deprecated_section, deprecated_key, _ = self.deprecated_options.get(
            (section, key), (None, None, None)
        )

        option = self._get_environment_variables(deprecated_key, deprecated_section, key, section)
        if option is not None:
            return option

        option = self._get_option_from_config_file(deprecated_key, deprecated_section, key, kwargs, section)
        if option is not None:
            return option

        option = self._get_option_from_commands(deprecated_key, deprecated_section, key, section)
        if option is not None:
            return option

        option = self._get_option_from_secrets(deprecated_key, deprecated_section, key, section)
        if option is not None:
            return option

        return self._get_option_from_default_config(section, key, **kwargs)

    def _get_option_from_default_config(self, section, key, **kwargs):
        # ...then the default config
        if self.airflow_defaults.has_option(section, key) or 'fallback' in kwargs:
            return expand_env_var(self.airflow_defaults.get(section, key, **kwargs))

        else:
            log.warning("section/key [%s/%s] not found in config", section, key)

            raise AirflowConfigException(f"section/key [{section}/{key}] not found in config")

    def _get_option_from_secrets(self, deprecated_key, deprecated_section, key, section):
        # ...then from secret backends
        option = self._get_secret_option(section, key)
        if option:
            return option
        if deprecated_section:
            option = self._get_secret_option(deprecated_section, deprecated_key)
            if option:
                self._warn_deprecate(section, key, deprecated_section, deprecated_key)
                return option
        return None

    def _get_option_from_commands(self, deprecated_key, deprecated_section, key, section):
        # ...then commands
        option = self._get_cmd_option(section, key)
        if option:
            return option
        if deprecated_section:
            option = self._get_cmd_option(deprecated_section, deprecated_key)
            if option:
                self._warn_deprecate(section, key, deprecated_section, deprecated_key)
                return option
        return None

    def _get_option_from_config_file(self, deprecated_key, deprecated_section, key, kwargs, section):
        # ...then the config file
        if super().has_option(section, key):
            # Use the parent's methods to get the actual config here to be able to
            # separate the config from default config.
            return expand_env_var(super().get(section, key, **kwargs))
        if deprecated_section:
            if super().has_option(deprecated_section, deprecated_key):
                self._warn_deprecate(section, key, deprecated_section, deprecated_key)
                return expand_env_var(super().get(deprecated_section, deprecated_key, **kwargs))
        return None

    def _get_environment_variables(self, deprecated_key, deprecated_section, key, section):
        # first check environment variables
        option = self._get_env_var_option(section, key)
        if option is not None:
            return option
        if deprecated_section:
            option = self._get_env_var_option(deprecated_section, deprecated_key)
            if option is not None:
                self._warn_deprecate(section, key, deprecated_section, deprecated_key)
                return option
        return None

    def getboolean(self, section, key, **kwargs):
        val = str(self.get(section, key, **kwargs)).lower().strip()
        if '#' in val:
            val = val.split('#')[0].strip()
        if val in ('t', 'true', '1'):
            return True
        elif val in ('f', 'false', '0'):
            return False
        else:
            raise AirflowConfigException(
                f'Failed to convert value to bool. Please check "{key}" key in "{section}" section. '
                f'Current value: "{val}".'
            )

    def getint(self, section, key, **kwargs):
        val = self.get(section, key, **kwargs)

        try:
            return int(val)
        except ValueError:
            raise AirflowConfigException(
                f'Failed to convert value to int. Please check "{key}" key in "{section}" section. '
                f'Current value: "{val}".'
            )

    def getfloat(self, section, key, **kwargs):
        val = self.get(section, key, **kwargs)

        try:
            return float(val)
        except ValueError:
            raise AirflowConfigException(
                f'Failed to convert value to float. Please check "{key}" key in "{section}" section. '
                f'Current value: "{val}".'
            )

    def getimport(self, section, key, **kwargs):  # noqa
        """
        Reads options, imports the full qualified name, and returns the object.

        In case of failure, it throws an exception a clear message with the key aad the section names

        :return: The object or None, if the option is empty
        """
        full_qualified_path = conf.get(section=section, key=key, **kwargs)
        if not full_qualified_path:
            return None

        try:
            return import_string(full_qualified_path)
        except ImportError as e:
            log.error(e)
            raise AirflowConfigException(
                f'The object could not be loaded. Please check "{key}" key in "{section}" section. '
                f'Current value: "{full_qualified_path}".'
            )

    def read(self, filenames, encoding=None):
        super().read(filenames=filenames, encoding=encoding)

    def read_dict(self, dictionary, source='<dict>'):
        super().read_dict(dictionary=dictionary, source=source)

    def has_option(self, section, option):
        try:
            # Using self.get() to avoid reimplementing the priority order
            # of config variables (env, config, cmd, defaults)
            # UNSET to avoid logging a warning about missing values
            self.get(section, option, fallback=_UNSET)
            return True
        except (NoOptionError, NoSectionError):
            return False

    def remove_option(self, section, option, remove_default=True):
        """
        Remove an option if it exists in config from a file or
        default config. If both of config have the same option, this removes
        the option in both configs unless remove_default=False.
        """
        if super().has_option(section, option):
            super().remove_option(section, option)

        if self.airflow_defaults.has_option(section, option) and remove_default:
            self.airflow_defaults.remove_option(section, option)

    def getsection(self, section: str) -> Optional[Dict[str, Union[str, int, float, bool]]]:
        """
        Returns the section as a dict. Values are converted to int, float, bool
        as required.

        :param section: section from the config
        :rtype: dict
        """
        if not self.has_section(section) and not self.airflow_defaults.has_section(section):
            return None

        if self.airflow_defaults.has_section(section):
            _section = OrderedDict(self.airflow_defaults.items(section))
        else:
            _section = OrderedDict()

        if self.has_section(section):
            _section.update(OrderedDict(self.items(section)))

        section_prefix = self._env_var_name(section, '')
        for env_var in sorted(os.environ.keys()):
            if env_var.startswith(section_prefix):
                key = env_var.replace(section_prefix, '')
                if key.endswith("_CMD"):
                    key = key[:-4]
                key = key.lower()
                _section[key] = self._get_env_var_option(section, key)

        for key, val in _section.items():
            try:
                val = int(val)
            except ValueError:
                try:
                    val = float(val)
                except ValueError:
                    if val.lower() in ('t', 'true'):
                        val = True
                    elif val.lower() in ('f', 'false'):
                        val = False
            _section[key] = val
        return _section

    def write(self, fp, space_around_delimiters=True):
        # This is based on the configparser.RawConfigParser.write method code to add support for
        # reading options from environment variables.
        if space_around_delimiters:
            delimiter = f" {self._delimiters[0]} "
        else:
            delimiter = self._delimiters[0]
        if self._defaults:
            self._write_section(fp, self.default_section, self._defaults.items(), delimiter)
        for section in self._sections:
            self._write_section(fp, section, self.getsection(section).items(), delimiter)

    def as_dict(
        self,
        display_source=False,
        display_sensitive=False,
        raw=False,
        include_env=True,
        include_cmds=True,
        include_secret=True,
    ) -> Dict[str, Dict[str, str]]:
        """
        Returns the current configuration as an OrderedDict of OrderedDicts.

        :param display_source: If False, the option value is returned. If True,
            a tuple of (option_value, source) is returned. Source is either
            'airflow.cfg', 'default', 'env var', or 'cmd'.
        :type display_source: bool
        :param display_sensitive: If True, the values of options set by env
            vars and bash commands will be displayed. If False, those options
            are shown as '< hidden >'
        :type display_sensitive: bool
        :param raw: Should the values be output as interpolated values, or the
            "raw" form that can be fed back in to ConfigParser
        :type raw: bool
        :param include_env: Should the value of configuration from AIRFLOW__
            environment variables be included or not
        :type include_env: bool
        :param include_cmds: Should the result of calling any *_cmd config be
            set (True, default), or should the _cmd options be left as the
            command to run (False)
        :type include_cmds: bool
        :param include_secret: Should the result of calling any *_secret config be
            set (True, default), or should the _secret options be left as the
            path to get the secret from (False)
        :type include_secret: bool
        :rtype: Dict[str, Dict[str, str]]
        :return: Dictionary, where the key is the name of the section and the content is
            the dictionary with the name of the parameter and its value.
        """
        config_sources: Dict[str, Dict[str, str]] = {}
        configs = [
            ('default', self.airflow_defaults),
            ('airflow.cfg', self),
        ]

        self._replace_config_with_display_sources(config_sources, configs, display_source, raw)

        # add env vars and overwrite because they have priority
        if include_env:
            self._include_envs(config_sources, display_sensitive, display_source, raw)

        # add bash commands
        if include_cmds:
            self._include_commands(config_sources, display_sensitive, display_source, raw)

        # add config from secret backends
        if include_secret:
            self._include_secrets(config_sources, display_sensitive, display_source, raw)
        return config_sources

    def _include_secrets(self, config_sources, display_sensitive, display_source, raw):
        for (section, key) in self.sensitive_config_values:
            opt = self._get_secret_option(section, key)
            if opt:
                if not display_sensitive:
                    opt = '< hidden >'
                if display_source:
                    opt = (opt, 'secret')
                elif raw:
                    opt = opt.replace('%', '%%')
                config_sources.setdefault(section, OrderedDict()).update({key: opt})
                del config_sources[section][key + '_secret']

    def _include_commands(self, config_sources, display_sensitive, display_source, raw):
        for (section, key) in self.sensitive_config_values:
            opt = self._get_cmd_option(section, key)
            if not opt:
                continue
            if not display_sensitive:
                opt = '< hidden >'
            if display_source:
                opt = (opt, 'cmd')
            elif raw:
                opt = opt.replace('%', '%%')
            config_sources.setdefault(section, OrderedDict()).update({key: opt})
            del config_sources[section][key + '_cmd']

    def _include_envs(self, config_sources, display_sensitive, display_source, raw):
        for env_var in [
            os_environment for os_environment in os.environ if os_environment.startswith(self.ENV_VAR_PREFIX)
        ]:
            try:
                _, section, key = env_var.split('__', 2)
                opt = self._get_env_var_option(section, key)
            except ValueError:
                continue
            if opt is None:
                log.warning("Ignoring unknown env var '%s'", env_var)
                continue
            if not display_sensitive and env_var != self._env_var_name('core', 'unit_test_mode'):
                opt = '< hidden >'
            elif raw:
                opt = opt.replace('%', '%%')
            if display_source:
                opt = (opt, 'env var')

            section = section.lower()
            # if we lower key for kubernetes_environment_variables section,
            # then we won't be able to set any Airflow environment
            # variables. Airflow only parse environment variables starts
            # with AIRFLOW_. Therefore, we need to make it a special case.
            if section != 'kubernetes_environment_variables':
                key = key.lower()
            config_sources.setdefault(section, OrderedDict()).update({key: opt})

    @staticmethod
    def _replace_config_with_display_sources(config_sources, configs, display_source, raw):
        for (source_name, config) in configs:
            for section in config.sections():
                AirflowConfigParser._replace_section_config_with_display_sources(
                    config, config_sources, display_source, raw, section, source_name
                )

    @staticmethod
    def _replace_section_config_with_display_sources(
        config, config_sources, display_source, raw, section, source_name
    ):
        sect = config_sources.setdefault(section, OrderedDict())
        for (k, val) in config.items(section=section, raw=raw):
            if display_source:
                val = (val, source_name)
            sect[k] = val

    def load_test_config(self):
        """
        Load the unit test configuration.

        Note: this is not reversible.
        """
        # remove all sections, falling back to defaults
        for section in self.sections():
            self.remove_section(section)

        # then read test config

        path = _default_config_file_path('default_test.cfg')
        log.info("Reading default test configuration from %s", path)
        self.read_string(_parameterized_config_from_template('default_test.cfg'))
        # then read any "custom" test settings
        log.info("Reading test configuration from %s", TEST_CONFIG_FILE)
        self.read(TEST_CONFIG_FILE)

    @staticmethod
    def _warn_deprecate(section, key, deprecated_section, deprecated_name):
        if section == deprecated_section:
            warnings.warn(
                'The {old} option in [{section}] has been renamed to {new} - the old '
                'setting has been used, but please update your config.'.format(
                    old=deprecated_name,
                    new=key,
                    section=section,
                ),
                DeprecationWarning,
                stacklevel=3,
            )
        else:
            warnings.warn(
                'The {old_key} option in [{old_section}] has been moved to the {new_key} option in '
                '[{new_section}] - the old setting has been used, but please update your config.'.format(
                    old_section=deprecated_section,
                    old_key=deprecated_name,
                    new_key=key,
                    new_section=section,
                ),
                DeprecationWarning,
                stacklevel=3,
            )

    def __getstate__(self):
        return {
            name: getattr(self, name)
            for name in [
                '_sections',
                'is_validated',
                'airflow_defaults',
            ]
        }

    def __setstate__(self, state):
        self.__init__()
        config = state.pop('_sections')
        self.read_dict(config)
        self.__dict__.update(state)


def get_airflow_home():
    """Get path to Airflow Home"""
    return expand_env_var(os.environ.get('AIRFLOW_HOME', '~/airflow'))


def get_airflow_config(airflow_home):
    """Get Path to airflow.cfg path"""
    if 'AIRFLOW_CONFIG' not in os.environ:
        return os.path.join(airflow_home, 'airflow.cfg')
    return expand_env_var(os.environ['AIRFLOW_CONFIG'])


def _parameterized_config_from_template(filename) -> str:
    TEMPLATE_START = '# ----------------------- TEMPLATE BEGINS HERE -----------------------\n'

    path = _default_config_file_path(filename)
    with open(path) as fh:
        for line in fh:
            if line != TEMPLATE_START:
                continue
            return parameterized_config(fh.read().strip())
    raise RuntimeError(f"Template marker not found in {path!r}")


def parameterized_config(template):
    """
    Generates a configuration from the provided template + variables defined in
    current scope

    :param template: a config content templated with {{variables}}
    """
    all_vars = {k: v for d in [globals(), locals()] for k, v in d.items()}
    return template.format(**all_vars)  # noqa


def get_airflow_test_config(airflow_home):
    """Get path to unittests.cfg"""
    if 'AIRFLOW_TEST_CONFIG' not in os.environ:
        return os.path.join(airflow_home, 'unittests.cfg')
    return expand_env_var(os.environ['AIRFLOW_TEST_CONFIG'])


def _generate_fernet_key():
    from cryptography.fernet import Fernet

    return Fernet.generate_key().decode()


def initialize_config():
    """
    Load the Airflow config files.

    Called for you automatically as part of the Airflow boot process.
    """
    global FERNET_KEY, AIRFLOW_HOME

    default_config = _parameterized_config_from_template('default_airflow.cfg')

    conf = AirflowConfigParser(default_config=default_config)

    if conf.getboolean('core', 'unit_test_mode'):
        # Load test config only
        if not os.path.isfile(TEST_CONFIG_FILE):
            from cryptography.fernet import Fernet

            log.info('Creating new Airflow config file for unit tests in: %s', TEST_CONFIG_FILE)
            pathlib.Path(AIRFLOW_HOME).mkdir(parents=True, exist_ok=True)

            FERNET_KEY = Fernet.generate_key().decode()

            with open(TEST_CONFIG_FILE, 'w') as file:
                cfg = _parameterized_config_from_template('default_test.cfg')
                file.write(cfg)

        conf.load_test_config()
    else:
        # Load normal config
        if not os.path.isfile(AIRFLOW_CONFIG):
            from cryptography.fernet import Fernet

            log.info('Creating new Airflow config file in: %s', AIRFLOW_CONFIG)
            pathlib.Path(AIRFLOW_HOME).mkdir(parents=True, exist_ok=True)

            FERNET_KEY = Fernet.generate_key().decode()

            with open(AIRFLOW_CONFIG, 'w') as file:
                file.write(default_config)

        log.info("Reading the config from %s", AIRFLOW_CONFIG)

        conf.read(AIRFLOW_CONFIG)

        if conf.has_option('core', 'AIRFLOW_HOME'):
            msg = (
                'Specifying both AIRFLOW_HOME environment variable and airflow_home '
                'in the config file is deprecated. Please use only the AIRFLOW_HOME '
                'environment variable and remove the config file entry.'
            )
            if 'AIRFLOW_HOME' in os.environ:
                warnings.warn(msg, category=DeprecationWarning)
            elif conf.get('core', 'airflow_home') == AIRFLOW_HOME:
                warnings.warn(
                    'Specifying airflow_home in the config file is deprecated. As you '
                    'have left it at the default value you should remove the setting '
                    'from your airflow.cfg and suffer no change in behaviour.',
                    category=DeprecationWarning,
                )
            else:
                AIRFLOW_HOME = conf.get('core', 'airflow_home')
                warnings.warn(msg, category=DeprecationWarning)

        # They _might_ have set unit_test_mode in the airflow.cfg, we still
        # want to respect that and then load the unittests.cfg
        if conf.getboolean('core', 'unit_test_mode'):
            conf.load_test_config()

    # Make it no longer a proxy variable, just set it to an actual string
    global WEBSERVER_CONFIG
    WEBSERVER_CONFIG = AIRFLOW_HOME + '/webserver_config.py'

    if not os.path.isfile(WEBSERVER_CONFIG):
        import shutil

        log.info('Creating new FAB webserver config file in: %s', WEBSERVER_CONFIG)
        shutil.copy(_default_config_file_path('default_webserver_config.py'), WEBSERVER_CONFIG)

    conf.validate()

    return conf


# Historical convenience functions to access config entries
def load_test_config():  # noqa: D103
    """Historical load_test_config"""
    warnings.warn(
        "Accessing configuration method 'load_test_config' directly from the configuration module is "
        "deprecated. Please access the configuration from the 'configuration.conf' object via "
        "'conf.load_test_config'",
        DeprecationWarning,
        stacklevel=2,
    )
    conf.load_test_config()


def get(*args, **kwargs):  # noqa: D103
    """Historical get"""
    warnings.warn(
        "Accessing configuration method 'get' directly from the configuration module is "
        "deprecated. Please access the configuration from the 'configuration.conf' object via "
        "'conf.get'",
        DeprecationWarning,
        stacklevel=2,
    )
    return conf.get(*args, **kwargs)


def getboolean(*args, **kwargs):  # noqa: D103
    """Historical getboolean"""
    warnings.warn(
        "Accessing configuration method 'getboolean' directly from the configuration module is "
        "deprecated. Please access the configuration from the 'configuration.conf' object via "
        "'conf.getboolean'",
        DeprecationWarning,
        stacklevel=2,
    )
    return conf.getboolean(*args, **kwargs)


def getfloat(*args, **kwargs):  # noqa: D103
    """Historical getfloat"""
    warnings.warn(
        "Accessing configuration method 'getfloat' directly from the configuration module is "
        "deprecated. Please access the configuration from the 'configuration.conf' object via "
        "'conf.getfloat'",
        DeprecationWarning,
        stacklevel=2,
    )
    return conf.getfloat(*args, **kwargs)


def getint(*args, **kwargs):  # noqa: D103
    """Historical getint"""
    warnings.warn(
        "Accessing configuration method 'getint' directly from the configuration module is "
        "deprecated. Please access the configuration from the 'configuration.conf' object via "
        "'conf.getint'",
        DeprecationWarning,
        stacklevel=2,
    )
    return conf.getint(*args, **kwargs)


def getsection(*args, **kwargs):  # noqa: D103
    """Historical getsection"""
    warnings.warn(
        "Accessing configuration method 'getsection' directly from the configuration module is "
        "deprecated. Please access the configuration from the 'configuration.conf' object via "
        "'conf.getsection'",
        DeprecationWarning,
        stacklevel=2,
    )
    return conf.getsection(*args, **kwargs)


def has_option(*args, **kwargs):  # noqa: D103
    """Historical has_option"""
    warnings.warn(
        "Accessing configuration method 'has_option' directly from the configuration module is "
        "deprecated. Please access the configuration from the 'configuration.conf' object via "
        "'conf.has_option'",
        DeprecationWarning,
        stacklevel=2,
    )
    return conf.has_option(*args, **kwargs)


def remove_option(*args, **kwargs):  # noqa: D103
    """Historical remove_option"""
    warnings.warn(
        "Accessing configuration method 'remove_option' directly from the configuration module is "
        "deprecated. Please access the configuration from the 'configuration.conf' object via "
        "'conf.remove_option'",
        DeprecationWarning,
        stacklevel=2,
    )
    return conf.remove_option(*args, **kwargs)


def as_dict(*args, **kwargs):  # noqa: D103
    """Historical as_dict"""
    warnings.warn(
        "Accessing configuration method 'as_dict' directly from the configuration module is "
        "deprecated. Please access the configuration from the 'configuration.conf' object via "
        "'conf.as_dict'",
        DeprecationWarning,
        stacklevel=2,
    )
    return conf.as_dict(*args, **kwargs)


def set(*args, **kwargs):  # noqa pylint: disable=redefined-builtin
    """Historical set"""
    warnings.warn(
        "Accessing configuration method 'set' directly from the configuration module is "
        "deprecated. Please access the configuration from the 'configuration.conf' object via "
        "'conf.set'",
        DeprecationWarning,
        stacklevel=2,
    )
    return conf.set(*args, **kwargs)


def ensure_secrets_loaded() -> List[BaseSecretsBackend]:
    """
    Ensure that all secrets backends are loaded.
    If the secrets_backend_list contains only 2 default backends, reload it.
    """
    # Check if the secrets_backend_list contains only 2 default backends
    if len(secrets_backend_list) == 2:
        return initialize_secrets_backends()
    return secrets_backend_list


def get_custom_secret_backend() -> Optional[BaseSecretsBackend]:
    """Get Secret Backend if defined in airflow.cfg"""
    secrets_backend_cls = conf.getimport(section='secrets', key='backend')

    if secrets_backend_cls:
        try:
            alternative_secrets_config_dict = json.loads(
                conf.get(section='secrets', key='backend_kwargs', fallback='{}')
            )
        except JSONDecodeError:
            alternative_secrets_config_dict = {}

        return secrets_backend_cls(**alternative_secrets_config_dict)
    return None


def initialize_secrets_backends() -> List[BaseSecretsBackend]:
    """
    * import secrets backend classes
    * instantiate them and return them in a list
    """
    backend_list = []

    custom_secret_backend = get_custom_secret_backend()

    if custom_secret_backend is not None:
        backend_list.append(custom_secret_backend)

    for class_name in DEFAULT_SECRETS_SEARCH_PATH:
        secrets_backend_cls = import_string(class_name)
        backend_list.append(secrets_backend_cls())

    return backend_list


@functools.lru_cache(maxsize=None)
def _DEFAULT_CONFIG():
    path = _default_config_file_path('default_airflow.cfg')
    with open(path) as fh:
        return fh.read()


@functools.lru_cache(maxsize=None)
def _TEST_CONFIG():
    path = _default_config_file_path('default_test.cfg')
    with open(path) as fh:
        return fh.read()


_deprecated = {
    'DEFAULT_CONFIG': _DEFAULT_CONFIG,
    'TEST_CONFIG': _TEST_CONFIG,
    'TEST_CONFIG_FILE_PATH': functools.partial(_default_config_file_path, ('default_test.cfg')),
    'DEFAULT_CONFIG_FILE_PATH': functools.partial(_default_config_file_path, ('default_airflow.cfg')),
}


def __getattr__(name):
    if name in _deprecated:
        warnings.warn(
            f"{__name__}.{name} is deprecated and will be removed in future",
            DeprecationWarning,
            stacklevel=2,
        )
        return _deprecated[name]()
    raise AttributeError(f"module {__name__} has no attribute {name}")


# Setting AIRFLOW_HOME and AIRFLOW_CONFIG from environment variables, using
# "~/airflow" and "$AIRFLOW_HOME/airflow.cfg" respectively as defaults.

AIRFLOW_HOME = get_airflow_home()
AIRFLOW_CONFIG = get_airflow_config(AIRFLOW_HOME)


# Set up dags folder for unit tests
# this directory won't exist if users install via pip
_TEST_DAGS_FOLDER = os.path.join(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'tests', 'dags'
)
if os.path.exists(_TEST_DAGS_FOLDER):
    TEST_DAGS_FOLDER = _TEST_DAGS_FOLDER
else:
    TEST_DAGS_FOLDER = os.path.join(AIRFLOW_HOME, 'dags')

# Set up plugins folder for unit tests
_TEST_PLUGINS_FOLDER = os.path.join(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'tests', 'plugins'
)
if os.path.exists(_TEST_PLUGINS_FOLDER):
    TEST_PLUGINS_FOLDER = _TEST_PLUGINS_FOLDER
else:
    TEST_PLUGINS_FOLDER = os.path.join(AIRFLOW_HOME, 'plugins')


TEST_CONFIG_FILE = get_airflow_test_config(AIRFLOW_HOME)

SECRET_KEY = b64encode(os.urandom(16)).decode('utf-8')
FERNET_KEY = ''  # Set only if needed when generating a new file
WEBSERVER_CONFIG = ''  # Set by initialize_config

conf = initialize_config()
secrets_backend_list = initialize_secrets_backends()


PY37 = sys.version_info >= (3, 7)
if not PY37:
    from pep562 import Pep562

    Pep562(__name__)
