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
import io
import os
import re
import tempfile
import textwrap
import unittest
import warnings
from collections import OrderedDict
from unittest import mock

import pytest

from airflow import configuration
from airflow.configuration import (
    AirflowConfigException,
    AirflowConfigParser,
    conf,
    expand_env_var,
    get_airflow_config,
    get_airflow_home,
    parameterized_config,
    run_command,
)
from tests.test_utils.config import conf_vars
from tests.test_utils.reset_warning_registry import reset_warning_registry


@unittest.mock.patch.dict(
    'os.environ',
    {
        'AIRFLOW__TESTSECTION__TESTKEY': 'testvalue',
        'AIRFLOW__TESTSECTION__TESTPERCENT': 'with%percent',
        'AIRFLOW__TESTCMDENV__ITSACOMMAND_CMD': 'echo -n "OK"',
        'AIRFLOW__TESTCMDENV__NOTACOMMAND_CMD': 'echo -n "NOT OK"',
    },
)
class TestConf(unittest.TestCase):
    def test_airflow_home_default(self):
        with unittest.mock.patch.dict('os.environ'):
            if 'AIRFLOW_HOME' in os.environ:
                del os.environ['AIRFLOW_HOME']
            assert get_airflow_home() == expand_env_var('~/airflow')

    def test_airflow_home_override(self):
        with unittest.mock.patch.dict('os.environ', AIRFLOW_HOME='/path/to/airflow'):
            assert get_airflow_home() == '/path/to/airflow'

    def test_airflow_config_default(self):
        with unittest.mock.patch.dict('os.environ'):
            if 'AIRFLOW_CONFIG' in os.environ:
                del os.environ['AIRFLOW_CONFIG']
            assert get_airflow_config('/home/airflow') == expand_env_var('/home/airflow/airflow.cfg')

    def test_airflow_config_override(self):
        with unittest.mock.patch.dict('os.environ', AIRFLOW_CONFIG='/path/to/airflow/airflow.cfg'):
            assert get_airflow_config('/home//airflow') == '/path/to/airflow/airflow.cfg'

    @conf_vars({("core", "percent"): "with%%inside"})
    def test_case_sensitivity(self):
        # section and key are case insensitive for get method
        # note: this is not the case for as_dict method
        assert conf.get("core", "percent") == "with%inside"
        assert conf.get("core", "PERCENT") == "with%inside"
        assert conf.get("CORE", "PERCENT") == "with%inside"

    def test_config_as_dict(self):
        """Test that getting config as dict works even if
        environment has non-legal env vars"""
        with unittest.mock.patch.dict('os.environ'):
            os.environ['AIRFLOW__VAR__broken'] = "not_ok"
            asdict = conf.as_dict(raw=True, display_sensitive=True)
        assert asdict.get('VAR') is None
        assert asdict['testsection']['testkey'] == 'testvalue'

    def test_env_var_config(self):
        opt = conf.get('testsection', 'testkey')
        assert opt == 'testvalue'

        opt = conf.get('testsection', 'testpercent')
        assert opt == 'with%percent'

        assert conf.has_option('testsection', 'testkey')

        with unittest.mock.patch.dict(
            'os.environ', AIRFLOW__KUBERNETES_ENVIRONMENT_VARIABLES__AIRFLOW__TESTSECTION__TESTKEY='nested'
        ):
            opt = conf.get('kubernetes_environment_variables', 'AIRFLOW__TESTSECTION__TESTKEY')
            assert opt == 'nested'

    @mock.patch.dict(
        'os.environ', AIRFLOW__KUBERNETES_ENVIRONMENT_VARIABLES__AIRFLOW__TESTSECTION__TESTKEY='nested'
    )
    @conf_vars({("core", "percent"): "with%%inside"})
    def test_conf_as_dict(self):
        cfg_dict = conf.as_dict()

        # test that configs are picked up
        assert cfg_dict['core']['unit_test_mode'] == 'True'

        assert cfg_dict['core']['percent'] == 'with%inside'

        # test env vars
        assert cfg_dict['testsection']['testkey'] == '< hidden >'
        assert cfg_dict['kubernetes_environment_variables']['AIRFLOW__TESTSECTION__TESTKEY'] == '< hidden >'

    def test_conf_as_dict_source(self):
        # test display_source
        cfg_dict = conf.as_dict(display_source=True)
        assert cfg_dict['core']['load_examples'][1] == 'airflow.cfg'
        assert cfg_dict['core']['load_default_connections'][1] == 'airflow.cfg'
        assert cfg_dict['testsection']['testkey'] == ('< hidden >', 'env var')

    def test_conf_as_dict_sensitive(self):
        # test display_sensitive
        cfg_dict = conf.as_dict(display_sensitive=True)
        assert cfg_dict['testsection']['testkey'] == 'testvalue'
        assert cfg_dict['testsection']['testpercent'] == 'with%percent'

        # test display_source and display_sensitive
        cfg_dict = conf.as_dict(display_sensitive=True, display_source=True)
        assert cfg_dict['testsection']['testkey'] == ('testvalue', 'env var')

    @conf_vars({("core", "percent"): "with%%inside"})
    def test_conf_as_dict_raw(self):
        # test display_sensitive
        cfg_dict = conf.as_dict(raw=True, display_sensitive=True)
        assert cfg_dict['testsection']['testkey'] == 'testvalue'

        # Values with '%' in them should be escaped
        assert cfg_dict['testsection']['testpercent'] == 'with%%percent'
        assert cfg_dict['core']['percent'] == 'with%%inside'

    def test_conf_as_dict_exclude_env(self):
        # test display_sensitive
        cfg_dict = conf.as_dict(include_env=False, display_sensitive=True)

        # Since testsection is only created from env vars, it shouldn't be
        # present at all if we don't ask for env vars to be included.
        assert 'testsection' not in cfg_dict

    def test_command_precedence(self):
        test_config = '''[test]
key1 = hello
key2_cmd = printf cmd_result
key3 = airflow
key4_cmd = printf key4_result
'''
        test_config_default = '''[test]
key1 = awesome
key2 = airflow

[another]
key6 = value6
'''

        test_conf = AirflowConfigParser(default_config=parameterized_config(test_config_default))
        test_conf.read_string(test_config)
        test_conf.sensitive_config_values = test_conf.sensitive_config_values | {
            ('test', 'key2'),
            ('test', 'key4'),
        }
        assert 'hello' == test_conf.get('test', 'key1')
        assert 'cmd_result' == test_conf.get('test', 'key2')
        assert 'airflow' == test_conf.get('test', 'key3')
        assert 'key4_result' == test_conf.get('test', 'key4')
        assert 'value6' == test_conf.get('another', 'key6')

        assert 'hello' == test_conf.get('test', 'key1', fallback='fb')
        assert 'value6' == test_conf.get('another', 'key6', fallback='fb')
        assert 'fb' == test_conf.get('another', 'key7', fallback='fb')
        assert test_conf.getboolean('another', 'key8_boolean', fallback='True') is True
        assert 10 == test_conf.getint('another', 'key8_int', fallback='10')
        assert 1.0 == test_conf.getfloat('another', 'key8_float', fallback='1')

        assert test_conf.has_option('test', 'key1')
        assert test_conf.has_option('test', 'key2')
        assert test_conf.has_option('test', 'key3')
        assert test_conf.has_option('test', 'key4')
        assert not test_conf.has_option('test', 'key5')
        assert test_conf.has_option('another', 'key6')

        cfg_dict = test_conf.as_dict(display_sensitive=True)
        assert 'cmd_result' == cfg_dict['test']['key2']
        assert 'key2_cmd' not in cfg_dict['test']

        # If we exclude _cmds then we should still see the commands to run, not
        # their values
        cfg_dict = test_conf.as_dict(include_cmds=False, display_sensitive=True)
        assert 'key4' not in cfg_dict['test']
        assert 'printf key4_result' == cfg_dict['test']['key4_cmd']

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    @conf_vars(
        {
            ("secrets", "backend"): "airflow.providers.hashicorp.secrets.vault.VaultBackend",
            ("secrets", "backend_kwargs"): '{"url": "http://127.0.0.1:8200", "token": "token"}',
        }
    )
    def test_config_from_secret_backend(self, mock_hvac):
        """Get Config Value from a Secret Backend"""
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            'request_id': '2d48a2ad-6bcb-e5b6-429d-da35fdf31f56',
            'lease_id': '',
            'renewable': False,
            'lease_duration': 0,
            'data': {
                'data': {'value': 'sqlite:////Users/airflow/airflow/airflow.db'},
                'metadata': {
                    'created_time': '2020-03-28T02:10:54.301784Z',
                    'deletion_time': '',
                    'destroyed': False,
                    'version': 1,
                },
            },
            'wrap_info': None,
            'warnings': None,
            'auth': None,
        }

        test_config = '''[test]
sql_alchemy_conn_secret = sql_alchemy_conn
'''
        test_config_default = '''[test]
sql_alchemy_conn = airflow
'''

        test_conf = AirflowConfigParser(default_config=parameterized_config(test_config_default))
        test_conf.read_string(test_config)
        test_conf.sensitive_config_values = test_conf.sensitive_config_values | {
            ('test', 'sql_alchemy_conn'),
        }

        assert 'sqlite:////Users/airflow/airflow/airflow.db' == test_conf.get('test', 'sql_alchemy_conn')

    def test_getboolean(self):
        """Test AirflowConfigParser.getboolean"""
        test_config = """
[type_validation]
key1 = non_bool_value

[true]
key2 = t
key3 = true
key4 = 1

[false]
key5 = f
key6 = false
key7 = 0

[inline-comment]
key8 = true #123
"""
        test_conf = AirflowConfigParser(default_config=test_config)
        with pytest.raises(
            AirflowConfigException,
            match=re.escape(
                'Failed to convert value to bool. Please check "key1" key in "type_validation" section. '
                'Current value: "non_bool_value".'
            ),
        ):
            test_conf.getboolean('type_validation', 'key1')
        assert isinstance(test_conf.getboolean('true', 'key3'), bool)
        assert test_conf.getboolean('true', 'key2') is True
        assert test_conf.getboolean('true', 'key3') is True
        assert test_conf.getboolean('true', 'key4') is True
        assert test_conf.getboolean('false', 'key5') is False
        assert test_conf.getboolean('false', 'key6') is False
        assert test_conf.getboolean('false', 'key7') is False
        assert test_conf.getboolean('inline-comment', 'key8') is True

    def test_getint(self):
        """Test AirflowConfigParser.getint"""
        test_config = """
[invalid]
key1 = str

[valid]
key2 = 1
"""
        test_conf = AirflowConfigParser(default_config=test_config)
        with pytest.raises(
            AirflowConfigException,
            match=re.escape(
                'Failed to convert value to int. Please check "key1" key in "invalid" section. '
                'Current value: "str".'
            ),
        ):
            test_conf.getint('invalid', 'key1')
        assert isinstance(test_conf.getint('valid', 'key2'), int)
        assert 1 == test_conf.getint('valid', 'key2')

    def test_getfloat(self):
        """Test AirflowConfigParser.getfloat"""
        test_config = """
[invalid]
key1 = str

[valid]
key2 = 1.23
"""
        test_conf = AirflowConfigParser(default_config=test_config)
        with pytest.raises(
            AirflowConfigException,
            match=re.escape(
                'Failed to convert value to float. Please check "key1" key in "invalid" section. '
                'Current value: "str".'
            ),
        ):
            test_conf.getfloat('invalid', 'key1')
        assert isinstance(test_conf.getfloat('valid', 'key2'), float)
        assert 1.23 == test_conf.getfloat('valid', 'key2')

    def test_has_option(self):
        test_config = '''[test]
key1 = value1
'''
        test_conf = AirflowConfigParser()
        test_conf.read_string(test_config)
        assert test_conf.has_option('test', 'key1')
        assert not test_conf.has_option('test', 'key_not_exists')
        assert not test_conf.has_option('section_not_exists', 'key1')

    def test_remove_option(self):
        test_config = '''[test]
key1 = hello
key2 = airflow
'''
        test_config_default = '''[test]
key1 = awesome
key2 = airflow
'''

        test_conf = AirflowConfigParser(default_config=parameterized_config(test_config_default))
        test_conf.read_string(test_config)

        assert 'hello' == test_conf.get('test', 'key1')
        test_conf.remove_option('test', 'key1', remove_default=False)
        assert 'awesome' == test_conf.get('test', 'key1')

        test_conf.remove_option('test', 'key2')
        assert not test_conf.has_option('test', 'key2')

    def test_getsection(self):
        test_config = '''
[test]
key1 = hello
[new_section]
key = value
'''
        test_config_default = '''
[test]
key1 = awesome
key2 = airflow

[testsection]
key3 = value3
'''
        test_conf = AirflowConfigParser(default_config=parameterized_config(test_config_default))
        test_conf.read_string(test_config)

        assert OrderedDict([('key1', 'hello'), ('key2', 'airflow')]) == test_conf.getsection('test')
        assert OrderedDict(
            [('key3', 'value3'), ('testkey', 'testvalue'), ('testpercent', 'with%percent')]
        ) == test_conf.getsection('testsection')

        assert OrderedDict([('key', 'value')]) == test_conf.getsection('new_section')

        assert test_conf.getsection('non_existent_section') is None

    def test_get_section_should_respect_cmd_env_variable(self):
        with tempfile.NamedTemporaryFile(delete=False) as cmd_file:
            cmd_file.write(b"#!/usr/bin/env bash\n")
            cmd_file.write(b"echo -n difficult_unpredictable_cat_password\n")
            cmd_file.flush()
            os.chmod(cmd_file.name, 0o0555)
            cmd_file.close()

            with mock.patch.dict("os.environ", {"AIRFLOW__WEBSERVER__SECRET_KEY_CMD": cmd_file.name}):
                content = conf.getsection("webserver")
            os.unlink(cmd_file.name)
        assert content["secret_key"] == "difficult_unpredictable_cat_password"

    def test_kubernetes_environment_variables_section(self):
        test_config = '''
[kubernetes_environment_variables]
key1 = hello
AIRFLOW_HOME = /root/airflow
'''
        test_config_default = '''
[kubernetes_environment_variables]
'''
        test_conf = AirflowConfigParser(default_config=parameterized_config(test_config_default))
        test_conf.read_string(test_config)

        assert OrderedDict([('key1', 'hello'), ('AIRFLOW_HOME', '/root/airflow')]) == test_conf.getsection(
            'kubernetes_environment_variables'
        )

    def test_broker_transport_options(self):
        section_dict = conf.getsection("celery_broker_transport_options")
        assert isinstance(section_dict['visibility_timeout'], int)
        assert isinstance(section_dict['_test_only_bool'], bool)
        assert isinstance(section_dict['_test_only_float'], float)
        assert isinstance(section_dict['_test_only_string'], str)

    @conf_vars(
        {
            ("celery", "worker_concurrency"): None,
            ("celery", "celeryd_concurrency"): None,
        }
    )
    def test_deprecated_options(self):
        # Guarantee we have a deprecated setting, so we test the deprecation
        # lookup even if we remove this explicit fallback
        conf.deprecated_options = {
            ('celery', 'worker_concurrency'): ('celery', 'celeryd_concurrency', '2.0.0'),
        }

        # Remove it so we are sure we use the right setting
        conf.remove_option('celery', 'worker_concurrency')

        with pytest.warns(DeprecationWarning):
            with mock.patch.dict('os.environ', AIRFLOW__CELERY__CELERYD_CONCURRENCY="99"):
                assert conf.getint('celery', 'worker_concurrency') == 99

        with pytest.warns(DeprecationWarning), conf_vars({('celery', 'celeryd_concurrency'): '99'}):
            assert conf.getint('celery', 'worker_concurrency') == 99

    @conf_vars(
        {
            ('logging', 'logging_level'): None,
            ('core', 'logging_level'): None,
        }
    )
    def test_deprecated_options_with_new_section(self):
        # Guarantee we have a deprecated setting, so we test the deprecation
        # lookup even if we remove this explicit fallback
        conf.deprecated_options = {
            ('logging', 'logging_level'): ('core', 'logging_level', '2.0.0'),
        }

        # Remove it so we are sure we use the right setting
        conf.remove_option('core', 'logging_level')
        conf.remove_option('logging', 'logging_level')

        with pytest.warns(DeprecationWarning):
            with mock.patch.dict('os.environ', AIRFLOW__CORE__LOGGING_LEVEL="VALUE"):
                assert conf.get('logging', 'logging_level') == "VALUE"

        with pytest.warns(DeprecationWarning), conf_vars({('core', 'logging_level'): 'VALUE'}):
            assert conf.get('logging', 'logging_level') == "VALUE"

    @conf_vars(
        {
            ("celery", "result_backend"): None,
            ("celery", "celery_result_backend"): None,
            ("celery", "celery_result_backend_cmd"): None,
        }
    )
    def test_deprecated_options_cmd(self):
        # Guarantee we have a deprecated setting, so we test the deprecation
        # lookup even if we remove this explicit fallback
        conf.deprecated_options[('celery', "result_backend")] = 'celery', 'celery_result_backend', '2.0.0'
        conf.sensitive_config_values.add(('celery', 'celery_result_backend'))

        conf.remove_option('celery', 'result_backend')
        with conf_vars({('celery', 'celery_result_backend_cmd'): '/bin/echo 99'}):
            with pytest.warns(DeprecationWarning):
                tmp = None
                if 'AIRFLOW__CELERY__RESULT_BACKEND' in os.environ:
                    tmp = os.environ.pop('AIRFLOW__CELERY__RESULT_BACKEND')
                assert conf.getint('celery', 'result_backend') == 99
                if tmp:
                    os.environ['AIRFLOW__CELERY__RESULT_BACKEND'] = tmp

    def test_deprecated_values(self):
        def make_config():
            test_conf = AirflowConfigParser(default_config='')
            # Guarantee we have a deprecated setting, so we test the deprecation
            # lookup even if we remove this explicit fallback
            test_conf.deprecated_values = {
                'core': {
                    'hostname_callable': (re.compile(r':'), r'.', '2.1'),
                },
            }
            test_conf.read_dict(
                {
                    'core': {
                        'executor': 'SequentialExecutor',
                        'sql_alchemy_conn': 'sqlite://',
                        'hostname_callable': 'socket:getfqdn',
                    },
                }
            )
            test_conf.validate()
            return test_conf

        with pytest.warns(FutureWarning):
            test_conf = make_config()
            assert test_conf.get('core', 'hostname_callable') == 'socket.getfqdn'

        with pytest.warns(FutureWarning):
            with unittest.mock.patch.dict('os.environ', AIRFLOW__CORE__HOSTNAME_CALLABLE='socket:getfqdn'):
                test_conf = make_config()
                assert test_conf.get('core', 'hostname_callable') == 'socket.getfqdn'

        with reset_warning_registry():
            with warnings.catch_warnings(record=True) as warning:
                with unittest.mock.patch.dict(
                    'os.environ',
                    AIRFLOW__CORE__HOSTNAME_CALLABLE='CarrierPigeon',
                ):
                    test_conf = make_config()
                    assert test_conf.get('core', 'hostname_callable') == 'CarrierPigeon'
                    assert [] == warning

    def test_deprecated_funcs(self):
        for func in [
            'load_test_config',
            'get',
            'getboolean',
            'getfloat',
            'getint',
            'has_option',
            'remove_option',
            'as_dict',
            'set',
        ]:
            with mock.patch(f'airflow.configuration.conf.{func}') as mock_method:
                with pytest.warns(DeprecationWarning):
                    getattr(configuration, func)()
                mock_method.assert_called_once()

    def test_command_from_env(self):
        test_cmdenv_config = '''[testcmdenv]
itsacommand = NOT OK
notacommand = OK
'''
        test_cmdenv_conf = AirflowConfigParser()
        test_cmdenv_conf.read_string(test_cmdenv_config)
        test_cmdenv_conf.sensitive_config_values.add(('testcmdenv', 'itsacommand'))
        with unittest.mock.patch.dict('os.environ'):
            # AIRFLOW__TESTCMDENV__ITSACOMMAND_CMD maps to ('testcmdenv', 'itsacommand') in
            # sensitive_config_values and therefore should return 'OK' from the environment variable's
            # echo command, and must not return 'NOT OK' from the configuration
            assert test_cmdenv_conf.get('testcmdenv', 'itsacommand') == 'OK'
            # AIRFLOW__TESTCMDENV__NOTACOMMAND_CMD maps to no entry in sensitive_config_values and therefore
            # the option should return 'OK' from the configuration, and must not return 'NOT OK' from
            # the environment variable's echo command
            assert test_cmdenv_conf.get('testcmdenv', 'notacommand') == 'OK'

    def test_parameterized_config_gen(self):
        config = textwrap.dedent(
            """
            [core]
            dags_folder = {AIRFLOW_HOME}/dags
            sql_alchemy_conn = sqlite:///{AIRFLOW_HOME}/airflow.db
            parallelism = 32
            fernet_key = {FERNET_KEY}
        """
        )

        cfg = parameterized_config(config)

        # making sure some basic building blocks are present:
        assert "[core]" in cfg
        assert "dags_folder" in cfg
        assert "sql_alchemy_conn" in cfg
        assert "fernet_key" in cfg

        # making sure replacement actually happened
        assert "{AIRFLOW_HOME}" not in cfg
        assert "{FERNET_KEY}" not in cfg

    def test_config_use_original_when_original_and_fallback_are_present(self):
        assert conf.has_option("core", "FERNET_KEY")
        assert not conf.has_option("core", "FERNET_KEY_CMD")

        fernet_key = conf.get('core', 'FERNET_KEY')

        with conf_vars({('core', 'FERNET_KEY_CMD'): 'printf HELLO'}):
            fallback_fernet_key = conf.get("core", "FERNET_KEY")

        assert fernet_key == fallback_fernet_key

    def test_config_throw_error_when_original_and_fallback_is_absent(self):
        assert conf.has_option("core", "FERNET_KEY")
        assert not conf.has_option("core", "FERNET_KEY_CMD")

        with conf_vars({('core', 'fernet_key'): None}):
            with pytest.raises(AirflowConfigException) as ctx:
                conf.get("core", "FERNET_KEY")

        exception = str(ctx.value)
        message = "section/key [core/fernet_key] not found in config"
        assert message == exception

    def test_config_override_original_when_non_empty_envvar_is_provided(self):
        key = "AIRFLOW__CORE__FERNET_KEY"
        value = "some value"

        with mock.patch.dict('os.environ', {key: value}):
            fernet_key = conf.get('core', 'FERNET_KEY')

        assert value == fernet_key

    def test_config_override_original_when_empty_envvar_is_provided(self):
        key = "AIRFLOW__CORE__FERNET_KEY"
        value = "some value"

        with mock.patch.dict('os.environ', {key: value}):
            fernet_key = conf.get('core', 'FERNET_KEY')

        assert value == fernet_key

    @mock.patch.dict("os.environ", {"AIRFLOW__CORE__DAGS_FOLDER": "/tmp/test_folder"})
    def test_write_should_respect_env_variable(self):
        with io.StringIO() as string_file:
            conf.write(string_file)
            content = string_file.getvalue()
        assert "dags_folder = /tmp/test_folder" in content

    def test_run_command(self):
        write = r'sys.stdout.buffer.write("\u1000foo".encode("utf8"))'

        cmd = f'import sys; {write}; sys.stdout.flush()'

        assert run_command(f"python -c '{cmd}'") == '\u1000foo'

        assert run_command('echo "foo bar"') == 'foo bar\n'
        with pytest.raises(AirflowConfigException):
            run_command('bash -c "exit 1"')

    def test_confirm_unittest_mod(self):
        assert conf.get('core', 'unit_test_mode')

    @conf_vars({("core", "store_serialized_dags"): "True"})
    def test_store_dag_code_default_config(self):
        store_serialized_dags = conf.getboolean('core', 'store_serialized_dags', fallback=False)
        store_dag_code = conf.getboolean("core", "store_dag_code", fallback=store_serialized_dags)
        assert not conf.has_option("core", "store_dag_code")
        assert store_serialized_dags
        assert store_dag_code

    @conf_vars({("core", "store_serialized_dags"): "True", ("core", "store_dag_code"): "False"})
    def test_store_dag_code_config_when_set(self):
        store_serialized_dags = conf.getboolean('core', 'store_serialized_dags', fallback=False)
        store_dag_code = conf.getboolean("core", "store_dag_code", fallback=store_serialized_dags)
        assert conf.has_option("core", "store_dag_code")
        assert store_serialized_dags
        assert not store_dag_code
