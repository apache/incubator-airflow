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
import inspect
import logging
import logging.config
import os
import textwrap

import pytest

from airflow.utils.log.secrets_masker import SecretsMasker, should_hide_value_for_key
from tests.test_utils.config import conf_vars


@pytest.fixture
def logger(caplog):
    logging.config.dictConfig(
        {
            'version': 1,
            'handlers': {
                __name__: {
                    # Reset later
                    'class': 'logging.StreamHandler',
                    'stream': 'ext://sys.stdout',
                }
            },
            'loggers': {
                __name__: {
                    'handlers': [__name__],
                    'level': logging.INFO,
                    'propagate': False,
                }
            },
            'disable_existing_loggers': False,
        }
    )
    formatter = ShortExcFormatter("%(levelname)s %(message)s")
    logger = logging.getLogger(__name__)

    caplog.handler.setFormatter(formatter)
    logger.handlers = [caplog.handler]
    filt = SecretsMasker()
    logger.addFilter(filt)

    filt.add_mask('password')

    return logger


class TestSecretsMasker:
    def test_message(self, logger, caplog):
        logger.info("XpasswordY")

        assert caplog.text == "INFO X***Y\n"

    def test_args(self, logger, caplog):
        logger.info("Cannot connect to %s", "user:password")

        assert caplog.text == "INFO Cannot connect to user:***\n"

    def test_extra(self, logger, caplog):
        logger.handlers[0].formatter = ShortExcFormatter("%(levelname)s %(message)s %(conn)s")
        logger.info("Cannot connect", extra={'conn': "user:password"})

        assert caplog.text == "INFO Cannot connect user:***\n"

    def test_exception(self, logger, caplog):
        try:
            conn = "user:password"
            raise RuntimeError("Cannot connect to " + conn)
        except RuntimeError:
            logger.exception("Err")

        line = lineno() - 4

        assert caplog.text == textwrap.dedent(
            f"""\
            ERROR Err
            Traceback (most recent call last):
              File ".../test_secrets_masker.py", line {line}, in test_exception
                raise RuntimeError("Cannot connect to " + conn)
            RuntimeError: Cannot connect to user:***
            """
        )

    @pytest.mark.xfail(reason="Cannot filter secrets in traceback source")
    def test_exc_tb(self, logger, caplog):
        """
        Show it is not possible to filter secrets in the source.

        It is not possible to (regularly/reliably) filter out secrets that
        appear directly in the source code. This is because the formatting of
        exc_info is not done in the filter, it is done after the filter is
        called, and fixing this "properly" is hard/impossible.

        (It would likely need to construct a custom traceback that changed the
        source. I have no idead if that is even possible)

        This test illustrates that, but ix marked xfail in case someone wants to
        fix this later.
        """
        try:
            raise RuntimeError("Cannot connect to user:password")
        except RuntimeError:
            logger.exception("Err")

        line = lineno() - 4

        assert caplog.text == textwrap.dedent(
            f"""\
            ERROR Err
            Traceback (most recent call last):
              File ".../test_secrets_masker.py", line {line}, in test_exc_tb
                raise RuntimeError("Cannot connect to user:***)
            RuntimeError: Cannot connect to user:***
            """
        )

    @pytest.mark.parametrize(
        ("name", "value", "expected_mask"),
        [
            (None, "secret", {"secret"}),
            ("apikey", "secret", {"secret"}),
            # the value for "apikey", and "password" should end up masked
            (None, {"apikey": "secret", "other": {"val": "innocent", "password": "foo"}}, {"secret", "foo"}),
            (None, ["secret", "other"], {"secret", "other"}),
            # When the "sensitive value" is a dict, don't mask anything
            # (Or should this be mask _everything_ under it ?
            ("api_key", {"other": "innoent"}, set()),
        ],
    )
    def test_mask_secret(self, name, value, expected_mask):
        filt = SecretsMasker()
        filt.add_mask(value, name)

        assert filt.patterns == expected_mask

    @pytest.mark.parametrize(
        ("patterns", "name", "value", "expected"),
        [
            ({"secret"}, None, "secret", "***"),
            (
                {"secret", "foo"},
                None,
                {"apikey": "secret", "other": {"val": "innocent", "password": "foo"}},
                {"apikey": "***", "other": {"val": "innocent", "password": "***"}},
            ),
            ({"secret", "other"}, None, ["secret", "other"], ["***", "***"]),
            # We don't mask dict _keys_.
            ({"secret", "other"}, None, {"data": {"secret": "secret"}}, {"data": {"secret": "***"}}),
            (
                # Since this is a sensitive name, all the values should be redacted!
                {"secret"},
                "api_key",
                {"other": "innoent", "nested": ["x", "y"]},
                {"other": "***", "nested": ["***", "***"]},
            ),
            (
                # Test that masking still works based on name even when no patterns given
                set(),
                'env',
                {'api_key': 'masked based on key name', 'other': 'foo'},
                {'api_key': '***', 'other': 'foo'},
            ),
        ],
    )
    def test_redact(self, patterns, name, value, expected):
        filt = SecretsMasker()
        for val in patterns:
            filt.add_mask(val)

        assert filt.redact(value, name) == expected


class TestShouldHideValueForKey:
    @pytest.mark.parametrize(
        ("key", "expected_result"),
        [
            ('', False),
            (None, False),
            ("key", False),
            ("google_api_key", True),
            ("GOOGLE_API_KEY", True),
            ("GOOGLE_APIKEY", True),
        ],
    )
    def test_hiding_defaults(self, key, expected_result):
        assert expected_result == should_hide_value_for_key(key)

    @pytest.mark.parametrize(
        ("sensitive_variable_fields", "key", "expected_result"),
        [
            ('key', 'TRELLO_KEY', True),
            ('key', 'TRELLO_API_KEY', True),
            ('key', 'GITHUB_APIKEY', True),
            ('key, token', 'TRELLO_TOKEN', True),
            ('mysecretword, mysensitivekey', 'GITHUB_mysecretword', True),
            (None, 'TRELLO_API', False),
            ('token', 'TRELLO_KEY', False),
            ('token, mysecretword', 'TRELLO_KEY', False),
        ],
    )
    def test_hiding_config(self, sensitive_variable_fields, key, expected_result):
        from airflow.utils.log.secrets_masker import get_sensitive_variables_fields

        with conf_vars({('core', 'sensitive_var_conn_names'): str(sensitive_variable_fields)}):
            get_sensitive_variables_fields.cache_clear()
            assert expected_result == should_hide_value_for_key(key)
        get_sensitive_variables_fields.cache_clear()


class ShortExcFormatter(logging.Formatter):
    """Don't include full path in exc_info messages"""

    def formatException(self, exc_info):
        formatted = super().formatException(exc_info)
        return formatted.replace(__file__, ".../" + os.path.basename(__file__))


def lineno():
    """Returns the current line number in our program."""
    return inspect.currentframe().f_back.f_lineno
