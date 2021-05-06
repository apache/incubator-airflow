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
"""Mask sensitive information from logs"""
import collections
import logging
import re
from typing import TYPE_CHECKING, Iterable, Optional, Set, TypeVar, Union

try:
    # 3.8+
    from functools import cached_property
except ImportError:
    from cached_property import cached_property

try:
    # 3.9+
    from functools import cache
except ImportError:
    from functools import lru_cache

    cache = lru_cache(maxsize=None)


if TYPE_CHECKING:
    from airflow.typing_compat import RePatternType

    RedactableItem = TypeVar('RedctableItem')

DEFAULT_SENSITIVE_FIELDS = frozenset(
    {
        'password',
        'secret',
        'passwd',
        'authorization',
        'api_key',
        'apikey',
        'access_token',
    }
)
"""Names of fields (Connection extra, Variable key name etc.) that are deemed sensitive"""


@cache
def get_sensitive_variables_fields():
    """Get comma-separated sensitive Variable Fields from airflow.cfg."""
    from airflow.configuration import conf

    sensitive_fields = DEFAULT_SENSITIVE_FIELDS.copy()
    sensitive_variable_fields = conf.get('core', 'sensitive_var_conn_names')
    if sensitive_variable_fields:
        sensitive_fields |= frozenset({field.strip() for field in sensitive_variable_fields.split(',')})
    return sensitive_fields


def should_hide_value_for_key(name):
    """Should the value for this given name (Variable name, or key in conn.extra_dejson) be hidden"""
    from airflow import settings

    if name and settings.HIDE_SENSITIVE_VAR_CONN_FIELDS:
        name = name.strip().lower()
        return any(s in name for s in get_sensitive_variables_fields())
    return False


def mask_secret(secret: Union[str, dict, Iterable], name: str = None) -> None:
    """
    Mask a secret from appearing in the task logs.

    If ``name`` is provided, then it will only be masked if the name matches
    one of the configured "sensitive" names.

    If ``secret`` is a dict or a iterable (excluding str) then it will be
    recursively walked and keys with sensitive names will be hidden.
    """
    # Delay import
    from airflow import settings

    # Filtering all log messages is not a free process, so we only do it when
    # running tasks
    if not settings.MASK_SECRETS_IN_LOGS or not secret:
        return

    _secrets_masker().add_mask(secret, name)


def redact(value: "RedactableItem", name: str = None) -> "RedactableItem":
    """Redact any secrets found in ``value``."""
    return _secrets_masker().redact(value, name)


@cache
def _secrets_masker() -> "SecretsMasker":

    for flt in logging.getLogger('airflow.task').filters:
        if isinstance(flt, SecretsMasker):
            return flt
    raise RuntimeError("No SecretsMasker found!")


class SecretsMasker(logging.Filter):
    """Redact secrets from logs"""

    replacer: Optional["RePatternType"] = None
    patterns: Set[str]

    ALREADY_FILTERED_FLAG = "__SecretsMasker_filtered"

    def __init__(self):
        super().__init__()
        self.patterns = set()

    @cached_property
    def _record_attrs_to_ignore(self) -> Iterable[str]:
        # Doing log.info(..., extra={'foo': 2}) sets extra properties on
        # record, i.e. record.foo. And we need to filter those too. Fun
        #
        # Create a record, and look at what attributes are on it, and ignore
        # all the default ones!

        record = logging.getLogRecordFactory()(
            # name, level, pathname, lineno, msg, args, exc_info, func=None, sinfo=None,
            "x",
            logging.INFO,
            __file__,
            1,
            "",
            tuple(),
            exc_info=None,
            func="funcname",
        )
        return frozenset(record.__dict__).difference({'msg', 'args'})

    def filter(self, record) -> bool:
        if self.ALREADY_FILTERED_FLAG in record.__dict__:
            # Filters are attached to multiple handlers and logs, keep a
            # "private" flag that stops us needing to process it more than once
            return True

        if self.replacer:
            for k, v in record.__dict__.items():
                if k in self._record_attrs_to_ignore:
                    continue
                record.__dict__[k] = self.redact(v)
            if record.exc_info:
                exc = record.exc_info[1]
                # I'm not sure if this is a good idea!
                exc.args = (self.redact(v) for v in exc.args)
        record.__dict__[self.ALREADY_FILTERED_FLAG] = True

        return True

    def _redact_all(self, item: "RedactableItem") -> "RedactableItem":
        if isinstance(item, dict):
            return {dict_key: self._redact_all(subval) for dict_key, subval in item.items()}
        elif isinstance(item, str):
            return '***'
        elif isinstance(item, (tuple, set)):
            # Turn set in to tuple!
            return tuple(self._redact_all(subval) for subval in item)
        elif isinstance(item, Iterable):
            return list(self._redact_all(subval) for subval in item)
        else:
            return item

    # pylint: disable=too-many-return-statements
    def redact(self, item: "RedactableItem", name: str = None) -> "RedactableItem":
        """
        Redact an any secrets found in ``item``, if it is a string.

        If ``name`` is given, and it's a "sensitive" name (see
        :func:`should_hide_value_for_key`) then all string values in the item
        is redacted.

        """
        if name and should_hide_value_for_key(name):
            return self._redact_all(item)

        if isinstance(item, dict):
            return {dict_key: self.redact(subval, dict_key) for dict_key, subval in item.items()}
        elif isinstance(item, str):
            if self.replacer:
                # We can't replace specific values, but the key-based redacting
                # can still happen, so we can't short-circuit, we need to walk
                # the structure.
                return self.replacer.sub('***', item)
            return item
        elif isinstance(item, (tuple, set)):
            # Turn set in to tuple!
            return tuple(self.redact(subval) for subval in item)
        elif isinstance(item, Iterable):
            return list(self.redact(subval) for subval in item)
        else:
            return item

    # pylint: enable=too-many-return-statements

    def add_mask(self, secret: Union[str, dict, Iterable], name: str = None):
        """Add a new secret to be masked to this filter instance."""
        if isinstance(secret, dict):
            for k, v in secret.items():
                self.add_mask(v, k)
        elif isinstance(secret, str):
            pattern = re.escape(secret)
            if pattern not in self.patterns and (not name or should_hide_value_for_key(name)):
                self.patterns.add(pattern)
                self.replacer = re.compile('|'.join(self.patterns))
        elif isinstance(secret, collections.abc.Iterable):
            for v in secret:
                self.add_mask(v, name)
