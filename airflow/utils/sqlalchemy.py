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

import datetime
import json
import logging
from typing import Any, Dict

import pendulum
from dateutil import relativedelta
from sqlalchemy import event, nullsfirst
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm.session import Session
from sqlalchemy.types import JSON, DateTime, Text, TypeDecorator

from airflow.configuration import conf

log = logging.getLogger(__name__)

utc = pendulum.tz.timezone('UTC')

using_mysql = conf.get('core', 'sql_alchemy_conn').lower().startswith('mysql')


# pylint: enable=unused-argument
class UtcDateTime(TypeDecorator):
    """
    Almost equivalent to :class:`~sqlalchemy.types.DateTime` with
    ``timezone=True`` option, but it differs from that by:

    - Never silently take naive :class:`~datetime.datetime`, instead it
      always raise :exc:`ValueError` unless time zone aware value.
    - :class:`~datetime.datetime` value's :attr:`~datetime.datetime.tzinfo`
      is always converted to UTC.
    - Unlike SQLAlchemy's built-in :class:`~sqlalchemy.types.DateTime`,
      it never return naive :class:`~datetime.datetime`, but time zone
      aware value, even with SQLite or MySQL.
    - Always returns DateTime in UTC

    """

    impl = DateTime(timezone=True)

    def process_bind_param(self, value, dialect):
        if value is not None:
            if not isinstance(value, datetime.datetime):
                raise TypeError('expected datetime.datetime, not ' + repr(value))
            elif value.tzinfo is None:
                raise ValueError('naive datetime is disallowed')
            # For mysql we should store timestamps as naive values
            # Timestamp in MYSQL is not timezone aware. In MySQL 5.6
            # timezone added at the end is ignored but in MySQL 5.7
            # inserting timezone value fails with 'invalid-date'
            # See https://issues.apache.org/jira/browse/AIRFLOW-7001
            if using_mysql:
                from airflow.utils.timezone import make_naive

                return make_naive(value, timezone=utc)
            return value.astimezone(utc)
        return None

    def process_result_value(self, value, dialect):
        """
        Processes DateTimes from the DB making sure it is always
        returning UTC. Not using timezone.convert_to_utc as that
        converts to configured TIMEZONE while the DB might be
        running with some other setting. We assume UTC datetimes
        in the database.
        """
        if value is not None:
            if value.tzinfo is None:
                value = value.replace(tzinfo=utc)
            else:
                value = value.astimezone(utc)

        return value


class ExtendedJSON(TypeDecorator):
    """
    A version of the JSON column that uses the Airflow extended JSON
    serialization provided by airflow.serialization.
    """

    impl = JSON()

    def process_bind_param(self, value, dialect):
        from airflow.serialization.serialized_objects import BaseSerialization

        return BaseSerialization._serialize(value)  # pylint: disable=protected-access

    def process_result_value(self, value, dialect):
        from airflow.serialization.serialized_objects import BaseSerialization

        return BaseSerialization._deserialize(value)  # pylint: disable=protected-access


class Interval(TypeDecorator):
    """Base class representing a time interval."""

    impl = Text

    attr_keys = {
        datetime.timedelta: ('days', 'seconds', 'microseconds'),
        relativedelta.relativedelta: (
            'years',
            'months',
            'days',
            'leapdays',
            'hours',
            'minutes',
            'seconds',
            'microseconds',
            'year',
            'month',
            'day',
            'hour',
            'minute',
            'second',
            'microsecond',
        ),
    }

    def process_bind_param(self, value, dialect):
        if isinstance(value, tuple(self.attr_keys)):
            attrs = {key: getattr(value, key) for key in self.attr_keys[type(value)]}
            return json.dumps({'type': type(value).__name__, 'attrs': attrs})
        return json.dumps(value)

    def process_result_value(self, value, dialect):
        if not value:
            return value
        data = json.loads(value)
        if isinstance(data, dict):
            type_map = {key.__name__: key for key in self.attr_keys}
            return type_map[data['type']](**data['attrs'])
        return data


def skip_locked(session: Session) -> Dict[str, Any]:
    """
    Return kargs for passing to `with_for_update()` suitable for the current DB engine version.

    We do this as we document the fact that on DB engines that don't support this construct, we do not
    support/recommend running HA scheduler. If a user ignores this and tries anyway everything will still
    work, just slightly slower in some circumstances.

    Specifically don't emit SKIP LOCKED for MySQL < 8, or MariaDB, neither of which support this construct

    See https://jira.mariadb.org/browse/MDEV-13115
    """
    dialect = session.bind.dialect

    if dialect.name != "mysql" or dialect.supports_for_update_of:
        return {'skip_locked': True}
    else:
        return {}


def nowait(session: Session) -> Dict[str, Any]:
    """
    Return kwargs for passing to `with_for_update()` suitable for the current DB engine version.

    We do this as we document the fact that on DB engines that don't support this construct, we do not
    support/recommend running HA scheduler. If a user ignores this and tries anyway everything will still
    work, just slightly slower in some circumstances.

    Specifically don't emit NOWAIT for MySQL < 8, or MariaDB, neither of which support this construct

    See https://jira.mariadb.org/browse/MDEV-13115
    """
    dialect = session.bind.dialect

    if dialect.name != "mysql" or dialect.supports_for_update_of:
        return {'nowait': True}
    else:
        return {}


def nulls_first(col, session: Session) -> Dict[str, Any]:
    """
    Adds a nullsfirst construct to the column ordering. Currently only Postgres supports it.
    In MySQL & Sqlite NULL values are considered lower than any non-NULL value, therefore, NULL values
    appear first when the order is ASC (ascending)
    """
    if session.bind.dialect.name == "postgresql":
        return nullsfirst(col)
    else:
        return col


USE_ROW_LEVEL_LOCKING: bool = conf.getboolean('scheduler', 'use_row_level_locking', fallback=True)


def with_row_locks(query, session: Session, **kwargs):
    """
    Apply with_for_update to an SQLAlchemy query, if row level locking is in use.

    :param query: An SQLAlchemy Query object
    :param session: ORM Session
    :param kwargs: Extra kwargs to pass to with_for_update (of, nowait, skip_locked, etc)
    :return: updated query
    """
    dialect = session.bind.dialect

    # Don't use row level locks if the MySQL dialect (Mariadb & MySQL < 8) does not support it.
    if USE_ROW_LEVEL_LOCKING and (dialect.name != "mysql" or dialect.supports_for_update_of):
        return query.with_for_update(**kwargs)
    else:
        return query


class CommitProhibitorGuard:
    """Context manager class that powers prohibit_commit"""

    expected_commit = False

    def __init__(self, session: Session):
        self.session = session

    def _validate_commit(self, _):
        if self.expected_commit:
            self.expected_commit = False
            return
        raise RuntimeError("UNEXPECTED COMMIT - THIS WILL BREAK HA LOCKS!")

    def __enter__(self):
        event.listen(self.session, 'before_commit', self._validate_commit)
        return self

    def __exit__(self, *exc_info):
        event.remove(self.session, 'before_commit', self._validate_commit)

    def commit(self):
        """
        Commit the session.

        This is the required way to commit when the guard is in scope
        """
        self.expected_commit = True
        self.session.commit()


def prohibit_commit(session):
    """
    Return a context manager that will disallow any commit that isn't done via the context manager.

    The aim of this is to ensure that transaction lifetime is strictly controlled which is especially
    important in the core scheduler loop. Any commit on the session that is _not_ via this context manager
    will result in RuntimeError

    Example usage:

    .. code:: python

        with prohibit_commit(session) as guard:
            # ... do something with session
            guard.commit()

            # This would throw an error
            # session.commit()
    """
    return CommitProhibitorGuard(session)


def is_lock_not_available_error(error: OperationalError):
    """Check if the Error is about not being able to acquire lock"""
    # DB specific error codes:
    # Postgres: 55P03
    # MySQL: 3572, 'Statement aborted because lock(s) could not be acquired immediately and NOWAIT
    #               is set.'
    # MySQL: 1205, 'Lock wait timeout exceeded; try restarting transaction
    #              (when NOWAIT isn't available)
    db_err_code = getattr(error.orig, 'pgcode', None) or error.orig.args[0]

    # We could test if error.orig is an instance of
    # psycopg2.errors.LockNotAvailable/_mysql_exceptions.OperationalError, but that involves
    # importing it. This doesn't
    if db_err_code in ('55P03', 1205, 3572):
        return True
    return False
