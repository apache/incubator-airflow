# pylint: disable=c-extension-no-member
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
"""This module contains ODBC hook."""
from typing import Any, Optional
from urllib.parse import quote_plus

import pyodbc

from airflow.hooks.dbapi import DbApiHook
from airflow.utils.helpers import merge_dicts


class OdbcHook(DbApiHook):
    """
    Interact with odbc data sources using pyodbc.

    See :doc:`/connections/odbc` for full documentation.
    """

    DEFAULT_SQLALCHEMY_SCHEME = 'mssql+pyodbc'
    conn_name_attr = 'odbc_conn_id'
    default_conn_name = 'odbc_default'
    conn_type = 'odbc'
    hook_name = 'ODBC'
    supports_autocommit = True

    def __init__(
        self,
        *args,
        database: Optional[str] = None,
        driver: Optional[str] = None,
        dsn: Optional[str] = None,
        connect_kwargs: Optional[dict] = None,
        sqlalchemy_scheme: Optional[str] = None,
        **kwargs,
    ) -> None:
        """
        :param args: passed to DbApiHook
        :param database: database to use -- overrides connection ``schema``
        :param driver: name of driver or path to driver. overrides driver supplied in connection ``extra``
        :param dsn: name of DSN to use.  overrides DSN supplied in connection ``extra``
        :param connect_kwargs: keyword arguments passed to ``pyodbc.connect``
        :param sqlalchemy_scheme: Scheme sqlalchemy connection.  Default is ``mssql+pyodbc`` Only used for
          ``get_sqlalchemy_engine`` and ``get_sqlalchemy_connection`` methods.
        :param kwargs: passed to DbApiHook
        """
        super().__init__(*args, **kwargs)
        self._database = database
        self._driver = driver
        self._dsn = dsn
        self._conn_str = None
        self._sqlalchemy_scheme = sqlalchemy_scheme
        self._connection = None
        self._connect_kwargs = connect_kwargs

    @property
    def connection(self):
        """``airflow.Connection`` object with connection id ``odbc_conn_id``"""
        if not self._connection:
            self._connection = self.get_connection(getattr(self, self.conn_name_attr))
        return self._connection

    @property
    def database(self) -> Optional[str]:
        """Database provided in init if exists; otherwise, ``schema`` from ``Connection`` object."""
        return self._database or self.connection.schema

    @property
    def sqlalchemy_scheme(self) -> Optional[str]:
        """Database provided in init if exists; otherwise, ``schema`` from ``Connection`` object."""
        return (
            self._sqlalchemy_scheme
            or self.connection_extra_lower.get('sqlalchemy_scheme')
            or self.DEFAULT_SQLALCHEMY_SCHEME
        )

    @property
    def connection_extra_lower(self) -> dict:
        """
        ``connection.extra_dejson`` but where keys are converted to lower case.

        This is used internally for case-insensitive access of odbc params.
        """
        return {k.lower(): v for k, v in self.connection.extra_dejson.items()}

    @property
    def driver(self) -> Optional[str]:
        """Driver from init param if given; else try to find one in connection extra."""
        if not self._driver:
            driver = self.connection_extra_lower.get('driver')
            if driver:
                self._driver = driver
        return self._driver and self._driver.strip().lstrip('{').rstrip('}').strip()

    @property
    def dsn(self) -> Optional[str]:
        """DSN from init param if given; else try to find one in connection extra."""
        if not self._dsn:
            dsn = self.connection_extra_lower.get('dsn')
            if dsn:
                self._dsn = dsn.strip()
        return self._dsn

    @property
    def odbc_connection_string(self):
        """
        ODBC connection string
        We build connection string instead of using ``pyodbc.connect`` params because, for example, there is
        no param representing ``ApplicationIntent=ReadOnly``.  Any key-value pairs provided in
        ``Connection.extra`` will be added to the connection string.
        """
        if not self._conn_str:
            conn_str = ''
            if self.driver:
                conn_str += f"DRIVER={{{self.driver}}};"
            if self.dsn:
                conn_str += f"DSN={self.dsn};"
            if self.connection.host:
                conn_str += f"SERVER={self.connection.host};"
            database = self.database or self.connection.schema
            if database:
                conn_str += f"DATABASE={database};"
            if self.connection.login:
                conn_str += f"UID={self.connection.login};"
            if self.connection.password:
                conn_str += f"PWD={self.connection.password};"
            if self.connection.port:
                f"PORT={self.connection.port};"

            extra_exclude = {'driver', 'dsn', 'connect_kwargs', 'sqlalchemy_scheme'}
            extra_params = {
                k: v for k, v in self.connection.extra_dejson.items() if not k.lower() in extra_exclude
            }
            for k, v in extra_params.items():
                conn_str += f"{k}={v};"

            self._conn_str = conn_str
        return self._conn_str

    @property
    def connect_kwargs(self) -> dict:
        """
        Returns effective kwargs to be passed to ``pyodbc.connect`` after merging between conn extra,
        ``connect_kwargs`` and hook init.

        Hook ``connect_kwargs`` precedes ``connect_kwargs`` from conn extra.

        If ``attrs_before`` provided, keys and values are converted to int, as required by pyodbc.
        """

        conn_connect_kwargs = self.connection_extra_lower.get('connect_kwargs', {})
        hook_connect_kwargs = self._connect_kwargs or {}
        merged_connect_kwargs = merge_dicts(conn_connect_kwargs, hook_connect_kwargs)

        if 'attrs_before' in merged_connect_kwargs:
            merged_connect_kwargs['attrs_before'] = {
                int(k): int(v) for k, v in merged_connect_kwargs['attrs_before'].items()
            }

        return {k: v for k, v in merged_connect_kwargs.items()}

    def get_conn(self) -> pyodbc.Connection:
        """Returns a pyodbc connection object."""
        conn = pyodbc.connect(self.odbc_connection_string, **self.connect_kwargs)
        return conn

    def get_uri(self) -> str:
        """URI invoked in :py:meth:`~airflow.hooks.dbapi.DbApiHook.get_sqlalchemy_engine` method"""
        quoted_conn_str = quote_plus(self.odbc_connection_string)
        uri = f"{self.sqlalchemy_scheme}:///?odbc_connect={quoted_conn_str}"
        return uri

    def get_sqlalchemy_connection(
        self, connect_kwargs: Optional[dict] = None, engine_kwargs: Optional[dict] = None
    ) -> Any:
        """Sqlalchemy connection object"""
        engine = self.get_sqlalchemy_engine(engine_kwargs=engine_kwargs)
        cnx = engine.connect(**(connect_kwargs or {}))
        return cnx
