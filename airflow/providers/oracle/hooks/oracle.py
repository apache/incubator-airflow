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

from datetime import datetime
from typing import List, Optional

import cx_Oracle
import numpy

from airflow.hooks.dbapi import DbApiHook


class OracleHook(DbApiHook):
    """
    Interact with Oracle SQL.

    :param oracle_conn_id: The :ref:`Oracle connection id <howto/connection:oracle>`
        used for Oracle credentials.
    :type oracle_conn_id: str
    """

    conn_name_attr = 'oracle_conn_id'
    default_conn_name = 'oracle_default'
    conn_type = 'oracle'
    hook_name = 'Oracle'

    supports_autocommit = False

    # pylint: disable=c-extension-no-member
    def get_conn(self) -> 'OracleHook':
        """
        Returns a oracle connection object
        Optional parameters for using a custom DSN connection
        (instead of using a server alias from tnsnames.ora)
        The dsn (data source name) is the TNS entry
        (from the Oracle names server or tnsnames.ora file)
        or is a string like the one returned from makedsn().

        :param dsn: the host address for the Oracle server
        :param service_name: the db_unique_name of the database
              that you are connecting to (CONNECT_DATA part of TNS)

        You can set these parameters in the extra fields of your connection
        as in ``{ "dsn":"some.host.address" , "service_name":"some.service.name" }``
        see more param detail in
        `cx_Oracle.connect <https://cx-oracle.readthedocs.io/en/latest/module.html#cx_Oracle.connect>`_


        """
        conn = self.get_connection(
            self.oracle_conn_id  # type: ignore[attr-defined]  # pylint: disable=no-member
        )
        conn_config = {'user': conn.login, 'password': conn.password}
        dsn = conn.extra_dejson.get('dsn')
        sid = conn.extra_dejson.get('sid')
        mod = conn.extra_dejson.get('module')

        service_name = conn.extra_dejson.get('service_name')
        port = conn.port if conn.port else 1521
        if dsn and sid and not service_name:
            conn_config['dsn'] = cx_Oracle.makedsn(dsn, port, sid)
        elif dsn and service_name and not sid:
            conn_config['dsn'] = cx_Oracle.makedsn(dsn, port, service_name=service_name)
        else:
            conn_config['dsn'] = conn.host

        if 'encoding' in conn.extra_dejson:
            conn_config['encoding'] = conn.extra_dejson.get('encoding')
            # if `encoding` is specific but `nencoding` is not
            # `nencoding` should use same values as `encoding` to set encoding, inspired by
            # https://github.com/oracle/python-cx_Oracle/issues/157#issuecomment-371877993
            if 'nencoding' not in conn.extra_dejson:
                conn_config['nencoding'] = conn.extra_dejson.get('encoding')
        if 'nencoding' in conn.extra_dejson:
            conn_config['nencoding'] = conn.extra_dejson.get('nencoding')
        if 'threaded' in conn.extra_dejson:
            conn_config['threaded'] = conn.extra_dejson.get('threaded')
        if 'events' in conn.extra_dejson:
            conn_config['events'] = conn.extra_dejson.get('events')

        mode = conn.extra_dejson.get('mode', '').lower()
        if mode == 'sysdba':
            conn_config['mode'] = cx_Oracle.SYSDBA
        elif mode == 'sysasm':
            conn_config['mode'] = cx_Oracle.SYSASM
        elif mode == 'sysoper':
            conn_config['mode'] = cx_Oracle.SYSOPER
        elif mode == 'sysbkp':
            conn_config['mode'] = cx_Oracle.SYSBKP
        elif mode == 'sysdgd':
            conn_config['mode'] = cx_Oracle.SYSDGD
        elif mode == 'syskmt':
            conn_config['mode'] = cx_Oracle.SYSKMT
        elif mode == 'sysrac':
            conn_config['mode'] = cx_Oracle.SYSRAC

        purity = conn.extra_dejson.get('purity', '').lower()
        if purity == 'new':
            conn_config['purity'] = cx_Oracle.ATTR_PURITY_NEW
        elif purity == 'self':
            conn_config['purity'] = cx_Oracle.ATTR_PURITY_SELF
        elif purity == 'default':
            conn_config['purity'] = cx_Oracle.ATTR_PURITY_DEFAULT

        conn = cx_Oracle.connect(**conn_config)
        if mod is not None:
            conn.module = mod

        return conn

    def insert_rows(
        self,
        table: str,
        rows: List[tuple],
        target_fields=None,
        commit_every: int = 1000,
        replace: Optional[bool] = False,
        **kwargs,
    ) -> None:
        """
        A generic way to insert a set of tuples into a table,
        the whole set of inserts is treated as one transaction
        Changes from standard DbApiHook implementation:

        - Oracle SQL queries in cx_Oracle can not be terminated with a semicolon (`;`)
        - Replace NaN values with NULL using `numpy.nan_to_num` (not using
          `is_nan()` because of input types error for strings)
        - Coerce datetime cells to Oracle DATETIME format during insert

        :param table: target Oracle table, use dot notation to target a
            specific database
        :type table: str
        :param rows: the rows to insert into the table
        :type rows: iterable of tuples
        :param target_fields: the names of the columns to fill in the table
        :type target_fields: iterable of str
        :param commit_every: the maximum number of rows to insert in one transaction
            Default 1000, Set greater than 0.
            Set 1 to insert each row in each single transaction
        :type commit_every: int
        :param replace: Whether to replace instead of insert
        :type replace: bool
        """
        if target_fields:
            target_fields = ', '.join(target_fields)
            target_fields = f'({target_fields})'
        else:
            target_fields = ''
        conn = self.get_conn()
        cur = conn.cursor()  # type: ignore[attr-defined]
        if self.supports_autocommit:
            cur.execute('SET autocommit = 0')
        conn.commit()  # type: ignore[attr-defined]
        i = 0
        for row in rows:
            i += 1
            lst = []
            for cell in row:
                if isinstance(cell, str):
                    lst.append("'" + str(cell).replace("'", "''") + "'")
                elif cell is None:
                    lst.append('NULL')
                elif isinstance(cell, float) and numpy.isnan(cell):  # coerce numpy NaN to NULL
                    lst.append('NULL')
                elif isinstance(cell, numpy.datetime64):
                    lst.append("'" + str(cell) + "'")
                elif isinstance(cell, datetime):
                    lst.append(
                        "to_date('" + cell.strftime('%Y-%m-%d %H:%M:%S') + "','YYYY-MM-DD HH24:MI:SS')"
                    )
                else:
                    lst.append(str(cell))
            values = tuple(lst)
            sql = f"INSERT /*+ APPEND */ INTO {table} {target_fields} VALUES ({','.join(values)})"
            cur.execute(sql)
            if i % commit_every == 0:
                conn.commit()  # type: ignore[attr-defined]
                self.log.info('Loaded %s into %s rows so far', i, table)
        conn.commit()  # type: ignore[attr-defined]
        cur.close()
        conn.close()  # type: ignore[attr-defined]
        self.log.info('Done loading. Loaded a total of %s rows', i)

    def bulk_insert_rows(
        self,
        table: str,
        rows: List[tuple],
        target_fields: Optional[List[str]] = None,
        commit_every: int = 5000,
    ):
        """
        A performant bulk insert for cx_Oracle
        that uses prepared statements via `executemany()`.
        For best performance, pass in `rows` as an iterator.

        :param table: target Oracle table, use dot notation to target a
            specific database
        :type table: str
        :param rows: the rows to insert into the table
        :type rows: iterable of tuples
        :param target_fields: the names of the columns to fill in the table, default None.
            If None, each rows should have some order as table columns name
        :type target_fields: iterable of str Or None
        :param commit_every: the maximum number of rows to insert in one transaction
            Default 5000. Set greater than 0. Set 1 to insert each row in each transaction
        :type commit_every: int
        """
        if not rows:
            raise ValueError("parameter rows could not be None or empty iterable")
        conn = self.get_conn()
        cursor = conn.cursor()  # type: ignore[attr-defined]
        values_base = target_fields if target_fields else rows[0]
        prepared_stm = 'insert into {tablename} {columns} values ({values})'.format(
            tablename=table,
            columns='({})'.format(', '.join(target_fields)) if target_fields else '',
            values=', '.join(':%s' % i for i in range(1, len(values_base) + 1)),
        )
        row_count = 0
        # Chunk the rows
        row_chunk = []
        for row in rows:
            row_chunk.append(row)
            row_count += 1
            if row_count % commit_every == 0:
                cursor.prepare(prepared_stm)
                cursor.executemany(None, row_chunk)
                conn.commit()  # type: ignore[attr-defined]
                self.log.info('[%s] inserted %s rows', table, row_count)
                # Empty chunk
                row_chunk = []
        # Commit the leftover chunk
        cursor.prepare(prepared_stm)
        cursor.executemany(None, row_chunk)
        conn.commit()  # type: ignore[attr-defined]
        self.log.info('[%s] inserted %s rows', table, row_count)
        cursor.close()
        conn.close()  # type: ignore[attr-defined]
