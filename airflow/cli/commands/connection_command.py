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
"""Connection sub-commands"""
import io
import json
import os
import sys
from typing import Any, Dict, List
from urllib.parse import urlparse, urlunparse

from sqlalchemy.orm import exc

import airflow.utils.yaml as yaml
from airflow.cli.simple_table import AirflowConsole
from airflow.exceptions import AirflowNotFoundException
from airflow.hooks.base import BaseHook
from airflow.models import Connection
from airflow.utils import cli as cli_utils
from airflow.utils.cli import suppress_logs_and_warning
from airflow.utils.session import create_session


def _connection_mapper(conn: Connection) -> Dict[str, Any]:
    return {
        'id': conn.id,
        'conn_id': conn.conn_id,
        'conn_type': conn.conn_type,
        'description': conn.description,
        'host': conn.host,
        'schema': conn.schema,
        'login': conn.login,
        'password': conn.password,
        'port': conn.port,
        'is_encrypted': conn.is_encrypted,
        'is_extra_encrypted': conn.is_encrypted,
        'extra_dejson': conn.extra_dejson,
        'get_uri': conn.get_uri(),
    }


@suppress_logs_and_warning
def connections_get(args):
    """Get a connection."""
    try:
        conn = BaseHook.get_connection(args.conn_id)
    except AirflowNotFoundException:
        raise SystemExit("Connection not found.")
    AirflowConsole().print_as(
        data=[conn],
        output=args.output,
        mapper=_connection_mapper,
    )


@suppress_logs_and_warning
def connections_list(args):
    """Lists all connections at the command line"""
    with create_session() as session:
        query = session.query(Connection)
        if args.conn_id:
            query = query.filter(Connection.conn_id == args.conn_id)
        conns = query.all()

        AirflowConsole().print_as(
            data=conns,
            output=args.output,
            mapper=_connection_mapper,
        )


def _format_connections(conns: List[Connection], fmt: str) -> str:
    if fmt == '.env':
        connections_env = ""
        for conn in conns:
            connections_env += f"{conn.conn_id}={conn.get_uri()}\n"
        return connections_env

    connections_dict = {}
    for conn in conns:
        connections_dict[conn.conn_id] = {
            'conn_type': conn.conn_type,
            'description': conn.description,
            'host': conn.host,
            'login': conn.login,
            'password': conn.password,
            'schema': conn.schema,
            'port': conn.port,
            'extra': conn.extra,
        }

    if fmt == '.yaml':
        return yaml.dump(connections_dict)

    if fmt == '.json':
        return json.dumps(connections_dict, indent=2)

    return json.dumps(connections_dict)


def _is_stdout(fileio: io.TextIOWrapper) -> bool:
    return fileio.name == '<stdout>'


def _valid_uri(uri: str) -> bool:
    """Check if a URI is valid, by checking if both scheme and netloc are available"""
    uri_parts = urlparse(uri)
    return uri_parts.scheme != '' and uri_parts.netloc != ''


def connections_export(args):
    """Exports all connections to a file"""
    allowed_formats = ['.yaml', '.json', '.env']
    provided_format = None if args.format is None else f".{args.format.lower()}"
    default_format = provided_format or '.json'

    with create_session() as session:
        if _is_stdout(args.file):
            filetype = default_format
        elif provided_format is not None:
            filetype = provided_format
        else:
            _, filetype = os.path.splitext(args.file.name)
            filetype = filetype.lower()
            if filetype not in allowed_formats:
                raise SystemExit(
                    f"Unsupported file format. The file must have "
                    f"the extension {', '.join(allowed_formats)}."
                )

        connections = session.query(Connection).order_by(Connection.conn_id).all()
        msg = _format_connections(connections, filetype)
        args.file.write(msg)

        if _is_stdout(args.file):
            print("Connections successfully exported.", file=sys.stderr)
        else:
            print(f"Connections successfully exported to {args.file.name}.")


alternative_conn_specs = ['conn_type', 'conn_host', 'conn_login', 'conn_password', 'conn_schema', 'conn_port']


@cli_utils.action_logging
def connections_add(args):
    """Adds new connection"""
    # Check that the conn_id and conn_uri args were passed to the command:
    missing_args = []
    invalid_args = []
    if args.conn_uri:
        if not _valid_uri(args.conn_uri):
            raise SystemExit(f'The URI provided to --conn-uri is invalid: {args.conn_uri}')
        for arg in alternative_conn_specs:
            if getattr(args, arg) is not None:
                invalid_args.append(arg)
    elif not args.conn_type:
        missing_args.append('conn-uri or conn-type')
    if missing_args:
        raise SystemExit(f'The following args are required to add a connection: {missing_args!r}')
    if invalid_args:
        raise SystemExit(
            f'The following args are not compatible with the '
            f'add flag and --conn-uri flag: {invalid_args!r}'
        )

    if args.conn_uri:
        new_conn = Connection(conn_id=args.conn_id, description=args.conn_description, uri=args.conn_uri)
    else:
        new_conn = Connection(
            conn_id=args.conn_id,
            conn_type=args.conn_type,
            description=args.conn_description,
            host=args.conn_host,
            login=args.conn_login,
            password=args.conn_password,
            schema=args.conn_schema,
            port=args.conn_port,
        )
    if args.conn_extra is not None:
        new_conn.set_extra(args.conn_extra)

    with create_session() as session:
        if not session.query(Connection).filter(Connection.conn_id == new_conn.conn_id).first():
            session.add(new_conn)
            msg = 'Successfully added `conn_id`={conn_id} : {uri}'
            msg = msg.format(
                conn_id=new_conn.conn_id,
                uri=args.conn_uri
                or urlunparse(
                    (
                        args.conn_type,
                        '{login}:{password}@{host}:{port}'.format(
                            login=args.conn_login or '',
                            password='******' if args.conn_password else '',
                            host=args.conn_host or '',
                            port=args.conn_port or '',
                        ),
                        args.conn_schema or '',
                        '',
                        '',
                        '',
                    )
                ),
            )
            print(msg)
        else:
            msg = f'A connection with `conn_id`={new_conn.conn_id} already exists.'
            raise SystemExit(msg)


@cli_utils.action_logging
def connections_delete(args):
    """Deletes connection from DB"""
    with create_session() as session:
        try:
            to_delete = session.query(Connection).filter(Connection.conn_id == args.conn_id).one()
        except exc.NoResultFound:
            raise SystemExit(f'Did not find a connection with `conn_id`={args.conn_id}')
        except exc.MultipleResultsFound:
            raise SystemExit(f'Found more than one connection with `conn_id`={args.conn_id}')
        else:
            session.delete(to_delete)
            print(f"Successfully deleted connection with `conn_id`={to_delete.conn_id}")
