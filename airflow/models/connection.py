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

import json
import warnings
from json import JSONDecodeError
from typing import Dict, Optional
from urllib.parse import parse_qsl, quote, unquote, urlencode, urlparse

from sqlalchemy import Boolean, Column, Integer, String, Text
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import reconstructor, synonym

from airflow.configuration import ensure_secrets_loaded
from airflow.exceptions import AirflowException, AirflowNotFoundException
from airflow.models.base import ID_LEN, Base
from airflow.models.crypto import get_fernet
from airflow.providers_manager import ProvidersManager
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.log.secrets_masker import mask_secret
from airflow.utils.module_loading import import_string


def parse_netloc_to_hostname(*args, **kwargs):
    """This method is deprecated."""
    warnings.warn("This method is deprecated.", DeprecationWarning)
    return _parse_netloc_to_hostname(*args, **kwargs)


# Python automatically converts all letters to lowercase in hostname
# See: https://issues.apache.org/jira/browse/AIRFLOW-3615
def _parse_netloc_to_hostname(uri_parts):
    """Parse a URI string to get correct Hostname."""
    hostname = unquote(uri_parts.hostname or '') if "," not in uri_parts.netloc else unquote(uri_parts.netloc)
    if '/' in hostname:
        hostname = uri_parts.netloc
        if "@" in hostname:
            hostname = hostname.rsplit("@", 1)[1]
        if ":" in hostname:
            hostname = hostname.split(":", 1)[0]
        hostname = unquote(hostname)
    return hostname


class Connection(Base, LoggingMixin):  # pylint: disable=too-many-instance-attributes
    """
    Placeholder to store information about different database instances
    connection information. The idea here is that scripts use references to
    database instances (conn_id) instead of hard coding hostname, logins and
    passwords when using operators or hooks.

    .. seealso::
        For more information on how to use this class, see: :doc:`/howto/connection`

    :param conn_id: The connection ID.
    :type conn_id: str
    :param conn_type: The connection type.
    :type conn_type: str
    :param description: The connection description.
    :type description: str
    :param host: The host.
    :type host: str
    :param login: The login.
    :type login: str
    :param password: The password.
    :type password: str
    :param schema: The schema.
    :type schema: str
    :param port: The port number.
    :type port: int
    :param extra: Extra metadata. Non-standard data such as private/SSH keys can be saved here. JSON
        encoded object.
    :type extra: str
    :param uri: URI address describing connection parameters.
    :type uri: str
    """

    EXTRA_KEY = '__extra__'

    __tablename__ = "connection"

    id = Column(Integer(), primary_key=True)
    conn_id = Column(String(ID_LEN), unique=True, nullable=False)
    conn_type = Column(String(500), nullable=False)
    description = Column(Text(5000))
    host = Column(String(500))
    schema = Column(String(500))
    login = Column(String(500))
    _password = Column('password', String(5000))
    port = Column(Integer())
    is_encrypted = Column(Boolean, unique=False, default=False)
    is_extra_encrypted = Column(Boolean, unique=False, default=False)
    _extra = Column('extra', Text())

    def __init__(  # pylint: disable=too-many-arguments
        self,
        conn_id: Optional[str] = None,
        conn_type: Optional[str] = None,
        description: Optional[str] = None,
        host: Optional[str] = None,
        login: Optional[str] = None,
        password: Optional[str] = None,
        schema: Optional[str] = None,
        port: Optional[int] = None,
        extra: Optional[str] = None,
        uri: Optional[str] = None,
    ):
        super().__init__()
        self.conn_id = conn_id
        self.description = description
        if uri and (  # pylint: disable=too-many-boolean-expressions
            conn_type or host or login or password or schema or port or extra
        ):
            raise AirflowException(
                "You must create an object using the URI or individual values "
                "(conn_type, host, login, password, schema, port or extra)."
                "You can't mix these two ways to create this object."
            )
        if uri:
            self._parse_from_uri(uri)
        else:
            self.conn_type = conn_type
            self.host = host
            self.login = login
            self.password = password
            self.schema = schema
            self.port = port
            self.extra = extra

        if self.password:
            mask_secret(self.password)

    @reconstructor
    def on_db_load(self):  # pylint: disable=missing-function-docstring
        if self.password:
            mask_secret(self.password)

    def parse_from_uri(self, **uri):
        """This method is deprecated. Please use uri parameter in constructor."""
        warnings.warn(
            "This method is deprecated. Please use uri parameter in constructor.", DeprecationWarning
        )
        self._parse_from_uri(**uri)

    def _parse_from_uri(self, uri: str):
        uri_parts = urlparse(uri)
        conn_type = uri_parts.scheme
        if conn_type == 'postgresql':
            conn_type = 'postgres'
        elif '-' in conn_type:
            conn_type = conn_type.replace('-', '_')
        self.conn_type = conn_type
        self.host = _parse_netloc_to_hostname(uri_parts)
        quoted_schema = uri_parts.path[1:]
        self.schema = unquote(quoted_schema) if quoted_schema else quoted_schema
        self.login = unquote(uri_parts.username) if uri_parts.username else uri_parts.username
        self.password = unquote(uri_parts.password) if uri_parts.password else uri_parts.password
        self.port = uri_parts.port if "," not in self.host else None
        if uri_parts.query:
            query = dict(parse_qsl(uri_parts.query, keep_blank_values=True))
            if self.EXTRA_KEY in query:
                self.extra = query[self.EXTRA_KEY]
            else:
                self.extra = json.dumps(query)

    def get_uri(self) -> str:
        """Return connection in URI format"""
        uri = f"{str(self.conn_type).lower().replace('_', '-')}://"

        authority_block = ''
        if self.login is not None:
            authority_block += quote(self.login, safe='')

        if self.password is not None:
            authority_block += ':' + quote(self.password, safe='')

        if authority_block > '':
            authority_block += '@'

            uri += authority_block

        host_block = ''
        if self.host:
            host_block += quote(self.host, safe='')

        if self.port:
            if host_block > '':
                host_block += f':{self.port}'
            else:
                host_block += f'@:{self.port}'

        if self.schema:
            host_block += f"/{quote(self.schema, safe='')}"

        uri += host_block

        if self.extra:
            try:
                query = urlencode(self.extra_dejson)
            except TypeError:
                query = None
            if query and self.extra_dejson == dict(parse_qsl(query, keep_blank_values=True)):
                uri += '?' + query
            else:
                uri += '?' + urlencode({self.EXTRA_KEY: self.extra})

        return uri

    def get_password(self) -> Optional[str]:
        """Return encrypted password."""
        if self._password and self.is_encrypted:
            fernet = get_fernet()
            if not fernet.is_encrypted:
                raise AirflowException(
                    "Can't decrypt encrypted password for login={}, \
                    FERNET_KEY configuration is missing".format(
                        self.login
                    )
                )
            return fernet.decrypt(bytes(self._password, 'utf-8')).decode()
        else:
            return self._password

    def set_password(self, value: Optional[str]):
        """Encrypt password and set in object attribute."""
        if value:
            fernet = get_fernet()
            self._password = fernet.encrypt(bytes(value, 'utf-8')).decode()
            self.is_encrypted = fernet.is_encrypted

    @declared_attr
    def password(cls):  # pylint: disable=no-self-argument
        """Password. The value is decrypted/encrypted when reading/setting the value."""
        return synonym('_password', descriptor=property(cls.get_password, cls.set_password))

    def get_extra(self) -> Dict:
        """Return encrypted extra-data."""
        if self._extra and self.is_extra_encrypted:
            fernet = get_fernet()
            if not fernet.is_encrypted:
                raise AirflowException(
                    "Can't decrypt `extra` params for login={},\
                    FERNET_KEY configuration is missing".format(
                        self.login
                    )
                )
            return fernet.decrypt(bytes(self._extra, 'utf-8')).decode()
        else:
            return self._extra

    def set_extra(self, value: str):
        """Encrypt extra-data and save in object attribute to object."""
        if value:
            fernet = get_fernet()
            self._extra = fernet.encrypt(bytes(value, 'utf-8')).decode()
            self.is_extra_encrypted = fernet.is_encrypted
        else:
            self._extra = value
            self.is_extra_encrypted = False

    @declared_attr
    def extra(cls):  # pylint: disable=no-self-argument
        """Extra data. The value is decrypted/encrypted when reading/setting the value."""
        return synonym('_extra', descriptor=property(cls.get_extra, cls.set_extra))

    def rotate_fernet_key(self):
        """Encrypts data with a new key. See: :ref:`security/fernet`"""
        fernet = get_fernet()
        if self._password and self.is_encrypted:
            self._password = fernet.rotate(self._password.encode('utf-8')).decode()
        if self._extra and self.is_extra_encrypted:
            self._extra = fernet.rotate(self._extra.encode('utf-8')).decode()

    def get_hook(self):
        """Return hook based on conn_type."""
        hook_class_name, conn_id_param, package_name, hook_name = ProvidersManager().hooks.get(
            self.conn_type, (None, None, None, None)
        )

        if not hook_class_name:
            raise AirflowException(f'Unknown hook type "{self.conn_type}"')
        try:
            hook_class = import_string(hook_class_name)
        except ImportError:
            warnings.warn(
                "Could not import %s when discovering %s %s", hook_class_name, hook_name, package_name
            )
            raise
        return hook_class(**{conn_id_param: self.conn_id})

    def __repr__(self):
        return self.conn_id

    def log_info(self):
        """
        This method is deprecated. You can read each field individually or use the
        default representation (`__repr__`).
        """
        warnings.warn(
            "This method is deprecated. You can read each field individually or "
            "use the default representation (__repr__).",
            DeprecationWarning,
            stacklevel=2,
        )
        return "id: {}. Host: {}, Port: {}, Schema: {}, Login: {}, Password: {}, extra: {}".format(
            self.conn_id,
            self.host,
            self.port,
            self.schema,
            self.login,
            "XXXXXXXX" if self.password else None,
            "XXXXXXXX" if self.extra_dejson else None,
        )

    def debug_info(self):
        """
        This method is deprecated. You can read each field individually or use the
        default representation (`__repr__`).
        """
        warnings.warn(
            "This method is deprecated. You can read each field individually or "
            "use the default representation (__repr__).",
            DeprecationWarning,
            stacklevel=2,
        )
        return "id: {}. Host: {}, Port: {}, Schema: {}, Login: {}, Password: {}, extra: {}".format(
            self.conn_id,
            self.host,
            self.port,
            self.schema,
            self.login,
            "XXXXXXXX" if self.password else None,
            self.extra_dejson,
        )

    @property
    def extra_dejson(self) -> Dict:
        """Returns the extra property by deserializing json."""
        obj = {}
        if self.extra:
            try:
                obj = json.loads(self.extra)

            except JSONDecodeError:
                self.log.exception("Failed parsing the json for conn_id %s", self.conn_id)

            # Mask sensitive keys from this list
            mask_secret(obj)

        return obj

    @classmethod
    def get_connection_from_secrets(cls, conn_id: str) -> 'Connection':
        """
        Get connection by conn_id.

        :param conn_id: connection id
        :return: connection
        """
        for secrets_backend in ensure_secrets_loaded():
            conn = secrets_backend.get_connection(conn_id=conn_id)
            if conn:
                return conn
        raise AirflowNotFoundException(f"The conn_id `{conn_id}` isn't defined")
