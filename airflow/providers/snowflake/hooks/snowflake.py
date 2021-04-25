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
from typing import Any, Dict, Optional, Tuple

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

# pylint: disable=no-name-in-module
from snowflake import connector
from snowflake.connector import SnowflakeConnection

from airflow.hooks.dbapi import DbApiHook


class SnowflakeHook(DbApiHook):
    """
    A client to interact with Snowflake.

    This hook requires the snowflake_conn_id connection. The snowflake host, login,
    and, password field must be setup in the connection. Other inputs can be defined
    in the connection or hook instantiation. If used with the S3ToSnowflakeOperator
    add 'aws_access_key_id' and 'aws_secret_access_key' to extra field in the connection.

    :param account: snowflake account name
    :type account: Optional[str]
    :param authenticator: authenticator for Snowflake.
        'snowflake' (default) to use the internal Snowflake authenticator
        'externalbrowser' to authenticate using your web browser and
        Okta, ADFS or any other SAML 2.0-compliant identify provider
        (IdP) that has been defined for your account
        'https://<your_okta_account_name>.okta.com' to authenticate
        through native Okta.
    :type authenticator: Optional[str]
    :param warehouse: name of snowflake warehouse
    :type warehouse: Optional[str]
    :param database: name of snowflake database
    :type database: Optional[str]
    :param region: name of snowflake region
    :type region: Optional[str]
    :param role: name of snowflake role
    :type role: Optional[str]
    :param schema: name of snowflake schema
    :type schema: Optional[str]
    :param session_parameters: You can set session-level parameters at
        the time you connect to Snowflake
    :type session_parameters: Optional[dict]

    .. note::
        get_sqlalchemy_engine() depends on snowflake-sqlalchemy

    .. seealso::
        For more information on how to use this Snowflake connection, take a look at the guide:
        :ref:`howto/operator:SnowflakeOperator`
    """

    conn_name_attr = 'snowflake_conn_id'
    default_conn_name = 'snowflake_default'
    conn_type = 'snowflake'
    hook_name = 'Snowflake'
    supports_autocommit = True

    @staticmethod
    def get_connection_form_widgets() -> Dict[str, Any]:
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3PasswordFieldWidget, BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import PasswordField, StringField

        return {
            "extra__snowflake__account": StringField(lazy_gettext('Account'), widget=BS3TextFieldWidget()),
            "extra__snowflake__warehouse": StringField(
                lazy_gettext('Warehouse'), widget=BS3TextFieldWidget()
            ),
            "extra__snowflake__database": StringField(lazy_gettext('Database'), widget=BS3TextFieldWidget()),
            "extra__snowflake__region": StringField(lazy_gettext('Region'), widget=BS3TextFieldWidget()),
            "extra__snowflake__aws_access_key_id": StringField(
                lazy_gettext('AWS Access Key'), widget=BS3TextFieldWidget()
            ),
            "extra__snowflake__aws_secret_access_key": PasswordField(
                lazy_gettext('AWS Secret Key'), widget=BS3PasswordFieldWidget()
            ),
        }

    @staticmethod
    def get_ui_field_behaviour() -> Dict:
        """Returns custom field behaviour"""
        import json

        return {
            "hidden_fields": ['port'],
            "relabeling": {},
            "placeholders": {
                'extra': json.dumps(
                    {
                        "role": "snowflake role",
                        "authenticator": "snowflake oauth",
                        "private_key_file": "private key",
                        "session_parameters": "session parameters",
                    },
                    indent=1,
                ),
                'host': 'snowflake hostname',
                'schema': 'snowflake schema',
                'login': 'snowflake username',
                'password': 'snowflake password',
                'extra__snowflake__account': 'snowflake account name',
                'extra__snowflake__warehouse': 'snowflake warehouse name',
                'extra__snowflake__database': 'snowflake db name',
                'extra__snowflake__region': 'snowflake hosted region',
                'extra__snowflake__aws_access_key_id': 'aws access key id (S3ToSnowflakeOperator)',
                'extra__snowflake__aws_secret_access_key': 'aws secret access key (S3ToSnowflakeOperator)',
            },
        }

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.account = kwargs.pop("account", None)
        self.warehouse = kwargs.pop("warehouse", None)
        self.database = kwargs.pop("database", None)
        self.region = kwargs.pop("region", None)
        self.role = kwargs.pop("role", None)
        self.schema = kwargs.pop("schema", None)
        self.authenticator = kwargs.pop("authenticator", None)
        self.session_parameters = kwargs.pop("session_parameters", None)

    def _get_conn_params(self) -> Dict[str, Optional[str]]:
        """
        One method to fetch connection params as a dict
        used in get_uri() and get_connection()
        """
        conn = self.get_connection(
            self.snowflake_conn_id  # type: ignore[attr-defined] # pylint: disable=no-member
        )
        account = conn.extra_dejson.get('extra__snowflake__account', '') or conn.extra_dejson.get(
            'account', ''
        )
        warehouse = conn.extra_dejson.get('extra__snowflake__warehouse', '') or conn.extra_dejson.get(
            'warehouse', ''
        )
        database = conn.extra_dejson.get('extra__snowflake__database', '') or conn.extra_dejson.get(
            'database', ''
        )
        region = conn.extra_dejson.get('extra__snowflake__region', '') or conn.extra_dejson.get('region', '')
        role = conn.extra_dejson.get('role', '')
        schema = conn.schema or ''
        authenticator = conn.extra_dejson.get('authenticator', 'snowflake')
        session_parameters = conn.extra_dejson.get('session_parameters')

        conn_config = {
            "user": conn.login,
            "password": conn.password or '',
            "schema": self.schema or schema,
            "database": self.database or database,
            "account": self.account or account,
            "warehouse": self.warehouse or warehouse,
            "region": self.region or region,
            "role": self.role or role,
            "authenticator": self.authenticator or authenticator,
            "session_parameters": self.session_parameters or session_parameters,
        }

        # If private_key_file is specified in the extra json, load the contents of the file as a private
        # key and specify that in the connection configuration. The connection password then becomes the
        # passphrase for the private key. If your private key file is not encrypted (not recommended), then
        # leave the password empty.

        private_key_file = conn.extra_dejson.get('private_key_file')
        if private_key_file:
            with open(private_key_file, "rb") as key:
                passphrase = None
                if conn.password:
                    passphrase = conn.password.strip().encode()

                p_key = serialization.load_pem_private_key(
                    key.read(), password=passphrase, backend=default_backend()
                )

            pkb = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )

            conn_config['private_key'] = pkb
            conn_config.pop('password', None)

        return conn_config

    def get_uri(self) -> str:
        """Override DbApiHook get_uri method for get_sqlalchemy_engine()"""
        conn_config = self._get_conn_params()
        uri = (
            'snowflake://{user}:{password}@{account}/{database}/{schema}'
            '?warehouse={warehouse}&role={role}&authenticator={authenticator}'
        )
        return uri.format(**conn_config)

    def get_conn(self) -> SnowflakeConnection:
        """Returns a snowflake.connection object"""
        conn_config = self._get_conn_params()
        conn = connector.connect(**conn_config)
        return conn

    def _get_aws_credentials(self) -> Tuple[Optional[Any], Optional[Any]]:
        """
        Returns aws_access_key_id, aws_secret_access_key
        from extra

        intended to be used by external import and export statements
        """
        if self.snowflake_conn_id:  # type: ignore[attr-defined]  # pylint: disable=no-member
            connection_object = self.get_connection(
                self.snowflake_conn_id  # type: ignore[attr-defined]  # pylint: disable=no-member
            )
            if 'aws_secret_access_key' in connection_object.extra_dejson:
                aws_access_key_id = connection_object.extra_dejson.get(
                    'aws_access_key_id'
                ) or connection_object.extra_dejson.get('aws_access_key_id')
                aws_secret_access_key = connection_object.extra_dejson.get(
                    'aws_secret_access_key'
                ) or connection_object.extra_dejson.get('aws_secret_access_key')
        return aws_access_key_id, aws_secret_access_key

    def set_autocommit(self, conn, autocommit: Any) -> None:
        conn.autocommit(autocommit)
        conn.autocommit_mode = autocommit

    def get_autocommit(self, conn):
        return getattr(conn, 'autocommit_mode', False)
