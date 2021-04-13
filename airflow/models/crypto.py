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
import logging
from typing import Optional

from cryptography.fernet import Fernet, MultiFernet
from typing_extensions import Protocol

from airflow.configuration import conf
from airflow.exceptions import AirflowException

log = logging.getLogger(__name__)


class FernetProtocol(Protocol):
    """This class is only used for TypeChecking (for IDEs, mypy, pylint, etc)"""

    def decrypt(self, b):
        """Decrypt with Fernet"""

    def encrypt(self, b):
        """Encrypt with Fernet"""


class NullFernet:
    """
    A "Null" encryptor class that doesn't encrypt or decrypt but that presents
    a similar interface to Fernet.

    The purpose of this is to make the rest of the code not have to know the
    difference, and to only display the message once, not 20 times when
    `airflow db init` is ran.
    """

    is_encrypted = False

    def decrypt(self, b):
        """Decrypt with Fernet."""
        return b

    def encrypt(self, b):
        """Encrypt with Fernet."""
        return b


_fernet = None  # type: Optional[FernetProtocol]


def get_fernet():
    """
    Deferred load of Fernet key.

    This function could fail either because Cryptography is not installed
    or because the Fernet key is invalid.

    :return: Fernet object
    :raises: airflow.exceptions.AirflowException if there's a problem trying to load Fernet
    """
    global _fernet  # pylint: disable=global-statement

    if _fernet:
        return _fernet

    try:
        fernet_key = conf.get('core', 'FERNET_KEY')
        if not fernet_key:
            log.warning("empty cryptography key - values will not be stored encrypted.")
            _fernet = NullFernet()
        else:
            _fernet = MultiFernet(
                [Fernet(fernet_part.encode('utf-8')) for fernet_part in fernet_key.split(',')]
            )
            _fernet.is_encrypted = True
    except (ValueError, TypeError) as value_error:
        raise AirflowException(f"Could not create Fernet object: {value_error}")

    return _fernet
