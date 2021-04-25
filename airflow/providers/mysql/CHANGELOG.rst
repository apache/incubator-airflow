 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

The version of MySQL server has to be 5.6.4+. The exact version upper bound depends
on the version of ``mysqlclient`` package. For example, ``mysqlclient`` 1.3.12 can only be
used with MySQL server 5.6.4 through 5.7.

Changelog
---------

1.0.2
.....

Bug fixes
~~~~~~~~~

* ``MySQL hook respects conn_name_attr (#14240)``

1.0.1
.....

Updated documentation and readme files.


1.0.0
.....

Initial version of the provider.
