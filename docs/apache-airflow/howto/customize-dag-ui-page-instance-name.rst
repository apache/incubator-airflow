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

Customizing DAG UI Header and Airflow Page Titles
=================================================

Airflow now allows you to customize the DAG home page header and page title. This will help
distinguish between various installations of Airflow or simply amend the page text.

Note: the custom title will be applied to both the page header and the page title.

To make this change, simply:

1.  Add the configuration option of ``instance_name`` under ``webserver`` inside ``airflow.cfg``:

.. code-block::

  [webserver]

  instance_name = "DevEnv"


2.  Alternatively, you can set a custom title using the environment variable:

.. code-block::

  AIRFLOW__WEBSERVER__SITE_TITLE = "DevEnv"


Screenshots
-----------

Before
^^^^^^

.. image:: ../img/change-site-title/default_instance_name_configuration.png

After
^^^^^

.. image:: ../img/change-site-title/example_instance_name_configuration.png
