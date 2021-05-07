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

Customizing state colours in UI
===============================

.. versionadded:: 1.10.11

To change the colors for TaskInstance/DagRun State in the Airflow Webserver, perform the
following steps:

1.  Create ``airflow_local_settings.py`` file and put in on ``$PYTHONPATH`` or
    to ``$AIRFLOW_HOME/config`` folder. (Airflow adds ``$AIRFLOW_HOME/config`` on ``PYTHONPATH`` when
    Airflow is initialized)

2.  Add the following contents to ``airflow_local_settings.py`` file. Change the colors to whatever you
    would like.

    .. code-block:: python

      STATE_COLORS = {
        "queued": 'darkgray',
        "running": '#01FF70',
        "success": '#2ECC40',
        "failed": 'firebrick',
        "up_for_retry": 'yellow',
        "up_for_reschedule": 'turquoise',
        "upstream_failed": 'orange',
        "skipped": 'darkorchid',
        "scheduled": 'tan',
        "deferred": 'lightseagreen',
      }



3.  Restart Airflow Webserver.

Screenshots
-----------

Before
^^^^^^

.. image:: ../img/change-ui-colors/dags-page-old.png

.. image:: ../img/change-ui-colors/graph-view-old.png

.. image:: ../img/change-ui-colors/tree-view-old.png

After
^^^^^^

.. image:: ../img/change-ui-colors/dags-page-new.png

.. image:: ../img/change-ui-colors/graph-view-new.png

.. image:: ../img/change-ui-colors/tree-view-new.png


.. note::

    See :doc:`../modules_management` for details on how Python and Airflow manage modules.
