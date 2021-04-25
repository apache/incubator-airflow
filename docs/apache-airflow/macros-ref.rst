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

.. _macros:

Macros reference
================

Variables and macros can be used in templates (see the :ref:`jinja-templating` section)

The following come for free out of the box with Airflow.
Additional custom macros can be added globally through :doc:`plugins`, or at a DAG level through the ``DAG.user_defined_macros`` argument.

Default Variables
-----------------
The Airflow engine passes a few variables by default that are accessible
in all templates

=====================================   ====================================
Variable                                Description
=====================================   ====================================
``{{ ds }}``                            the execution date as ``YYYY-MM-DD``
``{{ ds_nodash }}``                     the execution date as ``YYYYMMDD``
``{{ prev_ds }}``                       the previous execution date as ``YYYY-MM-DD``
                                        if ``{{ ds }}`` is ``2018-01-08`` and ``schedule_interval`` is ``@weekly``,
                                        ``{{ prev_ds }}`` will be ``2018-01-01``
``{{ prev_ds_nodash }}``                the previous execution date as ``YYYYMMDD`` if exists, else ``None``
``{{ next_ds }}``                       the next execution date as ``YYYY-MM-DD``
                                        if ``{{ ds }}`` is ``2018-01-01`` and ``schedule_interval`` is ``@weekly``,
                                        ``{{ next_ds }}`` will be ``2018-01-08``
``{{ next_ds_nodash }}``                the next execution date as ``YYYYMMDD`` if exists, else ``None``
``{{ yesterday_ds }}``                  the day before the execution date as ``YYYY-MM-DD``
``{{ yesterday_ds_nodash }}``           the day before the execution date as ``YYYYMMDD``
``{{ tomorrow_ds }}``                   the day after the execution date as ``YYYY-MM-DD``
``{{ tomorrow_ds_nodash }}``            the day after the execution date as ``YYYYMMDD``
``{{ ts }}``                            same as ``execution_date.isoformat()``. Example: ``2018-01-01T00:00:00+00:00``
``{{ ts_nodash }}``                     same as ``ts`` without ``-``, ``:`` and TimeZone info. Example: ``20180101T000000``
``{{ ts_nodash_with_tz }}``             same as ``ts`` without ``-`` and ``:``. Example: ``20180101T000000+0000``
``{{ execution_date }}``                the execution_date (logical date) (`pendulum.Pendulum`_)
``{{ prev_execution_date }}``           the previous execution date (if available) (`pendulum.Pendulum`_)
``{{ prev_execution_date_success }}``   execution date from prior successful dag run (if available) (`pendulum.Pendulum`_)
``{{ prev_start_date_success }}``       start date from prior successful dag run (if available) (`pendulum.Pendulum`_)
``{{ next_execution_date }}``           the next execution date (`pendulum.Pendulum`_)
``{{ dag }}``                           the DAG object
``{{ task }}``                          the Task object
``{{ macros }}``                        a reference to the macros package, described below
``{{ task_instance }}``                 the task_instance object
``{{ ti }}``                            same as ``{{ task_instance }}``
``{{ params }}``                        a reference to the user-defined params dictionary which can be overridden by
                                        the dictionary passed through ``trigger_dag -c`` if you enabled
                                        ``dag_run_conf_overrides_params`` in ``airflow.cfg``
``{{ var.value.my_var }}``              global defined variables represented as a dictionary
``{{ var.json.my_var.path }}``          global defined variables represented as a dictionary
                                        with deserialized JSON object, append the path to the
                                        key within the JSON object
``{{ task_instance_key_str }}``         a unique, human-readable key to the task instance
                                        formatted ``{dag_id}__{task_id}__{ds_nodash}``
``{{ conf }}``                          the full configuration object located at
                                        ``airflow.configuration.conf`` which
                                        represents the content of your
                                        ``airflow.cfg``
``{{ run_id }}``                        the ``run_id`` of the current DAG run
``{{ dag_run }}``                       a reference to the DagRun object
``{{ test_mode }}``                     whether the task instance was called using
                                        the CLI's test subcommand
=====================================   ====================================

Note that you can access the object's attributes and methods with simple
dot notation. Here are some examples of what is possible:
``{{ task.owner }}``, ``{{ task.task_id }}``, ``{{ ti.hostname }}``, ...
Refer to the models documentation for more information on the objects'
attributes and methods.

The ``var`` template variable allows you to access variables defined in Airflow's
UI. You can access them as either plain-text or JSON. If you use JSON, you are
also able to walk nested structures, such as dictionaries like:
``{{ var.json.my_dict_var.key1 }}``.

It is also possible to fetch a variable by string if needed with
``{{ var.value.get('my.var', 'fallback') }}`` or
``{{ var.json.get('my.dict.var', {'key1': 'val1'}) }}``. Defaults can be
supplied in case the variable does not exist.

Macros
------
Macros are a way to expose objects to your templates and live under the
``macros`` namespace in your templates.

A few commonly used libraries and methods are made available.


=================================   ==============================================
Variable                            Description
=================================   ==============================================
``macros.datetime``                 The standard lib's :class:`datetime.datetime`
``macros.timedelta``                The standard lib's :class:`datetime.timedelta`
``macros.dateutil``                 A reference to the ``dateutil`` package
``macros.time``                     The standard lib's :class:`datetime.time`
``macros.uuid``                     The standard lib's :mod:`uuid`
``macros.random``                   The standard lib's :mod:`random`
=================================   ==============================================


Some airflow specific macros are also defined:

.. automodule:: airflow.macros
    :show-inheritance:
    :members:

.. autofunction:: airflow.macros.hive.closest_ds_partition
.. autofunction:: airflow.macros.hive.max_partition
.. _pendulum.Pendulum: https://pendulum.eustace.io/docs/1.x/#introduction
