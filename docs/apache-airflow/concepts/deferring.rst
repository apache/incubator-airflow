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

Deferrable Operators & Triggers
===============================

Standard :doc:`Operators <operators>` and :doc:`Sensors <sensors>` take up a full *worker slot* for the entire time they are running, even if they are idle; for example, if you only have 100 worker slots available to run Tasks, and you have 100 DAGs waiting on a Sensor that's currently running but idle, then you *cannot run anything else* - even though your entire Airflow cluster is essentially idle.

This is where *Deferrable Operators* come in. A deferrable operator is one that is written with the ability to suspend itself and remove itself from the worker when it knows that it will have to wait, and hand off the job of resuming it to something called a *Trigger*. As a result, while it is suspended (deferred), it is not taking up a worker slot and your cluster will have a lot less resources wasted on idle Operators or Sensors.

*Triggers* are small, asynchronous pieces of Python code designed to be run all together in a single Python process; because they are asynchronous, they are able to all co-exist efficiently. As an overview of how this process works:

* A task instance (running operator) gets to a point where it has to wait, and defers itself with a trigger tied to the event that should resume it. It then removes itself from its current worker and frees up space.
* The new Trigger instance is registered inside Airflow, and picked up by one or more *triggerer* processes
* The trigger is run until it fires, at which point its source task is re-scheduled
* The task instance resumes

Using deferrable operators as a DAG author is almost transparent; writing them, however, takes a bit more work.


Using Deferrable Operators
--------------------------

If all you wish to do is use pre-written Deferrable Operators (such as ``TimeSensorAsync``, which comes with Airflow), then there are only two steps you need:

* Ensure your Airflow installation is running at least one *triggerer* process, as well as the normal *scheduler*
* Use deferrable operators/sensors in your DAGs

That's it; everything else will be automatically handled for you. If you're upgrading existing DAGs, we even provide some API-compatible sensor variants (e.g. ``TimeSensorAsync`` for ``TimeSensor``) that you can swap into your DAG with no other changes required.

Note that you cannot yet use the deferral ability from inside custom PythonOperator/TaskFlow code; it is only available to pre-built Operators at the moment.


Writing Deferrable Operators
----------------------------

Writing a deferrable operator takes a bit more work. There are some main points to consider:

* Your Operator must defer itself based on a Trigger. If there is a Trigger in core Airflow you can use, great; otherwise, you will have to write one.
* Your Operator will be deleted and removed from its worker while deferred, and no state will persist automatically. You can persist state by asking Airflow to resume you at a certain method or pass certain kwargs, but that's it.
* You can defer multiple times, and you can defer before/after your Operator does significant work, or only defer if certain conditions are met (e.g. a system does not have an immediate answer). Deferral is entirely under your control.
* Any Operator can defer; no special marking on its class is needed, and it's not limited to Sensors.


Triggering Deferral
~~~~~~~~~~~~~~~~~~~

If you want to trigger deferral, at any place in your Operator you can call ``self.defer(trigger, method_name, kwargs, timeout)``, which will raise a special exception that Airflow will catch. The arguments are:

* ``trigger``: An instance of a Trigger that you wish to defer on. It will be serialized into the database.
* ``method_name``: The method name on your Operator you want Airflow to call when it resumes, other than ``execute``.
* ``kwargs``: Additional keyword arguments to pass to the method when it is called. Optional, defaults to ``{}``.
* ``timeout``: A timedelta that specifies a timeout after which this deferral will fail, and fail the task instance. Optional, defaults to ``None``, meaning no timeout.

When you opt to defer, your Operator will *stop executing at that point and be deleted from a worker*. No state will persist, and when your Operator is resumed it will be a *brand new instance* of it. The only way you can pass state from the old instance of the Operator to the new one is via ``method_name`` and ``kwargs``.

When your Operator is resumed, you will find an ``event`` item added to the kwargs passed to it, which contains the payload from the trigger event that resumed your Operator. Depending on the trigger, this may be useful to your operator (e.g. it's a status code or URL to fetch results), or it may not be important (it's just a datetime). Your ``method_name`` method, however, *must* accept ``event`` as a keyword argument.

If your Operator returns from either its first ``execute()`` method when it's new, or a subsequent method specified by ``method_name``, it will be considered complete and will finish executing.

You are free to set ``method_name`` to ``execute`` if you want your Operator to have one entrypoint, but it, too, will have to accept ``event`` as an optional keyword argument.

Here's a basic example of how a sensor might trigger deferral::

    class WaitOneHourSensor(BaseSensorOperator):
        def execute(self, context):
            self.defer(trigger=TimeDeltaTrigger(timedelta(hours=1), method_name="execute_complete")

        def execute_complete(self, context, event=None):
            # We have no more work to do here. Mark as complete.
            return

This Sensor is literally just a thin wrapper around the Trigger, so all it does is defer to the trigger, and specify a different method to come back to when the trigger fires - which, as it returns immediately, marks the Sensor as successful.

Under the hood, ``self.defer`` raises the ``TaskDeferred`` exception, so it will work anywhere inside your Operator's code, even buried many nested calls deep inside ``execute()``. You are free to raise ``TaskDeferred`` manually if you wish; it takes the same arguments as ``self.defer``.


Writing Triggers
~~~~~~~~~~~~~~~~

A Trigger is written as a class that inherits from ``BaseTrigger``, and implements three methods:

* ``__init__``, to receive arguments from Operators instantiating it
* ``run``, an asynchronous method that runs its logic and uses ``yield`` to send one or more of ``TriggerEvent``
* ``serialize``, which returns the information needed to re-construct this trigger, as a tuple of the classpath, and keyword arguments to pass to ``__init__``.

There's also some design constraints to be aware of:

* The ``run`` method *must be asynchronous* (using Python's asyncio), and correctly ``await`` whenever it does a blocking operation.
* It *must be able to run in parallel* with other copies of itself, both from multiple tasks depending on the same kind of Trigger as well as high-availability scenarios.
* When events are emitted, they *must* contain a payload that can be used to deduplicate events if the trigger is being run in multiple places.
* A trigger may be suddenly removed from one process and started on a new one (if partitions are being changed, or a deployment is happening). You may provide an optional ``cleanup`` method that gets called when this happens.

Here's the structure of a basic Trigger::


    class DateTimeTrigger(BaseTrigger):

        def __init__(self, moment):
            super().__init__()
            self.moment = moment

        def serialize(self):
            return ("airflow.triggers.temporal.DateTimeTrigger", {"moment": self.moment})

        async def run(self):
            while self.moment > timezone.utcnow():
                await asyncio.sleep(1)
            yield TriggerEvent(self.moment)

This is a very simplified version of Airflow's ``DateTimeTrigger``, and you can see several things here:

* ``__init__`` and ``serialize`` are written as a pair; the Trigger is instantiated once when it is submitted by the Operator as part of its deferral request, then serialized and re-instantiated on any *triggerer* process that runs the trigger.
* The ``run`` method is declared as an ``async def``, as it *must* be asynchronous, and uses ``asyncio.sleep`` rather than the regular ``time.sleep`` (as that would block the process).
* When it emits its event it packs ``self.moment`` in there, so if this trigger is being run redundantly on multiple hosts, the event can be de-duplicated.

Triggers can be as complex or as simple as you like provided you keep inside this contract; they are designed to be run in a highly-available fashion, auto-balanced and sharded across hosts running the *triggerer*. We encourage you to avoid any kind of persistent state in a trigger; they should get everything they need from their ``__init__``, so they can be serialized and moved around freely.

If you are new to writing asynchronous Python, you should be very careful writing your ``run()`` method; Python's async model means that any code that does not correctly ``await`` when it does a blocking operation will block the *entire process*. Airflow will attempt to detect this and warn you in the triggerer logs when it happens, but we strongly suggest you set the variable ``PYTHONASYNCIODEBUG=1`` when you are writing your Trigger to enable extra checks from Python to make sure you're writing non-blocking code. Be especially careful when doing filesystem calls, as if the underlying filesystem is network-backed it may be blocking.

Right now, Triggers are only used up to their first event, as they are only used for resuming deferred tasks (which happens on the first event fired). However, we plan to allow DAGs to be launched from triggers in future, which is where multi-event triggers will be more useful.


High Availability
-----------------

Triggers are designed from the ground-up to be highly-available and run on multiple hosts at once; if you want to run a highly-available setup, simply run multiple copies of ``triggerer`` on multiple hosts. Much like ``scheduler``, they will automatically co-exist with correct locking and HA.

Depending on how much work the triggers are doing, you can fit approximately thousands of triggers on a single ``triggerer`` host. By default, every ``triggerer`` will run all triggers; if you want, you can choose to *partition* (*shard*) your triggers by passing ``--partition`` to the ``triggerer`` process.

For example, if you want to run two partitions with two triggerers per partition, you would run these four processes on different hosts:

* ``airflow triggerer --partition=1/2``
* ``airflow triggerer --partition=1/2``
* ``airflow triggerer --partition=2/2``
* ``airflow triggerer --partition=2/2``

Under the hood, Airflow uses consistent hashing to evenly distribute triggers across the partitions. If you need to migrate to have more partitions, you can run triggerers with the old and new schemes at the same time during the transition - for example, this is a valid set of five processes:

* ``airflow triggerer --partition=1/2``
* ``airflow triggerer --partition=2/2``
* ``airflow triggerer --partition=1/3``
* ``airflow triggerer --partition=2/3``
* ``airflow triggerer --partition=3/3``

Note that every extra ``triggerer`` you run will result in an extra persistent connection to your database; this is generally the only scaling limit the triggerers have.


Smart Sensors
-------------

Deferrable Operators essentially supersede :doc:`Smart Sensors <smart-sensors>`, and should be preferred for almost all situations. They do solve fundamentally the same problem; Smart Sensors, however, only work for certain Sensor workload styles, have no redundancy, and require a custom DAG to run at all times.
