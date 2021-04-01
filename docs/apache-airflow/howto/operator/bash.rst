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



.. _howto/operator:BashOperator:

BashOperator
============

Use the :class:`~airflow.operators.bash.BashOperator` to execute
commands in a `Bash <https://www.gnu.org/software/bash/>`__ shell.

.. exampleinclude:: /../../airflow/example_dags/example_bash_operator.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bash]
    :end-before: [END howto_operator_bash]

Templating
----------

You can use :ref:`Jinja templates <jinja-templating>` to parameterize the
``bash_command`` argument.

.. exampleinclude:: /../../airflow/example_dags/example_bash_operator.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bash_template]
    :end-before: [END howto_operator_bash_template]


.. warning::

    Care should be taken with "user" input or when using Jinja templates in the
    ``bash_command``, as this bash operator does not perform any escaping or
    sanitization of the command.

    This applies mostly to using "dag_run" conf, as that can be submitted via
    users in the Web UI. Most of the default template variables are not at
    risk.

For example, do **not** do this:

.. code-block:: python

    bash_task = BashOperator(
        task_id="bash_task",
        bash_command='echo "Here is the message: \'{{ dag_run.conf["message"] if dag_run else "" }}\'"',
    )

Instead, you should pass this via the ``env`` kwarg and use double-quotes
inside the bash_command, as below:

.. code-block:: python

    bash_task = BashOperator(
        task_id="bash_task",
        bash_command='echo "here is the message: \'$message\'"',
        env={'message': '{{ dag_run.conf["message"] if dag_run else "" }}'},
    )

Skipping
--------

In general a non-zero exit code produces an AirflowException and thus a task failure.  In cases where it is desirable
to instead have the task end in a ``skipped`` state, you can exit with code ``99`` (or with another exit code if you
pass ``skip_exit_code``).

.. exampleinclude:: /../../airflow/example_dags/example_bash_operator.py
    :language: python
    :start-after: [START howto_operator_bash_skip]
    :end-before: [END howto_operator_bash_skip]


Troubleshooting
---------------

Jinja template not found
""""""""""""""""""""""""

Add a space after the script name when directly calling a Bash script with
the ``bash_command`` argument. This is because Airflow tries to apply a Jinja
template to it, which will fail.

.. code-block:: python

    t2 = BashOperator(
        task_id='bash_example',

        # This fails with 'Jinja template not found' error
        # bash_command="/home/batcher/test.sh",

        # This works (has a space after)
        bash_command="/home/batcher/test.sh ",
        dag=dag)

However, if you want to use templating in your bash script, do not add the space
and instead put your bash script in a location relative to the directory containing
the DAG file. So if your DAG file is in ``/usr/local/airflow/dags/test_dag.py``, you can
move your ``test.sh`` file to any location under ``/usr/local/airflow/dags/`` (Example:
``/usr/local/airflow/dags/scripts/test.sh``) and pass the relative path to ``bash_command``
as shown below:

.. code-block:: python

    t2 = BashOperator(
        task_id='bash_example',
        # "scripts" folder is under "/usr/local/airflow/dags"
        bash_command="scripts/test.sh",
        dag=dag)

Creating separate folder for bash scripts may be desirable for many reasons, like
separating your script's logic and pipeline code, allowing for proper code highlighting
in files composed in different languages, and general flexibility in structuring
pipelines.

It is also possible to define your ``template_searchpath`` as pointing to any folder
locations in the DAG constructor call.

Example:

.. code-block:: python

    dag = DAG("example_bash_dag", template_searchpath="/opt/scripts")
    t2 = BashOperator(
        task_id='bash_example',
        # "test.sh" is a file under "/opt/scripts"
        bash_command="test.sh ",
        dag=dag)
