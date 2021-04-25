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



Running Airflow locally
-----------------------

This quick start guide will help you bootstrap a Airflow standalone instance on your local machine.

.. note::

   On November 2020, new version of PIP (20.3) has been released with a new, 2020 resolver. This resolver
   might work with Apache Airflow as of 20.3.3, but it might lead to errors in installation. It might
   depend on your choice of extras. In order to install Airflow you might need to either downgrade
   pip to version 20.2.4 ``pip install --upgrade pip==20.2.4`` or, in case you use Pip 20.3,
   you need to add option ``--use-deprecated legacy-resolver`` to your pip install command.

   While ``pip 20.3.3`` solved most of the ``teething`` problems of 20.3, this note will remain here until we
   set ``pip 20.3`` as official version in our CI pipeline where we are testing the installation as well.
   Due to those constraints, only ``pip`` installation is currently officially supported.

   While they are some successes with using other tools like `poetry <https://python-poetry.org/>`_ or
   `pip-tools <https://pypi.org/project/pip-tools/>`_, they do not share the same workflow as
   ``pip`` - especially when it comes to constraint vs. requirements management.
   Installing via ``Poetry`` or ``pip-tools`` is not currently supported.

   If you wish to install airflow using those tools you should use the constraint files and convert
   them to appropriate format and workflow that your tool requires.

The installation of Airflow is painless if you are following the instructions below. Airflow uses
constraint files to enable reproducible installation, so using ``pip`` and constraint files is recommended.

.. code-block:: bash

    # airflow needs a home, ~/airflow is the default,
    # but you can lay foundation somewhere else if you prefer
    # (optional)
    export AIRFLOW_HOME=~/airflow

    AIRFLOW_VERSION=2.0.1
    PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
    # For example: 3.6
    CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
    # For example: https://raw.githubusercontent.com/apache/airflow/constraints-2.0.1/constraints-3.6.txt
    pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

    # initialize the database
    airflow db init

    airflow users create \
        --username admin \
        --firstname Peter \
        --lastname Parker \
        --role Admin \
        --email spiderman@superhero.org

    # start the web server, default port is 8080
    airflow webserver --port 8080

    # start the scheduler
    # open a new terminal or else run webserver with ``-D`` option to run it as a daemon
    airflow scheduler

    # visit localhost:8080 in the browser and use the admin account you just
    # created to login. Enable the example_bash_operator dag in the home page

Upon running these commands, Airflow will create the ``$AIRFLOW_HOME`` folder
and create the "airflow.cfg" file with defaults that will get you going fast.
You can inspect the file either in ``$AIRFLOW_HOME/airflow.cfg``, or through the UI in
the ``Admin->Configuration`` menu. The PID file for the webserver will be stored
in ``$AIRFLOW_HOME/airflow-webserver.pid`` or in ``/run/airflow/webserver.pid``
if started by systemd.

Out of the box, Airflow uses a SQLite database, which you should outgrow
fairly quickly since no parallelization is possible using this database
backend. It works in conjunction with the
:class:`~airflow.executors.sequential_executor.SequentialExecutor` which will
only run task instances sequentially. While this is very limiting, it allows
you to get up and running quickly and take a tour of the UI and the
command line utilities.

Here are a few commands that will trigger a few task instances. You should
be able to see the status of the jobs change in the ``example_bash_operator`` DAG as you
run the commands below.

.. code-block:: bash

    # run your first task instance
    airflow tasks run example_bash_operator runme_0 2015-01-01
    # run a backfill over 2 days
    airflow dags backfill example_bash_operator \
        --start-date 2015-01-01 \
        --end-date 2015-01-02

What's Next?
''''''''''''
From this point, you can head to the :doc:`/tutorial` section for further examples or the :doc:`/howto/index` section if you're ready to get your hands dirty.
