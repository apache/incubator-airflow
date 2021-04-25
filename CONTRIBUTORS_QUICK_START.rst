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

*************************
Contributor's Quick Guide
*************************

.. contents:: :local:


Prerequisites
#############

1. Docker Community Edition
2. Docker Compose
3. pyenv (you can also use pyenv-virtualenv or virtualenvwrapper)


Installing Prerequisites on Ubuntu
##################################


Docker Community Edition
------------------------


1. Installing required packages for Docker and setting up docker repo

.. code-block:: bash

  $ sudo apt-get update

  $ sudo apt-get install \
      apt-transport-https \
      ca-certificates \
      curl \
      gnupg-agent \
      software-properties-common

  $ curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -

  $ sudo add-apt-repository \
     "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
     $(lsb_release -cs) \
     stable"

2. Install Docker

.. code-block:: bash

  $ sudo apt-get update
  $ sudo apt-get install docker-ce docker-ce-cli containerd.io

3. Creating group for docker and adding current user to it.

.. code-block:: bash

  $ sudo groupadd docker
  $ sudo usermod -aG docker $USER

Note : After adding user to docker group Logout and Login again for group membership re-evaluation.

4. Test Docker installation

.. code-block:: bash

  $ docker run hello-world




Docker Compose
--------------

1. Installing latest version of Docker Compose

.. code-block:: bash

  $ COMPOSE_VERSION="$(curl -s https://api.github.com/repos/docker/compose/releases/latest | grep '"tag_name":'\
  | cut -d '"' -f 4)"

  $ COMPOSE_URL="https://github.com/docker/compose/releases/download/${COMPOSE_VERSION}/\
  docker-compose-$(uname -s)-$(uname -m)"

  $ sudo curl -L "${COMPOSE_URL}" -o /usr/local/bin/docker-compose

  $ sudo chmod +x /usr/local/bin/docker-compose

2. Verifying installation

.. code-block:: bash

  $ docker-compose --version



Pyenv and setting up virtual-env
--------------------------------

1. Checking required packages

.. code-block:: bash

  $ sudo apt-get install -y build-essential libssl-dev zlib1g-dev libbz2-dev \
      libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev libncursesw5-dev \
      xz-utils tk-dev libffi-dev liblzma-dev python-openssl git

  $ sudo apt install build-essentials python3.6-dev python3.7-dev python3.8-dev python-dev openssl \
       sqlite sqlite-dev default-libmysqlclient-dev libmysqld-dev postgresql

2. Install pyenv

.. code-block:: bash

  $ curl https://pyenv.run | bash

3. Add the lines suggested at the end of installation  to ~/.bashrc

4. Restart your shell so the path changes take effect and verifying installation

.. code-block:: bash

  $ exec $SHELL
  $ pyenv --version

5. Checking available version, installing required Python version to pyenv and verifying it

.. code-block:: bash

  $ pyenv install --list
  $ pyenv install 3.8.5
  $ pyenv versions

6. Creating new virtual environment named ``airflow-env`` for installed version python. In next chapter virtual
   environment ``airflow-env`` will be used for installing airflow.

.. code-block:: bash

  $ pyenv virtualenv 3.8.5 airflow-env

7. Entering virtual environment ``airflow-env``

.. code-block:: bash

  $ pyenv activate airflow-env




Setup Airflow with Breeze and PyCharm
#####################################


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


Forking and cloning Project
---------------------------

1. Goto |airflow_github| and fork the project.

   .. |airflow_github| raw:: html

     <a href="https://github.com/apache/airflow/" target="_blank">https://github.com/apache/airflow/</a>

   .. raw:: html

     <div align="center" style="padding-bottom:10px">
       <img src="images/quick_start/airflow_fork.png"
            alt="Forking Apache Airflow project">
     </div>

2. Goto your github account's fork of airflow click on ``Code`` and copy the clone link.

   .. raw:: html

      <div align="center" style="padding-bottom:10px">
        <img src="images/quick_start/airflow_clone.png"
             alt="Cloning github fork of Apache airflow">
      </div>



3. Open PyCharm and click ``Get from Version Control``

   .. raw:: html

      <div align="center" style="padding-bottom:10px">
        <img src="images/quick_start/pycharm_clone.png"
             alt="Cloning github fork to Pycharm">
      </div>


4. Paste the copied clone link in URL and click on clone.

   .. raw:: html

      <div align="center" style="padding-bottom:10px">
        <img src="images/quick_start/click_on_clone.png"
             alt="Cloning github fork to Pycharm">
      </div>


Setting up Breeze
-----------------
1. Open terminal and enter into virtual environment ``airflow-env`` and goto project directory

.. code-block:: bash

  $ pyenv activate airflow-env
  $ cd ~/PycharmProjects/airflow/

2. Initializing breeze autocomplete

.. code-block:: bash

  $ ./breeze setup-autocomplete
  $ source ~/.bash_completion.d/breeze-complete

3. Initialize breeze environment with required python version and backend. This may take a while for first time.

.. code-block:: bash

  $ ./breeze --python 3.8 --backend mysql

4. Creating airflow tables and users. ``airflow db reset`` is required to execute at least once for Airflow Breeze to get
   the database/tables created.

.. code-block:: bash

  $ airflow db reset
  $ airflow users create --role Admin --username admin --password admin --email admin@example.com --firstname\
    foo --lastname bar


5. Closing Breeze environment. After successfully finishing above command will leave you in container,
   type ``exit`` to exit the container

.. code-block:: bash

  root@b76fcb399bb6:/opt/airflow#
  root@b76fcb399bb6:/opt/airflow# exit

.. code-block:: bash

  $ ./breeze stop

6. Installing airflow in the local virtual environment ``airflow-env`` with breeze.

   It may requires some packages to be installed, watch the output of the command to see which ones are missing.

.. code-block:: bash

  $ sudo apt-get install sqlite libsqlite3-dev default-libmysqlclient-dev postgresql
  $ ./breeze initialize-local-virtualenv --python 3.8


7. Add following line to ~/.bashrc in order to call breeze command from anywhere.

.. code-block:: bash

  export PATH=${PATH}:"/home/${USER}/PycharmProjects/airflow"
  source ~/.bashrc

Using Breeze
------------

1. Starting breeze environment using ``breeze start-airflow`` starts Breeze environment with last configuration run(
   In this case python and backend will be picked up from last execution ``./breeze --python 3.8 --backend mysql``)
   It also automatically starts webserver, backend and scheduler. It drops you in tmux with scheduler in bottom left
   and webserver in bottom right. Use ``[Ctrl + B] and Arrow keys`` to navigate.

.. code-block:: bash

  $ breeze start-airflow

      Use CI image.

   Branch name:            master
   Docker image:           apache/airflow:master-python3.8-ci
   Airflow source version: 2.0.0b2
   Python version:         3.8
   DockerHub user:         apache
   DockerHub repo:         airflow
   Backend:                mysql 5.7


   Port forwarding:

   Ports are forwarded to the running docker containers for webserver and database
     * 28080 -> forwarded to Airflow webserver -> airflow:8080
     * 25555 -> forwarded to Flower dashboard -> airflow:5555
     * 25433 -> forwarded to Postgres database -> postgres:5432
     * 23306 -> forwarded to MySQL database  -> mysql:3306
     * 26379 -> forwarded to Redis broker -> redis:6379

   Here are links to those services that you can use on host:
     * Webserver: http://127.0.0.1:28080
     * Flower:    http://127.0.0.1:25555
     * Postgres:  jdbc:postgresql://127.0.0.1:25433/airflow?user=postgres&password=airflow
     * Mysql:     jdbc:mysql://127.0.0.1:23306/airflow?user=root
     * Redis:     redis://127.0.0.1:26379/0


.. raw:: html

      <div align="center" style="padding-bottom:10px">
        <img src="images/quick_start/start_airflow_tmux.png"
             alt="Accessing local airflow">
      </div>


- Alternatively you can start the same using following commands

  1. Start Breeze

  .. code-block:: bash

    $ breeze --python 3.8 --backend mysql

  2. Open tmux

  .. code-block:: bash

    $ root@0c6e4ff0ab3d:/opt/airflow# tmux

  3. Press Ctrl + B and "

  .. code-block:: bash

    $ root@0c6e4ff0ab3d:/opt/airflow# airflow scheduler


  4. Press Ctrl + B and %

  .. code-block:: bash

    $ root@0c6e4ff0ab3d:/opt/airflow# airflow webserver




2. Now you can access airflow web interface on your local machine at |http://127.0.0.1:28080| with user name ``admin``
   and password ``admin``.

   .. |http://127.0.0.1:28080| raw:: html

      <a href="http://127.0.0.1:28080" target="_blank">http://127.0.0.1:28080</a>

   .. raw:: html

      <div align="center" style="padding-bottom:10px">
        <img src="images/quick_start/local_airflow.png"
             alt="Accessing local airflow">
      </div>

3. Setup mysql database in
MySQL Workbench with Host ``127.0.0.1``, port ``23306``, user ``root`` and password
   blank(leave empty), default schema ``airflow``.

   .. raw:: html

      <div align="center" style="padding-bottom:10px">
        <img src="images/quick_start/mysql_connection.png"
             alt="Connecting to mysql">
      </div>

4. Stopping breeze

.. code-block:: bash

  root@f3619b74c59a:/opt/airflow# ./stop_airflow.sh
  root@f3619b74c59a:/opt/airflow# exit
  $ breeze stop

5. Knowing more about Breeze

.. code-block:: bash

  $ breeze --help


For more information visit : |Breeze documentation|

.. |Breeze documentation| raw:: html

   <a href="https://github.com/apache/airflow/blob/master/BREEZE.rst" target="_blank">Breeze documentation</a>

Following are some of important topics of Breeze documentation:


- |Choosing different Breeze environment configuration|

.. |Choosing different Breeze environment configuration| raw:: html

   <a href="https://github.com/apache/airflow/blob/master/BREEZE.rst#choosing-different-breeze-environment-configuration
   " target="_blank">Choosing different Breeze environment configuration</a>


- |Troubleshooting Breeze environment|

.. |Troubleshooting Breeze environment| raw:: html

   <a href="https://github.com/apache/airflow/blob/master/BREEZE.rst#troubleshooting" target="_blank">Troubleshooting
   Breeze environment</a>


- |Installing Additional tools to the Docker Image|

.. |Installing Additional tools to the Docker Image| raw:: html

   <a href="https://github.com/apache/airflow/blob/master/BREEZE.rst#additional-tools" target="_blank">Installing
   Additional tools to the Docker Image</a>


- |Internal details of Breeze|

.. |Internal details of Breeze| raw:: html

   <a href="https://github.com/apache/airflow/blob/master/BREEZE.rst#internal-details-of-breeze" target="_blank">
   Internal details of Breeze</a>


- |Breeze Command-Line Interface Reference|

.. |Breeze Command-Line Interface Reference| raw:: html

   <a href="https://github.com/apache/airflow/blob/master/BREEZE.rst#breeze-command-line-interface-reference"
   target="_blank">Breeze Command-Line Interface Reference</a>


- |Cleaning the environment|

.. |Cleaning the environment| raw:: html

   <a href="https://github.com/apache/airflow/blob/master/BREEZE.rst#cleaning-the-environment" target="_blank">
   Cleaning the environment</a>


- |Other uses of the Airflow Breeze environment|

.. |Other uses of the Airflow Breeze environment| raw:: html

   <a href="https://github.com/apache/airflow/blob/master/BREEZE.rst#other-uses-of-the-airflow-breeze-environment"
   target="_blank">Other uses of the Airflow Breeze environment</a>



Setting up Debug
----------------

1. Configuring Airflow database connection

- Airflow is by default configured to use SQLite database. Configuration can be seen on local machine
  ``~/airflow/airflow.cfg`` under ``sql_alchemy_conn``.

- Installing required dependency for MySQL connection in ``airflow-env`` on local machine.

  .. code-block:: bash

    $ pyenv activate airflow-env
    $ pip install PyMySQL

- Now set ``sql_alchemy_conn = mysql+pymysql://root:@127.0.0.1:23306/airflow?charset=utf8mb4`` in file
  ``~/airflow/airflow.cfg`` on local machine.

2. Debugging an example DAG in PyCharm

- Add Interpreter to PyCharm pointing interpreter path to ``~/.pyenv/versions/airflow-env/bin/python``, which is virtual
  environment ``airflow-env`` created with pyenv earlier. For adding an Interpreter go to ``File -> Setting -> Project:
  airflow -> Python Interpreter``.

  .. raw:: html

    <div align="center" style="padding-bottom:10px">
      <img src="images/quick_start/add Interpreter.png"
           alt="Adding existing interpreter">
    </div>

- In PyCharm IDE open airflow project, directory ``\files\dags`` of local machine is by default mounted to docker
  machine when breeze airflow is started. So any DAG file present in this directory will be picked automatically by
  scheduler running in docker machine and same can be seen on ``http://127.0.0.1:28080``.

- Copy any example DAG present in airflow project's ``\airflow\example_dags`` directory to ``\files\dags\``.

- Add main block at the end of your DAG file to make it runnable. It will run a back_fill job:

  .. code-block:: python

    if __name__ == '__main__':
      from airflow.utils.state import State
      dag.clear(dag_run_state=State.NONE)
      dag.run()

- Add ``AIRFLOW__CORE__EXECUTOR=DebugExecutor`` to Environment variable of Run Configuration.

  - Click on Add configuration

    .. raw:: html

        <div align="center" style="padding-bottom:10px">
          <img src="images/quick_start/add_configuration.png"
               alt="Add Configuration pycharm">
        </div>

  - Add Script Path and Environment Variable to new Python configuration

    .. raw:: html

        <div align="center" style="padding-bottom:10px">
          <img src="images/quick_start/add_env_variable.png"
               alt="Add environment variable pycharm">
        </div>


- Now Debug an example dag and view the entries in tables such as ``dag_run, xcom`` etc in mysql workbench.



Starting development
####################


Creating a branch
-----------------

1. Click on branch symbol in the bottom right corner of Pycharm

   .. raw:: html

      <div align="center" style="padding-bottom:10px">
        <img src="images/quick_start/creating_branch_1.png"
             alt="Creating a new branch">
      </div>

2. Give a name to branch and checkout

   .. raw:: html

      <div align="center" style="padding-bottom:10px">
        <img src="images/quick_start/creating_branch_2.png"
             alt="Giving name to a branch">
      </div>



Testing
-------

All Tests are inside ./tests directory.

- Running Unit tests inside Breeze environment.

  Just run ``pytest filepath+filename`` to run the tests.

.. code-block:: bash

   root@63528318c8b1:/opt/airflow# pytest tests/utils/test_decorators.py
   ======================================= test session starts =======================================
   platform linux -- Python 3.8.6, pytest-6.0.1, py-1.9.0, pluggy-0.13.1 -- /usr/local/bin/python
   cachedir: .pytest_cache
   rootdir: /opt/airflow, configfile: pytest.ini
   plugins: celery-4.4.7, requests-mock-1.8.0, xdist-1.34.0, flaky-3.7.0, rerunfailures-9.0, instafail
   -0.4.2, forked-1.3.0, timeouts-1.2.1, cov-2.10.0
   setup timeout: 0.0s, execution timeout: 0.0s, teardown timeout: 0.0s
   collected 3 items

   tests/utils/test_decorators.py::TestApplyDefault::test_apply PASSED                         [ 33%]
   tests/utils/test_decorators.py::TestApplyDefault::test_default_args PASSED                  [ 66%]
   tests/utils/test_decorators.py::TestApplyDefault::test_incorrect_default_args PASSED        [100%]

   ======================================== 3 passed in 1.49s ========================================

- Running All the test with Breeze by specifying required python version, backend, backend version

.. code-block:: bash

   $ breeze --backend mysql --mysql-version 5.7 --python 3.8 --db-reset --test-type All  tests


- Running specific test in container using shell scripts. Testing in container scripts are located in
  ``.\scripts\in_container`` directory.

.. code-block:: bash

   root@df8927308887:/opt/airflow# ./scripts/in_container/
      bin/                                        run_flake8.sh*
      check_environment.sh*                       run_generate_constraints.sh*
      entrypoint_ci.sh*                           run_init_script.sh*
      entrypoint_exec.sh*                         run_install_and_test_provider_packages.sh*
      _in_container_script_init.sh*               run_mypy.sh*
      prod/                                       run_prepare_provider_packages.sh*
      refresh_pylint_todo.sh*                     run_prepare_provider_documentation.sh*
      run_ci_tests.sh*                            run_pylint.sh*
      run_clear_tmp.sh*                           run_system_tests.sh*
      run_docs_build.sh*                          run_tmux_welcome.sh*
      run_extract_tests.sh*                       stop_tmux_airflow.sh*
      run_fix_ownership.sh*                       update_quarantined_test_status.py*

   root@df8927308887:/opt/airflow# ./scripts/in_container/run_docs_build.sh

- Running specific type of test

  - Types of tests

  - Running specific type of test

  .. code-block:: bash

    $ breeze --backend mysql --mysql-version 5.7 --python 3.8 --db-reset --test-type Core


- Running Integration test for specific test type

  - Running an Integration Test

  .. code-block:: bash

   $ breeze --backend mysql --mysql-version 5.7 --python 3.8 --db-reset --test-type All --integration mongo


- For more information on Testing visit : |TESTING.rst|

.. |TESTING.rst| raw:: html

   <a href="https://github.com/apache/airflow/blob/master/TESTING.rst" target="_blank">TESTING.rst</a>

- Following are the some of important topics of TESTING.rst

  - |Airflow Test Infrastructure|

  .. |Airflow Test Infrastructure| raw:: html

   <a href="https://github.com/apache/airflow/blob/master/TESTING.rst#airflow-test-infrastructure" target="_blank">
   Airflow Test Infrastructure</a>


  - |Airflow Unit Tests|

  .. |Airflow Unit Tests| raw:: html

   <a href="https://github.com/apache/airflow/blob/master/TESTING.rst#airflow-unit-tests" target="_blank">Airflow Unit
   Tests</a>


  - |Helm Unit Tests|

  .. |Helm Unit Tests| raw:: html

   <a href="https://github.com/apache/airflow/blob/master/TESTING.rst#helm-unit-tests" target="_blank">Helm Unit Tests
   </a>


  - |Airflow Integration Tests|

  .. |Airflow Integration Tests| raw:: html

   <a href="https://github.com/apache/airflow/blob/master/TESTING.rst#airflow-integration-tests" target="_blank">
   Airflow Integration Tests</a>


  - |Running Tests with Kubernetes|

  .. |Running Tests with Kubernetes| raw:: html

   <a href="https://github.com/apache/airflow/blob/master/TESTING.rst#running-tests-with-kubernetes" target="_blank">
   Running Tests with Kubernetes</a>


  - |Airflow System Tests|

  .. |Airflow System Tests| raw:: html

   <a href="https://github.com/apache/airflow/blob/master/TESTING.rst#airflow-system-tests" target="_blank">Airflow
   System Tests</a>


  - |Local and Remote Debugging in IDE|

  .. |Local and Remote Debugging in IDE| raw:: html

   <a href="https://github.com/apache/airflow/blob/master/TESTING.rst#local-and-remote-debugging-in-ide"
   target="_blank">Local and Remote Debugging in IDE</a>


  - |BASH Unit Testing (BATS)|

  .. |BASH Unit Testing (BATS)| raw:: html

   <a href="https://github.com/apache/airflow/blob/master/TESTING.rst#bash-unit-testing-bats" target="_blank">
   BASH Unit Testing (BATS)</a>


Pre-commit
----------

Before committing changes to github or raising a pull request, code needs to be checked for certain quality standards
such as spell check, code syntax, code formatting, compatibility with Apache License requirements etc. This set of
tests are applied when you commit your code.

.. raw:: html

  <div align="center" style="padding-bottom:20px">
    <img src="images/quick_start/ci_tests.png"
         alt="CI tests GitHub">
  </div>


To avoid burden on CI infrastructure and to save time, Pre-commit hooks can be run locally before committing changes.

1.  Installing required packages

.. code-block:: bash

  $ sudo apt install libxml2-utils

2. Installing required Python packages

.. code-block:: bash

  $ pyenv activate airflow-env
  $ pip install pre-commit

3. Go to your project directory

.. code-block:: bash

  $ cd ~/PycharmProjects/airflow


4. Running pre-commit hooks

.. code-block:: bash

  $ pre-commit run --all-files
    No-tabs checker......................................................Passed
    Add license for all SQL files........................................Passed
    Add license for all other files......................................Passed
    Add license for all rst files........................................Passed
    Add license for all JS/CSS/PUML files................................Passed
    Add license for all JINJA template files.............................Passed
    Add license for all shell files......................................Passed
    Add license for all python files.....................................Passed
    Add license for all XML files........................................Passed
    Add license for all yaml files.......................................Passed
    Add license for all md files.........................................Passed
    Add license for all mermaid files....................................Passed
    Add TOC for md files.................................................Passed
    Add TOC for upgrade documentation....................................Passed
    Check hooks apply to the repository..................................Passed
    black................................................................Passed
    Check for merge conflicts............................................Passed
    Debug Statements (Python)............................................Passed
    Check builtin type constructor use...................................Passed
    Detect Private Key...................................................Passed
    Fix End of Files.....................................................Passed
    ...........................................................................

5. Running pre-commit for selected files

.. code-block:: bash

  $ pre-commit run  --files airflow/decorators.py tests/utils/test_task_group.py



6. Running specific hook for selected files

.. code-block:: bash

  $ pre-commit run black --files airflow/decorators.py tests/utils/test_task_group.py
    black...............................................................Passed
  $ pre-commit run flake8 --files airflow/decorators.py tests/utils/test_task_group.py
    Run flake8..........................................................Passed




7. Running specific checks in container using shell scripts. Scripts are located in ``.\scripts\in_container``
   directory.

.. code-block:: bash

   root@df8927308887:/opt/airflow# ./scripts/in_container/
      bin/                                        run_flake8.sh*
      check_environment.sh*                       run_generate_constraints.sh*
      entrypoint_ci.sh*                           run_init_script.sh*
      entrypoint_exec.sh*                         run_install_and_test_provider_packages.sh*
      _in_container_script_init.sh*               run_mypy.sh*
      prod/                                       run_prepare_provider_packages.sh*
      refresh_pylint_todo.sh*                     run_prepare_provider_documentation.sh*
      run_ci_tests.sh*                            run_pylint.sh*
      run_clear_tmp.sh*                           run_system_tests.sh*
      run_docs_build.sh*                          run_tmux_welcome.sh*
      run_extract_tests.sh*                       stop_tmux_airflow.sh*
      run_fix_ownership.sh*                       update_quarantined_test_status.py*


   root@df8927308887:/opt/airflow# ./scripts/in_container/run_docs_build.sh




8. Enabling Pre-commit check before push. It will run pre-commit automatically before committing and stops the commit

.. code-block:: bash

  $ cd ~/PycharmProjects/airflow
  $ pre-commit install
  $ git commit -m "Added xyz"

9. To disable Pre-commit

.. code-block:: bash

  $ cd ~/PycharmProjects/airflow
  $ pre-commit uninstall


- For more information on visit |STATIC_CODE_CHECKS.rst|

.. |STATIC_CODE_CHECKS.rst| raw:: html

   <a href="https://github.com/apache/airflow/blob/master/STATIC_CODE_CHECKS.rst" target="_blank">
   STATIC_CODE_CHECKS.rst</a>

- Following are some of the important links of STATIC_CODE_CHECKS.rst

  - |Pre-commit Hooks|

  .. |Pre-commit Hooks| raw:: html

   <a href="https://github.com/apache/airflow/blob/master/STATIC_CODE_CHECKS.rst#pre-commit-hooks" target="_blank">
   Pre-commit Hooks</a>

  - |Pylint Static Code Checks|

  .. |Pylint Static Code Checks| raw:: html

   <a href="https://github.com/apache/airflow/blob/master/STATIC_CODE_CHECKS.rst#pylint-static-code-checks"
   target="_blank">Pylint Static Code Checks</a>


  - |Running Static Code Checks via Breeze|

  .. |Running Static Code Checks via Breeze| raw:: html

   <a href="https://github.com/apache/airflow/blob/master/STATIC_CODE_CHECKS.rst#running-static-code-checks-via-breeze"
   target="_blank">Running Static Code Checks via Breeze</a>





Contribution guide
------------------

- To know how to contribute to the project visit |CONTRIBUTING.rst|

.. |CONTRIBUTING.rst| raw:: html

   <a href="https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst" target="_blank">CONTRIBUTING.rst</a>

- Following are some of important links of CONTRIBUTING.rst

  - |Types of contributions|

  .. |Types of contributions| raw:: html

   <a href="https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst#contributions" target="_blank">
   Types of contributions</a>


  - |Roles of contributor|

  .. |Roles of contributor| raw:: html

   <a href="https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst#roles" target="_blank">Roles of
   contributor</a>


  - |Workflow for a contribution|

  .. |Workflow for a contribution| raw:: html

   <a href="https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst#contribution-workflow" target="_blank">
   Workflow for a contribution</a>



Raising Pull Request
--------------------

1. Go to your GitHub account and open your fork project and click on Branches

   .. raw:: html

    <div align="center" style="padding-bottom:10px">
      <img src="images/quick_start/pr1.png"
           alt="Goto fork and select branches">
    </div>

2. Click on ``New pull request`` button on branch from which you want to raise a pull request.

   .. raw:: html

      <div align="center" style="padding-bottom:10px">
        <img src="images/quick_start/pr2.png"
             alt="Accessing local airflow">
      </div>

3. Add title and description as per Contributing guidelines and click on ``Create pull request``.

   .. raw:: html

      <div align="center" style="padding-bottom:10px">
        <img src="images/quick_start/pr3.png"
             alt="Accessing local airflow">
      </div>


Syncing Fork and rebasing Pull request
--------------------------------------

Often it takes several days or weeks to discuss and iterate with the PR until it is ready to merge.
In the meantime new commits are merged, and you might run into conflicts, therefore you should periodically
synchronize master in your fork with the ``apache/airflow`` master and rebase your PR on top of it. Following
describes how to do it.


- |Syncing fork|

.. |Syncing fork| raw:: html

   <a href="https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst#how-to-sync-your-fork" target="_blank">
   Update new changes made to apache:airflow project to your fork</a>


- |Rebasing pull request|

.. |Rebasing pull request| raw:: html

   <a href="https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst#how-to-rebase-pr" target="_blank">
   Rebasing pull request</a>
