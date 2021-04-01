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

.. raw:: html

    <div align="center">
      <img src="images/AirflowBreeze_logo.png"
           alt="Airflow Breeze - Development and Test Environment for Apache Airflow">
    </div>

.. contents:: :local:

Airflow Breeze CI environment
=============================

Airflow Breeze is an easy-to-use development and test environment using
`Docker Compose <https://docs.docker.com/compose/>`_.
The environment is available for local use and is also used in Airflow's CI tests.

We called it *Airflow Breeze* as **It's a Breeze to contribute to Airflow**.

The advantages and disadvantages of using the Breeze environment vs. other ways of testing Airflow
are described in `CONTRIBUTING.rst <CONTRIBUTING.rst#integration-test-development-environment>`_.

All the output from the last ./breeze command is automatically logged to the ``logs/breeze.out`` file.

Watch the video below about Airflow Breeze. It explains the motivation for Breeze
and screencasts all its uses.

.. raw:: html

    <div align="center">
      <a href="https://youtu.be/4MCTXq-oF68">
        <img src="images/breeze/overlayed_breeze.png" width="640"
             alt="Airflow Breeze - Development and Test Environment for Apache Airflow">
      </a>
    </div>

Prerequisites
=============

Docker Community Edition
------------------------

- **Version**: Install the latest stable Docker Community Edition and add it to the PATH.
- **Permissions**: Configure to run the ``docker`` commands directly and not only via root user.
  Your user should be in the ``docker`` group.
  See `Docker installation guide <https://docs.docker.com/install/>`_ for details.
- **Disk space**: On macOS, increase your available disk space before starting to work with
  the environment. At least 128 GB of free disk space is recommended. You can also get by with a
  smaller space but make sure to clean up the Docker disk space periodically.
  See also `Docker for Mac - Space <https://docs.docker.com/docker-for-mac/space>`_ for details
  on increasing disk space available for Docker on Mac.
- **Docker problems**: Sometimes it is not obvious that space is an issue when you run into
  a problem with Docker. If you see a weird behaviour, try ``breeze cleanup-image`` command.
  Also see `pruning <https://docs.docker.com/config/pruning/>`_ instructions from Docker.

Here is an example configuration with more than 200GB disk space for Docker:

.. raw:: html

    <div align="center">
        <img src="images/disk_space_osx.png" width="640"
             alt="Disk space MacOS">
    </div>

Docker Compose
--------------

- **Version**: Install the latest stable Docker Compose and add it to the PATH.
  See `Docker Compose Installation Guide <https://docs.docker.com/compose/install/>`_ for details.

- **Permissions**: Configure permission to run the ``docker-compose`` command.

Docker in WSL 2
---------------

- **WSL 2 installation** :
    Install WSL 2 and a Linux Distro (e.g. Ubuntu) see
    `WSL 2 Installation Guide <https://docs.microsoft.com/en-us/windows/wsl/install-win10>`_ for details.

- **Docker Desktop installation** :
    Install Docker Desktop for Windows. For Windows Home follow the
    `Docker Windows Home Installation Guide <https://docs.docker.com/docker-for-windows/install-windows-home>`_.
    For Windows Pro, Enterprise, or Education follow the
    `Docker Windows Installation Guide <https://docs.docker.com/docker-for-windows/install/>`_.

- **Docker setting** :
    WSL integration needs to be enabled

.. raw:: html

    <div align="center">
        <img src="images/docker_wsl_integration.png" width="640"
             alt="Airflow Breeze - Docker WSL2 integration">
    </div>

- **WSL 2 Filesystem Performance** :
    Accessing the host Windows filesystem incurs a performance penalty,
    it is therefore recommended to do development on the Linux filesystem.
    E.g. Run ``cd ~`` and create a development folder in your Linux distro home
    and git pull the Airflow repo there.

- **WSL 2 Memory Usage** :
    WSL 2 can consume a lot of memory under the process name "Vmmem". To reclaim the memory after
    development you can:

      * On the Linux distro clear cached memory: ``sudo sysctl -w vm.drop_caches=3``
      * If no longer using Docker you can quit Docker Desktop
        (right click system try icon and select "Quit Docker Desktop")
      * If no longer using WSL you can shut it down on the Windows Host
        with the following command: ``wsl --shutdown``

- **Developing in WSL 2** :
    You can use all the standard Linux command line utilities to develop on WSL 2.
    Further VS Code supports developing in Windows but remotely executing in WSL.
    If VS Code is installed on the Windows host system then in the WSL Linux Distro
    you can run ``code .`` in the root directory of you Airflow repo to launch VS Code.

Getopt and gstat
----------------

* For Linux, run ``apt install util-linux coreutils`` or an equivalent if your system is not Debian-based.
* For macOS, install GNU ``getopt`` and ``gstat`` utilities to get Airflow Breeze running.

  Run ``brew install gnu-getopt coreutils`` and then follow instructions to link the gnu-getopt version to
  become the first on the PATH. Make sure to re-login after you make the suggested changes.

**Examples:**

If you use bash, run this command and re-login:

.. code-block:: bash

    echo 'export PATH="/usr/local/opt/gnu-getopt/bin:$PATH"' >> ~/.bash_profile
    . ~/.bash_profile


If you use zsh, run this command and re-login:

.. code-block:: bash

    echo 'export PATH="/usr/local/opt/gnu-getopt/bin:$PATH"' >> ~/.zprofile
    . ~/.zprofile


Let's confirm that ``getopt`` and ``gstat`` utilities are successfully installed

.. code-block:: bash

    $ getopt --version
    getopt from util-linux *
    $ gstat --version
    stat (GNU coreutils) *
    Copyright (C) 2020 Free Software Foundation, Inc.
    License GPLv3+: GNU GPL version 3 or later <https://gnu.org/licenses/gpl.html>.
    This is free software: you are free to change and redistribute it.
    There is NO WARRANTY, to the extent permitted by law.

    Written by Michael Meskes.

Resources required
==================

Memory
------

Minimum 4GB RAM for Docker Engine is required to run the full Breeze environment.

On macOS, 2GB of RAM are available for your Docker containers by default, but more memory is recommended
(4GB should be comfortable). For details see
`Docker for Mac - Advanced tab <https://docs.docker.com/v17.12/docker-for-mac/#advanced-tab>`_.

On Windows WSL 2 expect the Linux Distro and Docker containers to use 7 - 8 GB of RAM.

Disk
----

Minimum 40GB free disk space is required for your Docker Containers.

On Mac OS This might deteriorate over time so you might need to increase it or run ``docker system --prune``
periodically. For details see
`Docker for Mac - Advanced tab <https://docs.docker.com/v17.12/docker-for-mac/#advanced-tab>`_.

On WSL2 you might want to increase your Virtual Hard Disk by following:
`Expanding the size of your WSL 2 Virtual Hard Disk <https://docs.microsoft.com/en-us/windows/wsl/compare-versions#expanding-the-size-of-your-wsl-2-virtual-hard-disk>`_

Cleaning the environment
------------------------

You may need to clean up your Docker environment occasionally. The images are quite big
(1.5GB for both images needed for static code analysis and CI tests) and, if you often rebuild/update
them, you may end up with some unused image data.

To clean up the Docker environment:

1. Stop Breeze with ``./breeze stop``. (If Breeze is already running)

2. Run the ``docker system prune`` command.

3. Run ``docker images --all`` and ``docker ps --all`` to verify that your Docker is clean.

   Both commands should return an empty list of images and containers respectively.

If you run into disk space errors, consider pruning your Docker images with the ``docker system prune --all``
command. You may need to restart the Docker Engine before running this command.

In case of disk space errors on macOS, increase the disk space available for Docker. See
`Prerequisites <#prerequisites>`_ for details.


Installation
============

Installation is as easy as checking out Airflow repository and running Breeze command.
You enter the Breeze test environment by running the ``./breeze`` script. You can run it with
the ``help`` command to see the list of available options. See `Breeze Command-Line Interface Reference`_
for details.

.. code-block:: bash

  ./breeze

The First time you run Breeze, it pulls and builds a local version of Docker images.
It pulls the latest Airflow CI images from `Airflow DockerHub <https://hub.docker.com/r/apache/airflow>`_
and uses them to build your local Docker images. Note that the first run (per python) might take up to 10
minutes on a fast connection to start. Subsequent runs should be much faster.

Once you enter the environment, you are dropped into bash shell of the Airflow container and you can
run tests immediately.

To use the full potential of breeze you should set up autocomplete and you can
add the checked-out Airflow repository to your PATH to run Breeze without the ``./`` and from any directory.

The ``breeze`` command comes with a built-in bash/zsh autocomplete setup command. After installing, when you
start typing the command, you can use <TAB> to show all the available switches and get
auto-completion on typical values of parameters that you can use.

You should set up the autocomplete option automatically by running:

.. code-block:: bash

   ./breeze setup-autocomplete

You get the auto-completion working when you re-enter the shell.

When you enter the Breeze environment, automatically an environment file is sourced from
``files/airflow-breeze-config/variables.env``. The ``files`` folder from your local sources is
automatically mounted to the container under ``/files`` path and you can put there any files you want
to make available for the Breeze container.

.. raw:: html

    <div align="center">
      <a href="https://youtu.be/4MCTXq-oF68?t=78">
        <img src="images/breeze/overlayed_breeze_installation.png" width="640"
             alt="Airflow Breeze - Installation">
      </a>
    </div>

Running tests in the CI interactive environment
===============================================

Breeze helps with running tests in the same environment/way as CI tests are run. You can run various
types of tests while you enter Breeze CI interactive environment - this is described in detail
in `<TESTING.rst>`_

.. raw:: html

    <div align="center">
      <a href="https://youtu.be/4MCTXq-oF68?t=262">
        <img src="images/breeze/overlayed_breeze_running_tests.png" width="640"
             alt="Airflow Breeze - Running tests">
      </a>
    </div>

Choosing different Breeze environment configuration
===================================================

You can use additional ``breeze`` flags to choose your environment. You can specify a Python
version to use, and backend (the meta-data database). Thanks to that, with Breeze, you can recreate the same
environments as we have in matrix builds in the CI.

For example, you can choose to run Python 3.6 tests with MySQL as backend and in the Docker environment as
follows:

.. code-block:: bash

    ./breeze --python 3.6 --backend mysql

The choices you make are persisted in the ``./.build/`` cache directory so that next time when you use the
``breeze`` script, it could use the values that were used previously. This way you do not have to specify
them when you run the script. You can delete the ``.build/`` directory in case you want to restore the
default settings.

The defaults when you run the Breeze environment are Python 3.6 version and SQLite database.

.. raw:: html

    <div align="center">
      <a href="https://youtu.be/4MCTXq-oF68?t=389">
        <img src="images/breeze/overlayed_breeze_select_backend_python.png" width="640"
             alt="Airflow Breeze - Selecting Python and Backend version">
      </a>
    </div>


Troubleshooting
===============

If you are having problems with the Breeze environment, try the steps below. After each step you
can check whether your problem is fixed.

1. If you are on macOS, check if you have enough disk space for Docker.
2. Restart Breeze with ``./breeze restart``.
3. Delete the ``.build`` directory and run ``./breeze build-image``.
4. Clean up Docker images via ``breeze cleanup-image`` command.
5. Restart your Docker Engine and try again.
6. Restart your machine and try again.
7. Re-install Docker CE and try again.

In case the problems are not solved, you can set the VERBOSE_COMMANDS variable to "true":

.. code-block::

        export VERBOSE_COMMANDS="true"


Then run the failed command, copy-and-paste the output from your terminal to the
`Airflow Slack <https://s.apache.org/airflow-slack>`_  #airflow-breeze channel and
describe your problem.

Other uses of the Airflow Breeze environment
============================================

Airflow Breeze is a bash script serving as a "swiss-army-knife" of Airflow testing. Under the
hood it uses other scripts that you can also run manually if you have problem with running the Breeze
environment.

Breeze script allows performing the following tasks:

Managing CI environment:

    * Build CI docker image with ``breeze build-image`` command
    * Enter interactive shell in CI container when ``shell`` (or no command) is specified
    * Join running interactive shell with ``breeze exec`` command
    * Stop running interactive environment with ``breeze stop`` command
    * Restart running interactive environment with ``breeze restart`` command
    * Run test specified with ``breeze tests`` command
    * Generate constraints with ``breeze generate-constraints``
    * Execute arbitrary command in the test environment with ``breeze shell`` command
    * Execute arbitrary docker-compose command with ``breeze docker-compose`` command
    * Push docker images with ``breeze push-image`` command (require committers rights to push images)

You can optionally reset the Airflow metada database if specified as extra ``--db-reset`` flag and for CI image
you can also start integrations (separate Docker images) if specified as extra ``--integration`` flags. You can also
chose which backend database should be used with ``--backend`` flag and python version with ``--python`` flag.

You can also have breeze launch Airflow automatically ``breeze start-airflow``, this will drop you in a
tmux session with four panes:

   - one to monitor the scheduler,
   - one for the webserver,
   - one monitors and compiles Javascript files,
   - one with a shell for additional commands.

Managing Prod environment (with ``--production-image`` flag):

    * Build CI docker image with ``breeze build-image`` command
    * Enter interactive shell in PROD container when ``shell`` (or no command) is specified
    * Join running interactive shell with ``breeze exec`` command
    * Stop running interactive environment with ``breeze stop`` command
    * Restart running interactive environment with ``breeze restart`` command
    * Execute arbitrary command in the test environment with ``breeze shell`` command
    * Execute arbitrary docker-compose command with ``breeze docker-compose`` command
    * Push docker images with ``breeze push-image`` command (require committers rights to push images)

You can optionally reset database if specified as extra ``--db-reset`` flag. You can also
chose which backend database should be used with ``--backend`` flag and python version with ``--python`` flag.


Manage and Interact with Kubernetes tests environment:

    * Manage KinD Kubernetes cluster and deploy Airflow to KinD cluster ``breeze kind-cluster`` commands
    * Run Kubernetes tests  specified with ``breeze kind-cluster tests`` command
    * Enter the interactive kubernetes test environment with ``breeze kind-cluster shell`` command

Run static checks:

    * Run static checks - either for currently staged change or for all files with
      ``breeze static-check`` command

Build documentation:

    * Build documentation with ``breeze build-docs`` command

Set up local development environment:

    * Setup local virtualenv with ``breeze setup-virtualenv`` command
    * Setup autocomplete for itself with ``breeze setup-autocomplete`` command

Database volumes in Breeze
--------------------------

Breeze keeps data for all it's integration in named docker volumes. Each backend and integration
keeps data in their own volume. Those volumes are persisted until ``./breeze stop`` command or
``./breeze restart`` command is run. You can also preserve the volumes by adding flag
``--preserve-volumes`` when you run either of those commands. Then, next time when you start
``Breeze``, it will have the data pre-populated. You can always delete the volumes by
running ``./breeze stop`` without the ``--preserve-volumes`` flag.

Launching multiple terminals
----------------------------

Often if you want to run full airflow in the Breeze environment you need to launch multiple terminals and
run ``airflow webserver``, ``airflow scheduler``, ``airflow worker`` in separate terminals.

This can be achieved either via ``tmux`` or via exec-ing into the running container from the host. Tmux
is installed inside the container and you can launch it with ``tmux`` command. Tmux provides you with the
capability of creating multiple virtual terminals and multiplex between them. More about ``tmux`` can be
found at `tmux GitHub wiki page <https://github.com/tmux/tmux/wiki>`_ . Tmux has several useful shortcuts
that allow you to split the terminals, open new tabs etc - it's pretty useful to learn it.

.. raw:: html

    <div align="center">
      <a href="https://youtu.be/4MCTXq-oF68?t=824">
        <img src="images/breeze/overlayed_breeze_using_tmux.png" width="640"
             alt="Airflow Breeze - Using tmux">
      </a>
    </div>


Another way is to exec into Breeze terminal from the host's terminal. Often you can
have multiple terminals in the host (Linux/MacOS/WSL2 on Windows) and you can simply use those terminals
to enter the running container. It's as easy as launching ``breeze exec`` while you already started the
Breeze environment. You will be dropped into bash and environment variables will be read in the same
way as when you enter the environment. You can do it multiple times and open as many terminals as you need.

.. raw:: html

    <div align="center">
      <a href="https://youtu.be/4MCTXq-oF68?t=978">
        <img src="images/breeze/overlayed_breeze_using_exec.png" width="640"
             alt="Airflow Breeze - Using tmux">
      </a>
    </div>


Additional tools
----------------

To shrink the Docker image, not all tools are pre-installed in the Docker image. But we have made sure that there
is an easy process to install additional tools.

Additional tools are installed in ``/files/bin``. This path is added to ``$PATH``, so your shell will
automatically autocomplete files that are in that directory. You can also keep the binaries for your tools
in this directory if you need to.

**Installation scripts**

For the development convenience, we have also provided installation scripts for commonly used tools. They are
installed to ``/files/opt/``, so they are preserved after restarting the Breeze environment. Each script
is also available in ``$PATH``, so just type ``install_<TAB>`` to get a list of tools.

Currently available scripts:

* ``install_aws.sh`` - installs `the AWS CLI <https://aws.amazon.com/cli/>`__ including
* ``install_az.sh`` - installs `the Azure CLI <https://github.com/Azure/azure-cli>`__ including
* ``install_gcloud.sh`` - installs `the Google Cloud SDK <https://cloud.google.com/sdk>`__ including
  ``gcloud``, ``gsutil``.
* ``install_imgcat.sh`` - installs `imgcat - Inline Images Protocol <https://iterm2.com/documentation-images.html>`__
  for iTerm2 (Mac OS only)
* ``install_java.sh`` - installs `the OpenJDK 8u41 <https://openjdk.java.net/>`__
* ``install_kubectl.sh`` - installs `the Kubernetes command-line tool, kubectl <https://kubernetes.io/docs/reference/kubectl/kubectl/>`__
* ``install_terraform.sh`` - installs `Terraform <https://www.terraform.io/docs/index.html>`__

Launching Breeze integrations
-----------------------------

When Breeze starts, it can start additional integrations. Those are additional docker containers
that are started in the same docker-compose command. Those are required by some of the tests
as described in `<TESTING.rst#airflow-integration-tests>`_.

By default Breeze starts only airflow container without any integration enabled. If you selected
``postgres`` or ``mysql`` backend, the container for the selected backend is also started (but only the one
that is selected). You can start the additional integrations by passing ``--integration`` flag
with appropriate integration name when starting Breeze. You can specify several ``--integration`` flags
to start more than one integration at a time.
Finally you can specify ``--integration all`` to start all integrations.

Once integration is started, it will continue to run until the environment is stopped with
``breeze stop`` command. or restarted via ``breeze restart`` command

Note that running integrations uses significant resources - CPU and memory.

.. raw:: html

    <div align="center">
      <a href="https://youtu.be/4MCTXq-oF68?t=1187">
        <img src="images/breeze/overlayed_breeze_integrations.png" width="640"
             alt="Airflow Breeze - Integrations">
      </a>
    </div>

Building CI images
------------------

With Breeze you can build images that are used by Airflow CI and production ones.

For all development tasks, unit tests, integration tests, and static code checks, we use the
**CI image** maintained on the DockerHub in the ``apache/airflow`` repository.
This Docker image contains a lot of test-related packages (size of ~1GB).
Its tag follows the pattern of ``<BRANCH>-python<PYTHON_MAJOR_MINOR_VERSION>-ci``
(for example, ``apache/airflow:master-python3.6-ci`` or ``apache/airflow:v1-10-test-python3.6-ci``).
The image is built using the `<Dockerfile.ci>`_ Dockerfile.

The CI image is built automatically as needed, however it can be rebuilt manually with
``build-image`` command. The production
image should be built manually - but also a variant of this image is built automatically when
kubernetes tests are executed see `Running Kubernetes tests <#running-kubernetes-tests>`_

.. raw:: html

    <div align="center">
      <a href="https://youtu.be/4MCTXq-oF68?t=1387">
        <img src="images/breeze/overlayed_breeze_build_images.png" width="640"
             alt="Airflow Breeze - Building images">
      </a>
    </div>

Building the image first time pulls a pre-built version of images from the Docker Hub, which may take some
time. But for subsequent source code changes, no wait time is expected.
However, changes to sensitive files like ``setup.py`` or ``Dockerfile.ci`` will trigger a rebuild
that may take more time though it is highly optimized to only rebuild what is needed.

Breeze has built in mechanism to check if your local image has not diverged too much from the
latest image build on CI. This might happen when for example latest patches have been released as new
Python images or when significant changes are made in the Dockerfile. In such cases, Breeze will
download the latest images before rebuilding because this is usually faster than rebuilding the image.

In most cases, rebuilding an image requires network connectivity (for example, to download new
dependencies). If you work offline and do not want to rebuild the images when needed, you can set the
``FORCE_ANSWER_TO_QUESTIONS`` variable to ``no`` as described in the
`Setting default behaviour for user interaction <#setting-default-behaviour-for-user-interaction>`_ section.

Preparing packages
------------------

Breeze can also be used to prepare airflow packages - both "apache-airflow" main package and
provider packages.

You can read more about testing provider packages in
`TESTING.rst <TESTING.rst#running-tests-with-packages>`_

There are several commands that you can run in Breeze to manage and build packages:

* preparing Provider Readme files
* preparing Airflow packages
* preparing Provider packages

Preparing provider readme files is part of the release procedure by the release managers
and it is described in detail in `dev <dev/README.md>`_ .

The packages are prepared in ``dist`` folder. Note, that this command cleans up the ``dist`` folder
before running, so you should run it before generating airflow package below as it will be removed.

The below example builds provider packages in the wheel format.

.. code-block:: bash

     ./breeze prepare-provider-packages

If you run this command without packages, you will prepare all packages, you can however specify
providers that you would like to build. By default ``both`` types of packages are prepared (
``wheel`` and ``sdist``, but you can change it providing optional --package-format flag.

.. code-block:: bash

     ./breeze prepare-provider-packages google amazon

You can see all providers available by running this command:

.. code-block:: bash

     ./breeze prepare-provider-packages -- --help


You can also prepare airflow packages using breeze:

.. code-block:: bash

     ./breeze prepare-airflow-packages

This prepares airflow .whl package in the dist folder.

Again, you can specify optional ``--package-format`` flag to build selected formats of airflow packages,
default is to build ``both`` type of packages ``sdist`` and ``wheel``.

.. code-block:: bash

     ./breeze prepare-airflow-packages --package-format=wheel


Building Production images
--------------------------

The **Production image** is also maintained on the DockerHub in the
``apache/airflow`` repository. This Docker image (and Dockerfile) contains size-optimised Airflow
installation with selected extras and dependencies. Its tag follows the pattern of
``<BRANCH>-python<PYTHON_MAJOR_MINOR_VERSION>`` (for example, ``apache/airflow:master-python3.6``
or ``apache/airflow:v1-10-test-python3.6``).

However in many cases you want to add your own custom version of the image - with added apt dependencies,
python dependencies, additional Airflow extras. Breeze's ``build-image`` command helps to build your own,
customized variant of the image that contains everything you need.

You can switch to building the production image by adding ``--production-image`` flag to the ``build_image``
command. Note, that the images can also be build using ``docker build`` command by passing appropriate
build-args as described in `IMAGES.rst <IMAGES.rst>`_ , but Breeze provides several flags that
makes it easier to do it. You can see all the flags by running ``./breeze build-image --help``,
but here typical examples are presented:

.. code-block:: bash

     ./breeze build-image --production-image --additional-extras "jira"

This installs additional ``jira`` extra while installing airflow in the image.


.. code-block:: bash

     ./breeze build-image --production-image --additional-python-deps "torchio==0.17.10"

This install additional pypi dependency - torchio in specified version.


.. code-block:: bash

     ./breeze build-image --production-image --additional-dev-apt-deps "libasound2-dev" \
        --additional-runtime-apt-deps "libasound2"

This install additional apt dependencies - ``libasound2-dev`` in build image and ``libasound`` in the
final image. Those are development dependencies that might be needed to build and use python packages added
via the ``--additional-python-deps`` flag. The ``dev`` dependencies are not installed in the final
production image, they are only installed in the build "segment" of the production image that is used
as an intermediate step to build the final image. Usually names of the ``dev`` dependencies end with ``-dev``
suffix and they need to also be paired with corresponding runtime dependency added for the runtime image
(without -dev).

.. code-block:: bash

     ./breeze build-image --production-image --python 3.7 --additional-dev-deps "libasound2-dev" \
        --additional-runtime-apt-deps "libasound2"

Same as above but uses python 3.7.

.. raw:: html

    <div align="center">
      <a href="https://youtu.be/4MCTXq-oF68?t=1496">
        <img src="images/breeze/overlayed_breeze_build_images_prod.png" width="640"
             alt="Airflow Breeze - Building Production images">
      </a>
    </div>

Building Production images for 1.10 Airflow versions
----------------------------------------------------

With Breeze you can also use the master Dockerfile to build custom images for released Airflow versions.
This works in the same way as building production image from master, but you need to add additional switch
``--install-airflow-version``. You should pass version of airflow (as released in PyPI). It can be used
to install both released versions and release candidates. Similarly as in case of master images,
we can pass additional extras/dependencies to install via the additional flags.

.. code-block:: bash

     ./breeze build-image --production-image --additional-extras "jira" --install-airflow-version="1.10.11"

Builds airflow image with released Airflow version 1.10.11 and additional extra "jira" added.

.. code-block:: bash

     ./breeze build-image --production-image --install-airflow-version="1.10.11rc2"

Builds airflow image with released Airflow version 1.10.11rc2.


You can also build airflow directly from GitHub source code - by providing Git Reference via
``--install-airflow-reference``. The reference can be a branch name, tag name, or commit hash. This
is useful mostly for testing.

.. code-block:: bash

     ./breeze build-image --production-image --install-airflow-reference="v1-10-test"

This Builds airflow image from the current ``v1-10-test`` branch of Airflow.

.. code-block:: bash

     ./breeze build-image --production-image \
          --install-airflow-reference="0d91fcf725f69e10f0969ca36f9e38e1d74110d0"

This Builds airflow image from the  ``0d91fcf725f69e10f0969ca36f9e38e1d74110d0`` commit hash on
GitHub.

.. raw:: html

    <div align="center">
      <a href="https://youtu.be/4MCTXq-oF68?t=1586">
        <img src="images/breeze/overlayed_breeze_build_images_released_versions.png" width="640"
             alt="Airflow Breeze - Building Production images for 1.10 Airflow versions">
      </a>
    </div>


Running static checks
---------------------

You can run static checks via Breeze. You can also run them via pre-commit command but with auto-completion
Breeze makes it easier to run selective static checks. If you press <TAB> after the static-check and if
you have auto-complete setup you should see auto-completable list of all checks available.

.. code-block:: bash

     ./breeze static-check mypy

The above will run mypy check for currently staged files.

You can also add arbitrary pre-commit flag after ``--``

.. code-block:: bash

     ./breeze static-check mypy -- --all-files

The above will run mypy check for all files.

.. raw:: html

    <div align="center">
      <a href="https://youtu.be/4MCTXq-oF68?t=1675">
        <img src="images/breeze/overlayed_breeze_static_checks.png" width="640"
             alt="Airflow Breeze - Static checks">
      </a>
    </div>

If you want ever need to get a list of the files that will be checked (for troubleshooting when playing with the
``--from-ref`` and ``--to-ref``

.. code-block:: bash

     breeze static-check identity --verbose # currently staged files
     breeze static-check identity --verbose -- --from-ref $(git merge-base master HEAD) --to-ref HEAD #  branch updates

Building the Documentation
--------------------------

To build documentation in Breeze, use the ``build-docs`` command:

.. code-block:: bash

     ./breeze build-docs

Results of the build can be found in the ``docs/_build`` folder.

The documentation build consists of three steps:

* verifying consistency of indexes
* building documentation
* spell checking

You can choose only one stage of the two by providing ``--spellcheck-only`` or ``--docs-only`` after
extra ``--`` flag.

.. code-block:: bash

     ./breeze build-docs -- --spellcheck-only


Often errors during documentation generation come from the docstrings of auto-api generated classes.
During the docs building auto-api generated files are stored in the ``docs/_api`` folder. This helps you
easily identify the location the problems with documentation originated from.

.. raw:: html

    <div align="center">
      <a href="https://youtu.be/4MCTXq-oF68?t=1760">
        <img src="images/breeze/overlayed_breeze_build_docs.png" width="640"
             alt="Airflow Breeze - Build docs">
      </a>
    </div>

Generating constraints
----------------------

Whenever setup.py gets modified, the CI master job will re-generate constraint files. Those constraint
files are stored in separated orphan branches: ``constraints-master``, ``constraints-2-0``
and ``constraints-1-10``. They are stored separately for each python version and there are separate
constraints for:

* 'constraints' - those are constraints generated by matching the current airflow version from sources
   and providers that are installed from PyPI. Those are constraints used by the users who want to
   install airflow with pip

* "constraints-source-providers" - those are constraints generated by using providers installed from
  current sources. While adding new providers their dependencies might change, so this set of providers
  is the current set of the constraints for airflow and providers from the current master sources.
  Those providers are used by CI system to keep "stable" set of constraints.

* "constraints-no-providers" - those are constraints generated from only Apache Airflow, without any
  providers. If you want to manage airflow separately and then add providers individually, you can
  use those.

Those are constraint files as described in detail in the
`<CONTRIBUTING.rst#pinned-constraint-files>`_ contributing documentation.

In case someone modifies setup.py, the ``CRON`` scheduled CI build automatically upgrades and
pushes changed to the constraint files, however you can also perform test run of this locally using
``generate-constraints`` command of Breeze.

.. code-block:: bash

  for python_version in 3.6 3.7 3.8
  do
    ./breeze generate-constraints --generate-constraints-mode source-providers --python ${python_version}
    ./breeze generate-constraints --generate-constraints-mode pypi-providers --python ${python_version}
    ./breeze generate-constraints --generate-constraints-mode no-providers --python ${python_version}
  done


This bumps the constraint files to latest versions and stores hash of setup.py. The generated constraint
and setup.py hash files are stored in the ``files`` folder and while generating the constraints diff
of changes vs the previous constraint files is printed.


Using local virtualenv environment in Your Host IDE
---------------------------------------------------

You can set up your host IDE (for example, IntelliJ's PyCharm/Idea) to work with Breeze
and benefit from all the features provided by your IDE, such as local and remote debugging,
language auto-completion, documentation support, etc.

To use your host IDE with Breeze:

1. Create a local virtual environment:

   You can use any of the following wrappers to create and manage your virtual environments:
   `pyenv <https://github.com/pyenv/pyenv>`_, `pyenv-virtualenv <https://github.com/pyenv/pyenv-virtualenv>`_,
   or `virtualenvwrapper <https://virtualenvwrapper.readthedocs.io/en/latest/>`_.

   Ideally, you should have virtualenvs for all Python versions supported by Airflow (3.5, 3.6, 3.7)

2. Use the right command to activate the virtualenv (``workon`` if you use virtualenvwrapper or
   ``pyenv activate`` if you use pyenv.

3. Initialize the created local virtualenv:

.. code-block:: bash

  ./breeze initialize-local-virtualenv --python 3.8

4. Select the virtualenv you created as the project's default virtualenv in your IDE.

Note that you can also use the local virtualenv for Airflow development without Breeze.
This is a lightweight solution that has its own limitations.

More details on using the local virtualenv are available in the `LOCAL_VIRTUALENV.rst <LOCAL_VIRTUALENV.rst>`_.

.. raw:: html

    <div align="center">
      <a href="https://youtu.be/4MCTXq-oF68?t=1920">
        <img src="images/breeze/overlayed_breeze_initialize_virtualenv.png" width="640"
             alt="Airflow Breeze - Initialize virtualenv">
      </a>
    </div>

Running Kubernetes tests
------------------------

Breeze helps with running Kubernetes tests in the same environment/way as CI tests are run.
Breeze helps to setup KinD cluster for testing, setting up virtualenv and downloads the right tools
automatically to run the tests.

This is described in detail in `Testing Kubernetes <TESTING.rst#running-tests-with-kubernetes>`_.

.. raw:: html

    <div align="center">
      <a href="https://youtu.be/4MCTXq-oF68?t=2093">
        <img src="images/breeze/overlayed_breeze_kubernetes_tests.png" width="640"
             alt="Airflow Breeze - Kubernetes tests">
      </a>
    </div>

Stopping the interactive environment
------------------------------------

After starting up, the environment runs in the background and takes precious memory.
You can always stop it via:

.. code-block:: bash

   ./breeze stop


.. raw:: html

    <div align="center">
      <a href="https://youtu.be/4MCTXq-oF68?t=2639">
        <img src="images/breeze/overlayed_breeze_stop.png" width="640"
             alt="Airflow Breeze - Stop environment">
      </a>
    </div>


Internal details of Breeze
==========================

Airflow directory structure inside container
--------------------------------------------

When you are in the CI container, the following directories are used:

.. code-block:: text

  /opt/airflow - Contains sources of Airflow mounted from the host (AIRFLOW_SOURCES).
  /root/airflow - Contains all the "dynamic" Airflow files (AIRFLOW_HOME), such as:
      airflow.db - sqlite database in case sqlite is used;
      dags - folder with non-test dags (test dags are in /opt/airflow/tests/dags);
      logs - logs from Airflow executions;
      unittest.cfg - unit test configuration generated when entering the environment;
      webserver_config.py - webserver configuration generated when running Airflow in the container.

Note that when running in your local environment, the ``/root/airflow/logs`` folder is actually mounted
from your ``logs`` directory in the Airflow sources, so all logs created in the container are automatically
visible in the host as well. Every time you enter the container, the ``logs`` directory is
cleaned so that logs do not accumulate.

When you are in the production container, the following directories are used:

.. code-block:: text

  /opt/airflow - Contains sources of Airflow mounted from the host (AIRFLOW_SOURCES).
  /root/airflow - Contains all the "dynamic" Airflow files (AIRFLOW_HOME), such as:
      airflow.db - sqlite database in case sqlite is used;
      dags - folder with non-test dags (test dags are in /opt/airflow/tests/dags);
      logs - logs from Airflow executions;
      unittest.cfg - unit test configuration generated when entering the environment;
      webserver_config.py - webserver configuration generated when running Airflow in the container.

Note that when running in your local environment, the ``/root/airflow/logs`` folder is actually mounted
from your ``logs`` directory in the Airflow sources, so all logs created in the container are automatically
visible in the host as well. Every time you enter the container, the ``logs`` directory is
cleaned so that logs do not accumulate.

Running Arbitrary commands in the Breeze environment
----------------------------------------------------

To run other commands/executables inside the Breeze Docker-based environment, use the
``./breeze shell`` command. You should add your command as -c "command" after ``--`` as extra arguments.

.. code-block:: bash

     ./breeze shell -- -c "ls -la"

Running "Docker Compose" commands
---------------------------------

To run Docker Compose commands (such as ``help``, ``pull``, etc), use the
``docker-compose`` command. To add extra arguments, specify them
after ``--`` as extra arguments.

.. code-block:: bash

     ./breeze docker-compose pull -- --ignore-pull-failures

Restarting Breeze environment
-----------------------------

You can also  restart the environment and enter it via:

.. code-block:: bash

   ./breeze restart


Setting default answers for user interaction
--------------------------------------------

Sometimes during the build, you are asked whether to perform an action, skip it, or quit. This happens
when rebuilding or removing an image - actions that take a lot of time and could be potentially destructive.

For automation scripts, you can export one of the three variables to control the default
interaction behaviour:

.. code-block::

  export FORCE_ANSWER_TO_QUESTIONS="yes"

If ``FORCE_ANSWER_TO_QUESTIONS`` is set to ``yes``, the images are automatically rebuilt when needed.
Images are deleted without asking.

.. code-block::

  export FORCE_ANSWER_TO_QUESTIONS="no"

If ``FORCE_ANSWER_TO_QUESTIONS`` is set to ``no``, the old images are used even if rebuilding is needed.
This is useful when you work offline. Deleting images is aborted.

.. code-block::

  export FORCE_ANSWER_TO_QUESTIONS="quit"

If ``FORCE_ANSWER_TO_QUESTIONS`` is set to ``quit``, the whole script is aborted. Deleting images is aborted.

If more than one variable is set, ``yes`` takes precedence over ``no``, which takes precedence over ``quit``.

Fixing File/Directory Ownership
-------------------------------

On Linux, there is a problem with propagating ownership of created files (a known Docker problem). The
files and directories created in the container are not owned by the host user (but by the root user in our
case). This may prevent you from switching branches, for example, if files owned by the root user are
created within your sources. In case you are on a Linux host and have some files in your sources created
by the root user, you can fix the ownership of those files by running this script:

.. code-block::

  ./scripts/ci/tools/ci_fix_ownership.sh

Mounting Local Sources to Breeze
--------------------------------

Important sources of Airflow are mounted inside the ``airflow`` container that you enter.
This means that you can continue editing your changes on the host in your favourite IDE and have them
visible in the Docker immediately and ready to test without rebuilding images. You can disable mounting
by specifying ``--skip-mounting-local-sources`` flag when running Breeze. In this case you will have sources
embedded in the container and changes to these sources will not be persistent.


After you run Breeze for the first time, you will have empty directory ``files`` in your source code,
which will be mapped to ``/files`` in your Docker container. You can pass there any files you need to
configure and run Docker. They will not be removed between Docker runs.

By default ``/files/dags`` folder is mounted from your local ``<AIRFLOW_SOURCES>/files/dags`` and this is
the directory used by airflow scheduler and webserver to scan dags for. You can use it to test your dags
from local sources in Airflow. If you wish to add local DAGs that can be run by Breeze.

Port Forwarding
---------------

When you run Airflow Breeze, the following ports are automatically forwarded:

* 28080 -> forwarded to Airflow webserver -> airflow:8080
* 25555 -> forwarded to Flower dashboard -> airflow:5555
* 25433 -> forwarded to Postgres database -> postgres:5432
* 23306 -> forwarded to MySQL database  -> mysql:3306
* 26379 -> forwarded to Redis broker -> redis:6379

You can connect to these ports/databases using:

* Webserver: ``http://127.0.0.1:28080``
* Flower: ``http://127.0.0.1:25555``
* Postgres: ``jdbc:postgresql://127.0.0.1:25433/airflow?user=postgres&password=airflow``
* Mysql: ``jdbc:mysql://127.0.0.1:23306/airflow?user=root``
* Redis: ``redis://127.0.0.1:26379/0```

Start the webserver manually with the ``airflow webserver`` command if you want to connect
to the webserver. You can use ``tmux`` to multiply terminals. You may need to create a user prior to
running the webserver in order to log in. This can be done with the following command:

.. code-block:: bash

    airflow users create --role Admin --username admin --password admin --email admin@example.com --firstname foo --lastname bar

For databases, you need to run ``airflow db reset`` at least once (or run some tests) after you started
Airflow Breeze to get the database/tables created. You can connect to databases with IDE or any other
database client:


.. raw:: html

    <div align="center">
        <img src="images/database_view.png" width="640"
             alt="Airflow Breeze - Database view">
    </div>

You can change the used host port numbers by setting appropriate environment variables:

* ``WEBSERVER_HOST_PORT``
* ``POSTGRES_HOST_PORT``
* ``MYSQL_HOST_PORT``

If you set these variables, next time when you enter the environment the new ports should be in effect.

Managing Dependencies
---------------------

If you need to change apt dependencies in the ``Dockerfile.ci``, add Python packages in ``setup.py`` or
add JavaScript dependencies in ``package.json``, you can either add dependencies temporarily for a single
Breeze session or permanently in ``setup.py``, ``Dockerfile.ci``, or ``package.json`` files.

Installing Dependencies for a Single Breeze Session
...................................................

You can install dependencies inside the container using ``sudo apt install``, ``pip install`` or
``yarn install`` (in ``airflow/www`` folder) respectively. This is useful if you want to test something
quickly while you are in the container. However, these changes are not retained: they disappear once you
exit the container (except for the node.js dependencies if your sources are mounted to the container).
Therefore, if you want to retain a new dependency, follow the second option described below.

Adding Dependencies Permanently
...............................

You can add dependencies to the ``Dockerfile.ci``, ``setup.py`` or ``package.json`` and rebuild the image.
This should happen automatically if you modify any of these files.
After you exit the container and re-run ``breeze``, Breeze detects changes in dependencies,
asks you to confirm rebuilding the image and proceeds with rebuilding if you confirm (or skip it
if you do not confirm). After rebuilding is done, Breeze drops you to shell. You may also use the
``build-image`` command to only rebuild CI image and not to go into shell.

Incremental apt Dependencies in the Dockerfile.ci during development
....................................................................

During development, changing dependencies in ``apt-get`` closer to the top of the ``Dockerfile.ci``
invalidates cache for most of the image. It takes long time for Breeze to rebuild the image.
So, it is a recommended practice to add new dependencies initially closer to the end
of the ``Dockerfile.ci``. This way dependencies will be added incrementally.

Before merge, these dependencies should be moved to the appropriate ``apt-get install`` command,
which is already in the ``Dockerfile.ci``.


Breeze Command-Line Interface Reference
=======================================

Airflow Breeze Syntax
---------------------

This is the current syntax for  `./breeze <./breeze>`_:

 .. START BREEZE HELP MARKER

.. code-block:: text


  ####################################################################################################

  usage: breeze [FLAGS] [COMMAND] -- <EXTRA_ARGS>

  By default the script enters the  CI container and drops you to bash shell, but you can choose
  one of the commands to run specific actions instead.

  Add --help after each command to see details:

  Commands without arguments:

    shell                                    [Default] Enters interactive shell in the container
    build-docs                               Builds documentation in the container
    build-image                              Builds CI or Production docker image
    cleanup-image                            Cleans up the container image created
    exec                                     Execs into running breeze container in new terminal
    generate-constraints                     Generates pinned constraint files
    push-image                               Pushes images to registry
    initialize-local-virtualenv              Initializes local virtualenv
    prepare-airflow-packages                 Prepares airflow packages
    setup-autocomplete                       Sets up autocomplete for breeze
    start-airflow                            Starts Scheduler and Webserver and enters the shell
    stop                                     Stops the docker-compose environment
    restart                                  Stops the docker-compose environment including DB cleanup
    toggle-suppress-cheatsheet               Toggles on/off cheatsheet
    toggle-suppress-asciiart                 Toggles on/off asciiart

  Commands with arguments:

    docker-compose                     <ARG>      Executes specified docker-compose command
    kind-cluster                       <ARG>      Manages KinD cluster on the host
    prepare-provider-documentation     <ARG>      Prepares provider packages documentation
    prepare-provider-packages          <ARG>      Prepares provider packages
    static-check                       <ARG>      Performs selected static check for changed files
    tests                              <ARG>      Runs selected tests in the container

  Help commands:

    flags                                    Shows all breeze's flags
    help                                     Shows this help message
    help-all                                 Shows detailed help for all commands and flags

  ####################################################################################################

  Detailed usage

  ####################################################################################################


  Detailed usage for command: shell


  breeze shell [FLAGS] [-- <EXTRA_ARGS>]

        This is default subcommand if no subcommand is used.

        Enters interactive shell where you can run all tests, start Airflow webserver, scheduler,
        workers, interact with the database, run DAGs etc. It is the default command if no command
        is selected. The shell is executed in the container and in case integrations are chosen,
        the integrations will be started as separated docker containers - under the docker-compose
        supervision. Local sources are by default mounted to within the container so you can edit
        them locally and run tests immediately in the container. Several folders ('files', 'dist')
        are also mounted so that you can exchange files between the host and container.

        The 'files/airflow-breeze-config/variables.env' file can contain additional variables
        and setup. This file is automatically sourced when you enter the container. Database
        and webserver ports are forwarded to appropriate database/webserver so that you can
        connect to it from your host environment.

        You can also pass <EXTRA_ARGS> after -- they will be passed as bash parameters, this is
        especially useful to pass bash options, for example -c to execute command:

        'breeze shell -- -c "ls -la"'
        'breeze -- -c "ls -la"'

        For DockerHub pull --dockerhub-user and --dockerhub-repo flags can be used to specify
        the repository to pull from. For GitHub repository, the --github-repository
        flag can be used for the same purpose. You can also use
        --github-image-id <COMMIT_SHA>|<RUN_ID> in case you want to pull the image
        with specific COMMIT_SHA tag or RUN_ID.

        'breeze shell \
              --github-image-id 9a621eaa394c0a0a336f8e1b31b35eff4e4ee86e' - pull/use image with SHA
        'breeze \
              --github-image-id 9a621eaa394c0a0a336f8e1b31b35eff4e4ee86e' - pull/use image with SHA
        'breeze shell \
              --github-image-id 209845560' - pull/use image with RUN_ID
        'breeze \
              --github-image-id 209845560' - pull/use image with RUN_ID


  ####################################################################################################


  Detailed usage for command: build-docs


  breeze build-docs [-- <EXTRA_ARGS>]

        Builds Airflow documentation. The documentation is build inside docker container - to
        maintain the same build environment for everyone. Appropriate sources are mapped from
        the host to the container so that latest sources are used. The folders where documentation
        is generated ('docs/_build') are also mounted to the container - this way results of
        the documentation build is available in the host.

        The possible extra args are: --docs-only, --spellcheck-only, --package-filter, --help


  ####################################################################################################


  Detailed usage for command: build-image


  breeze build-image [FLAGS]

        Builds docker image (CI or production) without entering the container. You can pass
        additional options to this command, such as:

        Choosing python version:
          '--python'

        Choosing cache option:
           '--build-cache-local' or '-build-cache-pulled', or '--build-cache-none'

        Choosing whether to force pull images or force build the image:
            '--force-build-image',
             '--force-pull-image', '--force-pull-base-python-image'

        You can also pass '--production-image' flag to build production image rather than CI image.

        For DockerHub pull. '--dockerhub-user' and '--dockerhub-repo' flags can be used to specify
        the repository to pull from. For GitHub repository, the '--github-repository'
        flag can be used for the same purpose. You can also use
        '--github-image-id <COMMIT_SHA>|<RUN_ID>' in case you want to pull the image with
        specific COMMIT_SHA tag or RUN_ID.

  Flags:

  -p, --python PYTHON_MAJOR_MINOR_VERSION
          Python version used for the image. This is always major/minor version.

          Note that versions 2.7 and 3.5 are only valid when installing Airflow 1.10 with
          --install-airflow-version or --install-airflow-reference flags.

          One of:

                 2.7 3.5 3.6 3.7 3.8

  -a, --install-airflow-version INSTALL_AIRFLOW_VERSION
          In CI image, installs Airflow (in entrypoint) from PIP released version or using
          the installation method specified (sdist, wheel, none).

          In PROD image the installation of selected method or version happens during image building.
          For PROD image, the 'none' options is not valid.

          One of:

                 2.0.1 2.0.0 1.10.15 1.10.14 1.10.12 1.10.11 1.10.10 1.10.9 none wheel sdist

          When 'none' is used, you can install airflow from local packages. When building image,
          airflow package should be added to 'docker-context-files' and
          --install-from-docker-context-files flag should be used. When running an image, airflow
          package should be added to dist folder and --install-packages-from-dist flag should be used.

  -t, --install-airflow-reference INSTALL_AIRFLOW_REFERENCE
          If specified, installs Airflow directly from reference in GitHub. This happens at
          image building time in production image and at container entering time for CI image.
          This can be a GitHub branch like master or v1-10-test, or a tag like 2.0.0a1.

  --installation-method INSTALLATION_METHOD
          Method of installing airflow for production image - either from the sources ('.')
          or from package 'apache-airflow' to install from PyPI.
          Default in Breeze is to install from sources. One of:

                 . apache-airflow

  --no-rbac-ui
          Disables RBAC UI when Airflow 1.10.* is installed.

  --install-packages-from-dist
          If specified it will look for packages placed in dist folder and it will install the
          packages after installing Airflow. This is useful for testing provider
          packages.

  --upgrade-to-newer-dependencies
          Upgrades PIP packages to latest versions available without looking at the constraints.

  --continue-on-pip-check-failure
          Continue even if 'pip check' fails.

  -I, --production-image
          Use production image for entering the environment and builds (not for tests).

  -F, --force-build-images
          Forces building of the local docker images. The images are rebuilt
          automatically for the first time or when changes are detected in
          package-related files, but you can force it using this flag.

  -P, --force-pull-images
          Forces pulling of images from DockerHub before building to populate cache. The
          images are pulled by default only for the first time you run the
          environment, later the locally build images are used as cache.

  --force-pull-base-python-image
          Forces pulling of Python base image from DockerHub before building to
          populate cache. This should only be run in case we need to update to latest available
          Python base image. This should be a rare and manually triggered event. Also this flag
          is used in the scheduled run in CI when we rebuild all the images from the scratch
          and run the tests to see if the latest python images do not fail our tests.

  Customization options:

  -E, --extras EXTRAS
          Extras to pass to build images The default are different for CI and production images:

          CI image:
                 devel_ci

          Production image:
                 async,amazon,celery,cncf.kubernetes,docker,dask,elasticsearch,ftp,grpc,hashicorp,
                 http,ldap,google,microsoft.azure,mysql,postgres,redis,sendgrid,sftp,slack,ssh,statsd,
                 virtualenv

  --image-tag TAG
          Additional tag in the image.

  --skip-installing-airflow-providers-from-sources
          By default 'pip install' in Airflow 2.0 installs only the provider packages that
          are needed by the extras. When you build image during the development (which is
          default in Breeze) all providers are installed by default from sources.
          You can disable it by adding this flag but then you have to install providers from
          wheel packages via --install-packages-from-dist flag.

  --disable-pypi-when-building
          Disable installing Airflow from pypi when building. If you use this flag and want
          to install Airflow, you have to install it from packages placed in
          'docker-context-files' and use --install-from-local-files-when-building flag.

  --additional-extras ADDITIONAL_EXTRAS
          Additional extras to pass to build images The default is no additional extras.

  --additional-python-deps ADDITIONAL_PYTHON_DEPS
          Additional python dependencies to use when building the images.

  --dev-apt-command DEV_APT_COMMAND
          The basic command executed before dev apt deps are installed.

  --additional-dev-apt-command ADDITIONAL_DEV_APT_COMMAND
          Additional command executed before dev apt deps are installed.

  --additional-dev-apt-deps ADDITIONAL_DEV_APT_DEPS
          Additional apt dev dependencies to use when building the images.

  --dev-apt-deps DEV_APT_DEPS
          The basic apt dev dependencies to use when building the images.

  --additional-dev-apt-deps ADDITIONAL_DEV_DEPS
          Additional apt dev dependencies to use when building the images.

  --additional-dev-apt-envs ADDITIONAL_DEV_APT_ENVS
          Additional environment variables set when adding dev dependencies.

  --runtime-apt-command RUNTIME_APT_COMMAND
          The basic command executed before runtime apt deps are installed.

  --additional-runtime-apt-command ADDITIONAL_RUNTIME_APT_COMMAND
          Additional command executed before runtime apt deps are installed.

  --runtime-apt-deps ADDITIONAL_RUNTIME_APT_DEPS
          The basic apt runtime dependencies to use when building the images.

  --additional-runtime-apt-deps ADDITIONAL_RUNTIME_DEPS
          Additional apt runtime dependencies to use when building the images.

  --additional-runtime-apt-envs ADDITIONAL_RUNTIME_APT_DEPS
          Additional environment variables set when adding runtime dependencies.

  Build options:

  --disable-mysql-client-installation
          Disables installation of the mysql client which might be problematic if you are building
          image in controlled environment. Only valid for production image.

  --constraints-location
          Url to the constraints file. In case of the production image it can also be a path to the
          constraint file placed in 'docker-context-files' folder, in which case it has to be
          in the form of '/docker-context-files/<NAME_OF_THE_FILE>'

  --disable-pip-cache
          Disables GitHub PIP cache during the build. Useful if GitHub is not reachable during build.

  --install-from-local-files-when-building
          This flag is used during image building. If it is used additionally to installing
          Airflow from PyPI, the packages are installed from the .whl and .tar.gz packages placed
          in the 'docker-context-files' folder. The same flag can be used during entering the image in
          the CI image - in this case also the .whl and .tar.gz files will be installed automatically

  -C, --force-clean-images
          Force build images with cache disabled. This will remove the pulled or build images
          and start building images from scratch. This might take a long time.

  -r, --skip-rebuild-check
          Skips checking image for rebuilds. It will use whatever image is available locally/pulled.

  -L, --build-cache-local
          Uses local cache to build images. No pulled images will be used, but results of local
          builds in the Docker cache are used instead. This will take longer than when the pulled
          cache is used for the first time, but subsequent '--build-cache-local' builds will be
          faster as they will use mostly the locally build cache.

          This is default strategy used by the Production image builds.

  -U, --build-cache-pulled
          Uses images pulled from registry (either DockerHub or GitHub depending on
          --use-github-registry flag) to build images. The pulled images will be used as cache.
          Those builds are usually faster than when ''--build-cache-local'' with the exception if
          the registry images are not yet updated. The DockerHub images are updated nightly and the
          GitHub images are updated after merges to master so it might be that the images are still
          outdated vs. the latest version of the Dockerfiles you are using. In this case, the
          ''--build-cache-local'' might be faster, especially if you iterate and change the
          Dockerfiles yourself.

          This is default strategy used by the CI image builds.

  -X, --build-cache-disabled
          Disables cache during docker builds. This is useful if you want to make sure you want to
          rebuild everything from scratch.

          This strategy is used by default for both Production and CI images for the scheduled
          (nightly) builds in CI.

  -D, --dockerhub-user DOCKERHUB_USER
          DockerHub user used to pull, push and build images. Default: apache.

  -H, --dockerhub-repo DOCKERHUB_REPO
          DockerHub repository used to pull, push, build images. Default: airflow.

  -c, --use-github-registry
          If GitHub registry is enabled, pulls and pushes are done from the GitHub registry not
          DockerHub. You need to be logged in to the registry in order to be able to pull/push from
          and you need to be committer to push to Apache Airflow' GitHub registry.

  --github-registry GITHUB_REGISTRY
          GitHub registry used. GitHub has legacy Packages registry and Public Beta Container
          registry.

          Default: docker.pkg.github.com.

          If you use this flag, automatically --use-github-registry flag is enabled.

                 docker.pkg.github.com ghcr.io

  -g, --github-repository GITHUB_REPOSITORY
          GitHub repository used to pull, push images when cache is used.
          Default: apache/airflow.

          If you use this flag, automatically --use-github-registry flag is enabled.

  -s, --github-image-id COMMIT_SHA|RUN_ID
          <RUN_ID> or <COMMIT_SHA> of the image. Images in GitHub registry are stored with those
          to be able to easily find the image for particular CI runs. Once you know the
          <RUN_ID> or <COMMIT_SHA>, you can specify it in github-image-id flag and Breeze will
          automatically pull and use that image so that you can easily reproduce a problem
          that occurred in CI.

          If you use this flag, automatically --use-github-registry is enabled.


          Default: latest.

  -v, --verbose
          Show verbose information about executed docker, kind, kubectl, helm commands. Useful for
          debugging - when you run breeze with --verbose flags you will be able to see the commands
          executed under the hood and copy&paste them to your terminal to debug them more easily.

          Note that you can further increase verbosity and see all the commands executed by breeze
          by running 'export VERBOSE_COMMANDS="true"' before running breeze.

  --dry-run-docker
          Only show docker commands to execute instead of actually executing them. The docker
          commands are printed in yellow color.


  ####################################################################################################


  Detailed usage for command: cleanup-image


  breeze cleanup-image [FLAGS]

        Removes the breeze-related images created in your local docker image cache. This will
        not reclaim space in docker cache. You need to 'docker system prune' (optionally
        with --all) to reclaim that space.

  Flags:

  -p, --python PYTHON_MAJOR_MINOR_VERSION
          Python version used for the image. This is always major/minor version.

          Note that versions 2.7 and 3.5 are only valid when installing Airflow 1.10 with
          --install-airflow-version or --install-airflow-reference flags.

          One of:

                 2.7 3.5 3.6 3.7 3.8

  -I, --production-image
          Use production image for entering the environment and builds (not for tests).

  -v, --verbose
          Show verbose information about executed docker, kind, kubectl, helm commands. Useful for
          debugging - when you run breeze with --verbose flags you will be able to see the commands
          executed under the hood and copy&paste them to your terminal to debug them more easily.

          Note that you can further increase verbosity and see all the commands executed by breeze
          by running 'export VERBOSE_COMMANDS="true"' before running breeze.

  --dry-run-docker
          Only show docker commands to execute instead of actually executing them. The docker
          commands are printed in yellow color.


  ####################################################################################################


  Detailed usage for command: exec


  breeze exec [-- <EXTRA_ARGS>]

        Execs into interactive shell to an already running container. The container mus be started
        already by breeze shell command. If you are not familiar with tmux, this is the best
        way to run multiple processes in the same container at the same time for example scheduler,
        webserver, workers, database console and interactive terminal.


  ####################################################################################################


  Detailed usage for command: generate-constraints


  breeze generate-constraints [FLAGS]

        Generates pinned constraint files with all extras from setup.py. Those files are generated in
        files folder - separate files for different python version. Those constraint files when
        pushed to orphan constraints-master, constraints-2-0 and constraints-1-10 branches are used
        to generate repeatable CI builds as well as run repeatable production image builds and
        upgrades when you want to include installing or updating some of the released providers
        released at the time particular airflow version was released. You can use those
        constraints to predictably install released Airflow versions. This is mainly used to test
        the constraint generation or manually fix them - constraints are pushed to the orphan
        branches by a successful scheduled CRON job in CI automatically, but sometimes manual fix
        might be needed.

  Flags:

  --generate-constraints-mode GENERATE_CONSTRAINTS_MODE
          Mode of generating constraints - determines whether providers are installed when generating
          constraints and which version of them (either the ones from sources are used or the ones
          from pypi.

          One of:

                 source-providers pypi-providers no-providers

  -p, --python PYTHON_MAJOR_MINOR_VERSION
          Python version used for the image. This is always major/minor version.

          Note that versions 2.7 and 3.5 are only valid when installing Airflow 1.10 with
          --install-airflow-version or --install-airflow-reference flags.

          One of:

                 2.7 3.5 3.6 3.7 3.8

  -v, --verbose
          Show verbose information about executed docker, kind, kubectl, helm commands. Useful for
          debugging - when you run breeze with --verbose flags you will be able to see the commands
          executed under the hood and copy&paste them to your terminal to debug them more easily.

          Note that you can further increase verbosity and see all the commands executed by breeze
          by running 'export VERBOSE_COMMANDS="true"' before running breeze.

  --dry-run-docker
          Only show docker commands to execute instead of actually executing them. The docker
          commands are printed in yellow color.


  ####################################################################################################


  Detailed usage for command: push-image


  breeze push_image [FLAGS]

        Pushes images to docker registry. You can push the images to DockerHub registry (default)
        or to the GitHub registry (if --use-github-registry flag is used).

        For DockerHub pushes --dockerhub-user and --dockerhub-repo flags can be used to specify
        the repository to push to. For GitHub repository, the --github-repository
        flag can be used for the same purpose. You can also add
        --github-image-id <COMMIT_SHA>|<RUN_ID> in case you want to push image with specific
        SHA tag or run id. In case you specify --github-repository or --github-image-id, you
        do not need to specify --use-github-registry flag.

        You can also add --production-image flag to switch to production image (default is CI one)

        Examples:

        'breeze push-image' or
        'breeze push-image --dockerhub-user user' to push to your private registry or
        'breeze push-image --production-image' - to push production image or
        'breeze push-image --use-github-registry' - to push to GitHub image registry or
        'breeze push-image \
              --github-repository user/airflow' - to push to your user's fork
        'breeze push-image \
              --github-image-id 9a621eaa394c0a0a336f8e1b31b35eff4e4ee86e' - to push with COMMIT_SHA
        'breeze push-image \
              --github-image-id 209845560' - to push with RUN_ID

  Flags:

  -D, --dockerhub-user DOCKERHUB_USER
          DockerHub user used to pull, push and build images. Default: apache.

  -H, --dockerhub-repo DOCKERHUB_REPO
          DockerHub repository used to pull, push, build images. Default: airflow.

  -c, --use-github-registry
          If GitHub registry is enabled, pulls and pushes are done from the GitHub registry not
          DockerHub. You need to be logged in to the registry in order to be able to pull/push from
          and you need to be committer to push to Apache Airflow' GitHub registry.

  --github-registry GITHUB_REGISTRY
          GitHub registry used. GitHub has legacy Packages registry and Public Beta Container
          registry.

          Default: docker.pkg.github.com.

          If you use this flag, automatically --use-github-registry flag is enabled.

                 docker.pkg.github.com ghcr.io

  -g, --github-repository GITHUB_REPOSITORY
          GitHub repository used to pull, push images when cache is used.
          Default: apache/airflow.

          If you use this flag, automatically --use-github-registry flag is enabled.

  -s, --github-image-id COMMIT_SHA|RUN_ID
          <RUN_ID> or <COMMIT_SHA> of the image. Images in GitHub registry are stored with those
          to be able to easily find the image for particular CI runs. Once you know the
          <RUN_ID> or <COMMIT_SHA>, you can specify it in github-image-id flag and Breeze will
          automatically pull and use that image so that you can easily reproduce a problem
          that occurred in CI.

          If you use this flag, automatically --use-github-registry is enabled.


          Default: latest.

  -v, --verbose
          Show verbose information about executed docker, kind, kubectl, helm commands. Useful for
          debugging - when you run breeze with --verbose flags you will be able to see the commands
          executed under the hood and copy&paste them to your terminal to debug them more easily.

          Note that you can further increase verbosity and see all the commands executed by breeze
          by running 'export VERBOSE_COMMANDS="true"' before running breeze.

  --dry-run-docker
          Only show docker commands to execute instead of actually executing them. The docker
          commands are printed in yellow color.


  ####################################################################################################


  Detailed usage for command: initialize-local-virtualenv


  breeze initialize-local-virtualenv [FLAGS]

        Initializes locally created virtualenv installing all dependencies of Airflow
        taking into account the constraints for the version specified.
        This local virtualenv can be used to aid auto-completion and IDE support as
        well as run unit tests directly from the IDE. You need to have virtualenv
        activated before running this command.

  Flags:

  -p, --python PYTHON_MAJOR_MINOR_VERSION
          Python version used for the image. This is always major/minor version.

          Note that versions 2.7 and 3.5 are only valid when installing Airflow 1.10 with
          --install-airflow-version or --install-airflow-reference flags.

          One of:

                 2.7 3.5 3.6 3.7 3.8


  ####################################################################################################


  Detailed usage for command: prepare-airflow-packages


  breeze prepare-airflow-packages [FLAGS]

        Prepares airflow packages (sdist and wheel) in dist folder. Note that
        prepare-provider-packages command cleans up the dist folder, so if you want also
        to generate provider packages, make sure you run prepare-provider-packages first,
        and prepare-airflow-packages second.

        General form:

        'breeze prepare-airflow-packages

  Flags:

  --package-format PACKAGE_FORMAT

          Chooses format of packages to prepare.

          One of:

                 both,sdist,wheel

          Default: both

  -v, --verbose
          Show verbose information about executed docker, kind, kubectl, helm commands. Useful for
          debugging - when you run breeze with --verbose flags you will be able to see the commands
          executed under the hood and copy&paste them to your terminal to debug them more easily.

          Note that you can further increase verbosity and see all the commands executed by breeze
          by running 'export VERBOSE_COMMANDS="true"' before running breeze.

  --dry-run-docker
          Only show docker commands to execute instead of actually executing them. The docker
          commands are printed in yellow color.


  ####################################################################################################


  Detailed usage for command: setup-autocomplete


  breeze setup-autocomplete

        Sets up autocomplete for breeze commands. Once you do it you need to re-enter the bash
        shell and when typing breeze command <TAB> will provide autocomplete for
        parameters and values.


  ####################################################################################################


  Detailed usage for command: start-airflow


  breeze start-airflow

        Like the Shell command this will enter the interactive shell, but it will also start
        automatically the Scheduler and the Webserver. It will leave you in a tmux session where you
        can also observe what is happening in your Airflow.

        This is a convenient way to setup a development environment. Your dags will be loaded from the
        folder 'files/dags' on your host machine (it could take some times).

        If you want to load default connections and example dags you can use the dedicated flags.

  Flags:

  --load-example-dags
          Include Airflow example dags.

  --load-default-connections
          Include Airflow Default Connections.


  ####################################################################################################


  Detailed usage for command: stop


  breeze stop

        Brings down running docker compose environment. When you start the environment, the docker
        containers will continue running so that startup time is shorter. But they take quite a lot of
        memory and CPU. This command stops all running containers from the environment.

  Flags:

  --preserve-volumes
          Use this flag if you would like to preserve data volumes from the databases used
          by the integrations. By default, those volumes are deleted, so when you run 'stop'
          or 'restart' commands you start from scratch, but by using this flag you can
          preserve them. If you want to delete those volumes after stopping Breeze, just
          run the 'breeze stop' again without this flag.


  ####################################################################################################


  Detailed usage for command: restart


  breeze restart [FLAGS]

        Restarts running docker compose environment. When you restart the environment, the docker
        containers will be restarted. That includes cleaning up the databases. This is
        especially useful if you switch between different versions of Airflow.

  Flags:

  --preserve-volumes
          Use this flag if you would like to preserve data volumes from the databases used
          by the integrations. By default, those volumes are deleted, so when you run 'stop'
          or 'restart' commands you start from scratch, but by using this flag you can
          preserve them. If you want to delete those volumes after stopping Breeze, just
          run the 'breeze stop' again without this flag.


  ####################################################################################################


  Detailed usage for command: toggle-suppress-cheatsheet


  breeze toggle-suppress-cheatsheet

        Toggles on/off cheatsheet displayed before starting bash shell.


  ####################################################################################################


  Detailed usage for command: toggle-suppress-asciiart


  breeze toggle-suppress-asciiart

        Toggles on/off asciiart displayed before starting bash shell.


  ####################################################################################################


  Detailed usage for command: docker-compose


  breeze docker-compose [FLAGS] COMMAND [-- <EXTRA_ARGS>]

        Run docker-compose command instead of entering the environment. Use 'help' as command
        to see available commands. The <EXTRA_ARGS> passed after -- are treated
        as additional options passed to docker-compose. For example

        'breeze docker-compose pull -- --ignore-pull-failures'

  Flags:

  -p, --python PYTHON_MAJOR_MINOR_VERSION
          Python version used for the image. This is always major/minor version.

          Note that versions 2.7 and 3.5 are only valid when installing Airflow 1.10 with
          --install-airflow-version or --install-airflow-reference flags.

          One of:

                 2.7 3.5 3.6 3.7 3.8

  -b, --backend BACKEND
          Backend to use for tests - it determines which database is used.
          One of:

                 sqlite mysql postgres

          Default: sqlite

  --postgres-version POSTGRES_VERSION
          Postgres version used. One of:

                 9.6 10 11 12 13

  --mysql-version MYSQL_VERSION
          Mysql version used. One of:

                 5.7 8

  -v, --verbose
          Show verbose information about executed docker, kind, kubectl, helm commands. Useful for
          debugging - when you run breeze with --verbose flags you will be able to see the commands
          executed under the hood and copy&paste them to your terminal to debug them more easily.

          Note that you can further increase verbosity and see all the commands executed by breeze
          by running 'export VERBOSE_COMMANDS="true"' before running breeze.

  --dry-run-docker
          Only show docker commands to execute instead of actually executing them. The docker
          commands are printed in yellow color.


  ####################################################################################################


  Detailed usage for command: kind-cluster


  breeze kind-cluster [FLAGS] OPERATION

        Manages host-side Kind Kubernetes cluster that is used to run Kubernetes integration tests.
        It allows to start/stop/restart/status the Kind Kubernetes cluster and deploy Airflow to it.
        This enables you to run tests inside the breeze environment with latest airflow images.
        Note that in case of deploying airflow, the first step is to rebuild the image and loading it
        to the cluster so you can also pass appropriate build image flags that will influence
        rebuilding the production image. Operation is one of:

                 start stop restart status deploy test shell k9s

        The last two operations - shell and k9s allow you to perform interactive testing with
        kubernetes tests. You can enter the shell from which you can run kubernetes tests and in
        another terminal you can start the k9s CLI to debug kubernetes instance. It is an easy
        way to debug the kubernetes deployments.

        You can read more about k9s at https://k9scli.io/

  Flags:

  -p, --python PYTHON_MAJOR_MINOR_VERSION
          Python version used for the image. This is always major/minor version.

          Note that versions 2.7 and 3.5 are only valid when installing Airflow 1.10 with
          --install-airflow-version or --install-airflow-reference flags.

          One of:

                 2.7 3.5 3.6 3.7 3.8

  -F, --force-build-images
          Forces building of the local docker images. The images are rebuilt
          automatically for the first time or when changes are detected in
          package-related files, but you can force it using this flag.

  -P, --force-pull-images
          Forces pulling of images from DockerHub before building to populate cache. The
          images are pulled by default only for the first time you run the
          environment, later the locally build images are used as cache.

  --force-pull-base-python-image
          Forces pulling of Python base image from DockerHub before building to
          populate cache. This should only be run in case we need to update to latest available
          Python base image. This should be a rare and manually triggered event. Also this flag
          is used in the scheduled run in CI when we rebuild all the images from the scratch
          and run the tests to see if the latest python images do not fail our tests.

  Customization options:

  -E, --extras EXTRAS
          Extras to pass to build images The default are different for CI and production images:

          CI image:
                 devel_ci

          Production image:
                 async,amazon,celery,cncf.kubernetes,docker,dask,elasticsearch,ftp,grpc,hashicorp,
                 http,ldap,google,microsoft.azure,mysql,postgres,redis,sendgrid,sftp,slack,ssh,statsd,
                 virtualenv

  --image-tag TAG
          Additional tag in the image.

  --skip-installing-airflow-providers-from-sources
          By default 'pip install' in Airflow 2.0 installs only the provider packages that
          are needed by the extras. When you build image during the development (which is
          default in Breeze) all providers are installed by default from sources.
          You can disable it by adding this flag but then you have to install providers from
          wheel packages via --install-packages-from-dist flag.

  --disable-pypi-when-building
          Disable installing Airflow from pypi when building. If you use this flag and want
          to install Airflow, you have to install it from packages placed in
          'docker-context-files' and use --install-from-local-files-when-building flag.

  --additional-extras ADDITIONAL_EXTRAS
          Additional extras to pass to build images The default is no additional extras.

  --additional-python-deps ADDITIONAL_PYTHON_DEPS
          Additional python dependencies to use when building the images.

  --dev-apt-command DEV_APT_COMMAND
          The basic command executed before dev apt deps are installed.

  --additional-dev-apt-command ADDITIONAL_DEV_APT_COMMAND
          Additional command executed before dev apt deps are installed.

  --additional-dev-apt-deps ADDITIONAL_DEV_APT_DEPS
          Additional apt dev dependencies to use when building the images.

  --dev-apt-deps DEV_APT_DEPS
          The basic apt dev dependencies to use when building the images.

  --additional-dev-apt-deps ADDITIONAL_DEV_DEPS
          Additional apt dev dependencies to use when building the images.

  --additional-dev-apt-envs ADDITIONAL_DEV_APT_ENVS
          Additional environment variables set when adding dev dependencies.

  --runtime-apt-command RUNTIME_APT_COMMAND
          The basic command executed before runtime apt deps are installed.

  --additional-runtime-apt-command ADDITIONAL_RUNTIME_APT_COMMAND
          Additional command executed before runtime apt deps are installed.

  --runtime-apt-deps ADDITIONAL_RUNTIME_APT_DEPS
          The basic apt runtime dependencies to use when building the images.

  --additional-runtime-apt-deps ADDITIONAL_RUNTIME_DEPS
          Additional apt runtime dependencies to use when building the images.

  --additional-runtime-apt-envs ADDITIONAL_RUNTIME_APT_DEPS
          Additional environment variables set when adding runtime dependencies.

  Build options:

  --disable-mysql-client-installation
          Disables installation of the mysql client which might be problematic if you are building
          image in controlled environment. Only valid for production image.

  --constraints-location
          Url to the constraints file. In case of the production image it can also be a path to the
          constraint file placed in 'docker-context-files' folder, in which case it has to be
          in the form of '/docker-context-files/<NAME_OF_THE_FILE>'

  --disable-pip-cache
          Disables GitHub PIP cache during the build. Useful if GitHub is not reachable during build.

  --install-from-local-files-when-building
          This flag is used during image building. If it is used additionally to installing
          Airflow from PyPI, the packages are installed from the .whl and .tar.gz packages placed
          in the 'docker-context-files' folder. The same flag can be used during entering the image in
          the CI image - in this case also the .whl and .tar.gz files will be installed automatically

  -C, --force-clean-images
          Force build images with cache disabled. This will remove the pulled or build images
          and start building images from scratch. This might take a long time.

  -r, --skip-rebuild-check
          Skips checking image for rebuilds. It will use whatever image is available locally/pulled.

  -L, --build-cache-local
          Uses local cache to build images. No pulled images will be used, but results of local
          builds in the Docker cache are used instead. This will take longer than when the pulled
          cache is used for the first time, but subsequent '--build-cache-local' builds will be
          faster as they will use mostly the locally build cache.

          This is default strategy used by the Production image builds.

  -U, --build-cache-pulled
          Uses images pulled from registry (either DockerHub or GitHub depending on
          --use-github-registry flag) to build images. The pulled images will be used as cache.
          Those builds are usually faster than when ''--build-cache-local'' with the exception if
          the registry images are not yet updated. The DockerHub images are updated nightly and the
          GitHub images are updated after merges to master so it might be that the images are still
          outdated vs. the latest version of the Dockerfiles you are using. In this case, the
          ''--build-cache-local'' might be faster, especially if you iterate and change the
          Dockerfiles yourself.

          This is default strategy used by the CI image builds.

  -X, --build-cache-disabled
          Disables cache during docker builds. This is useful if you want to make sure you want to
          rebuild everything from scratch.

          This strategy is used by default for both Production and CI images for the scheduled
          (nightly) builds in CI.


  ####################################################################################################


  Detailed usage for command: prepare-provider-documentation


  breeze prepare-provider-documentation [FLAGS] [PACKAGE_ID ...]

        Prepares documentation files for provider packages.

        The command is optionally followed by the list of packages to generate readme for.
        If the first parameter is not formatted as a date, then today is regenerated.
        If no packages are specified, readme for all packages are generated.
        If no date is specified, current date + 3 days is used (allowing for PMC votes to pass).

        Examples:

        'breeze prepare-provider-documentation' or
        'breeze prepare-provider-documentation --version-suffix-for-pypi rc1'

        General form:

        'breeze prepare-provider-documentation <PACKAGE_ID> ...'

        * <PACKAGE_ID> is usually directory in the airflow/providers folder (for example
          'google' but in several cases, it might be one level deeper separated with
          '.' for example 'apache.hive'

  Flags:

  -S, --version-suffix-for-pypi SUFFIX
          Adds optional suffix to the version in the generated provider package. It can be used
          to generate rc1/rc2 ... versions of the packages to be uploaded to PyPI.

  -N, --version-suffix-for-svn SUFFIX
          Adds optional suffix to the generated names of package. It can be used to generate
          rc1/rc2 ... versions of the packages to be uploaded to SVN.

  --package-format PACKAGE_FORMAT

          Chooses format of packages to prepare.

          One of:

                 both,sdist,wheel

          Default: both

  -v, --verbose
          Show verbose information about executed docker, kind, kubectl, helm commands. Useful for
          debugging - when you run breeze with --verbose flags you will be able to see the commands
          executed under the hood and copy&paste them to your terminal to debug them more easily.

          Note that you can further increase verbosity and see all the commands executed by breeze
          by running 'export VERBOSE_COMMANDS="true"' before running breeze.

  --dry-run-docker
          Only show docker commands to execute instead of actually executing them. The docker
          commands are printed in yellow color.


  ####################################################################################################


  Detailed usage for command: prepare-provider-packages


  breeze prepare-provider-packages [FLAGS] [PACKAGE_ID ...]

        Prepares provider packages. You can provide (after --) optional list of packages to prepare.
        If no packages are specified, readme for all packages are generated. You can specify optional
        --version-suffix-for-svn flag to generate rc candidate packages to upload to SVN or
        --version-suffix-for-pypi flag to generate rc candidates for PyPI packages. You can also
        provide both suffixes in case you prepare alpha/beta versions. The packages are prepared in
        dist folder. Note that this command also cleans up dist folder before generating the packages
        so that you do not have accidental files there. This will delete airflow package if it is
        prepared there so make sure you run prepare-provider-packages first,
        and prepare-airflow-packages second.

        Examples:

        'breeze prepare-provider-packages' or
        'breeze prepare-provider-packages google' or
        'breeze prepare-provider-packages --package-format wheel google' or
        'breeze prepare-provider-packages --version-suffix-for-svn rc1 http google amazon' or
        'breeze prepare-provider-packages --version-suffix-for-pypi rc1 http google amazon'
        'breeze prepare-provider-packages --version-suffix-for-pypi a1
                                              --version-suffix-for-svn a1 http google amazon'

        General form:

        'breeze prepare-provider-packages [--package-format PACKAGE_FORMAT] \
              [--version-suffix-for-svn|--version-suffix-for-pypi] <PACKAGE_ID> ...'

        * <PACKAGE_ID> is usually directory in the airflow/providers folder (for example
          'google'), but in several cases, it might be one level deeper separated with '.'
          for example 'apache.hive'

  Flags:

  --package-format PACKAGE_FORMAT

          Chooses format of packages to prepare.

          One of:

                 both,sdist,wheel

          Default: both

  -S, --version-suffix-for-pypi SUFFIX
          Adds optional suffix to the version in the generated provider package. It can be used
          to generate rc1/rc2 ... versions of the packages to be uploaded to PyPI.

  -N, --version-suffix-for-svn SUFFIX
          Adds optional suffix to the generated names of package. It can be used to generate
          rc1/rc2 ... versions of the packages to be uploaded to SVN.

  -v, --verbose
          Show verbose information about executed docker, kind, kubectl, helm commands. Useful for
          debugging - when you run breeze with --verbose flags you will be able to see the commands
          executed under the hood and copy&paste them to your terminal to debug them more easily.

          Note that you can further increase verbosity and see all the commands executed by breeze
          by running 'export VERBOSE_COMMANDS="true"' before running breeze.

  --dry-run-docker
          Only show docker commands to execute instead of actually executing them. The docker
          commands are printed in yellow color.


  ####################################################################################################


  Detailed usage for command: static-check


  breeze static-check [FLAGS] static_check [-- <EXTRA_ARGS>]

        Run selected static checks for currently changed files. You should specify static check that
        you would like to run or 'all' to run all checks. One of:

                 all all-but-pylint airflow-config-yaml airflow-providers-available
                 airflow-provider-yaml-files-ok base-operator bats-tests bats-in-container-tests
                 black build build-providers-dependencies check-apache-license check-builtin-literals
                 check-executables-have-shebangs check-hooks-apply check-integrations
                 check-merge-conflict check-xml consistent-pylint daysago-import-check
                 debug-statements detect-private-key doctoc dont-use-safe-filter end-of-file-fixer
                 fix-encoding-pragma flake8 flynt forbid-tabs helm-lint identity
                 incorrect-use-of-LoggingMixin insert-license isort json-schema language-matters
                 lint-dockerfile lint-openapi markdownlint mermaid mixed-line-ending mypy mypy-helm
                 no-providers-in-core-examples no-relative-imports pre-commit-descriptions
                 pre-commit-hook-names provide-create-sessions providers-init-file provider-yamls
                 pydevd pydocstyle pylint pylint-tests python-no-log-warn pyupgrade
                 restrict-start_date rst-backticks setup-order setup-extra-packages shellcheck
                 sort-in-the-wild sort-spelling-wordlist stylelint trailing-whitespace ui-lint
                 update-breeze-file update-extras update-local-yml-file update-setup-cfg-file
                 version-sync yamllint

        You can pass extra arguments including options to the pre-commit framework as
        <EXTRA_ARGS> passed after --. For example:

        'breeze static-check mypy' or
        'breeze static-check mypy -- --files tests/core.py'
        'breeze static-check mypy -- --all-files'

        To check all files that differ between you current branch and master run:

        'breeze static-check all -- --from-ref $(git merge-base master HEAD) --to-ref HEAD'

        To check all files that are in the HEAD commit run:

        'breeze static-check mypy -- --from-ref HEAD^ --to-ref HEAD'


        You can see all the options by adding --help EXTRA_ARG:

        'breeze static-check mypy -- --help'


  ####################################################################################################


  Detailed usage for command: tests


  breeze tests [FLAGS] [TEST_TARGET ..] [-- <EXTRA_ARGS>]

        Run the specified unit test target. There might be multiple
        targets specified separated with comas. The <EXTRA_ARGS> passed after -- are treated
        as additional options passed to pytest. You can pass 'tests' as target to
        run all tests. For example:

        'breeze tests tests/core/test_core.py -- --logging-level=DEBUG'
        'breeze tests tests

  Flags:

  --test-type TEST_TYPE
          Type of the test to run. One of:

                 All,Core,Providers,API,CLI,Integration,Other,WWW,Postgres,MySQL,Helm,Quarantined

          Default: All


  ####################################################################################################


  Detailed usage for command: flags


        Explains in detail all the flags that can be used with breeze.


  ####################################################################################################


  Detailed usage for command: help


  breeze help

        Shows general help message for all commands.


  ####################################################################################################


  Detailed usage for command: help-all


  breeze help-all

        Shows detailed help for all commands and flags.


  ####################################################################################################


  ####################################################################################################

  Summary of all flags supported by Breeze:

  ****************************************************************************************************
   Choose Airflow variant

  -p, --python PYTHON_MAJOR_MINOR_VERSION
          Python version used for the image. This is always major/minor version.

          Note that versions 2.7 and 3.5 are only valid when installing Airflow 1.10 with
          --install-airflow-version or --install-airflow-reference flags.

          One of:

                 2.7 3.5 3.6 3.7 3.8

  ****************************************************************************************************
   Choose backend to run for Airflow

  -b, --backend BACKEND
          Backend to use for tests - it determines which database is used.
          One of:

                 sqlite mysql postgres

          Default: sqlite

  --postgres-version POSTGRES_VERSION
          Postgres version used. One of:

                 9.6 10 11 12 13

  --mysql-version MYSQL_VERSION
          Mysql version used. One of:

                 5.7 8

  ****************************************************************************************************
   Enable production image

  -I, --production-image
          Use production image for entering the environment and builds (not for tests).

  ****************************************************************************************************
   Additional actions executed while entering breeze

  -d, --db-reset
          Resets the database at entry to the environment. It will drop all the tables
          and data and recreate the DB from scratch even if 'restart' command was not used.
          Combined with 'restart' command it enters the environment in the state that is
          ready to start Airflow webserver/scheduler/worker. Without the switch, the database
          does not have any tables and you need to run reset db manually.

  -i, --integration INTEGRATION
          Integration to start during tests - it determines which integrations are started
          for integration tests. There can be more than one integration started, or all to
          start all integrations. Selected integrations are not saved for future execution.
          One of:

                 cassandra kerberos mongo openldap pinot presto rabbitmq redis statsd all

  --init-script INIT_SCRIPT_FILE
          Initialization script name - Sourced from files/airflow-breeze-config. Default value
          init.sh. It will be executed after the environment is configured and started.

  ****************************************************************************************************
   Additional actions executed while starting Airflow

  --load-example-dags
          Include Airflow example dags.

  --load-default-connections
          Include Airflow Default Connections.

  ****************************************************************************************************
   Cleanup options when stopping Airflow

  --preserve-volumes
          Use this flag if you would like to preserve data volumes from the databases used
          by the integrations. By default, those volumes are deleted, so when you run 'stop'
          or 'restart' commands you start from scratch, but by using this flag you can
          preserve them. If you want to delete those volumes after stopping Breeze, just
          run the 'breeze stop' again without this flag.

  ****************************************************************************************************
   Kind kubernetes and Kubernetes tests configuration(optional)

  Configuration for the KinD Kubernetes cluster and tests:

  -K, --kubernetes-mode KUBERNETES_MODE
          Kubernetes mode - only used in case one of kind-cluster commands is used.
          One of:

                 image

          Default: image

  -V, --kubernetes-version KUBERNETES_VERSION
          Kubernetes version - only used in case one of kind-cluster commands is used.
          One of:

                 v1.18.6 v1.17.5 v1.16.9

          Default: v1.18.6

  --kind-version KIND_VERSION
          Kind version - only used in case one of kind-cluster commands is used.
          One of:

                 v0.8.0

          Default: v0.8.0

  --helm-version HELM_VERSION
          Helm version - only used in case one of kind-cluster commands is used.
          One of:

                 v3.2.4

          Default: v3.2.4

  ****************************************************************************************************
   Manage mounting local files

  -l, --skip-mounting-local-sources
          Skips mounting local volume with sources - you get exactly what is in the
          docker image rather than your current local sources of Airflow.

  ****************************************************************************************************
   Assume answers to questions

  -y, --assume-yes
          Assume 'yes' answer to all questions.

  -n, --assume-no
          Assume 'no' answer to all questions.

  -q, --assume-quit
          Assume 'quit' answer to all questions.

  ****************************************************************************************************
   Choose different Airflow version to install or run

  -a, --install-airflow-version INSTALL_AIRFLOW_VERSION
          In CI image, installs Airflow (in entrypoint) from PIP released version or using
          the installation method specified (sdist, wheel, none).

          In PROD image the installation of selected method or version happens during image building.
          For PROD image, the 'none' options is not valid.

          One of:

                 2.0.1 2.0.0 1.10.15 1.10.14 1.10.12 1.10.11 1.10.10 1.10.9 none wheel sdist

          When 'none' is used, you can install airflow from local packages. When building image,
          airflow package should be added to 'docker-context-files' and
          --install-from-docker-context-files flag should be used. When running an image, airflow
          package should be added to dist folder and --install-packages-from-dist flag should be used.

  -t, --install-airflow-reference INSTALL_AIRFLOW_REFERENCE
          If specified, installs Airflow directly from reference in GitHub. This happens at
          image building time in production image and at container entering time for CI image.
          This can be a GitHub branch like master or v1-10-test, or a tag like 2.0.0a1.

  --installation-method INSTALLATION_METHOD
          Method of installing airflow for production image - either from the sources ('.')
          or from package 'apache-airflow' to install from PyPI.
          Default in Breeze is to install from sources. One of:

                 . apache-airflow

  --no-rbac-ui
          Disables RBAC UI when Airflow 1.10.* is installed.

  --install-packages-from-dist
          If specified it will look for packages placed in dist folder and it will install the
          packages after installing Airflow. This is useful for testing provider
          packages.

  --upgrade-to-newer-dependencies
          Upgrades PIP packages to latest versions available without looking at the constraints.

  --continue-on-pip-check-failure
          Continue even if 'pip check' fails.

  ****************************************************************************************************
   Credentials

  -f, --forward-credentials
          Forwards host credentials to docker container. Use with care as it will make
          your credentials available to everything you install in Docker.

  ****************************************************************************************************
   Flags for building Docker images (both CI and production)

  -F, --force-build-images
          Forces building of the local docker images. The images are rebuilt
          automatically for the first time or when changes are detected in
          package-related files, but you can force it using this flag.

  -P, --force-pull-images
          Forces pulling of images from DockerHub before building to populate cache. The
          images are pulled by default only for the first time you run the
          environment, later the locally build images are used as cache.

  --force-pull-base-python-image
          Forces pulling of Python base image from DockerHub before building to
          populate cache. This should only be run in case we need to update to latest available
          Python base image. This should be a rare and manually triggered event. Also this flag
          is used in the scheduled run in CI when we rebuild all the images from the scratch
          and run the tests to see if the latest python images do not fail our tests.

  Customization options:

  -E, --extras EXTRAS
          Extras to pass to build images The default are different for CI and production images:

          CI image:
                 devel_ci

          Production image:
                 async,amazon,celery,cncf.kubernetes,docker,dask,elasticsearch,ftp,grpc,hashicorp,
                 http,ldap,google,microsoft.azure,mysql,postgres,redis,sendgrid,sftp,slack,ssh,statsd,
                 virtualenv

  --image-tag TAG
          Additional tag in the image.

  --skip-installing-airflow-providers-from-sources
          By default 'pip install' in Airflow 2.0 installs only the provider packages that
          are needed by the extras. When you build image during the development (which is
          default in Breeze) all providers are installed by default from sources.
          You can disable it by adding this flag but then you have to install providers from
          wheel packages via --install-packages-from-dist flag.

  --disable-pypi-when-building
          Disable installing Airflow from pypi when building. If you use this flag and want
          to install Airflow, you have to install it from packages placed in
          'docker-context-files' and use --install-from-local-files-when-building flag.

  --additional-extras ADDITIONAL_EXTRAS
          Additional extras to pass to build images The default is no additional extras.

  --additional-python-deps ADDITIONAL_PYTHON_DEPS
          Additional python dependencies to use when building the images.

  --dev-apt-command DEV_APT_COMMAND
          The basic command executed before dev apt deps are installed.

  --additional-dev-apt-command ADDITIONAL_DEV_APT_COMMAND
          Additional command executed before dev apt deps are installed.

  --additional-dev-apt-deps ADDITIONAL_DEV_APT_DEPS
          Additional apt dev dependencies to use when building the images.

  --dev-apt-deps DEV_APT_DEPS
          The basic apt dev dependencies to use when building the images.

  --additional-dev-apt-deps ADDITIONAL_DEV_DEPS
          Additional apt dev dependencies to use when building the images.

  --additional-dev-apt-envs ADDITIONAL_DEV_APT_ENVS
          Additional environment variables set when adding dev dependencies.

  --runtime-apt-command RUNTIME_APT_COMMAND
          The basic command executed before runtime apt deps are installed.

  --additional-runtime-apt-command ADDITIONAL_RUNTIME_APT_COMMAND
          Additional command executed before runtime apt deps are installed.

  --runtime-apt-deps ADDITIONAL_RUNTIME_APT_DEPS
          The basic apt runtime dependencies to use when building the images.

  --additional-runtime-apt-deps ADDITIONAL_RUNTIME_DEPS
          Additional apt runtime dependencies to use when building the images.

  --additional-runtime-apt-envs ADDITIONAL_RUNTIME_APT_DEPS
          Additional environment variables set when adding runtime dependencies.

  Build options:

  --disable-mysql-client-installation
          Disables installation of the mysql client which might be problematic if you are building
          image in controlled environment. Only valid for production image.

  --constraints-location
          Url to the constraints file. In case of the production image it can also be a path to the
          constraint file placed in 'docker-context-files' folder, in which case it has to be
          in the form of '/docker-context-files/<NAME_OF_THE_FILE>'

  --disable-pip-cache
          Disables GitHub PIP cache during the build. Useful if GitHub is not reachable during build.

  --install-from-local-files-when-building
          This flag is used during image building. If it is used additionally to installing
          Airflow from PyPI, the packages are installed from the .whl and .tar.gz packages placed
          in the 'docker-context-files' folder. The same flag can be used during entering the image in
          the CI image - in this case also the .whl and .tar.gz files will be installed automatically

  -C, --force-clean-images
          Force build images with cache disabled. This will remove the pulled or build images
          and start building images from scratch. This might take a long time.

  -r, --skip-rebuild-check
          Skips checking image for rebuilds. It will use whatever image is available locally/pulled.

  -L, --build-cache-local
          Uses local cache to build images. No pulled images will be used, but results of local
          builds in the Docker cache are used instead. This will take longer than when the pulled
          cache is used for the first time, but subsequent '--build-cache-local' builds will be
          faster as they will use mostly the locally build cache.

          This is default strategy used by the Production image builds.

  -U, --build-cache-pulled
          Uses images pulled from registry (either DockerHub or GitHub depending on
          --use-github-registry flag) to build images. The pulled images will be used as cache.
          Those builds are usually faster than when ''--build-cache-local'' with the exception if
          the registry images are not yet updated. The DockerHub images are updated nightly and the
          GitHub images are updated after merges to master so it might be that the images are still
          outdated vs. the latest version of the Dockerfiles you are using. In this case, the
          ''--build-cache-local'' might be faster, especially if you iterate and change the
          Dockerfiles yourself.

          This is default strategy used by the CI image builds.

  -X, --build-cache-disabled
          Disables cache during docker builds. This is useful if you want to make sure you want to
          rebuild everything from scratch.

          This strategy is used by default for both Production and CI images for the scheduled
          (nightly) builds in CI.

  ****************************************************************************************************
   Flags for pulling/pushing Docker images (both CI and production)

  -D, --dockerhub-user DOCKERHUB_USER
          DockerHub user used to pull, push and build images. Default: apache.

  -H, --dockerhub-repo DOCKERHUB_REPO
          DockerHub repository used to pull, push, build images. Default: airflow.

  -c, --use-github-registry
          If GitHub registry is enabled, pulls and pushes are done from the GitHub registry not
          DockerHub. You need to be logged in to the registry in order to be able to pull/push from
          and you need to be committer to push to Apache Airflow' GitHub registry.

  --github-registry GITHUB_REGISTRY
          GitHub registry used. GitHub has legacy Packages registry and Public Beta Container
          registry.

          Default: docker.pkg.github.com.

          If you use this flag, automatically --use-github-registry flag is enabled.

                 docker.pkg.github.com ghcr.io

  -g, --github-repository GITHUB_REPOSITORY
          GitHub repository used to pull, push images when cache is used.
          Default: apache/airflow.

          If you use this flag, automatically --use-github-registry flag is enabled.

  -s, --github-image-id COMMIT_SHA|RUN_ID
          <RUN_ID> or <COMMIT_SHA> of the image. Images in GitHub registry are stored with those
          to be able to easily find the image for particular CI runs. Once you know the
          <RUN_ID> or <COMMIT_SHA>, you can specify it in github-image-id flag and Breeze will
          automatically pull and use that image so that you can easily reproduce a problem
          that occurred in CI.

          If you use this flag, automatically --use-github-registry is enabled.


          Default: latest.

  ****************************************************************************************************
   Flags for running tests

  --test-type TEST_TYPE
          Type of the test to run. One of:

                 All,Core,Providers,API,CLI,Integration,Other,WWW,Postgres,MySQL,Helm,Quarantined

          Default: All

  ****************************************************************************************************
   Flags for generation of the provider packages

  -S, --version-suffix-for-pypi SUFFIX
          Adds optional suffix to the version in the generated provider package. It can be used
          to generate rc1/rc2 ... versions of the packages to be uploaded to PyPI.

  -N, --version-suffix-for-svn SUFFIX
          Adds optional suffix to the generated names of package. It can be used to generate
          rc1/rc2 ... versions of the packages to be uploaded to SVN.

  ****************************************************************************************************
   Increase verbosity of the scripts

  -v, --verbose
          Show verbose information about executed docker, kind, kubectl, helm commands. Useful for
          debugging - when you run breeze with --verbose flags you will be able to see the commands
          executed under the hood and copy&paste them to your terminal to debug them more easily.

          Note that you can further increase verbosity and see all the commands executed by breeze
          by running 'export VERBOSE_COMMANDS="true"' before running breeze.

  --dry-run-docker
          Only show docker commands to execute instead of actually executing them. The docker
          commands are printed in yellow color.

  ****************************************************************************************************
   Print detailed help message

  -h, --help
          Shows detailed help message for the command specified.

 .. END BREEZE HELP MARKER
