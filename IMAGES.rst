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

.. contents:: :local:

Airflow docker images
=====================

Airflow has two images (build from Dockerfiles):

  * Production image (Dockerfile) - that can be used to build your own production-ready Airflow installation
    You can read more about building and using the production image in the
    `Production Deployments <https://airflow.apache.org/docs/apache-airflow/stable/production-deployment.html>`_ document.
    The image is built using `Dockerfile <Dockerfile>`_

  * CI image (Dockerfile.ci) - used for running tests and local development. The image is built using
    `Dockerfile.ci <Dockerfile.ci>`_

Image naming conventions
========================

The images are named as follows:

``apache/airflow:<BRANCH_OR_TAG>-python<PYTHON_MAJOR_MINOR_VERSION>[-ci][-manifest]``

where:

* ``BRANCH_OR_TAG`` - branch or tag used when creating the image. Examples: ``master``,
  ``v2-0-test``, ``v1-10-test``, ``2.0.0``. The ``master``, ``v1-10-test`` ``v2-0-test`` labels are
  built from branches so they change over time. The ``1.10.*`` and ``2.*`` labels are built from git tags
  and they are "fixed" once built.
* ``PYTHON_MAJOR_MINOR_VERSION`` - version of Python used to build the image. Examples: ``3.6``, ``3.7``,
  ``3.8``
* The ``-ci`` suffix is added for CI images
* The ``-manifest`` is added for manifest images (see below for explanation of manifest images)

We also store (to increase speed of local build/pulls) Python images that were used to build
the CI images. Each CI image, when built uses current Python version of the base images. Those
python images are regularly updated (with bugfixes/security fixes), so for example Python 3.8 from
last week might be a different image than Python 3.8 today. Therefore whenever we push CI image
to airflow repository, we also push the Python image that was used to build it this image is stored
as ``apache/airflow:python<PYTHON_MAJOR_MINOR_VERSION>-<BRANCH_OR_TAG>``.

Since those are simply snapshots of the existing Python images, DockerHub does not create a separate
copy of those images - all layers are mounted from the original Python images and those are merely
labels pointing to those.

Building docker images
======================

The easiest way to build those images is to use `<BREEZE.rst>`_.

Note! Breeze by default builds production image from local sources. You can change it's behaviour by
providing ``--install-airflow-version`` parameter, where you can specify the
tag/branch used to download Airflow package from in GitHub repository. You can
also change the repository itself by adding ``--dockerhub-user`` and ``--dockerhub-repo`` flag values.

You can build the CI image using this command:

.. code-block:: bash

  ./breeze build-image

You can build production image using this command:

.. code-block:: bash

  ./breeze build-image --production-image

By adding ``--python <PYTHON_MAJOR_MINOR_VERSION>`` parameter you can build the
image version for the chosen Python version.

The images are build with default extras - different extras for CI and production image and you
can change the extras via the ``--extras`` parameters and add new ones with ``--additional-extras``.
You can see default extras used via ``./breeze flags``.

For example if you want to build Python 3.7 version of production image with
"all" extras installed you should run this command:

.. code-block:: bash

  ./breeze build-image --python 3.7 --extras "all" --production-image

If you just want to add new extras you can add them like that:

.. code-block:: bash

  ./breeze build-image --python 3.7 --additional-extras "all" --production-image

The command that builds the CI image is optimized to minimize the time needed to rebuild the image when
the source code of Airflow evolves. This means that if you already have the image locally downloaded and
built, the scripts will determine whether the rebuild is needed in the first place. Then the scripts will
make sure that minimal number of steps are executed to rebuild parts of the image (for example,
PIP dependencies) and will give you an image consistent with the one used during Continuous Integration.

The command that builds the production image is optimised for size of the image.

In Breeze by default, the airflow is installed using local sources of Apache Airflow.

You can also build production images from PIP packages via providing ``--install-airflow-version``
parameter to Breeze:

.. code-block:: bash

  ./breeze build-image --python 3.7 --additional-extras=presto \
      --production-image --install-airflow-version=2.0.0


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


This will build the image using command similar to:

.. code-block:: bash

    pip install \
      apache-airflow[async,amazon,celery,cncf.kubernetes,docker,dask,elasticsearch,ftp,grpc,hashicorp,http,ldap,google,microsoft.azure,mysql,postgres,redis,sendgrid,sftp,slack,ssh,statsd,virtualenv]==2.0.0 \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.0.0/constraints-3.6.txt"

You can also build production images from specific Git version via providing ``--install-airflow-reference``
parameter to Breeze (this time constraints are taken from the ``constraints-master`` branch which is the
HEAD of development for constraints):

.. code-block:: bash

    pip install "https://github.com/apache/airflow/archive/<tag>.tar.gz#egg=apache-airflow" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-master/constraints-3.6.txt"

You can also skip installing airflow and install it from locally provided files by using
``--install-from-local-files-when-building`` parameter and ``--disable-pypi-when-building`` to Breeze:

.. code-block:: bash

  ./breeze build-image --python 3.7 --additional-extras=presto \
      --production-image --disable-pypi-when-building --install-from-local-files-when-building

In this case you airflow and all packages (.whl files) should be placed in ``docker-context-files`` folder.


Using cache during builds
=========================

Default mechanism used in Breeze for building CI images uses images pulled from DockerHub or
GitHub Image Registry. This is done to speed up local builds and CI builds - instead of 15 minutes
for rebuild of CI images, it takes usually less than 3 minutes when cache is used. For CI builds this is
usually the best strategy - to use default "pull" cache. This is default strategy when
`<BREEZE.rst>`_ builds are performed.

For Production Image - which is far smaller and faster to build, it's better to use local build cache (the
standard mechanism that docker uses. This is the default strategy for production images when
`<BREEZE.rst>`_ builds are performed. The first time you run it, it will take considerably longer time than
if you use the pull mechanism, but then when you do small, incremental changes to local sources,
Dockerfile image= and scripts further rebuilds with local build cache will be considerably faster.

You can also disable build cache altogether. This is the strategy used by the scheduled builds in CI - they
will always rebuild all the images from scratch.

You can change the strategy by providing one of the ``--build-cache-local``, ``--build-cache-pulled`` or
even ``--build-cache-disabled`` flags when you run Breeze commands. For example:

.. code-block:: bash

  ./breeze build-image --python 3.7 --build-cache-local

Will build the CI image using local build cache (note that it will take quite a long time the first
time you run it).

.. code-block:: bash

  ./breeze build-image --python 3.7 --production-image --build-cache-pulled

Will build the production image with pulled images as cache.


.. code-block:: bash

  ./breeze build-image --python 3.7 --production-image --build-cache-disabled

Will build the production image from the scratch.

You can also turn local docker caching by setting ``DOCKER_CACHE`` variable to "local", "pulled",
"disabled" and exporting it.

.. code-block:: bash

  export DOCKER_CACHE="local"

or

.. code-block:: bash

  export DOCKER_CACHE="disabled"


Choosing image registry
=======================

By default images are pulled and pushed from and to DockerHub registry when you use Breeze's push-image
or build commands. But as described in `CI Documentation <CI.rst>`_, you can choose different image
registry by setting ``GITHUB_REGISTRY`` to ``docker.pkg.github.com`` for GitHub Package Registry or
``ghcr.io`` for GitHub Container Registry.

Default is the GitHub Package Registry one. The Pull Request forks have no access to the secret but they
auto-detect the registry used when they wait for the images.

Our images are named like that:

.. code-block:: bash

  apache/airflow:<BRANCH_OR_TAG>-pythonX.Y         - for production images
  apache/airflow:<BRANCH_OR_TAG>-pythonX.Y-ci      - for CI images
  apache/airflow:<BRANCH_OR_TAG>-pythonX.Y-build   - for production build stage
  apache/airflow:pythonX.Y-<BRANCH_OR_TAG>         - for Python base image used for both CI and PROD image

For example:

.. code-block:: bash

  apache/airflow:master-python3.6                - production "latest" image from current master
  apache/airflow:master-python3.6-ci             - CI "latest" image from current master
  apache/airflow:v2-0-test-python3.6-ci          - CI "latest" image from current v2-0-test branch
  apache/airflow:2.0.0-python3.6                 - production image for 2.0.0 release
  apache/airflow:python3.6-master                - base Python image for the master branch

You can see DockerHub images at `<https://hub.docker.com/r/apache/airflow>`_

Using GitHub registries as build cache
--------------------------------------

By default DockerHub registry is used when you push or pull such images.
However for CI builds we keep the images in GitHub registry as well - this way we can easily push
the images automatically after merge requests and use such images for Pull Requests
as cache - which makes it much it much faster for CI builds (images are available in cache
right after merged request in master finishes it's build), The difference is visible especially if
significant changes are done in the Dockerfile.CI.

The images are named differently (in Docker definition of image names - registry URL is part of the
image name if DockerHub is not used as registry). Also GitHub has its own structure for registries
each project has its own registry naming convention that should be followed. The name of
images for GitHub registry are different as they must follow limitation of the registry used.

We are still using GitHub Packages as registry, but we are in the process of testing and switching
to GitHub Container Registry, and the naming conventions are slightly different (GitHub Packages
required all packages to have "organization/repository/" URL prefix ("apache/airflow/",
where in GitHub Container Registry, all images are in "organization" not in "repository" and they are all
in organization wide "apache/" namespace rather than in "apache/airflow/" one).
We are adding "airflow-" as prefix for image names of all Airflow images instead.
The images are linked to the repository via ``org.opencontainers.image.source`` label in the image.

Naming convention for GitHub Packages
-------------------------------------

Images built as "Run ID snapshot":

.. code-block:: bash

  docker.pkg.github.com.io/apache-airflow/<BRANCH>-pythonX.Y-ci-v2:<RUNID>    - for CI images
  docker.pkg.github.com/apache-airflow/<BRANCH>-pythonX.Y-v2:<RUNID>       - for production images
  docker.pkg.github.com/apache-airflow/<BRANCH>-pythonX.Y-build-v2:<RUNID> - for production build stage
  docker.pkg.github.com/apache-airflow/pythonX.Y-<BRANCH>-v2:X.Y-slim-buster-<RUN_ID>  - for base Python images

Latest images (pushed when master merge succeeds):

.. code-block:: bash

  docker.pkg.github.com/apache/airflow/<BRANCH>-pythonX.Y-ci-v2:latest    - for CI images
  docker.pkg.github.com/apache/airflow/<BRANCH>-pythonX.Y-v2:latest       - for production images
  docker.pkg.github.com/apache/airflow/<BRANCH>-pythonX.Y-build-v2:latest - for production build stage
  docker.pkg.github.com/apache/airflow/python-<BRANCH>-v1:X.Y-slim-buster - for base Python images


Naming convention for GitHub Container Registry
-----------------------------------------------

Images built as "Run ID snapshot":

.. code-block:: bash

  ghcr.io/apache/airflow-<BRANCH>-pythonX.Y-ci-v2:<RUNID>                - for CI images
  ghcr.io/apache/airflow-<BRANCH>-pythonX.Y-v2:<RUNID>                   - for production images
  ghcr.io/apache/airflow-<BRANCH>-pythonX.Y-build-v2:<RUNID>             - for production build stage
  ghcr.io/apache/airflow-pythonX.Y-<BRANCH>-v2:X.Y-slim-buster-<RUN_ID>  - for base Python images

Latest images (pushed when master merge succeeds):

.. code-block:: bash

  ghcr.io/apache/airflow-<BRANCH>-pythonX.Y-ci-v2:latest    - for CI images
  ghcr.io/apache/airflow-<BRANCH>-pythonX.Y-v2:latest       - for production images
  ghcr.io/apache/airflow-<BRANCH>-pythonX.Y-build-v2:latest - for production build stage
  ghcr.io/apache/airflow-python-<BRANCH>-v2:X.Y-slim-buster - for base Python images

Note that we never push or pull "release" images to GitHub registry. It is only used for CI builds

You can see all the current GitHub images at `<https://github.com/apache/airflow/packages>`_


In order to interact with the GitHub images you need to add ``--use-github-registry`` flag to the pull/push
commands in Breeze. This way the images will be pulled/pushed from/to GitHub rather than from/to
DockerHub. Images are build locally as ``apache/airflow`` images but then they are tagged with the right
GitHub tags for you. You can also specify ``--github-registry`` option and choose which of the
GitHub registries are used (``docker.pkg.github.com`` chooses GitHub Packages and ``ghcr.io`` chooses
GitHub Container Registry).

You can read more about the CI configuration and how CI builds are using DockerHub/GitHub images
in `<CI.rst>`_.

Note that you need to be committer and have the right to push to DockerHub and GitHub and you need to
be logged in. Only committers can push images directly. You need to login with your
Personal Access Token with "packages" scope to be able to push to those repositories or pull from them
in case of GitHub Packages.

GitHub Packages:

.. code-block:: bash

  docker login docker.pkg.github.com

GitHub Container Registry

.. code-block:: bash

  docker login ghcr.io

Interacting with container registries
=====================================

Since there are different naming conventions used for Airflow images and there are multiple images used,
`Breeze <BREEZE.rst>`_ provides easy to use management interface for the images. The
`CI system of ours <CI.rst>`_ is designed in the way that it should automatically refresh caches, rebuild
the images periodically and update them whenever new version of base Python is released.
However, occasionally, you might need to rebuild images locally and push them directly to the registries
to refresh them.

This can be done with ``Breeze`` command line which has easy-to-use tool to manage those images. For
example:


Force building Python 3.6 CI image using local cache and pushing it container registry:

.. code-block:: bash

  ./breeze build-image --python 3.6 --force-build-images --build-cache-local
  ./breeze push-image --python 3.6 --github-registry ghcr.io


Building Python 3.7 PROD images (both build and final image) using cache pulled
from ``docker.pkg.github.com`` and pushing it back:

.. code-block:: bash

  ./breeze build-image --production-image --python 3.7 --github-registry docker.pkg.github.com
  ./breeze push-image --production-image --python 3.7 --github-registry docker.pkg.github.com


Building Python 3.8 CI image using cache pulled from DockerHub and pushing it back:

.. code-block:: bash

  ./breeze build-image --python 3.8
  ./breeze push-image --python 3.8

You can also pull and run images being result of a specific CI run in GitHub Actions. This is a powerful
tool that allows to reproduce CI failures locally, enter the images and fix them much faster. It is enough
to pass ``--github-image-id`` and the registry and Breeze will download and execute commands using
the same image that was used during the CI build.

For example this command will run the same Python 3.8 image as was used in 210056909
run with enabled Kerberos integration (assuming docker.pkg.github.com was used as build cache).

.. code-block:: bash

  ./breeze --github-image-id 210056909 \
    --github-registry docker.pkg.github.com \
    --python 3.8 --integration kerberos

You can see more details and examples in `Breeze <BREEZE.rst>`_


Technical details of Airflow images
===================================

The CI image is used by Breeze as shell image but it is also used during CI build.
The image is single segment image that contains Airflow installation with "all" dependencies installed.
It is optimised for rebuild speed. It installs PIP dependencies from the current branch first -
so that any changes in setup.py do not trigger reinstalling of all dependencies.
There is a second step of installation that re-installs the dependencies
from the latest sources so that we are sure that latest dependencies are installed.

The production image is a multi-segment image. The first segment "airflow-build-image" contains all the
build essentials and related dependencies that allow to install airflow locally. By default the image is
build from a released version of Airflow from GitHub, but by providing some extra arguments you can also
build it from local sources. This is particularly useful in CI environment where we are using the image
to run Kubernetes tests. See below for the list of arguments that should be provided to build
production image from the local sources.

The image is primarily optimised for size of the final image, but also for speed of rebuilds - the
'airflow-build-image' segment uses the same technique as the CI builds for pre-installing PIP dependencies.
It first pre-installs them from the right GitHub branch and only after that final airflow installation is
done from either local sources or remote location (PIP or GitHub repository).

Customizing the image
---------------------

Customizing the image is an alternative way of adding your own dependencies to the image.

The easiest way to build the image is to use ``breeze`` script, but you can also build such customized
image by running appropriately crafted docker build in which you specify all the ``build-args``
that you need to add to customize it. You can read about all the args and ways you can build the image
in the `<#ci-image-build-arguments>`_ chapter below.

Here just a few examples are presented which should give you general understanding of what you can customize.

This builds the production image in version 3.7 with additional airflow extras from 2.0.0 PyPI package and
additional apt dev and runtime dependencies.

.. code-block:: bash

  docker build . -f Dockerfile.ci \
    --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.7 \
    --build-arg AIRFLOW_INSTALLATION_METHOD="apache-airflow" \
    --build-arg AIRFLOW_VERSION="2.0.0" \
    --build-arg AIRFLOW_VERSION_SPECIFICATION="==2.0.0" \
    --build-arg AIRFLOW_SOURCES_FROM="empty" \
    --build-arg AIRFLOW_SOURCES_TO="/empty" \
    --build-arg ADDITIONAL_AIRFLOW_EXTRAS="jdbc"
    --build-arg ADDITIONAL_PYTHON_DEPS="pandas"
    --build-arg ADDITIONAL_DEV_APT_DEPS="gcc g++"
    --build-arg ADDITIONAL_RUNTIME_APT_DEPS="default-jre-headless"
    --tag my-image


the same image can be built using ``breeze`` (it supports auto-completion of the options):

.. code-block:: bash

  ./breeze build-image -f Dockerfile.ci \
      --production-image  --python 3.7 --install-airflow-version=2.0.0 \
      --additional-extras=jdbc --additional-python-deps="pandas" \
      --additional-dev-apt-deps="gcc g++" --additional-runtime-apt-deps="default-jre-headless"
You can build the default production image with standard ``docker build`` command but they will only build
default versions of the image and will not use the dockerhub versions of images as cache.


You can customize more aspects of the image - such as additional commands executed before apt dependencies
are installed, or adding extra sources to install your dependencies from. You can see all the arguments
described below but here is an example of rather complex command to customize the image
based on example in `this comment <https://github.com/apache/airflow/issues/8605#issuecomment-690065621>`_:

.. code-block:: bash

  docker build . -f Dockerfile.ci \
    --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.7 \
    --build-arg AIRFLOW_INSTALLATION_METHOD="apache-airflow" \
    --build-arg AIRFLOW_VERSION="2.0.0" \
    --build-arg AIRFLOW_VERSION_SPECIFICATION="==2.0.0" \
    --build-arg AIRFLOW_SOURCES_FROM="empty" \
    --build-arg AIRFLOW_SOURCES_TO="/empty" \
    --build-arg ADDITIONAL_AIRFLOW_EXTRAS="slack" \
    --build-arg ADDITIONAL_PYTHON_DEPS="apache-airflow-providers-odbc \
        azure-storage-blob \
        sshtunnel \
        google-api-python-client \
        oauth2client \
        beautifulsoup4 \
        dateparser \
        rocketchat_API \
        typeform" \
    --build-arg ADDITIONAL_DEV_APT_DEPS="msodbcsql17 unixodbc-dev g++" \
    --build-arg ADDITIONAL_DEV_APT_COMMAND="curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add --no-tty - && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list" \
    --build-arg ADDITIONAL_DEV_ENV_VARS="ACCEPT_EULA=Y" \
    --build-arg ADDITIONAL_RUNTIME_APT_COMMAND="curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add --no-tty - && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list" \
    --build-arg ADDITIONAL_RUNTIME_APT_DEPS="msodbcsql17 unixodbc git procps vim" \
    --build-arg ADDITIONAL_RUNTIME_ENV_VARS="ACCEPT_EULA=Y" \
    --tag my-image

CI image build arguments
........................

The following build arguments (``--build-arg`` in docker build command) can be used for CI images:

+------------------------------------------+------------------------------------------+------------------------------------------+
| Build argument                           | Default value                            | Description                              |
+==========================================+==========================================+==========================================+
| ``PYTHON_BASE_IMAGE``                    | ``python:3.6-slim-buster``               | Base Python image                        |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_VERSION``                      | ``2.0.0``                                | version of Airflow                       |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``PYTHON_MAJOR_MINOR_VERSION``           | ``3.6``                                  | major/minor version of Python (should    |
|                                          |                                          | match base image)                        |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``DEPENDENCIES_EPOCH_NUMBER``            | ``2``                                    | increasing this number will reinstall    |
|                                          |                                          | all apt dependencies                     |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``PIP_NO_CACHE_DIR``                     | ``true``                                 | if true, then no pip cache will be       |
|                                          |                                          | stored                                   |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``HOME``                                 | ``/root``                                | Home directory of the root user (CI      |
|                                          |                                          | image has root user as default)          |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_HOME``                         | ``/root/airflow``                        | Airflow’s HOME (that’s where logs and    |
|                                          |                                          | sqlite databases are stored)             |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_SOURCES``                      | ``/opt/airflow``                         | Mounted sources of Airflow               |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``CASS_DRIVER_NO_CYTHON``                | ``1``                                    | if set to 1 no CYTHON compilation is     |
|                                          |                                          | done for cassandra driver (much faster)  |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_REPO``                         | ``apache/airflow``                       | the repository from which PIP            |
|                                          |                                          | dependencies are pre-installed           |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_BRANCH``                       | ``master``                               | the branch from which PIP dependencies   |
|                                          |                                          | are pre-installed                        |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_CI_BUILD_EPOCH``               | ``1``                                    | increasing this value will reinstall PIP |
|                                          |                                          | dependencies from the repository from    |
|                                          |                                          | scratch                                  |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_CONSTRAINTS_LOCATION``         |                                          | If not empty, it will override the       |
|                                          |                                          | source of the constraints with the       |
|                                          |                                          | specified URL or file. Note that the     |
|                                          |                                          | file has to be in docker context so      |
|                                          |                                          | it's best to place such file in          |
|                                          |                                          | one of the folders included in           |
|                                          |                                          | .dockerignore. for example in the        |
|                                          |                                          | 'docker-context-files'. Note that the    |
|                                          |                                          | location does not work for the first     |
|                                          |                                          | stage of installation when the           |
|                                          |                                          | stage of installation when the           |
|                                          |                                          | ``AIRFLOW_PRE_CACHED_PIP_PACKAGES`` is   |
|                                          |                                          | set to true. Default location from       |
|                                          |                                          | GitHub is used in this case.             |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_CONSTRAINTS_REFERENCE``        |                                          | reference (branch or tag) from GitHub    |
|                                          |                                          | repository from which constraints are    |
|                                          |                                          | used. By default it is set to            |
|                                          |                                          | ``constraints-master`` but can be        |
|                                          |                                          | ``constraints-2-0`` for 2.0.* versions   |
|                                          |                                          | ``constraints-1-10`` for 1.10.* versions |
|                                          |                                          | or it could point to specific version    |
|                                          |                                          | for example ``constraints-2.0.0``        |
|                                          |                                          | is empty, it is auto-detected            |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``INSTALL_PROVIDERS_FROM_SOURCES``       | ``true``                                 | If set to false and image is built from  |
|                                          |                                          | sources, all provider packages are not   |
|                                          |                                          | installed. By default when building from |
|                                          |                                          | sources, all provider packages are also  |
|                                          |                                          | installed together with the core airflow |
|                                          |                                          | package. It has no effect when           |
|                                          |                                          | installing from PyPI or GitHub repo.     |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``INSTALL_FROM_DOCKER_CONTEXT_FILES``    | ``false``                                | If set to true, Airflow, providers and   |
|                                          |                                          | all dependencies are installed from      |
|                                          |                                          | from locally built/downloaded            |
|                                          |                                          | .whl and .tar.gz files placed in the     |
|                                          |                                          | ``docker-context-files``. In certain     |
|                                          |                                          | corporate environments, this is required |
|                                          |                                          | to install airflow from such pre-vetted  |
|                                          |                                          | packages rather than from PyPI. For this |
|                                          |                                          | to work, also set ``INSTALL_FROM_PYPI``. |
|                                          |                                          | Note that packages starting with         |
|                                          |                                          | ``apache?airflow`` glob are treated      |
|                                          |                                          | differently than other packages. All     |
|                                          |                                          | ``apache?airflow`` packages are          |
|                                          |                                          | installed with dependencies limited by   |
|                                          |                                          | airflow constraints. All other packages  |
|                                          |                                          | are installed without dependencies       |
|                                          |                                          | 'as-is'. If you wish to install airflow  |
|                                          |                                          | via 'pip download' with all dependencies |
|                                          |                                          | downloaded, you have to rename the       |
|                                          |                                          | apache airflow and provider packages to  |
|                                          |                                          | not start with ``apache?airflow`` glob.  |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_EXTRAS``                       | ``all``                                  | extras to install                        |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``UPGRADE_TO_NEWER_DEPENDENCIES``        | ``false``                                | If set to true, the dependencies are     |
|                                          |                                          | upgraded to newer versions matching      |
|                                          |                                          | setup.py before installation.            |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``CONTINUE_ON_PIP_CHECK_FAILURE``        | ``false``                                | By default the image will fail if pip    |
|                                          |                                          | check fails for it. This is good for     |
|                                          |                                          | interactive building but on CI the       |
|                                          |                                          | image should be built regardless - we    |
|                                          |                                          | have a separate step to verify image.    |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``INSTALL_FROM_PYPI``                    | ``true``                                 | If set to true, Airflow is installed     |
|                                          |                                          | from PyPI. If you want to install        |
|                                          |                                          | Airflow from externally provided binary  |
|                                          |                                          | package you can set it to false, place   |
|                                          |                                          | the package in ``docker-context-files``  |
|                                          |                                          | and set                                  |
|                                          |                                          | ``INSTALL_FROM_DOCKER_CONTEXT_FILES`` to |
|                                          |                                          | true. For this you have to also set the  |
|                                          |                                          | ``AIRFLOW_PRE_CACHED_PIP_PACKAGES`` flag |
|                                          |                                          | to false                                 |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_PRE_CACHED_PIP_PACKAGES``      | ``true``                                 | Allows to pre-cache airflow PIP packages |
|                                          |                                          | from the GitHub of Apache Airflow        |
|                                          |                                          | This allows to optimize iterations for   |
|                                          |                                          | Image builds and speeds up CI builds     |
|                                          |                                          | But in some corporate environments it    |
|                                          |                                          | might be forbidden to download anything  |
|                                          |                                          | from public repositories.                |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_AIRFLOW_EXTRAS``            |                                          | additional extras to install             |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_PYTHON_DEPS``               |                                          | additional Python dependencies to        |
|                                          |                                          | install                                  |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``DEV_APT_COMMAND``                      | (see Dockerfile)                         | Dev apt command executed before dev deps |
|                                          |                                          | are installed in the first part of image |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_DEV_APT_COMMAND``           |                                          | Additional Dev apt command executed      |
|                                          |                                          | before dev dep are installed             |
|                                          |                                          | in the first part of the image           |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``DEV_APT_DEPS``                         | (see Dockerfile)                         | Dev APT dependencies installed           |
|                                          |                                          | in the first part of the image           |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_DEV_APT_DEPS``              |                                          | Additional apt dev dependencies          |
|                                          |                                          | installed in the first part of the image |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_DEV_APT_ENV``               |                                          | Additional env variables defined         |
|                                          |                                          | when installing dev deps                 |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``RUNTIME_APT_COMMAND``                  | (see Dockerfile)                         | Runtime apt command executed before deps |
|                                          |                                          | are installed in first part of the image |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_RUNTIME_APT_COMMAND``       |                                          | Additional Runtime apt command executed  |
|                                          |                                          | before runtime dep are installed         |
|                                          |                                          | in the second part of the image          |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``RUNTIME_APT_DEPS``                     | (see Dockerfile)                         | Runtime APT dependencies installed       |
|                                          |                                          | in the second part of the image          |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_RUNTIME_APT_DEPS``          |                                          | Additional apt runtime dependencies      |
|                                          |                                          | installed in second part of the image    |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_RUNTIME_APT_ENV``           |                                          | Additional env variables defined         |
|                                          |                                          | when installing runtime deps             |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_PIP_VERSION``                  | ``20.2.4``                               | PIP version used.                        |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``PIP_PROGRESS_BAR``                     | ``on``                                   | Progress bar for PIP installation        |
+------------------------------------------+------------------------------------------+------------------------------------------+

Here are some examples of how CI images can built manually. CI is always built from local sources.

This builds the CI image in version 3.7 with default extras ("all").

.. code-block:: bash

  docker build . -f Dockerfile.ci --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.7


This builds the CI image in version 3.6 with "gcp" extra only.

.. code-block:: bash

  docker build . -f Dockerfile.ci --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.6 --build-arg AIRFLOW_EXTRAS=gcp


This builds the CI image in version 3.6 with "apache-beam" extra added.

.. code-block:: bash

  docker build . -f Dockerfile.ci --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.6 --build-arg ADDITIONAL_AIRFLOW_EXTRAS="apache-beam"

This builds the CI image in version 3.6 with "mssql" additional package added.

.. code-block:: bash

  docker build . -f Dockerfile.ci --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.6 --build-arg ADDITIONAL_PYTHON_DEPS="mssql"

This builds the CI image in version 3.6 with "gcc" and "g++" additional apt dev dependencies added.

.. code-block::

  docker build . -f Dockerfile.ci --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.6 --build-arg ADDITIONAL_DEV_APT_DEPS="gcc g++"

This builds the CI image in version 3.6 with "jdbc" extra and "default-jre-headless" additional apt runtime dependencies added.

.. code-block::

  docker build . -f Dockerfile.ci --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.6 --build-arg AIRFLOW_EXTRAS=jdbc --build-arg ADDITIONAL_RUNTIME_DEPS="default-jre-headless"

Production images
-----------------

You can find details about using, building, extending and customising the production images in the
`Latest documentation <https://airflow.apache.org/docs/apache-airflow/stable/production-deployment.html>`_


Image manifests
---------------

Together with the main CI images we also build and push image manifests. Those manifests are very small images
that contain only content of randomly generated file at the 'crucial' part of the CI image building.
This is in order to be able to determine very quickly if the image in the docker registry has changed a
lot since the last time. Unfortunately docker registry (specifically DockerHub registry) has no anonymous
way of querying image details via API. You really need to download the image to inspect it.
We workaround it in the way that always when we build the image we build a very small image manifest
containing randomly generated UUID and push it to registry together with the main CI image.
The tag for the manifest image reflects the image it refers to with added ``-manifest`` suffix.
The manifest image for ``apache/airflow:master-python3.6-ci`` is named
``apache/airflow:master-python3.6-ci-manifest``.

The image is quickly pulled (it is really, really small) when important files change and the content
of the randomly generated UUID is compared with the one in our image. If the contents are different
this means that the user should rebase to latest master and rebuild the image with pulling the image from
the repo as this will likely be faster than rebuilding the image locally.

The random UUID is generated right after pre-cached pip install is run - and usually it means that
significant changes have been made to apt packages or even the base Python image has changed.

Pulling the Latest Images
-------------------------

Sometimes the image needs to be refreshed from the registry in DockerHub - because you have an outdated
version. You can do it via the ``--force-pull-images`` flag to force pulling the latest images from the
DockerHub.

For production image:

.. code-block:: bash

  ./breeze build-image --force-pull-images --production-image

For CI image Breeze automatically uses force pulling in case it determines that your image is very outdated,
however uou can also force it with the same flag.

.. code-block:: bash

  ./breeze build-image --force-pull-images

Refreshing Base Python images
=============================

Python base images are updated from time-to-time, usually as a result of implementing security fixes.
When you build your image locally using ``docker build`` you use the version of image that you have locally.
For the CI builds using ``breeze`` we use the image that is stored in our repository in order to use cache
efficiently. However we can refresh the image to latest available by specifying
``--force-pull-base-python-image`` and running it manually (you need to have access to DockerHub and our
GitHub Registies in order to be able to do that.

.. code-block:: bash

    #/bin/bash
    export DOCKERHUB_USER="apache"
    export GITHUB_REPOSITORY="apache/airflow"
    export FORCE_ANSWER_TO_QUESTIONS="true"
    export CI="true"

    for python_version in "3.6" "3.7" "3.8"
    do
            ./breeze build-image --python ${python_version} --build-cache-local \
                    --force-pull-base-python-image --verbose
            ./breeze build-image --python ${python_version} --build-cache-local \
                    --production-image --verbose
            ./breeze push-image
            ./breeze push-image --github-registry ghcr.io
            ./breeze push-image --github-registry docker.pkg.github.com
            ./breeze push-image --production-image
            ./breeze push-image --github-registry ghcr.io --production-image
            ./breeze push-image --github-registry docker.pkg.github.com --production-image
    done




Embedded image scripts
======================

Both images have a set of scripts that can be used in the image. Those are:
 * /entrypoint - entrypoint script used when entering the image
 * /clean-logs - script for periodic log cleaning

Running the CI image
====================

The entrypoint in the CI image contains all the initialisation needed for tests to be immediately executed.
It is copied from ``scripts/in_container/entrypoint_ci.sh``.

The default behaviour is that you are dropped into bash shell. However if RUN_TESTS variable is
set to "true", then tests passed as arguments are executed

The entrypoint performs those operations:

* checks if the environment is ready to test (including database and all integrations). It waits
  until all the components are ready to work

* installs older version of Airflow (if older version of Airflow is requested to be installed
  via ``INSTALL_AIRFLOW_VERSION`` variable.

* Sets up Kerberos if Kerberos integration is enabled (generates and configures Kerberos token)

* Sets up ssh keys for ssh tests and restarts the SSH server

* Sets all variables and configurations needed for unit tests to run

* Reads additional variables set in ``files/airflow-breeze-config/variables.env`` by sourcing that file

* In case of CI run sets parallelism to 2 to avoid excessive number of processes to run

* In case of CI run sets default parameters for pytest

* In case of running integration/long_running/quarantined tests - it sets the right pytest flags

* Sets default "tests" target in case the target is not explicitly set as additional argument

* Runs system tests if RUN_SYSTEM_TESTS flag is specified, otherwise runs regular unit and integration tests


Using, customising, and extending the production image
======================================================

You can read more about using, customising, and extending the production image in the
`documentation <https://airflow.apache.org/docs/apache-airflow/stable/production-deployment.html>`_.
