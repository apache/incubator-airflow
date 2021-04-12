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

Building the image
==================

Before you dive-deeply in the way how the Airflow Image is build, named and why we are doing it the
way we do, you might want to know very quickly how you can extend or customize the existing image
for Apache Airflow. This chapter gives you a short answer to those questions.

Extending vs. customizing the image
-----------------------------------

Here is the comparison of the two types of building images. Here is your guide if you want to choose
how you want to build your image.

+----------------------------------------------------+-----------+-------------+
|                                                    | Extending | Customizing |
+====================================================+===========+=============+
| Can be built without airflow sources               | Yes       | No          |
+----------------------------------------------------+-----------+-------------+
| Uses familiar 'FROM ' pattern of image building    | Yes       | No          |
+----------------------------------------------------+-----------+-------------+
| Requires only basic knowledge about images         | Yes       | No          |
+----------------------------------------------------+-----------+-------------+
| Builds quickly                                     | Yes       | No          |
+----------------------------------------------------+-----------+-------------+
| Produces image heavily optimized for size          | No        | Yes         |
+----------------------------------------------------+-----------+-------------+
| Can build from custom airflow sources (forks)      | No        | Yes         |
+----------------------------------------------------+-----------+-------------+
| Can build on air-gaped system                      | No        | Yes         |
+----------------------------------------------------+-----------+-------------+

TL;DR; If you have a need to build custom image, it is easier to start with "Extending" however if your
dependencies require compilation step or when your require to build the image from security vetted
packages, switching to "Customizing" the image provides much more optimized images. In the example further
where we compare equivalent "Extending" and "Customizing" the image, similar images build by
Extending vs. Customization had shown 1.1GB vs 874MB image sizes respectively - with 20% improvement in
size of the Customized image.

.. note::

  You can also combine both - customizing & extending the image in one. You can build your
  optimized base image first using ``customization`` method (for example by your admin team) with all
  the heavy compilation required dependencies and you can publish it in your registry and let others
  ``extend`` your image using ``FROM`` and add their own lightweight dependencies. This reflects well
  the split where typically "Casual" users will Extend the image and "Power-users" will customize it.

Airflow Summit 2020's `Production Docker Image <https://youtu.be/wDr3Y7q2XoI>`_ talk provides more
details about the context, architecture and customization/extension methods for the Production Image.

Extending the image
-------------------

Extending the image is easiest if you just need to add some dependencies that do not require
compiling. The compilation framework of Linux (so called ``build-essential``) is pretty big, and
for the production images, size is really important factor to optimize for, so our Production Image
does not contain ``build-essential``. If you need compiler like gcc or g++ or make/cmake etc. - those
are not found in the image and it is recommended that you follow the "customize" route instead.

How to extend the image - it is something you are most likely familiar with - simply
build a new image using Dockerfile's ``FROM`` directive and add whatever you need. Then you can add your
Debian dependencies with ``apt`` or PyPI dependencies with ``pip install`` or any other stuff you need.

You should be aware, about a few things:

* The production image of airflow uses "airflow" user, so if you want to add some of the tools
  as ``root`` user, you need to switch to it with ``USER`` directive of the Dockerfile and switch back to
  ``airflow`` user when you are done. Also you should remember about following the
  `best practises of Dockerfiles <https://docs.docker.com/develop/develop-images/dockerfile_best-practices/>`_
  to make sure your image is lean and small.

* The PyPI dependencies in Apache Airflow are installed in the user library, of the "airflow" user, so
  PIP packages are installed to ``~/.local`` folder as if the ``--user`` flag was specified when running PIP.
  Note also that using ``--no-cache-dir`` is a good idea that can help to make your image smaller.

.. note::
  Only as of ``2.0.1`` image the ``--user`` flag is turned on by default by setting ``PIP_USER`` environment
  variable to ``true``. This can be disabled by un-setting the variable or by setting it to ``false``. In the
  2.0.0 image you had to add the ``--user`` flag as ``pip install --user`` command.

* If your apt, or PyPI dependencies require some of the ``build-essential`` or other packages that need
  to compile your python dependencies, then your best choice is to follow the "Customize the image" route,
  because you can build a highly-optimized (for size) image this way. However it requires to checkout sources
  of Apache Airflow, so you might still want to choose to add ``build-essential`` to your image,
  even if your image will be significantly bigger.

* You can also embed your dags in the image by simply adding them with COPY directive of Airflow.
  The DAGs in production image are in ``/opt/airflow/dags`` folder.

* You can build your image without any need for Airflow sources. It is enough that you place the
  ``Dockerfile`` and any files that are referred to (such as Dag files) in a separate directory and run
  a command ``docker build . --tag my-image:my-tag`` (where ``my-image`` is the name you want to name it
  and ``my-tag`` is the tag you want to tag the image with.

* If your way of extending image requires to create writable directories, you MUST remember about adding
  ``umask 0002`` step in your RUN command. This is necessary in order to accommodate our approach for
  running the image with an arbitrary user. Such user will always run with ``GID=0`` -
  the entrypoint will prevent non-root GIDs. You can read more about it in
  :ref:`arbitrary docker user <arbitrary-docker-user>` documentation for the entrypoint. The
  ``umask 0002`` is set as default when you enter the image, so any directories you create by default
  in runtime, will have ``GID=0`` and will be group-writable.

.. note::
  Only as of ``2.0.2`` the default group of ``airflow`` user is ``root``. Previously it was ``airflow``,
  so if you are building your images based on an earlier image, you need to manually change the default
  group for airflow user:

.. code-block:: docker

    RUN usermod -g 0 airflow

Examples of image extending
---------------------------

An ``apt`` package example
..........................

The following example adds ``vim`` to the airflow image.

.. exampleinclude:: docker-examples/extending/add-apt-packages/Dockerfile
    :language: Dockerfile
    :start-after: [START Dockerfile]
    :end-before: [END Dockerfile]

A ``PyPI`` package example
..........................

The following example adds ``lxml`` python package from PyPI to the image.

.. exampleinclude:: docker-examples/extending/add-pypi-packages/Dockerfile
    :language: Dockerfile
    :start-after: [START Dockerfile]
    :end-before: [END Dockerfile]

A ``umask`` requiring example
.............................

The following example adds a new directory that is supposed to be writable for any arbitrary user
running the container.

.. exampleinclude:: docker-examples/extending/writable-directory/Dockerfile
    :language: Dockerfile
    :start-after: [START Dockerfile]
    :end-before: [END Dockerfile]


A ``build-essential`` requiring package example
...............................................

The following example adds ``mpi4py`` package which requires both ``build-essential`` and ``mpi compiler``.

.. exampleinclude:: docker-examples/extending/add-build-essential-extend/Dockerfile
    :language: Dockerfile
    :start-after: [START Dockerfile]
    :end-before: [END Dockerfile]

The size of this image is ~ 1.1 GB when build. As you will see further, you can achieve 20% reduction in
size of the image in case you use "Customizing" rather than "Extending" the image.

DAG embedding example
.....................

The following example adds ``test_dag.py`` to your image in the ``/opt/airflow/dags`` folder.

.. exampleinclude:: docker-examples/extending/embedding-dags/Dockerfile
    :language: Dockerfile
    :start-after: [START Dockerfile]
    :end-before: [END Dockerfile]


.. exampleinclude:: docker-examples/extending/embedding-dags/test_dag.py
    :language: Python
    :start-after: [START dag]
    :end-before: [END dag]

Customizing the image
---------------------

Customizing the image is an optimized way of adding your own dependencies to the image - better
suited to prepare highly optimized (for size) production images, especially when you have dependencies
that require to be compiled before installing (such as ``mpi4py``).

It also allows more sophisticated usages, needed by "Power-users" - for example using forked version
of Airflow, or building the images from security-vetted sources.

The big advantage of this method is that it produces optimized image even if you need some compile-time
dependencies that are not needed in the final image.

The disadvantage is that you need to use Airflow Sources to build such images from the
`official distribution repository of Apache Airflow <https://downloads.apache.org/airflow/>`_ for the
released versions, or from the checked out sources (using release tags or main branches) in the
`Airflow GitHub Project <https://github.com/apache/airflow>`_ or from your own fork
if you happen to do maintain your own fork of Airflow.

Another disadvantage is that the pattern of building Docker images with ``--build-arg`` is less familiar
to developers of such images. However it is quite well-known to "power-users". That's why the
customizing flow is better suited for those users who have more familiarity and have more custom
requirements.

The image also usually builds much longer than the equivalent "Extended" image because instead of
extending the layers that are already coming from the base image, it rebuilds the layers needed
to add extra dependencies needed at early stages of image building.

When customizing the image you can choose a number of options how you install Airflow:

   * From the PyPI releases (default)
   * From the custom installation sources - using additional/replacing the original apt or PyPI repositories
   * From local sources. This is used mostly during development.
   * From tag or branch, or specific commit from a GitHub Airflow repository (or fork). This is particularly
     useful when you build image for a custom version of Airflow that you keep in your fork and you do not
     want to release the custom Airflow version to PyPI.
   * From locally stored binary packages for Airflow, Airflow Providers and other dependencies. This is
     particularly useful if you want to build Airflow in a highly-secure environment where all such packages
     must be vetted by your security team and stored in your private artifact registry. This also
     allows to build airflow image in an air-gaped environment.
   * Side note. Building ``Airflow`` in an ``air-gaped`` environment sounds pretty funny, doesn't it?

You can also add a range of customizations while building the image:

   * base python image you use for Airflow
   * version of Airflow to install
   * extras to install for Airflow (or even removing some default extras)
   * additional apt/python dependencies to use while building Airflow (DEV dependencies)
   * additional apt/python dependencies to install for runtime version of Airflow (RUNTIME dependencies)
   * additional commands and variables to set if needed during building or preparing Airflow runtime
   * choosing constraint file to use when installing Airflow

Additional explanation is needed for the last point. Airflow uses constraints to make sure
that it can be predictably installed, even if some new versions of Airflow dependencies are
released (or even dependencies of our dependencies!). The docker image and accompanying scripts
usually determine automatically the right versions of constraints to be used based on the Airflow
version installed and Python version. For example 2.0.1 version of Airflow installed from PyPI
uses constraints from ``constraints-2.0.1`` tag). However in some cases - when installing airflow from
GitHub for example - you have to manually specify the version of constraints used, otherwise
it will default to the latest version of the constraints which might not be compatible with the
version of Airflow you use.

You can also download any version of Airflow constraints and adapt it with your own set of
constraints and manually set your own versions of dependencies in your own constraints and use the version
of constraints that you manually prepared.

You can read more about constraints in the documentation of the
`Installation <http://airflow.apache.org/docs/apache-airflow/stable/installation.html#constraints-files>`_

Examples of image customizing
-----------------------------

.. _image-build-pypi:


Building from PyPI packages
...........................

This is the basic way of building the custom images from sources.

The following example builds the production image in version ``3.6`` with latest PyPI-released Airflow,
with default set of Airflow extras and dependencies. The ``2.0.1`` constraints are used automatically.

.. exampleinclude:: docker-examples/customizing/stable-airflow.sh
    :language: bash
    :start-after: [START build]
    :end-before: [END build]

The following example builds the production image in version ``3.7`` with default extras from ``2.0.1`` PyPI
package. The ``2.0.1`` constraints are used automatically.

.. exampleinclude:: docker-examples/customizing/pypi-selected-version.sh
    :language: bash
    :start-after: [START build]
    :end-before: [END build]

The following example builds the production image in version ``3.8`` with additional airflow extras
(``mssql,hdfs``) from ``2.0.1`` PyPI package, and additional dependency (``oauth2client``).

.. exampleinclude:: docker-examples/customizing/pypi-extras-and-deps.sh
    :language: bash
    :start-after: [START build]
    :end-before: [END build]


The following example adds ``mpi4py`` package which requires both ``build-essential`` and ``mpi compiler``.

.. exampleinclude:: docker-examples/customizing/add-build-essential-custom.sh
    :language: bash
    :start-after: [START build]
    :end-before: [END build]

The above image is equivalent of the "extended" image from previous chapter but it's size is only
874 MB. Comparing to 1.1 GB of the "extended image" this is about 230 MB less, so you can achieve ~20%
improvement in size of the image by using "customization" vs. extension. The saving can increase in case you
have more complex dependencies to build.


.. _image-build-optimized:

Building optimized images
.........................

The following example the production image in version ``3.6`` with additional airflow extras from ``2.0.1``
PyPI package but it includes additional apt dev and runtime dependencies.

The dev dependencies are those that require ``build-essential`` and usually need to involve recompiling
of some python dependencies so those packages might require some additional DEV dependencies to be
present during recompilation. Those packages are not needed at runtime, so we only install them for the
"build" time. They are not installed in the final image, thus producing much smaller images.
In this case pandas requires recompilation so it also needs gcc and g++ as dev APT dependencies.
The ``jre-headless`` does not require recompiling so it can be installed as the runtime APT dependency.

.. exampleinclude:: docker-examples/customizing/pypi-dev-runtime-deps.sh
    :language: bash
    :start-after: [START build]
    :end-before: [END build]

.. _image-build-github:


Building from GitHub
....................

This method is usually used for development purpose. But in case you have your own fork you can point
it to your forked version of source code without having to release it to PyPI. It is enough to have
a branch or tag in your repository and use the tag or branch in the URL that you point the installation to.

In case of GitHyb builds you need to pass the constraints reference manually in case you want to use
specific constraints, otherwise the default ``constraints-master`` is used.

The following example builds the production image in version ``3.7`` with default extras from the latest master version and
constraints are taken from latest version of the constraints-master branch in GitHub.

.. exampleinclude:: docker-examples/customizing/github-master.sh
    :language: bash
    :start-after: [START build]
    :end-before: [END build]

The following example builds the production image with default extras from the
latest ``v2-0-test`` version and constraints are taken from the latest version of
the ``constraints-2-0`` branch in GitHub. Note that this command might fail occasionally as only
the "released version" constraints when building a version and "master" constraints when building
master are guaranteed to work.

.. exampleinclude:: docker-examples/customizing/github-v2-0-test.sh
    :language: bash
    :start-after: [START build]
    :end-before: [END build]

You can also specify another repository to build from. If you also want to use different constraints
repository source, you must specify it as additional ``CONSTRAINTS_GITHUB_REPOSITORY`` build arg.

The following example builds the production image using ``potiuk/airflow`` fork of Airflow and constraints
are also downloaded from that repository.

.. exampleinclude:: docker-examples/customizing/github-different-repository.sh
    :language: bash
    :start-after: [START build]
    :end-before: [END build]

.. _image-build-custom:

Using custom installation sources
.................................

You can customize more aspects of the image - such as additional commands executed before apt dependencies
are installed, or adding extra sources to install your dependencies from. You can see all the arguments
described below but here is an example of rather complex command to customize the image
based on example in `this comment <https://github.com/apache/airflow/issues/8605#issuecomment-690065621>`_:

In case you need to use your custom PyPI package indexes, you can also customize PYPI sources used during
image build by adding a ``docker-context-files``/``.pypirc`` file when building the image.
This ``.pypirc`` will not be committed to the repository (it is added to ``.gitignore``) and it will not be
present in the final production image. It is added and used only in the build segment of the image.
Therefore this ``.pypirc`` file can safely contain list of package indexes you want to use,
usernames and passwords used for authentication. More details about ``.pypirc`` file can be found in the
`pypirc specification <https://packaging.python.org/specifications/pypirc/>`_.

Such customizations are independent of the way how airflow is installed.

.. note::
  Similar results could be achieved by modifying the Dockerfile manually (see below) and injecting the
  commands needed, but by specifying the customizations via build-args, you avoid the need of
  synchronizing the changes from future Airflow Dockerfiles. Those customizations should work with the
  future version of Airflow's official ``Dockerfile`` at most with minimal modifications od parameter
  names (if any), so using the build command for your customizations makes your custom image more
  future-proof.

The following - rather complex - example shows capabilities of:

  * Adding airflow extras (slack, odbc)
  * Adding PyPI dependencies (``azure-storage-blob, oauth2client, beautifulsoup4, dateparser, rocketchat_API,typeform``)
  * Adding custom environment variables while installing ``apt`` dependencies - both DEV and RUNTIME
    (``ACCEPT_EULA=Y'``)
  * Adding custom curl command for adding keys and configuring additional apt sources needed to install
    ``apt`` dependencies (both DEV and RUNTIME)
  * Adding custom ``apt`` dependencies, both DEV (``msodbcsql17 unixodbc-dev g++) and runtime msodbcsql17 unixodbc git procps vim``)

.. exampleinclude:: docker-examples/customizing/custom-sources.sh
    :language: bash
    :start-after: [START build]
    :end-before: [END build]

.. _image-build-secure-environments:

Build images in security restricted environments
................................................

You can also make sure your image is only build using local constraint file and locally downloaded
wheel files. This is often useful in Enterprise environments where the binary files are verified and
vetted by the security teams. It is also the most complex way of building the image. You should be an
expert of building and using Dockerfiles in order to use it and have to have specific needs of security if
you want to follow that route.

This builds below builds the production image  with packages and constraints used from the local
``docker-context-files`` rather than installed from PyPI or GitHub. It also disables MySQL client
installation as it is using external installation method.

Note that as a prerequisite - you need to have downloaded wheel files. In the example below we
first download such constraint file locally and then use ``pip download`` to get the ``.whl`` files needed
but in most likely scenario, those wheel files should be copied from an internal repository of such .whl
files. Note that ``AIRFLOW_VERSION_SPECIFICATION`` is only there for reference, the apache airflow ``.whl`` file
in the right version is part of the ``.whl`` files downloaded.

Note that 'pip download' will only works on Linux host as some of the packages need to be compiled from
sources and you cannot install them providing ``--platform`` switch. They also need to be downloaded using
the same python version as the target image.

The ``pip download`` might happen in a separate environment. The files can be committed to a separate
binary repository and vetted/verified by the security team and used subsequently to build images
of Airflow when needed on an air-gaped system.

Example of preparing the constraint files and wheel files. Note that ``mysql`` dependency is removed
as ``mysqlclient`` is installed from Oracle's ``apt`` repository and if you want to add it, you need
to provide this library from you repository if you want to build Airflow image in an "air-gaped" system.

.. exampleinclude:: docker-examples/restricted/restricted_environments.sh
    :language: bash
    :start-after: [START download]
    :end-before: [END download]

After this step is finished, your ``docker-context-files`` folder will contain all the packages that
are needed to install Airflow from.

Those downloaded packages and constraint file can be pre-vetted by your security team before you attempt
to install the image. You can also store those downloaded binary packages in your private artifact registry
which allows for the flow where you will download the packages on one machine, submit only new packages for
security vetting and only use the new packages when they were vetted.

On a separate (air-gaped) system, all the PyPI packages can be copied to ``docker-context-files``
where you can build the image using the packages downloaded by passing those build args:

  * ``INSTALL_FROM_DOCKER_CONTEXT_FILES="true"``  - to use packages present in ``docker-context-files``
  * ``AIRFLOW_PRE_CACHED_PIP_PACKAGES="false"``  - to not pre-cache packages from PyPI when building image
  * ``AIRFLOW_CONSTRAINTS_LOCATION=/docker-context-files/YOUR_CONSTRAINT_FILE.txt`` - to downloaded constraint files
  * (Optional) ``INSTALL_MYSQL_CLIENT="false"`` if you do not want to install ``MySQL``
    client from the Oracle repositories. In this case also make sure that your

Note, that the solution we have for installing python packages from local packages, only solves the problem
of "air-gaped" python installation. The Docker image also downloads ``apt`` dependencies and ``node-modules``.
Those type of dependencies are however more likely to be available in your "air-gaped" system via transparent
proxies and it should automatically reach out to your private registries, however in the future the
solution might be applied to both of those installation steps.

You can also use techniques described in the previous chapter to make ``docker build`` use your private
apt sources or private PyPI repositories (via ``.pypirc``) available which can be security-vetted.

If you fulfill all the criteria, you can build the image on an air-gaped system by running command similar
to the below:

.. exampleinclude:: docker-examples/restricted/restricted_environments.sh
    :language: bash
    :start-after: [START build]
    :end-before: [END build]

Modifying the Dockerfile
........................

The build arg approach is a convenience method if you do not want to manually modify the ``Dockerfile``.
Our approach is flexible enough, to be able to accommodate most requirements and
customizations out-of-the-box. When you use it, you do not need to worry about adapting the image every
time new version of Airflow is released. However sometimes it is not enough if you have very
specific needs and want to build a very custom image. In such case you can simply modify the
``Dockerfile`` manually as you see fit and store it in your forked repository. However you will have to
make sure to rebase your changes whenever new version of Airflow is released, because we might modify
the approach of our Dockerfile builds in the future and you might need to resolve conflicts
and rebase your changes.

There are a few things to remember when you modify the ``Dockerfile``:

* We are using the widely recommended pattern of ``.dockerignore`` where everything is ignored by default
  and only the required folders are added through exclusion (!). This allows to keep docker context small
  because there are many binary artifacts generated in the sources of Airflow and if they are added to
  the context, the time of building the image would increase significantly. If you want to add any new
  folders to be available in the image you must add it here with leading ``!``.

  .. code-block:: text

      # Ignore everything
      **

      # Allow only these directories
      !airflow
      ...


* The ``docker-context-files`` folder is automatically added to the context of the image, so if you want
  to add individual files, binaries, requirement files etc you can add them there. The
  ``docker-context-files`` is copied to the ``/docker-context-files`` folder of the build segment of the
  image, so it is not present in the final image - which makes the final image smaller in case you want
  to use those files only in the ``build`` segment. You must copy any files from the directory manually,
  using COPY command if you want to get the files in your final image (in the main image segment).


More details
------------

Build Args reference
....................

The detailed ``--build-arg`` reference can be found in :doc:`build-arg-ref`.


The architecture of the images
..............................

You can read more details about the images - the context, their parameters and internal structure in the
`IMAGES.rst <https://github.com/apache/airflow/blob/master/IMAGES.rst>`_ document.
