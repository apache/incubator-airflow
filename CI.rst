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

CI Environment
==============

Continuous Integration is important component of making Apache Airflow robust and stable. We are running
a lot of tests for every pull request, for master and v1-10-test branches and regularly as CRON jobs.

Our execution environment for CI is `GitHub Actions <https://github.com/features/actions>`_. GitHub Actions
(GA) are very well integrated with GitHub code and Workflow and it has evolved fast in 2019/202 to become
a fully-fledged CI environment, easy to use and develop for, so we decided to switch to it. Our previous
CI system was Travis CI.

However part of the philosophy we have is that we are not tightly coupled with any of the CI
environments we use. Most of our CI jobs are written as bash scripts which are executed as steps in
the CI jobs. And we have  a number of variables determine build behaviour.


GitHub Actions runs
-------------------

Our builds on CI are highly optimized. They utilise some of the latest features provided by GitHub Actions
environment that make it possible to reuse parts of the build process across different Jobs.

Big part of our CI runs use Container Images. Airflow has a lot of dependencies and in order to make
sure that we are running tests in a well configured and repeatable environment, most of the tests,
documentation building, and some more sophisticated static checks are run inside a docker container
environment. This environment consist of two types of images: CI images and PROD images. CI Images
are used for most of the tests and checks where PROD images are used in the Kubernetes tests.

In order to run the tests, we need to make sure that the images are built using latest sources and that it
is done quickly (full rebuild of such image from scratch might take ~15 minutes). Therefore optimisation
techniques have been implemented that use efficiently cache from the GitHub Docker registry - in most cases
this brings down the time needed to rebuild the image to ~4 minutes. In some cases (when dependencies change)
it can be ~6-7 minutes and in case base image of Python releases new patch-level, it can be ~12 minutes.

Container Registry used as cache
--------------------------------

For the CI builds of our we are using Container Registry to store results of the "Build Image" workflow
and pass it to the "CI Build" workflow.

Currently in master version of Airflow we run tests in 3 different versions of Python (3.6, 3.7, 3.8)
which means that we have to build 6 images (3 CI ones and 3 PROD ones). Yet we run around 12 jobs
with each of the CI images. That is a lot of time to just build the environment to run. Therefore
we are utilising ``workflow_run`` feature of GitHub Actions.

This feature allows to run a separate, independent workflow, when the main workflow is run -
this separate workflow is different than the main one, because by default it runs using ``master`` version
of the sources but also - and most of all - that it has WRITE access to the repository.

This is especially important in our case where Pull Requests to Airflow might come from any repository,
and it would be a huge security issue if anyone from outside could
utilise the WRITE access to Apache Airflow repository via an external Pull Request.

Thanks to the WRITE access and fact that the 'workflow_run' by default uses the 'master' version of the
sources, we can safely run some logic there will checkout the incoming Pull Request, build the container
image from the sources from the incoming PR and push such image to an GitHub Docker Registry - so that
this image can be built only once and used by all the jobs running tests. The image is tagged with unique
``RUN_ID`` of the incoming Pull Request and the tests run in the Pull Request can simply pull such image
rather than build it from the scratch. Pulling such image takes ~ 1 minute, thanks to that we are saving
a lot of precious time for jobs.

We can use either of the two available GitHub Container registries as cache:

* Legacy `GitHub Package Registry <https://github.com/features/packages>`_ which is not very
  stable, uses old infrastructure of GitHub and it lacks certain features - notably it does not allow
  us to delete the old image. The benefit of using GitHub Package Registry is that it works
  out-of-the-box (write authentication is done using ``GITHUB_TOKEN`` and users do not have to do any
  action to make it work in case they want to run build using their own forks. Also those images
  do not provide public access, so you need to login to ``docker.pkg.github.com`` docker registry
  using your username and personal token to be able to pull those images.

* The new `GitHub Container Registry <https://docs.github.com/en/packages/guides/about-github-container-registry>`_
  which is in Public Beta, has many more features (including permission management, public access and
  image retention possibility). It has also the drawback (at least as of January 2020) that you need to
  have separate personal access token created as ``PAT_CR`` secret in your repository with write access
  to registry in order to make it works. You also have to manually manage permissions of the images,
  i.e. after creating images for the first time, you need to set their visibility to "Public" and
  add ``Admin`` permissions to group of people managing the images (in our case ``airflow-committers`` group).
  This makes it not very suitable to use GitHub container registry if you want to run builds of Airflow
  in your own forks (note - it does not affect pull requests from forks to Airflow).

Those two images have different naming schemas. See `Images documentation <IMAGES.rst>`_ for details.

You can choose which registry should be used by the repository by setting ``OVERRIDE_GITHUB_REGISTRY`` secret
to either ``docker.pkg.github.com`` for GitHub Package Registry or ``ghcr.io`` for GitHub Container Registry.
Default is the GitHub Package Registry one. The Pull Request forks have no access to the secret but they
auto-detect the registry used when they wait for the images.

You can interact with the GitHub Registry images (pull/push) via `Breeze <BREEZE.rst>`_  - you can
pass ``--github-registry`` flag wih  either ``docker.pkg.github.com`` for GitHub Package Registry or
``ghcr.io`` for GitHub Container Registry and pull/push operations will be performed using the chosen
registry, using appropriate naming convention. This allows building and pushing the images locally by
committers who have access to push/pull those images.


GitHub Container Registry Token
-------------------------------

Unlike GitHub Packages, GitHub Registry requires a personal access token added as ``PAT_CR`` secret in order
to make it works. This token has to have "Registry Write" scope. Ideally you should not use a token
of a person who has access to many repositories, because this token allows to write packages in
ANY repository, where the person has write access (including private organisations). Ideally, you need to have
a separate account with only access to that repository and generate Personal Access Token with Package
Registry write permission for that Account. Discussion about setting up such account is opened at
`ASF Jira <https://issues.apache.org/jira/projects/INFRA/issues/INFRA-20959>`_. More info about
the token for GitHub Container Registry can be found
`here <https://docs.github.com/en/packages/guides/migrating-to-github-container-registry-for-docker-images#authenticating-with-the-container-registry>`_


Locally replicating CI failures
-------------------------------

The main goal of the CI philosophy we have that no matter how complex the test and integration
infrastructure, as a developer you should be able to reproduce and re-run any of the failed checks
locally. One part of it are pre-commit checks, that allow you to run the same static checks in CI
and locally, but another part is the CI environment which is replicated locally with Breeze.

You can read more about Breeze in `BREEZE.rst <BREEZE.rst>`_ but in essence it is a script that allows
you to re-create CI environment in your local development instance and interact with it. In its basic
form, when you do development you can run all the same tests that will be run in CI - but locally,
before you submit them as PR. Another use case where Breeze is useful is when tests fail on CI. You can
take the ``RUN_ID`` of failed build pass it as ``--github-image-id`` parameter of Breeze and it will
download the very same version of image that was used in CI and run it locally. This way, you can very
easily reproduce any failed test that happens in CI - even if you do not check out the sources
connected with the run.

You can read more about it in `BREEZE.rst <BREEZE.rst>`_ and `TESTING.rst <TESTING.rst>`_


Difference between local runs and GitHub Action workflows
---------------------------------------------------------

Depending whether the scripts are run locally (most often via `Breeze <BREEZE.rst>`_) or whether they
are run in "CI Build" or "Build Image" workflows they can take different values.

You can use those variables when you try to reproduce the build locally.

+-----------------------------------------+-------------+-------------+------------+-------------------------------------------------+
| Variable                                | Local       | Build Image | Main CI    | Comment                                         |
|                                         | development | CI workflow | Workflow   |                                                 |
+=========================================+=============+=============+============+=================================================+
|                                                           Basic variables                                                          |
+-----------------------------------------+-------------+-------------+------------+-------------------------------------------------+
| ``PYTHON_MAJOR_MINOR_VERSION``          |             |             |            | Major/Minor version of Python used.             |
+-----------------------------------------+-------------+-------------+------------+-------------------------------------------------+
| ``DB_RESET``                            |    false    |     true    |    true    | Determines whether database should be reset     |
|                                         |             |             |            | at the container entry. By default locally      |
|                                         |             |             |            | the database is not reset, which allows to      |
|                                         |             |             |            | keep the database content between runs in       |
|                                         |             |             |            | case of Postgres or MySQL. However,             |
|                                         |             |             |            | it requires to perform manual init/reset        |
|                                         |             |             |            | if you stop the environment.                    |
+-----------------------------------------+-------------+-------------+------------+-------------------------------------------------+
| Dockerhub variables                                                                                                                |
+-----------------------------------------+----------------------------------------+-------------------------------------------------+
| ``DOCKERHUB_USER``                      |                 apache                 | Name of the DockerHub user to use               |
+-----------------------------------------+----------------------------------------+-------------------------------------------------+
| ``DOCKERHUB_REPO``                      |                 airflow                | Name of the DockerHub repository to use         |
+-----------------------------------------+----------------------------------------+-------------------------------------------------+
|                                                           Mount variables                                                          |
+-----------------------------------------+-------------+-------------+------------+-------------------------------------------------+
| ``MOUNT_SELECTED_LOCAL_SOURCES``        |     true    |    false    |    false   | Determines whether local sources are            |
|                                         |             |             |            | mounted to inside the container. Useful for     |
|                                         |             |             |            | local development, as changes you make          |
|                                         |             |             |            | locally can be immediately tested in            |
|                                         |             |             |            | the container. We mount only selected,          |
|                                         |             |             |            | important folders. We do not mount the whole    |
|                                         |             |             |            | project folder in order to avoid accidental     |
|                                         |             |             |            | use of artifacts (such as ``egg-info``          |
|                                         |             |             |            | directories) generated locally on the           |
|                                         |             |             |            | host during development.                        |
+-----------------------------------------+-------------+-------------+------------+-------------------------------------------------+
| ``MOUNT_ALL_LOCAL_SOURCES``             |     false   |    false    |    false   | Determines whether all local sources are        |
|                                         |             |             |            | mounted to inside the container. Useful for     |
|                                         |             |             |            | local development when you need to access .git  |
|                                         |             |             |            | folders and other folders excluded when         |
|                                         |             |             |            | ``MOUNT_SELECTED_LOCAL_SOURCES`` is true.       |
|                                         |             |             |            | You might need to manually delete egg-info      |
|                                         |             |             |            | folder when you enter breeze and the folder was |
|                                         |             |             |            | generated using different Python versions.      |
+-----------------------------------------+-------------+-------------+------------+-------------------------------------------------+
|                                                           Force variables                                                          |
+-----------------------------------------+-------------+-------------+------------+-------------------------------------------------+
| ``FORCE_PULL_IMAGES``                   |    true     |    true     |    true    | Determines if images are force-pulled,          |
|                                         |             |             |            | no matter if they are already present           |
|                                         |             |             |            | locally. This includes not only the             |
|                                         |             |             |            | CI/PROD images but also the Python base         |
|                                         |             |             |            | images. Note that if Python base images         |
|                                         |             |             |            | change, also the CI and PROD images             |
|                                         |             |             |            | need to be fully rebuild unless they were       |
|                                         |             |             |            | already built with that base Python             |
|                                         |             |             |            | image. This is false for local development      |
|                                         |             |             |            | to avoid often pulling and rebuilding           |
|                                         |             |             |            | the image. It is true for CI workflow in        |
|                                         |             |             |            | case waiting from images is enabled             |
|                                         |             |             |            | as the images needs to be force-pulled from     |
|                                         |             |             |            | GitHub Registry, but it is set to               |
|                                         |             |             |            | false when waiting for images is disabled.      |
+-----------------------------------------+-------------+-------------+------------+-------------------------------------------------+
| ``FORCE_BUILD_IMAGES``                  |    false    |    false    |    false   | Forces building images. This is generally not   |
|                                         |             |             |            | very useful in CI as in CI environment image    |
|                                         |             |             |            | is built or pulled only once, so there is no    |
|                                         |             |             |            | need to set the variable to true. For local     |
|                                         |             |             |            | builds it forces rebuild, regardless if it      |
|                                         |             |             |            | is determined to be needed.                     |
+-----------------------------------------+-------------+-------------+------------+-------------------------------------------------+
| ``FORCE_ANSWER_TO_QUESTIONS``           |             |     yes     |     yes    | This variable determines if answer to questions |
|                                         |             |             |            | during the build process should be              |
|                                         |             |             |            | automatically given. For local development,     |
|                                         |             |             |            | the user is occasionally asked to provide       |
|                                         |             |             |            | answers to questions such as - whether          |
|                                         |             |             |            | the image should be rebuilt. By default         |
|                                         |             |             |            | the user has to answer but in the CI            |
|                                         |             |             |            | environment, we force "yes" answer.             |
+-----------------------------------------+-------------+-------------+------------+-------------------------------------------------+
| ``SKIP_CHECK_REMOTE_IMAGE``             |    false    |     true    |    true    | Determines whether we check if remote image     |
|                                         |             |             |            | is "fresher" than the current image.            |
|                                         |             |             |            | When doing local breeze runs we try to          |
|                                         |             |             |            | determine if it will be faster to rebuild       |
|                                         |             |             |            | the image or whether the image should be        |
|                                         |             |             |            | pulled first from the cache because it has      |
|                                         |             |             |            | been rebuilt. This is slightly experimental     |
|                                         |             |             |            | feature and will be improved in the future      |
|                                         |             |             |            | as the current mechanism does not always        |
|                                         |             |             |            | work properly.                                  |
+-----------------------------------------+-------------+-------------+------------+-------------------------------------------------+
|                                                           Host variables                                                           |
+-----------------------------------------+-------------+-------------+------------+-------------------------------------------------+
| ``HOST_USER_ID``                        |             |             |            | User id of the host user.                       |
+-----------------------------------------+-------------+-------------+------------+-------------------------------------------------+
| ``HOST_GROUP_ID``                       |             |             |            | Group id of the host user.                      |
+-----------------------------------------+-------------+-------------+------------+-------------------------------------------------+
| ``HOST_OS``                             |             |    Linux    |    Linux   | OS of the Host (Darwin/Linux).                  |
+-----------------------------------------+-------------+-------------+------------+-------------------------------------------------+
| ``HOST_HOME``                           |             |             |            | Home directory on the host.                     |
+-----------------------------------------+-------------+-------------+------------+-------------------------------------------------+
|                                                           Image variables                                                          |
+-----------------------------------------+-------------+-------------+------------+-------------------------------------------------+
| ``INSTALL_AIRFLOW_VERSION``             |             |             |            | Installs Airflow version from PyPI when         |
|                                         |             |             |            | building image. Can be "none" to skip airflow   |
|                                         |             |             |            | installation so that it can be installed from   |
|                                         |             |             |            | locally prepared packages.                      |
+-----------------------------------------+-------------+-------------+------------+-------------------------------------------------+
| ``INSTALL_AIRFLOW_REFERENCE``           |             |             |            | Installs Airflow version from GitHub            |
|                                         |             |             |            | branch or tag.                                  |
+-----------------------------------------+-------------+-------------+------------+-------------------------------------------------+
|                                                      Version suffix variables                                                      |
+-----------------------------------------+-------------+-------------+------------+-------------------------------------------------+
| ``VERSION_SUFFIX_FOR_PYPI``             |             |             |            | Version suffix used during provider             |
|                                         |             |             |            | package preparation for PyPI builds.            |
+-----------------------------------------+-------------+-------------+------------+-------------------------------------------------+
| ``VERSION_SUFFIX_FOR_SVN``              |             |             |            | Version suffix used during provider             |
|                                         |             |             |            | package preparation for SVN builds.             |
+-----------------------------------------+-------------+-------------+------------+-------------------------------------------------+
|                                                            Git variables                                                           |
+-----------------------------------------+-------------+-------------+------------+-------------------------------------------------+
| COMMIT_SHA                              |             | GITHUB_SHA  | GITHUB_SHA | SHA of the commit of the build is run           |
+-----------------------------------------+-------------+-------------+------------+-------------------------------------------------+
|                                                         Verbosity variables                                                        |
+-----------------------------------------+-------------+-------------+------------+-------------------------------------------------+
| ``PRINT_INFO_FROM_SCRIPTS``             |   true\*    |    true\*   |    true\*  | Allows to print output to terminal from running |
|                                         |             |             |            | scripts. It prints some extra outputs if true   |
|                                         |             |             |            | including what the commands do, results of some |
|                                         |             |             |            | operations, summary of variable values, exit    |
|                                         |             |             |            | status from the scripts, outputs of failing     |
|                                         |             |             |            | commands. If verbose is on it also prints the   |
|                                         |             |             |            | commands executed by docker, kind, helm,        |
|                                         |             |             |            | kubectl. Disabled in pre-commit checks.         |
|                                         |             |             |            |                                                 |
|                                         |             |             |            | \* set to false in pre-commits                  |
+-----------------------------------------+-------------+-------------+------------+-------------------------------------------------+
| ``VERBOSE``                             |    false    |     true    |    true    | Determines whether docker, helm, kind,          |
|                                         |             |             |            | kubectl commands should be printed before       |
|                                         |             |             |            | execution. This is useful to determine          |
|                                         |             |             |            | what exact commands were executed for           |
|                                         |             |             |            | debugging purpose as well as allows             |
|                                         |             |             |            | to replicate those commands easily by           |
|                                         |             |             |            | copy&pasting them from the output.              |
|                                         |             |             |            | requires ``PRINT_INFO_FROM_SCRIPTS`` set to     |
|                                         |             |             |            | true.                                           |
+-----------------------------------------+-------------+-------------+------------+-------------------------------------------------+
| ``VERBOSE_COMMANDS``                    |    false    |    false    |    false   | Determines whether every command                |
|                                         |             |             |            | executed in bash should also be printed         |
|                                         |             |             |            | before execution. This is a low-level           |
|                                         |             |             |            | debugging feature of bash (set -x) and          |
|                                         |             |             |            | it should only be used if you are lost          |
|                                         |             |             |            | at where the script failed.                     |
+-----------------------------------------+-------------+-------------+------------+-------------------------------------------------+
|                                                        Image build variables                                                       |
+-----------------------------------------+-------------+-------------+------------+-------------------------------------------------+
| ``UPGRADE_TO_NEWER_DEPENDENCIES``       |    false    |    false    |   false\*  | Determines whether the build should             |
|                                         |             |             |            | attempt to upgrade Python base image and all    |
|                                         |             |             |            | PIP dependencies to latest ones matching        |
|                                         |             |             |            | ``setup.py`` limits. This tries to replicate    |
|                                         |             |             |            | the situation of "fresh" user who just installs |
|                                         |             |             |            | airflow and uses latest version of matching     |
|                                         |             |             |            | dependencies. By default we are using a         |
|                                         |             |             |            | tested set of dependency constraints            |
|                                         |             |             |            | stored in separated "orphan" branches           |
|                                         |             |             |            | of the airflow repository                       |
|                                         |             |             |            | ("constraints-master, "constraints-1-10")       |
|                                         |             |             |            | but when this flag is set to anything but false |
|                                         |             |             |            | (for example commit SHA), they are not used     |
|                                         |             |             |            | used and "eager" upgrade strategy is used       |
|                                         |             |             |            | when installing dependencies. We set it         |
|                                         |             |             |            | to true in case of direct pushes (merges)       |
|                                         |             |             |            | to master and scheduled builds so that          |
|                                         |             |             |            | the constraints are tested. In those builds,    |
|                                         |             |             |            | in case we determine that the tests pass        |
|                                         |             |             |            | we automatically push latest set of             |
|                                         |             |             |            | "tested" constraints to the repository.         |
|                                         |             |             |            |                                                 |
|                                         |             |             |            | Setting the value to commit SHA is best way     |
|                                         |             |             |            | to assure that constraints are upgraded even if |
|                                         |             |             |            | there is no change to setup.py                  |
|                                         |             |             |            |                                                 |
|                                         |             |             |            | This way our constraints are automatically      |
|                                         |             |             |            | tested and updated whenever new versions        |
|                                         |             |             |            | of libraries are released.                      |
|                                         |             |             |            |                                                 |
|                                         |             |             |            | \* true in case of direct pushes and            |
|                                         |             |             |            |    scheduled builds                             |
+-----------------------------------------+-------------+-------------+------------+-------------------------------------------------+
| ``CHECK_IMAGE_FOR_REBUILD``             |     true    |     true    |   true\*   | Determines whether attempt should be            |
|                                         |             |             |            | made to rebuild the CI image with latest        |
|                                         |             |             |            | sources. It is true by default for              |
|                                         |             |             |            | local builds, however it is set to              |
|                                         |             |             |            | true in case we know that the image             |
|                                         |             |             |            | we pulled or built already contains             |
|                                         |             |             |            | the right sources. In such case we              |
|                                         |             |             |            | should set it to false, especially              |
|                                         |             |             |            | in case our local sources are not the           |
|                                         |             |             |            | ones we intend to use (for example              |
|                                         |             |             |            | when ``--github-image-id`` is used              |
|                                         |             |             |            | in Breeze.                                      |
|                                         |             |             |            |                                                 |
|                                         |             |             |            | In CI builds it is set to true                  |
|                                         |             |             |            | in case of the "Build Image"                    |
|                                         |             |             |            | workflow or when                                |
|                                         |             |             |            | waiting for images is disabled                  |
|                                         |             |             |            | in the CI workflow.                             |
|                                         |             |             |            |                                                 |
|                                         |             |             |            | \* if waiting for images the variable is set    |
|                                         |             |             |            |    to false automatically.                      |
+-----------------------------------------+-------------+-------------+------------+-------------------------------------------------+
| ``SKIP_BUILDING_PROD_IMAGE``            |     false   |     false   |   false\*  | Determines whether we should skip building      |
|                                         |             |             |            | the PROD image with latest sources.             |
|                                         |             |             |            | It is set to false, but in deploy app for       |
|                                         |             |             |            | kubernetes step it is set to "true", because at |
|                                         |             |             |            | this stage we know we have good image build or  |
|                                         |             |             |            | pulled.                                         |
|                                         |             |             |            |                                                 |
|                                         |             |             |            | \* set to true in "Deploy App to Kubernetes"    |
|                                         |             |             |            |    to false automatically.                      |
+-----------------------------------------+-------------+-------------+------------+-------------------------------------------------+

Running CI Builds locally
=========================

The following variables are automatically determined based on CI environment variables.
You can locally by setting ``CI="true"`` and run the ci scripts from the ``scripts/ci`` folder:

* ``provider_packages`` - scripts to build and test provider packages
* ``constraints`` - scripts to build and publish latest set of valid constraints
* ``docs`` - scripts to build documentation
* ``images`` - scripts to build and push CI and PROD images
* ``kubernetes`` - scripts to setup kubernetes cluster, deploy airflow and run kubernetes tests with it
* ``testing`` - scripts that run unit and integration tests
* ``tools`` - scripts that perform various clean-up and preparation tasks

Common libraries of functions for all the scripts can be found in ``libraries`` folder.

For detailed use of those scripts you can refer to ``.github/workflows/`` - those scripts are used
by the CI workflows of ours.

The default values are "sane"  you can change them to interact with your own repositories or registries.
Note that you need to set "CI" variable to true in order to get the same results as in CI.

+------------------------------+----------------------+-----------------------------------------------------+
| Variable                     | Default              | Comment                                             |
+==============================+======================+=====================================================+
| CI                           | ``false``            | If set to "true", we simulate behaviour of          |
|                              |                      | all scripts as if they are in CI environment        |
+------------------------------+----------------------+-----------------------------------------------------+
| CI_TARGET_REPO               | ``apache/airflow``   | Target repository for the CI build. Used to         |
|                              |                      | compare incoming changes from PR with the target.   |
+------------------------------+----------------------+-----------------------------------------------------+
| CI_TARGET_BRANCH             | ``master``           | Target branch where the PR should land. Used to     |
|                              |                      | compare incoming changes from PR with the target.   |
+------------------------------+----------------------+-----------------------------------------------------+
| CI_BUILD_ID                  | ``0``                | Unique id of the build that is kept across re runs  |
|                              |                      | (for GitHub actions it is ``GITHUB_RUN_ID``)        |
+------------------------------+----------------------+-----------------------------------------------------+
| CI_JOB_ID                    | ``0``                | Unique id of the job - used to produce unique       |
|                              |                      | artifact names.                                     |
+------------------------------+----------------------+-----------------------------------------------------+
| CI_EVENT_TYPE                | ``pull_request``     | Type of the event. It can be one of                 |
|                              |                      | [``pull_request``, ``pull_request_target``,         |
|                              |                      |  ``schedule``, ``push``]                            |
+------------------------------+----------------------+-----------------------------------------------------+
| CI_REF                       | ``refs/head/master`` | Branch in the source repository that is used to     |
|                              |                      | make the pull request.                              |
+------------------------------+----------------------+-----------------------------------------------------+


GitHub Registry Variables
=========================

Our CI uses GitHub Registry to pull and push images to/from by default. You can however make it interact with
DockerHub registry or change the GitHub registry to interact with and use your own repo by changing
``GITHUB_REPOSITORY`` and providing your own GitHub Username and Token.

Currently we are using GitHub Packages to cache images for the build. GitHub Packages are "legacy"
storage of binary artifacts for GitHub and as of September 2020 they introduced GitHub Container Registry
as more stable, easier to manage replacement for container storage. It includes complete self-management
of the images including permission management, public access, retention management and many more.

More about it here:

https://github.blog/2020-09-01-introducing-github-container-registry/

Recently we started to experience unstable behaviour of the GitHub Packages ('unknown blob'
and manifest v1 vs. v2 when pushing images to it). So together with ASF we proposed to
enable GitHub Container Registry and it happened as of January 2020.

More about it in https://issues.apache.org/jira/browse/INFRA-20959

We are currently in the testing phase, especially when it comes to management of permissions -
the model of permission management is not the same for Container Registry as it was for GitHub Packages
(it was per-repository in GitHub Packages, but it is organization-wide in the Container Registry.

+--------------------------------+---------------------------+----------------------------------------------+
| Variable                       | Default                   | Comment                                      |
+================================+===========================+==============================================+
| USE_GITHUB_REGISTRY            | true                      | If set to "true", we interact with GitHub    |
|                                |                           | Registry registry not the DockerHub one.     |
+--------------------------------+---------------------------+----------------------------------------------+
| GITHUB_REGISTRY                | ``docker.pkg.github.com`` | Name of the GitHub registry to use. Can be   |
|                                |                           | ``docker.pkg.github.com`` or ``ghcr.io``     |
+--------------------------------+---------------------------+----------------------------------------------+
| GITHUB_REPOSITORY              | ``apache/airflow``        | Prefix of the image. It indicates which.     |
|                                |                           | registry from GitHub to use                  |
+--------------------------------+---------------------------+----------------------------------------------+
| GITHUB_USERNAME                |                           | Username to use to login to GitHub           |
|                                |                           |                                              |
+--------------------------------+---------------------------+----------------------------------------------+
| GITHUB_TOKEN                   |                           | Token to use to login to GitHub. This token  |
|                                |                           | is automatically set by GitHub CI to a       |
|                                |                           | to a READ-only token for PR builds from fork |
|                                |                           | and to WRITE token for direct pushes and     |
|                                |                           | scheduled or workflow_run types of builds    |
+--------------------------------+---------------------------+----------------------------------------------+
| CONTAINER_REGISTRY_TOKEN       |                           | Personal token to use to login to GitHub     |
|                                |                           | Container Registry. Should be retrieved      |
|                                |                           | from secret (in our case it is PAT_CR secret |
|                                |                           | following example in GitHub documentation.   |
|                                |                           | Only set in push/scheduled/workflow_run      |
|                                |                           | type of build.                               |
+--------------------------------+---------------------------+----------------------------------------------+
| GITHUB_REGISTRY_WAIT_FOR_IMAGE | ``false``                 | Wait for the image to be available. This is  |
|                                |                           | useful if commit SHA is used as pull tag     |
+--------------------------------+---------------------------+----------------------------------------------+
| GITHUB_REGISTRY_PULL_IMAGE_TAG | ``latest``                | Pull this image tag. This is "latest" by     |
|                                |                           | default, can be commit SHA or RUN_ID.        |
+--------------------------------+---------------------------+----------------------------------------------+
| GITHUB_REGISTRY_PUSH_IMAGE_TAG | ``latest``                | Pull this image tag. This is "latest" by     |
|                                |                           | default, can be commit SHA or RUN_ID.        |
+--------------------------------+---------------------------+----------------------------------------------+

Authentication in GitHub Registry
=================================

We are currently in the process of testing using GitHub Container Registry as cache for our images during
the CI process. The default registry is set to "GitHub Packages", but we are testing the GitHub
Container Registry. In case of GitHub Packages, authentication uses GITHUB_TOKEN mechanism. Authentication
is needed for both pushing the images (WRITE) and pulling them (READ) - which means that GitHub token
is used in "master" build (WRITE) and in fork builds (READ). For container registry, our images are
Publicly Visible and we do not need any authentication to pull them so the CONTAINER_REGISTRY_TOKEN is
only set in the "master" builds only ("Build Images" workflow and "Scheduled quarantine" one).

Dockerhub Variables
===================

If ``USE_GITHUB_REGISTRY`` is set to "false" you can interact directly with DockerHub. By default
you pull from/push to "apache/airflow" DockerHub repository, but you can change
that to your own repository by setting those environment variables:

+----------------+-------------+-----------------------------------+
| Variable       | Default     | Comment                           |
+================+=============+===================================+
| DOCKERHUB_USER | ``apache``  | Name of the DockerHub user to use |
+----------------+-------------+-----------------------------------+
| DOCKERHUB_REPO | ``airflow`` | Name of the DockerHub repo to use |
+----------------+-------------+-----------------------------------+

CI Architecture
===============

 .. This image is an export from the 'draw.io' graph available in
    https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-23+Migrate+out+of+Travis+CI
    You can edit it there and re-export.

.. image:: images/ci/CI.png
    :align: center
    :alt: CI architecture of Apache Airflow

The following components are part of the CI infrastructure

* **Apache Airflow Code Repository** - our code repository at https://github.com/apache/airflow
* **Apache Airflow Forks** - forks of the Apache Airflow Code Repository from which contributors make
  Pull Requests
* **GitHub Actions** -  (GA) UI + execution engine for our jobs
* **GA CRON trigger** - GitHub Actions CRON triggering our jobs
* **GA Workers** - virtual machines running our jobs at GitHub Actions (max 20 in parallel)
* **GitHub Private Image Registry**- image registry used as build cache for CI  jobs.
  It is at https://docker.pkg.github.com/apache/airflow/airflow
* **DockerHub Public Image Registry** - publicly available image registry at DockerHub.
  It is at https://hub.docker.com/r/apache/airflow
* **DockerHub Build Workers** - virtual machines running build jibs at DockerHub
* **Official Images** (future) - these are official images that are prominently visible in DockerHub.
  We aim our images to become official images so that you will be able to pull them
  with ``docker pull apache-airflow``

CI run types
============

The following CI Job run types are currently run for Apache Airflow (run by ci.yaml workflow)
and each of the run types has different purpose and context.

Pull request run
----------------

Those runs are results of PR from the forks made by contributors. Most builds for Apache Airflow fall
into this category. They are executed in the context of the "Fork", not main
Airflow Code Repository which means that they have only "read" permission to all the GitHub resources
(container registry, code repository). This is necessary as the code in those PRs (including CI job
definition) might be modified by people who are not committers for the Apache Airflow Code Repository.

The main purpose of those jobs is to check if PR builds cleanly, if the test run properly and if
the PR is ready to review and merge. The runs are using cached images from the Private GitHub registry -
CI, Production Images as well as base Python images that are also cached in the Private GitHub registry.
Also for those builds we only execute Python tests if important files changed (so for example if it is
"no-code" change, no tests will be executed.

The workflow involved in Pull Requests review and approval is a bit more complex than simple workflows
in most of other projects because we've implemented some optimizations related to efficient use
of queue slots we share with other Apache Software Foundation projects. More details about it
can be found in `PULL_REQUEST_WORKFLOW.rst <PULL_REQUEST_WORKFLOW.rst>`_.


Direct Push/Merge Run
---------------------

Those runs are results of direct pushes done by the committers or as result of merge of a Pull Request
by the committers. Those runs execute in the context of the Apache Airflow Code Repository and have also
write permission for GitHub resources (container registry, code repository).
The main purpose for the run is to check if the code after merge still holds all the assertions - like
whether it still builds, all tests are green.

This is needed because some of the conflicting changes from multiple PRs might cause build and test failures
after merge even if they do not fail in isolation. Also those runs are already reviewed and confirmed by the
committers so they can be used to do some housekeeping:
- pushing most recent image build in the PR to the GitHub Private Registry (for caching)
- upgrading to latest constraints and pushing those constraints if all tests succeed
- refresh latest Python base images in case new patch-level is released

The housekeeping is important - Python base images are refreshed with varying frequency (once every few months
usually but sometimes several times per week) with the latest security and bug fixes.
Those patch level images releases can occasionally break Airflow builds (specifically Docker image builds
based on those images) therefore in PRs we only use latest "good" Python image that we store in the
private GitHub cache. The direct push/master builds are not using registry cache to pull the Python images
- they are directly pulling the images from DockerHub, therefore they will try the latest images
after they are released and in case they are fine, CI Docker image is build and tests are passing -
those jobs will push the base images to the private GitHub Registry so that they be used by subsequent
PR runs.

Scheduled runs
--------------

Those runs are results of (nightly) triggered job - only for ``master`` branch. The
main purpose of the job is to check if there was no impact of external dependency changes on the Apache
Airflow code (for example transitive dependencies released that fail the build). It also checks if the
Docker images can be build from the scratch (again - to see if some dependencies have not changed - for
example downloaded package releases etc. Another reason for the nightly build is that the builds tags most
recent master with ``nightly-master`` tag so that DockerHub build can pick up the moved tag and prepare a
nightly public master build in the DockerHub registry. The ``v1-10-test`` branch images are build in
DockerHub when pushing ``v1-10-stable`` manually.

All runs consist of the same jobs, but the jobs behave slightly differently or they are skipped in different
run categories. Here is a summary of the run categories with regards of the jobs they are running.
Those jobs often have matrix run strategy which runs several different variations of the jobs
(with different Backend type / Python version, type of the tests to run for example). The following chapter
describes the workflows that execute for each run.

Those runs and their corresponding ``Build Images`` runs are only executed in main ``apache/airflow``
repository, they are not executed in forks - we want to be nice to the contributors and not use their
free build minutes on GitHub Actions.

Workflows
=========

Build Images Workflow
---------------------

This workflow has two purposes - it builds images for the CI Workflow but also it cancels duplicate or
failed builds in order to save job time in GitHub Actions and allow for faster feedback for developers.

It's a special type of workflow: ``workflow_run`` which means that it is triggered by other workflows (in our
case it is triggered by the ``CI Build`` workflow). This also means that the workflow has Write permission to
the Airflow repository and it can - for example - push to the GitHub registry the images used by CI Builds
which means that the images can be built only once and reused by all the CI jobs (including the matrix jobs).
We've implemented it in the way that the CI Build running will wait until the images are built by the
"Build Images" workflow.

It's possible to disable this feature and go back to the previous behaviour via
``GITHUB_REGISTRY_WAIT_FOR_IMAGE`` flag in the "Build Workflow image". Setting it to "false" switches back to
the behaviour that each job builds own image.

You can also switch back to jobs building the images on its own on the fork level by setting
``AIRFLOW_GITHUB_REGISTRY_WAIT_FOR_IMAGE`` secret to ``false``. This will disable pushing the "RUN_ID"
images to GitHub Registry and all the images will be built locally by each job. It is about 20%
slower for the whole build on average, but it does not require to have access to push images to
GitHub, which sometimes might be not available (depending on the account status).

The write permission also allows to cancel duplicate workflows. It is not possible for the Pull Request
CI Builds run from the forks as they have no Write permission allowing them to cancels running workflows.
In our case we perform several different cancellations:

* we cancel duplicate "CI Build" workflow runs s (i.e. workflows from the same repository and branch that
  were started in quick succession - this allows to save workers that would have been busy running older
  version of the same Pull Request (usually with fix-ups) and free them for other runs.

* we cancel duplicate "Build Images" workflow runs for the same reasons. The "Build Images" builds run image
  builds which takes quite some time, so pushing a fixup quickly on the same branch will also cancel the
  past "Build Images" workflows.

* last, but not least - we cancel any of the "CI Build" workflow runs that failed in some important jobs.
  This is another optimisations - GitHub does not have "fail-fast" on the whole run and this cancelling
  effectively implements "fail-fast" of runs for some important jobs. Note that it only works when you
  submit new PRs or push new changes. In case the jobs failed and no new PR is pushed after that, the whole
  run will run to completion.

The workflow has the following jobs:

+---------------------------+---------------------------------------------+
| Job                       | Description                                 |
|                           |                                             |
+===========================+=============================================+
| Cancel workflow runs      | Cancels duplicated and failed workflows     |
+---------------------------+---------------------------------------------+
| Build Info                | Prints detailed information about the build |
+---------------------------+---------------------------------------------+
| Build CI/PROD images      | Builds all configured CI and PROD images    |
+---------------------------+---------------------------------------------+

The images are stored in the `GitHub Registry <https://github.com/apache/airflow/packages>`_ and the
names of those images follow the patterns described in
`Naming conventions for stored images <#naming-conventions-for-stored-images>`_

Image building is configured in "fail-fast" mode. When any of the images
fails to build, it cancels other builds and the source "CI Build" workflow run
that triggered it.


CI Build Workflow
-----------------

This workflow is a regular workflow that performs all checks of Airflow code.

+---------------------------+----------------------------------------------+-------+-------+------+
| Job                       | Description                                  | PR    | Push  | CRON |
|                           |                                              |       | Merge | (1)  |
+===========================+==============================================+=======+=======+======+
| Build info                | Prints detailed information about the build  | Yes   | Yes   | Yes  |
+---------------------------+----------------------------------------------+-------+-------+------+
| Helm tests                | Runs tests for the Helm chart                | Yes   | Yes   | Yes  |
+---------------------------+----------------------------------------------+-------+-------+------+
| Test OpenAPI client gen   | Tests if OpenAPIClient continues to generate | Yes   | Yes   | Yes  |
+---------------------------+----------------------------------------------+-------+-------+------+
| CI Images                 | Waits for CI Images (3)                      | Yes   | Yes   | Yes  |
+---------------------------+----------------------------------------------+-------+-------+------+
| Static checks             | Performs static checks without pylint        | Yes   | Yes   | Yes  |
+---------------------------+----------------------------------------------+-------+-------+------+
| Static checks: pylint     | Performs pylint static checks                | Yes   | Yes   | Yes  |
+---------------------------+----------------------------------------------+-------+-------+------+
| Build docs                | Builds documentation                         | Yes   | Yes   | Yes  |
+---------------------------+----------------------------------------------+-------+-------+------+
| Spell check docs          | Spell check for documentation                | Yes   | Yes   | Yes  |
+---------------------------+----------------------------------------------+-------+-------+------+
| Trigger tests             | Checks if tests should be triggered          | Yes   | Yes   | Yes  |
+---------------------------+----------------------------------------------+-------+-------+------+
| Tests [Pg/Msql/Sqlite]    | Run all the Pytest tests for Python code     | Yes(2)| Yes   | Yes  |
+---------------------------+----------------------------------------------+-------+-------+------+
| Quarantined tests         | Flaky tests that we need to fix (5)          | Yes(2)| Yes   | Yes  |
+---------------------------+----------------------------------------------+-------+-------+------+
| Upload coverage           | Uploads test coverage from all the tests     | Yes   | Yes   | Yes  |
+---------------------------+----------------------------------------------+-------+-------+------+
| PROD Images               | Waits for CI Images (3)                      | Yes   | Yes   | Yes  |
+---------------------------+----------------------------------------------+-------+-------+------+
| Tests Kubernetes          | Run Kubernetes test                          | Yes(2)| Yes   | Yes  |
+---------------------------+----------------------------------------------+-------+-------+------+
| Push PROD images          | Pushes PROD images to GitHub Registry (4)    | -     | Yes   | -    |
+---------------------------+----------------------------------------------+-------+-------+------+
| Push CI images            | Pushes CI images to GitHub Registry (4)      | -     | Yes   | -    |
+---------------------------+----------------------------------------------+-------+-------+------+
| Constraints               | Upgrade constraints to latest ones (4)       | -     | Yes   | Yes  |
+---------------------------+----------------------------------------------+-------+-------+------+
| Constraints push          | Pushes all upgraded constraints (4)          | -     | Yes   | Yes  |
+---------------------------+----------------------------------------------+-------+-------+------+
| Tag Repo nightly          | Tags the repository with nightly tag (6)     | -     | -     | Yes  |
+---------------------------+----------------------------------------------+-------+-------+------+


Comments:

 (1) CRON jobs builds images from scratch - to test if everything works properly for clean builds
 (2) The tests are run when the Trigger Tests job determine that important files change (this allows
     for example "no-code" changes to build much faster)
 (3) The jobs wait for CI images if ``GITHUB_REGISTRY_WAIT_FOR_IMAGE`` variable is set to "true".
     You can set it to "false" to disable using shared images - this is slower though as the images
     are rebuilt in every job that needs them. You can also set your own fork's secret
     ``AIRFLOW_GITHUB_REGISTRY_WAIT_FOR_IMAGE`` to ``false`` to trigger the same behaviour.
 (4) PROD and CI images are pushed as "latest" to DockerHub registry and constraints are upgraded only if all
     tests are successful. Note that images are not pushed in CRON jobs because they are rebuilt from
     scratch and we want to push incremental changes to the DockerHub registry.
 (5) Flaky tests never fail in regular builds. See the next chapter where our approach to flaky tests
     is explained.
 (6) Nightly tag is pushed to the repository only in CRON job and only if all tests pass. This
     causes the DockerHub images are built automatically and made available to developers.

Scheduled quarantined builds
----------------------------

This workflow runs only quarantined tests. Those tests do not fail the build even if some tests fail (only if
the whole pytest execution fails). Instead this workflow updates one of the issues where we keep status
of quarantined tests. Once the test succeeds in NUM_RUNS subsequent runs, it is marked as stable and
can be removed from quarantine. You can read more about quarantine in `<TESTING.rst>`_

The issues are only updated if the test is run as direct push or scheduled run and only in the
``apache/airflow`` repository - so that the issues are not updated in forks.

The issues that gets updated are different for different branches:

* master: `Quarantine tests master <https://github.com/apache/airflow/issues/10118>`_
* v1-10-stable: `Quarantine tests v1-10-stable <https://github.com/apache/airflow/issues/10127>`_
* v1-10-test: `Quarantine tests v1-10-test <https://github.com/apache/airflow/issues/10128>`_

Those runs and their corresponding ``Build Images`` runs are only executed in main ``apache/airflow``
repository, they are not executed in forks - we want to be nice to the contributors and not use their
free build minutes on GitHub Actions.

Force sync master from apache/airflow
-------------------------------------

This is manually triggered workflow (via GitHub UI manual run) that should only be run in GitHub forks.
When triggered, it will force-push the "apache/airflow" master to the fork's master. It's the easiest
way to sync your fork master to the Apache Airflow's one.

Delete old artifacts
--------------------

This workflow is introduced, to delete old artifacts from the GitHub Actions build. We set it to
delete old artifacts that are > 7 days old. It only runs for the 'apache/airflow' repository.

We also have a script that can help to clean-up the old artifacts:
`remove_artifacts.sh <dev/remove_artifacts.sh>`_

CodeQL scan
-----------

The `CodeQL <https://securitylab.github.com/tools/codeql>`_ security scan uses GitHub security scan framework to scan our code for security violations.
It is run for JavaScript and Python code.

Publishing documentation
------------------------

Documentation from the ``master`` branch is automatically published on Amazon S3.

To make this possible, GitHub Action has secrets set up with credentials
for an Amazon Web Service account - ``DOCS_AWS_ACCESS_KEY_ID`` and ``DOCS_AWS_SECRET_ACCESS_KEY``.

This account has permission to write/list/put objects to bucket ``apache-airflow-docs``. This bucket has public access configured, which means it is accessible through the website endpoint. For more information, see: `Hosting a static website on Amazon S3 <https://docs.aws.amazon.com/AmazonS3/latest/dev/WebsiteHosting.html>`_

Website endpoint: http://apache-airflow-docs.s3-website.eu-central-1.amazonaws.com/

Naming conventions for stored images
====================================

The images produced during the CI builds are stored in the
`GitHub Registry <https://github.com/apache/airflow/packages>`_

The images are stored with both "latest" tag (for last master push image that passes all the tests as well
with the tags indicating the origin of the image.

The image names follow the patterns:

+--------------+----------------------------+--------------------------------+--------------------------------------------------------------------------------------------+
| Image        | Name pattern               | Tag for format                 | Comment                                                                                    |
+==============+============================+================================+============================================================================================+
| Python image | Python                     | <X.Y>-slim-buster-<RUN_ID>     | Base Python image used by both production and CI image.                                    |
|              |                            | <X.Y>-slim-buster-<COMMIT_SHA> | Python maintainer release new versions of those image with security fixes every few weeks. |
+--------------+----------------------------+--------------------------------+--------------------------------------------------------------------------------------------+
| CI image     | <BRANCH>-python<X.Y>-ci    | <RUN_ID>                       | CI image - this is the image used for most of the tests.                                   |
|              |                            | <COMMIT_SHA>                   |                                                                                            |
+--------------+----------------------------+--------------------------------+--------------------------------------------------------------------------------------------+
| PROD Build   | <BRANCH>-python<X.Y>-build | <RUN_ID>                       | Production Build image - this is the "build" segment of production image.                  |
| image        |                            | <COMMIT_SHA>                   | It contains build-essentials and all necessary packages to install PIP packages.           |
+--------------+----------------------------+--------------------------------+--------------------------------------------------------------------------------------------+
| PROD image   | <BRANCH>-python<X.Y>       | <RUN_ID>                       | Production image. This is the actual production image - optimized for size.                |
|              |                            | <COMMIT_SHA>                   | It contains only compiled libraries and minimal set of dependencies to run Airflow.        |
+--------------+----------------------------+--------------------------------+--------------------------------------------------------------------------------------------+

* <BRANCH> might be either "master" or "v1-10-test" or "v2-0-test"
* <X.Y> - Python version (Major + Minor). For "master" and "v2-0-test" should be in ["3.6", "3.7", "3.8"]. For
  v1-10-test it should be in ["2.7", "3.5", "3.6". "3.7", "3.8"].
* <RUN_ID> - GitHub Actions RUN_ID. You can get it from CI action job outputs (run id is printed in
  logs and displayed as part of the step name. All PRs belong to some RUN_ID and this way you can
  pull the very exact version of image used in that RUN_ID
* <COMMIT_SHA> - for images that get merged to "master", "v2-0-test" of "v1-10-test" the images are also tagged
  with the commit SHA of that particular commit. This way you can easily find the image that was used
  for testing for that "master", "v2-0-test" or "v1-10-test" test run.

Reproducing CI Runs locally
===========================

Since we store images from every CI run, you should be able easily reproduce any of the CI build problems
locally. You can do it by pulling and using the right image and running it with the right docker command,
For example knowing that the CI build had 210056909 RUN_ID (you can find it from GitHub CI logs):

.. code-block:: bash

  docker pull docker.pkg.github.com/apache/airflow/master-python3.6-ci:210056909

  docker run -it docker.pkg.github.com/apache/airflow/master-python3.6-ci:210056909


But you usually need to pass more variables and complex setup if you want to connect to a database or
enable some integrations. Therefore it is easiest to use `Breeze <BREEZE.rst>`_ for that. For example if
you need to reproduce a MySQL environment with kerberos integration enabled for run 210056909, in python
3.8 environment you can run:

.. code-block:: bash

  ./breeze --github-image-id 210056909 --github-registry docker.pkg.github.com --python 3.8

You will be dropped into a shell with the exact version that was used during the CI run and you will
be able to run pytest tests manually, easily reproducing the environment that was used in CI. Note that in
this case, you do not need to checkout the sources that were used for that run - they are already part of
the image - but remember that any changes you make in those sources are lost when you leave the image as
the sources are not mapped from your host machine.

CI Sequence diagrams
====================

Sequence diagrams are shown of the flow happening during the CI builds.

Pull request flow from fork
---------------------------

.. image:: images/ci/pull_request_ci_flow.png
    :align: center
    :alt: Pull request flow from fork


Direct Push/Merge flow
----------------------

.. image:: images/ci/push_ci_flow.png
    :align: center
    :alt: Direct Push/Merge flow

Scheduled build flow
---------------------

.. image:: images/ci/scheduled_ci_flow.png
    :align: center
    :alt: Scheduled build flow


Adding new Python versions to CI
--------------------------------

In 2.0 line we currently support Python 3.6, 3.7, 3.8.

In order to add a new version the following operations should be done (example uses Python 3.9)

* copy the latest constraints in ``constraints-master`` branch from previous versions and name it
  using the new Python version (``constraints-3.9.txt``). Commit and push

* add the new Python version to `breeze-complete <breeze-complete>`_ and
  `_initialization.sh <scripts/ci/libraries/_initialization.sh>`_ - tests will fail if they are not
  in sync.

* build image locally for both prod and CI locally using Breeze:

.. code-block:: bash

  ./breeze build-image --python 3.9

* push image as cache to DockerHub and both registries:

.. code-block:: bash

  ./breeze push-image --python 3.9
  ./breeze push-image --python 3.9 --github-registry ghcr.io
  ./breeze push-image --python 3.9 --github-registry docker.pkg.github.com

* Find the 3 new images (main, ci, build) created in
  `GitHub Container registry <https://github.com/orgs/apache/packages?tab=packages&ecosystem=container&q=airflow>`_
  go to Package Settings and turn on ``Public Visibility`` and add ``airflow-committers``
  group as ``Admin Role`` to all of them.

* In `DockerHub <https://hub.docker.com/repository/docker/apache/airflow/builds/edit>`_  create three entries
  for automatically built nightly-tag and release images:


+-------------+----------------+-----------------------+---------------------+---------------+-----------+---------------+------------------------------------------------------------------------+
| Source type | Source         | Docker Tag            | Dockerfile location | Build Context | Autobuild | Build caching | Comment                                                                |
+=============+================+=======================+=====================+===============+===========+===============+========================================================================+
| Tag         | nightly-master | master-python3.9      | Dockerfile          | /             | x         | -             | Nightly CI/PROD images from successful scheduled master nightly builds |
+-------------+----------------+-----------------------+---------------------+---------------+-----------+---------------+------------------------------------------------------------------------+
| Branch      | v2-0-stable    | v2-0-stable-python3.9 | Dockerfile          | /             | x         |               | CI/PROD images automatically built pushed stable branch                |
+-------------+----------------+-----------------------+---------------------+---------------+-----------+---------------+------------------------------------------------------------------------+
| Tag         | /^([1-2].*)$/  | {\1}-python3.9        | Dockerfile          | /             | x         |               | CI/PROD images automatically built from pushed release tags            |
+-------------+----------------+-----------------------+---------------------+---------------+-----------+---------------+------------------------------------------------------------------------+
