<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Provider packages](#provider-packages)
- [What the provider packages are](#what-the-provider-packages-are)
  - [Increasing version number](#increasing-version-number)
- [Generated release notes](#generated-release-notes)
  - [Generating release notes](#generating-release-notes)
  - [Preparing provider packages](#preparing-provider-packages)
- [Testing and debugging provider preparation](#testing-and-debugging-provider-preparation)
  - [Debugging import check](#debugging-import-check)
  - [Debugging verifying provider classes](#debugging-verifying-provider-classes)
  - [Debugging preparing package documentation](#debugging-preparing-package-documentation)
  - [Debugging preparing setup files](#debugging-preparing-setup-files)
  - [Debugging preparing the packages](#debugging-preparing-the-packages)
- [Testing provider packages](#testing-provider-packages)
  - [Regular packages](#regular-packages)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Provider packages

# What the provider packages are

The Provider packages are separate packages (one package per provider) that implement
integrations with external services for Airflow in the form of installable Python packages.

The Release Manager prepares packages separately from the main Airflow Release, using
`breeze` commands and accompanying scripts. This document provides an overview of the command line tools
needed to prepare the packages.

## Increasing version number

First thing that release manager has to do is to change version of the provider to a target
version. Each provider has a `provider.yaml` file that, among others, stores information
about provider versions. When you attempt to release a provider you should update that
information based on the changes for the provider, and it's `CHANGELOG.rst`. It might be that
`CHANGELOG.rst` already contains the right target version. This will be especially true if some
changes in the provider add new features (then minor version is increased) or when the changes
introduce backwards-incompatible, breaking change in the provider (then major version is
incremented). Committers, when approving and merging changes to the providers, should pay attention
that the `CHANGELOG.rst` is updated whenever anything other than bugfix is added.

If there are no new features or breaking changes, the release manager should simply increase the
patch-level version for the provider.

The new version should be first on the list.


# Generated release notes

Each of the provider packages contains Release notes in the form of the `CHANGELOG.rst` file that is
automatically generated from history of the changes and code of the provider.
They are stored in the documentation directory. The `README.md` file generated during package
preparation is not stored anywhere in the repository - it contains however link to the Changelog
generated.

The `README.rst` file contains the following information:

* summary of requirements for each provider package
* list of dependencies (including extras to install them) when package depends on other providers package
* link to the detailed changelog/index.rst file: generated documentation for the packages.

The `index.rst` stored in the `docs\apache-airflow-providers-<PROVIDER>` folder contains:

* Contents this is manually maintained there
* the general package information (same for all packages with the name change)
* summary of requirements for each provider package
* list of dependencies (including extras to install them) when package depends on other providers package
* Content of high-level CHANGELOG.rst file that is stored in the provider folder next to
  ``provider.yaml`` file.
* Detailed list of changes generated for all versions of the provider automatically

## Generating release notes

When you want to prepare release notes for a package, you need to run:

```
./breeze prepare-provider-documentation <PACKAGE_ID> ...
```

* <PACKAGE_ID> is usually directory in the `airflow/providers` folder (for example `google` but in several
  cases, it might be one level deeper separated with `.` for example `apache.hive`

The index.rst is updated automatically in the `docs/apache-airflow-providers-<provider>` folder

You can run the script with multiple package names if you want to prepare several packages at the same time.

As soon as you are satisfied with the release notes generated you can commit generated changes/new files
to the repository.

## Preparing provider packages

You build the packages in the breeze environment, so you do not have to worry about common environment.

Note that readme release notes have to be generated first, so that the package preparation script reads
the `provider.yaml`.

* The provider package ids PACKAGE_ID are subdirectories in the ``providers`` directory. Sometimes they
are one level deeper (`apache/hive` folder for example, in which case PACKAGE_ID uses "." to separate
the folders (for example Apache Hive's PACKAGE_ID is `apache.hive` ). You can see the list of all available
providers by running:

```bash
./breeze prepare-provider-packages -- --help
```

The examples below show how you can build selected packages, but you can also build all packages by
omitting the package ids altogether.

By default, you build `both` packages, but you can use `--package-format wheel` to generate
only wheel package, or `--package-format sdist` to only generate sdist package.

* To build the release candidate packages for SVN Apache upload run the following command:

```bash
./breeze prepare-provider-packages --version-suffix-for-svn=rc1 [PACKAGE_ID] ...
```

for example:

```bash
./breeze prepare-provider-packages --version-suffix-for-svn=rc1 http ...
```

* To build the release candidate packages for PyPI upload run the following command:

```bash
./breeze prepare-provider-packages --version-suffix-for-pypi=rc1 [PACKAGE_ID] ...
```

for example:

```bash
./breeze prepare-provider-packages --version-suffix-for-pypi=rc1 http ...
```


* To build the final release packages run the following command:

```bash
./breeze prepare-provider-packages [--package-format PACKAGE_FORMAT] [PACKAGE_ID] ...
```

Where PACKAGE_FORMAT might be one of : `wheel`, `sdist`, `both` (`wheel` is the default format)

for example:

```bash
./breeze prepare-provider-packages http ...
```

* For each package, this creates a wheel package and source distribution package in your `dist` folder with
  names following the patterns:

  * `apache_airflow_providers_<PROVIDER>_YYYY.[M]M.[D]D[suffix]-py3-none-any.whl`
  * `apache-airflow-providers-<PROVIDER>-YYYY.[M]M.[D]D[suffix].tar.gz`

Note! Even if we always use the two-digit month and day when generating the readme files,
the version in PyPI does not contain the leading 0s in version name - therefore the artifacts generated
also do not container the leading 0s.

* You can install the .whl packages with `pip install <PACKAGE_FILE>`

You can add `--verbose` flag if you want to see detailed commands executed by the script.

# Testing and debugging provider preparation

The provider preparation is done using `Breeze` development environment and CI image. This way we have
common environment for package preparation, and we can easily verify if provider packages are OK and can
be installed for released versions of Airflow (including 2.0.0 version).

The same scripts and environment is run in our [CI Workflow](../../CI.rst) - the packages are prepared,
installed and tested using the same CI image. The tests are performed via the Production image, also
in the CI workflow. Our production images are built using Airflow and Provider packages prepared on the
CI so that they are as close to what users will be using when they are installing from PyPI. Our scripts
prepare `wheel` and `sdist` packages for both - airflow and provider packages and install them during
building of the images. This is very helpful in case of testing new providers that do not yet have PyPI
package released, but also it allows checking if provider's authors did not make breaking changes.

All classes from all providers must be imported - otherwise our CI will fail. Also, verification
of the image is performed where expected providers should be installed (for production image) and
providers should be discoverable, as well as `pip check` with all the dependencies has to succeed.

You might want to occasionally modify the preparation scripts for providers. They are all present in
the `dev/provider_packages` folder. There are the `Breeze` commands above - they perform the sequence
of those  steps automatically, but you can manually run the scripts as follows to debug them:

The commands are best to execute in the Breeze environment as it has all the dependencies installed,
Examples below describe that. However, for development you might run them in your local development
environment as it makes it easier to debug. Just make sure you install your development environment
with 'devel_all' extra (make sure to ue the right python version).

Note that it is best to use `INSTALL_PROVIDERS_FROM_SOURCES` set to`true`, to make sure
that any new added providers are not added as packages (in case they are not yet available in PyPI.

```shell script
INSTALL_PROVIDERS_FROM_SOURCES="true" pip install -e ".[devel_all]" \
    --constraint https://raw.githubusercontent.com/apache/airflow/constraints-master/constraints-3.6.txt
```

Note that you might need to add some extra dependencies to your system to install "devel_all" - many
dependencies are needed to make a clean install - the `Breeze` environment has all the
dependencies installed in case you have problem with setting up your local virtualenv.

You can also use `breeze` to prepare your virtualenv (it will print extra information if some
dependencies are missing/installation fails and it will also reset your SQLite test db in
the `${HOME}/airflow` directory:

```shell script
./breeze initialize-local-virtualenv
```


You can find description of all the commands and more information about the "prepare"
tool by running it with `--help`

```shell script
./dev/provider_packages/prepare_provider_packages.py --help
```

You can see for example list of all provider packages:

```shell script
./dev/provider_packages/prepare_provider_packages.py list-providers-packages
```


You can add `--verbose` flag in breeze command if you want to see commands executed.

## Debugging import check

The script verifies if all provider's classes can be imported.

1) Enter Breeze environment (optionally if you have no local virtualenv):

```shell script
./breeze
```

All the rest is in-container in case you use Breeze, but can be in your local virtualenv if you have
it installed with `devel_all` extra.

2) Install remaining dependencies. Until we manage to bring the apache.beam due to i's dependencies without
   conflicting dependencies (requires fixing Snowflake and Azure providers). This is optional in case you
   already installed the environment with `devel_all` extra

```shell script
pip install -e ".[devel_all]"
```

3) Run import check:

```shell script
./dev/import_all_classes.py --path airflow/providers
```

It checks if all classes from provider packages can be imported.

## Debugging verifying provider classes

The script verifies if all provider's classes are correctly named.

1) Enter Breeze environment (optionally if you have no local virtualenv):

```shell script
./breeze
```

All the rest is in-container in case you use Breeze, but can be in your local virtualenv if you have
it installed with `devel_all` extra.

2) Install remaining dependencies. Until we manage to bring the apache.beam due to i's dependencies without
   conflicting dependencies (requires fixing Snowflake and Azure providers). This is optional in case you
   already installed the environment with `devel_all` extra

```shell script
pip install -e ".[devel_all]"
```

3) Run import check:

```shell script
./dev/provider_packages/prepare_provider_packages.py verify-provider-classes
```

It checks if all provider Operators/Hooks etc. are correctly named.


## Debugging preparing package documentation

The script updates documentation of the provider packages. Note that it uses airflow git and pulls
the latest version of tags available in Airflow, so you need to enter Breeze with
`--mount-all-local-sources flag`

1) Enter Breeze environment (optionally if you have no local virtualenv):

```shell script
./breeze --mount-all-local-sources
```

(all the rest is in-container)

2) Install remaining dependencies. Until we manage to bring the apache.beam due to i's dependencies without
   conflicting dependencies (requires fixing Snowflake and Azure providers).
   Optionally if you have no local virtualenv.

```shell script
pip install -e ".[devel_all]"
```

3) Run update documentation (version suffix might be empty):

```shell script
./dev/provider_packages/prepare_provider_packages.py update-package-documentation \
    --version-suffix <SUFFIX> \
    <PACKAGE>
```

This script will fetch the latest version of airflow from Airflow's repo (it will automatically add
`apache-https-for-providers` remote and pull airflow (read only) from there. There is no need
to setup any credentials for it.

In case version being prepared is already tagged in the repo documentation preparation returns immediately
and prints warning.

You can add `--verbose` flag if you want to see detailed commands executed by the script.

## Debugging preparing setup files

This script prepares the actual packages.

1) Enter Breeze environment:

```shell script
./breeze
```

(all the rest is in-container)

2) Cleanup the artifact directories:

This is needed because setup tools does not clean those files and generating packages one by one
without cleanup, might include artifacts from previous package to be included in the new one.

```shell script
rm -rf -- *.egg-info build/
```

3) Generate setup.py/setup.cfg/MANIFEST.in/provider_info.py/README files  files for:

* alpha/beta packages (specify a1,a2,.../b1,b2... suffix)
* release candidates (specify r1,r2,... suffix) - those are release candidate
* official package (to be released in PypI as official package)

The version suffix specified here will be appended to the version retrieved from
`provider.yaml`. Note that this command will fail if the tag denoted by the
version + suffix already exist. This means that the version was not updated since the
last time it was generated. In the CI we always add 'dev' suffix, and we never create
TAG for it, so in the CI the setup.py is generated and should never fail.

```shell script
./dev/provider_packages/prepare_provider_packages.py generate-setup-files \
    --version-suffix "<SUFFIX>" \
    <PACKAGE>
```

You can add `--verbose` flag if you want to see detailed commands executed by the script.

## Debugging preparing the packages

The script prepares the package after sources have been copied and setup files generated.
Note that it uses airflow git and pulls the latest version of tags available in Airflow,
so you need to enter Breeze with
`--mount-all-local-sources flag`

1) Enter Breeze environment (optionally if you have no local virtualenv):

```shell script
./breeze --mount-all-local-sources
```

(all the rest is in-container)

2) Install remaining dependencies. Until we manage to bring the apache.beam due to i's dependencies without
   conflicting dependencies (requires fixing Snowflake and Azure providers).
   Optionally if you have no local virtualenv.

```shell script
pip install -e ".[devel_all]"
```

3) Run update documentation (version suffix might be empty):

```shell script
./dev/provider_packages/prepare_provider_packages.py build-provider-packages \
    --version-suffix <SUFFIX> \
    <PACKAGE>
```

In case version being prepared is already tagged in the repo documentation preparation returns immediately
and prints error. You can prepare the error regardless and build the packages even if the tag exists, by
specifying ``--version-suffix`` (for example ``--version-suffix dev``).

By default, you prepare ``both`` packages, but you can add ``--package-format`` argument and specify
``wheel``, ``sdist`` to build only one of them.


# Testing provider packages

The provider packages importing and tests execute within the "CI" environment of Airflow -the
same image that is used by Breeze. They however require special mounts (no
sources of Airflow mounted to it) and possibility to install all extras and packages in order to test
if all classes can be imported. It is rather simple but requires some semi-automated process:


## Regular packages

1. Prepare regular packages

```shell script
./breeze prepare-provider-packages
```

This prepares all provider packages in the "dist" folder

2. Prepare airflow package from sources

```shell script
./breeze prepare-airflow-packages
```

This prepares airflow package in the "dist" folder

2. Enter the container:

```shell script
export INSTALL_AIRFLOW_VERSION="wheel"

./dev/provider_packages/enter_breeze_provider_package_tests.sh
```

(the rest of it is in the container)

3. \[IN CONTAINER\] Install apache-beam.

```shell script
pip install apache-beam[gcp]
```

4. \[IN CONTAINER\] Install the provider packages from /dist

```shell script
pip install --no-deps /dist/apache_airflow_providers_*.whl
```

Note! No-deps is because we are installing the version installed from wheel package.

5.  \[IN CONTAINER\] Check the installation folder for providers:

```shell script
python3 <<EOF 2>/dev/null
import airflow.providers;
path=airflow.providers.__path__
for p in path._path:
    print(p)
EOF
```

6.  \[IN CONTAINER\] Check if all the providers can be imported
python3 /opt/airflow/dev/import_all_classes.py --path <PATH_REPORTED_IN_THE_PREVIOUS_STEP>
