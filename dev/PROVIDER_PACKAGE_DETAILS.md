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
- [Overview](#overview)

- [Overview](#overview)
- [Deciding when to release](#deciding-when-to-release)
- [Generating provider documentation](#generating-provider-documentation)
- [Content of the release notes](#content-of-the-release-notes)
- [Preparing packages](#preparing-packages)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->


# Overview

This document describes the process of preparing provider packages for release and releasing them.
The provider packages are packages (per `provider`) that are not part of the core Airflow.

Once you release the packages, you can simply install them with:

```
pip install apache-airflow-providers-<PROVIDER>[<EXTRAS>]
```

Where `<PROVIDER>` is the provider id and `<EXTRAS>` are optional extra packages to install.
You can find the provider packages dependencies and extras in the README.md files in each provider
package (in `airflow/providers/<PROVIDER>` folder) as well as in the PyPI installation page.

Backport providers are a great way to migrate your DAGs to Airflow-2.0 compatible DAGs. You can
switch to the new Airflow-2.0 packages in your DAGs, long before you attempt to migrate
airflow to 2.0 line.

# Deciding when to release

Each provider package has its own version maintained separately when contributors implement changes,
marking those as patches/features/backwards incompatible changes.

Details to be hashed out in [the related issue](https://github.com/apache/airflow/issues/11425)


# Generating provider documentation

When you want to prepare release notes for a package, you need to run:

```
./breeze --backports prepare-provider-documentation <PACKAGE_ID> ...
```

The version for each package is going to be updated separately for each package when we agree to the
process.

Details to be hashed out in [the related issue](https://github.com/apache/airflow/issues/11425)

* <PACKAGE_ID> is usually directory in the `airflow/providers` folder (for example `google` but in several
  cases, it might be one level deeper separated with `.` for example `apache.hive`

You can run the script with multiple package names if you want to prepare several packages at the same time.

If you do not change version number, you can iterate with merges and release candidates you update the
release date without providing
the date (to update the existing release notes)

```
./breeze prepare-provider-documentation google
```


When you are satisfied with the release notes generated you can commit generated changes/new files
to the repository.


# Content of the release notes

The script generates all the necessary information:

* summary of requirements for each provider package
* list of dependencies (including extras to install them) when package
  depends on other providers packages
* link to the changelog of all the changes to the provider package

The information is placed in README.rst which is regenerated every time you run the script.

Note that our CI system builds the release notes for provider packages automatically with every build and
current date - this way you might be sure the automated generation of the release notes continues to
work. You can also preview the generated readme files (by downloading artifacts from GitHub Actions).
The script does not modify the README files if there is no change in the repo for that provider.

# Preparing packages

Airflow 2.0 is released as separate core package and separate set of provider packages.

You can build those packages in the breeze environment, so you do not have to worry about common environment.

Note that readme release notes have to be generated first, so that the package preparation script reads
the latest version from the latest version of release notes prepared.

* The provider package ids PACKAGE_ID are subdirectories in the ``providers`` directory. Sometimes they
are one level deeper (`apache/hive` folder for example, in which case PACKAGE_ID uses "." to separate
the folders (for example Apache Hive's PACKAGE_ID is `apache.hive` ). You can see the list of all available
providers by running:

```bash
./breeze prepare-provider-packages -- --help
```

The examples below show how you can build selected packages, but you can also build all packages by
omitting the package ids altogether.

* To build the release candidate packages for SVN Apache upload run the following command:

```bash
./breeze prepare-provider-packages package-format both --version-suffix-for-svn=rc1 [PACKAGE_ID] ...
```

for example:

```bash
./breeze prepare-provider-packages package-format both --version-suffix-for-svn=rc1 http ...
```

* To build the release candidate packages for PyPI upload run the following command:

```bash
./breeze prepare-provider-packages package-format both --version-suffix-for-pypi=rc1 [PACKAGE_ID] ...
```

for example:

```bash
./breeze prepare-provider-packages package-format both --version-suffix-for-pypi=rc1 http ...
```


* To build the final release packages run the following command:

```bash
./breeze prepare-provider-packages package-format both [PACKAGE_ID] ...
```

for example:

```bash
./breeze prepare-provider-packages package-format both http ...
```

* For each package, this creates a wheel package and source distribution package in your `dist` folder with
  names following the patterns:

  * `apache_airflow_providers_<PROVIDER>_MAJOR.MINOR.PATCHLEVEL[suffix]-py3-none-any.whl`
  * `apache-airflow-providers-<PROVIDER>-MAJOR.MINOR.PATCHLEVEL[suffix].tar.gz`

Where ``MAJOR.MINOR.PATCHLEVEL`` is the semver version of the packages.

* You can install the .whl packages with `pip install <PACKAGE_FILE>`

Releasing the packages is described in [README_RELEASE_PROVIDER_PACKAGES.md](README_RELEASE_PROVIDER_PACKAGES.md)
