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
**Table of contents**

- [Apache Airflow source releases](#apache-airflow-source-releases)
  - [Apache Airflow Package](#apache-airflow-package)
  - [Provider packages](#provider-packages)
- [Prerequisites for the release manager preparing the release](#prerequisites-for-the-release-manager-preparing-the-release)
  - [Upload Public keys to id.apache.org](#upload-public-keys-to-idapacheorg)
  - [Configure PyPI uploads](#configure-pypi-uploads)
  - [Hardware used to prepare and verify the packages](#hardware-used-to-prepare-and-verify-the-packages)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Apache Airflow source releases

The Apache Airflow releases are one of the two types:

* Releases of the Apache Airflow package
* Releases of the Providers Packages

## Apache Airflow Package

This package contains sources that allow the user building fully-functional Apache Airflow 2.0 package.
They contain sources for:

 * "apache-airflow" python package that installs "airflow" Python package and includes
   all the assets required to release the webserver UI coming with Apache Airflow
 * Dockerfile and corresponding scripts that build and use an official DockerImage
 * Breeze development environment that helps with building images and testing locally
   apache airflow built from sources
 * Provider packages - containing Airflow's providers - separate package per each service Airflow integrates
   with.

We also plan to release the official Helm Chart sources that will allow the user to install Apache Airflow
via helm 3.0 chart in a distributed fashion.

The Source releases are the only "official" Apache Software Foundation releases, and they are distributed
via [Official Apache Download sources](https://downloads.apache.org/)

Following source releases Apache Airflow release manager also distributes convenience packages:

* PyPI packages released via https://pypi.org/project/apache-airflow/
* Docker Images released via https://hub.docker.com/repository/docker/apache/airflow

Those convenience packages are not "official releases" of Apache Airflow, but the users who
cannot or do not want to build the packages themselves can use them as a convenient way of installing
Apache Airflow, however they are not considered as "official source releases". You can read more
details about it in the [ASF Release Policy](http://www.apache.org/legal/release-policy.html).

Detailed instruction of releasing Provider Packages can be found in the
[README_RELEASE_AIRFLOW.md](README_RELEASE_AIRFLOW.md)

## Provider packages

The Provider packages are packages (per provider) that make it possible to easily install Hooks,
Operators, Sensors, and Secrets for different providers (external services used by Airflow).

Once you release the packages, you can simply install them with:

```
pip install apache-airflow-providers-<PROVIDER>[<EXTRAS>]
```

Where `<PROVIDER>` is the provider id and `<EXTRAS>` are optional extra packages to install.
You can find the provider packages dependencies and extras in the README.md files in each provider
package (in `airflow/providers/<PROVIDER>` folder) as well as in the PyPI installation page.

The sources released in SVN allow to build all the provider packages by the user, following the
instructions and scripts provided. Those are also "official_source releases" as described in the
[ASF Release Policy](http://www.apache.org/legal/release-policy.html) and they are available
via [Official Apache Download for providers](https://downloads.apache.org/airflow/providers/).

The full provider's list can be found here:
[Provider Packages Reference](https://s.apache.org/airflow-docs)

There are also convenience packages released as "apache-airflow-providers"separately in PyPI.
[PyPI query for providers](https://pypi.org/search/?q=apache-airflow-providers)

We also have legacy backport providers available for Airflow 1.10.* series:
[Official Apache Download for backport-providers](https://downloads.apache.org/airflow/backport-providers/)

And available in PyPI:
[PyPI query for backport providers](https://pypi.org/search/?q=apache-airflow-backport-providers).

Note that Backport Providers for Airflow 1.10.* series are not released any more. The last release
of Backport Providers was done  on March 17, 2021.

Detailed instruction of releasing Provider Packages can be found in the
[README_RELEASE_PROVIDER_PACKAGES.md](README_RELEASE_PROVIDER_PACKAGES.md)

# Prerequisites for the release manager preparing the release

The person acting as release manager has to fulfill certain pre-requisites. More details and FAQs are
available in the [ASF Release Policy](http://www.apache.org/legal/release-policy.html) but here some important
pre-requisites are listed below. Note that release manager does not have to be a PMC - it is enough
to be committer to assume the release manager role, but there are final steps in the process (uploading
final releases to SVN) that can only be done by PMC member. If needed, the release manager
can ask PMC to perform that final step of release.

## Upload Public keys to id.apache.org

Make sure your public key is on id.apache.org and in KEYS. You will need to sign the release artifacts
with your pgp key. After you have created a key, make sure you:

- Add your GPG pub key to https://dist.apache.org/repos/dist/release/airflow/KEYS , follow the instructions at the top of that file. Upload your GPG public key to https://pgp.mit.edu
- Add your key fingerprint to https://id.apache.org/ (login with your apache credentials, paste your fingerprint into the pgp fingerprint field and hit save).

```shell script
# Create PGP Key
gpg --gen-key

# Checkout ASF dist repo
svn checkout https://dist.apache.org/repos/dist/release/airflow
cd airflow


# Add your GPG pub key to KEYS file. Replace "Kaxil Naik" with your name
(gpg --list-sigs "Kaxil Naik" && gpg --armor --export "Kaxil Naik" ) >> KEYS


# Commit the changes
svn commit -m "Add PGP keys of Airflow developers"
```

See this for more detail on creating keys and what is required for signing releases.

http://www.apache.org/dev/release-signing.html#basic-facts

## Configure PyPI uploads

In order to not reveal your password in plain text, it's best if you create and configure API Upload tokens.
You can add and copy the tokens here:

* [Test PyPI](https://test.pypi.org/manage/account/token/)
* [Prod PyPI](https://pypi.org/manage/account/token/)


Create a `~/.pypirc` file:

```ini
[distutils]
index-servers =
  pypi
  pypitest

[pypi]
username=__token__
password=<API Upload Token>

[pypitest]
repository=https://test.pypi.org/legacy/
username=__token__
password=<API Upload Token>
```

Set proper permissions for the pypirc file:

```shell script
chmod 600 ~/.pypirc
```

- Install [twine](https://pypi.org/project/twine/) if you do not have it already (it can be done
  in a separate virtual environment).

```shell script
pip install twine
```

(more details [here](https://peterdowns.com/posts/first-time-with-pypi.html).)

- Set proper permissions for the pypirc file:
`$ chmod 600 ~/.pypirc`


## Hardware used to prepare and verify the packages

The best way to prepare and verify the releases is to prepare them on a hardware owned and controlled
by the committer acting as release manager. While strictly speaking, releases must only be verified
on hardware owned and controlled by the committer, for practical reasons it's best if the packages are
prepared using such hardware. More information can be found in this
[FAQ](http://www.apache.org/legal/release-policy.html#owned-controlled-hardware)
